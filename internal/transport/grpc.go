// Package transport carries raft messages between nodes over gRPC client streams.
package transport

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/raft"
)

const (
	queueLen  = 4096
	chunkSize = 1 << 20 // snapshots are streamed in pieces of this size
)

type Receiver interface {
	Step(raft.Message)
}

type GRPC struct {
	kvpb.UnimplementedRaftTransportServer

	self  raft.NodeID
	addr  string // our own dial address, sent along so strangers can answer
	log   *slog.Logger
	creds credentials.TransportCredentials

	mu    sync.Mutex
	rcv   Receiver
	peers map[raft.NodeID]*peer
	done  chan struct{}
	wg    sync.WaitGroup
}

func NewGRPC(self raft.NodeID, addrs map[raft.NodeID]string, creds credentials.TransportCredentials, log *slog.Logger) *GRPC {
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	t := &GRPC{
		self: self, addr: addrs[self], log: log.With("component", "raft-transport"), creds: creds,
		peers: make(map[raft.NodeID]*peer), done: make(chan struct{}),
	}
	ms := make([]raft.Member, 0, len(addrs))
	for id, addr := range addrs {
		ms = append(ms, raft.Member{ID: id, Addr: addr})
	}
	t.SetMembers(ms)
	return t
}

// SetMembers makes the set of dialled peers match ms: new members get a
// sender, removed ones are shut down.
func (t *GRPC) SetMembers(ms []raft.Member) {
	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case <-t.done:
		return
	default:
	}
	want := make(map[raft.NodeID]string, len(ms))
	for _, m := range ms {
		if m.ID != t.self && m.Addr != "" {
			want[m.ID] = m.Addr
		}
	}
	for id, p := range t.peers {
		if addr, ok := want[id]; !ok || addr != p.addr {
			close(p.quit)
			delete(t.peers, id)
		}
	}
	for id, addr := range want {
		if t.peers[id] != nil {
			continue
		}
		p := &peer{t: t, id: id, addr: addr, q: make(chan raft.Message, queueLen), quit: make(chan struct{})}
		t.peers[id] = p
		t.wg.Add(1)
		go p.run()
	}
}

// learn lets a node that was started with only itself (kvd -join) dial the leader
// that is about to add it; once it knows other peers the config takes over.
func (t *GRPC) learn(pm *kvpb.RaftMessage) {
	if pm.FromAddr == "" || pm.From == t.self {
		return
	}
	t.mu.Lock()
	known := len(t.peers) > 0
	t.mu.Unlock()
	if !known {
		t.SetMembers([]raft.Member{{ID: pm.From, Addr: pm.FromAddr}})
	}
}

func (t *GRPC) SetReceiver(r Receiver) {
	t.mu.Lock()
	t.rcv = r
	t.mu.Unlock()
}

func (t *GRPC) Register(s grpc.ServiceRegistrar) { kvpb.RegisterRaftTransportServer(s, t) }

func (t *GRPC) Close() {
	select {
	case <-t.done:
		return
	default:
		close(t.done)
	}
	t.wg.Wait()
}

// Send implements raft.Transport. It never blocks; a full queue drops the message.
func (t *GRPC) Send(m raft.Message) {
	t.mu.Lock()
	p := t.peers[m.To]
	t.mu.Unlock()
	if p == nil {
		return
	}
	select {
	case p.q <- m:
	default:
	}
}

// Stream is the server side: every message received goes straight into raft.
func (t *GRPC) Stream(s kvpb.RaftTransport_StreamServer) error {
	var asm *snapAssembly
	for {
		pm, err := s.Recv()
		if err == io.EOF {
			return s.SendAndClose(&kvpb.StreamAck{})
		}
		if err != nil {
			return err
		}
		if pm.Type == uint32(raft.MsgSnap) && pm.SnapTotal > 0 {
			// chunks of one snapshot arrive in order on a single stream
			if pm.SnapOffset == 0 {
				asm = &snapAssembly{head: pm, buf: make([]byte, 0, pm.SnapTotal)}
			}
			if asm == nil || uint64(len(asm.buf)) != pm.SnapOffset {
				asm = nil
				continue
			}
			asm.buf = append(asm.buf, pm.SnapData...)
			if uint64(len(asm.buf)) < pm.SnapTotal {
				continue
			}
			asm.head.SnapData = asm.buf
			pm = asm.head
			asm = nil
		}
		t.learn(pm)
		t.mu.Lock()
		r := t.rcv
		t.mu.Unlock()
		if r != nil {
			r.Step(fromProto(pm))
		}
	}
}

type peer struct {
	t    *GRPC
	id   raft.NodeID
	addr string
	q    chan raft.Message
	quit chan struct{}
}

func (p *peer) stopped() bool {
	select {
	case <-p.quit:
		return true
	case <-p.t.done:
		return true
	default:
		return false
	}
}

func (p *peer) run() {
	defer p.t.wg.Done()
	backoff := 50 * time.Millisecond
	for {
		select {
		case <-p.t.done:
			return
		default:
		}
		start := time.Now()
		err := p.pump()
		if err != nil && time.Since(start) > time.Second {
			backoff = 50 * time.Millisecond
		}
		// whatever piled up while the link was down is stale, raft will resend
		p.drain()
		select {
		case <-p.t.done:
			return
		case <-p.quit:
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Second)
	}
}

func (p *peer) drain() {
	for {
		select {
		case <-p.q:
		default:
			return
		}
	}
}

func (p *peer) pump() error {
	conn, err := grpc.NewClient(p.addr,
		grpc.WithTransportCredentials(p.t.creds),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-p.t.done:
			cancel()
		case <-p.quit:
			cancel()
		case <-ctx.Done():
		}
	}()

	stream, err := kvpb.NewRaftTransportClient(conn).Stream(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-p.q:
			for _, pm := range toProtoChunks(m) {
				pm.FromAddr = p.t.addr
				if err := stream.Send(pm); err != nil {
					p.t.log.Debug("send failed", "peer", p.id, "err", err)
					return err
				}
			}
		}
	}
}

func toProto(m raft.Message) *kvpb.RaftMessage {
	pm := &kvpb.RaftMessage{
		Type: uint32(m.Type), Term: m.Term, From: m.From, To: m.To,
		LogIndex: m.LogIndex, LogTerm: m.LogTerm, Commit: m.Commit, Round: m.Round,
		Reject: m.Reject, ConflictIndex: m.ConflictIndex, ConflictTerm: m.ConflictTerm, Transfer: m.Transfer,
	}
	if len(m.Entries) > 0 {
		pm.Entries = make([]*kvpb.LogEntry, len(m.Entries))
		for i, e := range m.Entries {
			pm.Entries[i] = &kvpb.LogEntry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Data}
		}
	}
	if m.Snapshot != nil {
		pm.SnapIndex, pm.SnapTerm, pm.SnapData = m.Snapshot.Index, m.Snapshot.Term, m.Snapshot.Data
		for _, mb := range m.Snapshot.Members {
			pm.SnapMembers = append(pm.SnapMembers, &kvpb.Member{Id: mb.ID, Addr: mb.Addr, Learner: mb.Learner})
		}
	}
	return pm
}

func fromProto(pm *kvpb.RaftMessage) raft.Message {
	m := raft.Message{
		Type: raft.MsgType(pm.Type), Term: pm.Term, From: pm.From, To: pm.To,
		LogIndex: pm.LogIndex, LogTerm: pm.LogTerm, Commit: pm.Commit, Round: pm.Round,
		Reject: pm.Reject, ConflictIndex: pm.ConflictIndex, ConflictTerm: pm.ConflictTerm, Transfer: pm.Transfer,
	}
	if len(pm.Entries) > 0 {
		m.Entries = make([]raft.Entry, len(pm.Entries))
		for i, e := range pm.Entries {
			m.Entries[i] = raft.Entry{Index: e.Index, Term: e.Term, Type: raft.EntryType(e.Type), Data: e.Data}
		}
	}
	if m.Type == raft.MsgSnap {
		m.Snapshot = &raft.Snapshot{Index: pm.SnapIndex, Term: pm.SnapTerm, Data: pm.SnapData}
		for _, mb := range pm.SnapMembers {
			m.Snapshot.Members = append(m.Snapshot.Members, raft.Member{ID: mb.Id, Addr: mb.Addr, Learner: mb.Learner})
		}
	}
	return m
}

type snapAssembly struct {
	head *kvpb.RaftMessage
	buf  []byte
}

func toProtoChunks(m raft.Message) []*kvpb.RaftMessage {
	pm := toProto(m)
	if m.Snapshot == nil || len(m.Snapshot.Data) <= chunkSize {
		return []*kvpb.RaftMessage{pm}
	}
	data := m.Snapshot.Data
	pm.SnapData = nil
	var out []*kvpb.RaftMessage
	for off := 0; off < len(data); off += chunkSize {
		c := proto.Clone(pm).(*kvpb.RaftMessage)
		c.SnapData = data[off:min(off+chunkSize, len(data))]
		c.SnapOffset, c.SnapTotal = uint64(off), uint64(len(data))
		out = append(out, c)
	}
	return out
}
