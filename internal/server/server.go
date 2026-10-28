// Package server wires raft, the WAL and the state machine together and
// exposes them through the gRPC API.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/fsm"
	"github.com/monaaoua9/kvcoord/internal/raft"
	"github.com/monaaoua9/kvcoord/internal/transport"
	"github.com/monaaoua9/kvcoord/internal/wal"
)

type Config struct {
	NodeID uint64
	Peers  map[uint64]string // id -> host:port, includes this node; the founding set of a new cluster
	// Join starts an empty node that a leader must add with MemberAdd. Peers then
	// only needs this node.
	Join   bool
	Listen string // defaults to Peers[NodeID]

	DataDir string
	NoSync  bool

	TickInterval     time.Duration
	ElectionTick     int
	HeartbeatTick    int
	SnapshotEvery    uint64
	SnapshotTrailing uint64
	SegmentSize      int64

	MinLeaseTTL    time.Duration
	SessionTTL     time.Duration
	RequestTimeout time.Duration
	WatchBuffer    int
	HistoryLimit   int

	// AuthTokens maps a secret token to a role (read, write, admin, peer). Empty
	// means no authentication. Peers must present a "peer" token, see PeerToken.
	AuthTokens map[string]string
	// PeerToken is what this node sends to the other nodes.
	PeerToken string
	// CompactRetain keeps this many recent revisions readable; the leader compacts
	// everything older. 0 turns automatic compaction off.
	CompactRetain uint64
	CompactEvery  time.Duration // how often the leader checks, defaults to 10s

	ServerCreds credentials.TransportCredentials
	PeerCreds   credentials.TransportCredentials

	Logger *slog.Logger

	// Overrides used by tests to run on a simulated network and in-memory disk.
	Storage   raft.Storage
	Transport raft.Transport
	Listener  net.Listener
}

func (c *Config) defaults() error {
	if c.NodeID == 0 {
		return errors.New("server: node id must be set")
	}
	if _, ok := c.Peers[c.NodeID]; !ok {
		return errors.New("server: peers must include this node")
	}
	if c.Listen == "" {
		c.Listen = c.Peers[c.NodeID]
	}
	if c.Storage == nil && c.DataDir == "" {
		return errors.New("server: data dir must be set")
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.MinLeaseTTL == 0 {
		c.MinLeaseTTL = time.Second
	}
	if c.SessionTTL == 0 {
		c.SessionTTL = 10 * time.Minute
	}
	if c.CompactEvery == 0 {
		c.CompactEvery = 10 * time.Second
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.WatchBuffer == 0 {
		c.WatchBuffer = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

type Server struct {
	cfg  Config
	log  *slog.Logger
	node *raft.Node
	fsm  *fsm.Machine
	wal  *wal.WAL
	gt   *transport.GRPC
	grpc *grpc.Server
	lis  net.Listener

	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once

	m metrics
}

func New(cfg Config) (*Server, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, log: cfg.Logger.With("node", cfg.NodeID), stop: make(chan struct{})}
	s.fsm = fsm.New(fsm.Options{HistoryLimit: cfg.HistoryLimit})

	store := cfg.Storage
	if store == nil {
		w, err := wal.Open(wal.Options{Dir: cfg.DataDir, SegmentSize: cfg.SegmentSize, NoSync: cfg.NoSync})
		if err != nil {
			return nil, err
		}
		s.wal, store = w, w
	}

	tr := cfg.Transport
	if tr == nil {
		addrs := make(map[raft.NodeID]string, len(cfg.Peers))
		for id, a := range cfg.Peers {
			addrs[id] = a
		}
		s.gt = transport.NewGRPC(cfg.NodeID, addrs, cfg.PeerCreds, cfg.PeerToken, s.log)
		tr = s.gt
	}

	ids := make([]raft.NodeID, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	if cfg.Join {
		ids = nil
	}
	node, err := raft.NewNode(raft.Config{
		ID: cfg.NodeID, Peers: ids, Addrs: cfg.Peers, Join: cfg.Join,
		OnConfig: func(ms []raft.Member) {
			if s.gt != nil {
				s.gt.SetMembers(ms)
			}
		},
		TickInterval: cfg.TickInterval, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		SnapshotEvery: cfg.SnapshotEvery, SnapshotTrailing: cfg.SnapshotTrailing,
		Logger: cfg.Logger,
	}, store, s.fsm, tr)
	if err != nil {
		if s.gt != nil {
			s.gt.Close()
		}
		store.Close()
		return nil, err
	}
	s.node = node
	if s.gt != nil {
		s.gt.SetReceiver(node)
	}

	var opts []grpc.ServerOption
	if cfg.ServerCreds != nil {
		opts = append(opts, grpc.Creds(cfg.ServerCreds))
	}
	auth, err := newAuthn(cfg.AuthTokens)
	if err != nil {
		return nil, err
	}
	if auth != nil {
		opts = append(opts, grpc.UnaryInterceptor(auth.unary), grpc.StreamInterceptor(auth.stream))
	}
	s.grpc = grpc.NewServer(opts...)
	kvpb.RegisterKVServer(s.grpc, &kvService{s: s})
	kvpb.RegisterWatchServer(s.grpc, &watchService{s: s})
	kvpb.RegisterLeaseServer(s.grpc, &leaseService{s: s})
	kvpb.RegisterLockServer(s.grpc, &lockService{s: s})
	kvpb.RegisterClusterServer(s.grpc, &clusterService{s: s})
	if s.gt != nil {
		s.gt.Register(s.grpc)
	}
	return s, nil
}

// Node exposes the raft node so a simulated network can deliver messages to it.
func (s *Server) Node() *raft.Node { return s.node }

func (s *Server) Machine() *fsm.Machine { return s.fsm }

func (s *Server) Addr() string {
	if s.lis != nil {
		return s.lis.Addr().String()
	}
	return s.cfg.Listen
}

func (s *Server) Start() error {
	lis := s.cfg.Listener
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", s.cfg.Listen); err != nil {
			return err
		}
	}
	s.lis = lis
	s.node.Start()

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		if err := s.grpc.Serve(lis); err != nil {
			s.log.Error("grpc serve", "err", err)
		}
	}()
	go func() {
		defer s.wg.Done()
		s.leaderLoop()
	}()
	s.log.Info("node started", "addr", lis.Addr().String())
	return nil
}

func (s *Server) Stop() {
	s.once.Do(func() {
		close(s.stop)
		s.grpc.Stop()
		s.node.Stop()
		s.wg.Wait()
		if s.gt != nil {
			s.gt.Close()
		}
		if s.wal != nil {
			s.wal.Close()
		} else if s.cfg.Storage != nil {
			s.cfg.Storage.Close()
		}
	})
}

func (s *Server) header() *kvpb.ResponseHeader {
	return &kvpb.ResponseHeader{NodeId: s.cfg.NodeID, Term: s.node.Term(), Revision: s.fsm.Revision()}
}

// withTimeout applies the default deadline to callers that did not set one.
func (s *Server) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.cfg.RequestTimeout)
}

func (s *Server) propose(ctx context.Context, cmd *kvpb.Command) (*kvpb.CommandResult, error) {
	cmd.TimestampNs = time.Now().UnixNano()
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, kvpb.NewError(codes.InvalidArgument, kvpb.ErrInvalidArgument, err.Error(), nil)
	}
	s.m.proposals.Add(1)
	v, err := s.node.Propose(ctx, data)
	if err != nil {
		s.m.proposalErrs.Add(1)
		return nil, s.mapErr(err)
	}
	res := v.(*kvpb.CommandResult)
	if res.ErrorCode != "" {
		return nil, resultErr(res)
	}
	return res, nil
}

// linearizableRead blocks until a local read reflects every write that
// completed before the call.
func (s *Server) linearizableRead(ctx context.Context) error {
	s.m.reads.Add(1)
	idx, err := s.node.ReadIndex(ctx)
	if err != nil {
		return s.mapErr(err)
	}
	if err := s.node.WaitApplied(ctx, idx); err != nil {
		return s.mapErr(err)
	}
	return nil
}

func resultErr(res *kvpb.CommandResult) error {
	c := codes.Internal
	switch res.ErrorCode {
	case kvpb.ErrInvalidArgument:
		c = codes.InvalidArgument
	case kvpb.ErrLeaseNotFound:
		c = codes.NotFound
	case kvpb.ErrFutureRevision, kvpb.ErrCompacted:
		c = codes.OutOfRange
	case kvpb.ErrSessionExpired, kvpb.ErrStaleRequest:
		c = codes.FailedPrecondition
	}
	return kvpb.NewError(c, res.ErrorCode, res.ErrorMessage, nil)
}

func (s *Server) mapErr(err error) error {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		reason, md := kvpb.ErrNoLeader, map[string]string(nil)
		if nl.Leader != 0 {
			reason = kvpb.ErrNotLeader
			md = map[string]string{
				kvpb.MetaLeaderID:   fmt.Sprint(nl.Leader),
				kvpb.MetaLeaderAddr: s.addrOf(nl.Leader),
			}
		}
		return kvpb.NewError(codes.Unavailable, reason, err.Error(), md)
	case errors.Is(err, raft.ErrConfChangePending), errors.Is(err, raft.ErrNotReady):
		return kvpb.NewError(codes.FailedPrecondition, kvpb.ErrConfPending, err.Error(), nil)
	case errors.Is(err, raft.ErrTransferring), errors.Is(err, raft.ErrTransferTimedOut):
		return kvpb.NewError(codes.Unavailable, kvpb.ErrUnavailable, err.Error(), nil)
	case errors.Is(err, raft.ErrTransferInvalid):
		return kvpb.NewError(codes.InvalidArgument, kvpb.ErrInvalidArgument, err.Error(), nil)
	case errors.Is(err, raft.ErrLearnerLagging):
		return kvpb.NewError(codes.FailedPrecondition, kvpb.ErrLearnerLagging, err.Error(), nil)
	case errors.Is(err, raft.ErrConfChangeInvalid):
		return kvpb.NewError(codes.InvalidArgument, kvpb.ErrInvalidArgument, err.Error(), nil)
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrProposalDropped):
		// the entry may still commit under the new leader, callers retry with the same request id
		return kvpb.NewError(codes.Unavailable, kvpb.ErrUnavailable, err.Error(), nil)
	case errors.Is(err, raft.ErrBusy):
		return kvpb.NewError(codes.ResourceExhausted, kvpb.ErrBusy, err.Error(), nil)
	case errors.Is(err, raft.ErrStopped):
		return kvpb.NewError(codes.Unavailable, kvpb.ErrUnavailable, err.Error(), nil)
	case errors.Is(err, context.DeadlineExceeded):
		return kvpb.NewError(codes.DeadlineExceeded, kvpb.ErrTimeout, err.Error(), nil)
	case errors.Is(err, context.Canceled):
		return kvpb.NewError(codes.Canceled, kvpb.ErrUnavailable, err.Error(), nil)
	}
	return kvpb.NewError(codes.Internal, kvpb.ErrInternal, err.Error(), nil)
}

// leaderLoop does the housekeeping only the leader may do: renewing every
// lease after an election, expiring dead leases, and reaping idle sessions.
func (s *Server) leaderLoop() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	var refreshedTerm uint64
	lastReap := time.Now()
	lastCompact := time.Now()

	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		st := s.node.Status()
		if !st.LeaderReady {
			refreshedTerm = 0
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		if refreshedTerm != st.Term {
			// clients could not renew while we were electing, give everyone a full TTL
			if _, err := s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_RefreshLeases{RefreshLeases: &kvpb.RefreshLeasesRequest{}}}); err != nil {
				cancel()
				continue
			}
			refreshedTerm = st.Term
		}

		if ids := s.fsm.ExpiredLeases(time.Now().UnixNano()); len(ids) > 0 {
			s.m.leaseExpiries.Add(uint64(len(ids)))
			s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_LeaseExpire{LeaseExpire: &kvpb.LeaseExpireRequest{Ids: ids}}})
		}

		if r := s.cfg.CompactRetain; r > 0 && time.Since(lastCompact) > s.cfg.CompactEvery {
			lastCompact = time.Now()
			if rev := s.fsm.Revision(); rev > r+s.fsm.CompactRevision() {
				s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_Compact{Compact: &kvpb.CompactRequest{Revision: rev - r}}})
			}
		}

		if time.Since(lastReap) > 30*time.Second {
			lastReap = time.Now()
			s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_SessionExpire{
				SessionExpire: &kvpb.SessionExpireRequest{IdleMs: uint64(s.cfg.SessionTTL / time.Millisecond)},
			}})
		}
		cancel()
	}
}

// StepDown moves leadership away from this node. With target 0 it picks the
// voter whose log is furthest ahead. A node that is not the leader returns
// NotLeaderError, so callers can treat that as "nothing to do".
func (s *Server) StepDown(ctx context.Context, target uint64) error {
	st := s.node.Status()
	if st.State != raft.Leader {
		return &raft.NotLeaderError{Leader: st.Leader}
	}
	if target == 0 {
		var best uint64
		for _, m := range s.node.Members() {
			if m.Learner || m.ID == s.cfg.NodeID {
				continue
			}
			if mt := st.Match[m.ID]; target == 0 || mt > best {
				target, best = m.ID, mt
			}
		}
		if target == 0 {
			return raft.ErrTransferInvalid
		}
	}
	return s.node.TransferLeadership(ctx, target)
}

// addrOf resolves a member id to its address using the live membership.
func (s *Server) addrOf(id uint64) string {
	for _, m := range s.node.Members() {
		if m.ID == id {
			return m.Addr
		}
	}
	return s.cfg.Peers[id]
}
