package transport

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

type sink chan raft.Message

func (s sink) Step(m raft.Message) { s <- m }

func pair(t *testing.T) (a *GRPC, rcv sink) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rcv = make(sink, 16)
	b := NewGRPC(2, map[raft.NodeID]string{1: "unused", 2: lis.Addr().String()}, nil, log)
	b.SetReceiver(rcv)
	srv := grpc.NewServer()
	b.Register(srv)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop(); b.Close() })

	a = NewGRPC(1, map[raft.NodeID]string{1: "unused", 2: lis.Addr().String()}, nil, log)
	t.Cleanup(a.Close)
	return a, rcv
}

func recv(t *testing.T, c sink) raft.Message {
	t.Helper()
	select {
	case m := <-c:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
		return raft.Message{}
	}
}

func TestMessagesRoundTrip(t *testing.T) {
	a, rcv := pair(t)
	a.Send(raft.Message{
		Type: raft.MsgApp, Term: 7, From: 1, To: 2, LogIndex: 4, LogTerm: 6, Commit: 3, Round: 9,
		Entries: []raft.Entry{{Index: 5, Term: 7, Data: []byte("x")}, {Index: 6, Term: 7, Type: raft.EntryNoop}},
	})
	m := recv(t, rcv)
	if m.Type != raft.MsgApp || m.Term != 7 || m.Round != 9 || len(m.Entries) != 2 ||
		string(m.Entries[0].Data) != "x" || m.Entries[1].Type != raft.EntryNoop {
		t.Fatalf("mangled message: %+v", m)
	}
}

func TestLargeSnapshotIsChunkedAndReassembled(t *testing.T) {
	a, rcv := pair(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 5<<18) // 20 MiB, many chunks
	a.Send(raft.Message{Type: raft.MsgSnap, Term: 3, From: 1, To: 2, Snapshot: &raft.Snapshot{Index: 99, Term: 3, Data: data}})
	a.Send(raft.Message{Type: raft.MsgHeartbeat, Term: 3, From: 1, To: 2})

	m := recv(t, rcv)
	if m.Type != raft.MsgSnap || m.Snapshot == nil || m.Snapshot.Index != 99 || !bytes.Equal(m.Snapshot.Data, data) {
		t.Fatalf("snapshot did not survive the trip (type %v)", m.Type)
	}
	if next := recv(t, rcv); next.Type != raft.MsgHeartbeat {
		t.Fatalf("message after the snapshot was %v", next.Type)
	}
}
