package client

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

type fakeKV struct {
	kvpb.UnimplementedKVServer
	hint string // empty: this node is the leader
}

func (f *fakeKV) Range(context.Context, *kvpb.RangeRequest) (*kvpb.RangeResponse, error) {
	if f.hint != "" {
		return nil, kvpb.NewError(codes.Unavailable, kvpb.ErrNotLeader, "not leader",
			map[string]string{kvpb.MetaLeaderAddr: f.hint})
	}
	return &kvpb.RangeResponse{Kvs: []*kvpb.KeyValue{{Key: []byte("k"), Value: []byte("v")}}}, nil
}

func fake(t *testing.T, hint string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	kvpb.RegisterKVServer(s, &fakeKV{hint: hint})
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l.Addr().String()
}

// Behind NAT or in front of containers the nodes name the leader by an address
// the client cannot resolve. That used to trap the client in a loop between a
// follower and the dead hint, so it never tried the real leader.
func TestUnresolvableLeaderHintDoesNotTrapClient(t *testing.T) {
	const dead = "node-that-only-exists-inside-the-cluster.invalid:7001"
	f1, f2 := fake(t, dead), fake(t, dead)
	leader := fake(t, "")

	cl, err := New(Config{Endpoints: []string{f1, f2, leader}, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	r, err := cl.Get(ctx, "k")
	if err != nil || len(r.Kvs) != 1 {
		t.Fatalf("client never reached the leader: %v", err)
	}
	if got := len(cl.Endpoints()); got != 3 {
		t.Fatalf("a redirect hint was added to the endpoint list (%d endpoints)", got)
	}
}

func TestReachableLeaderHintIsFollowed(t *testing.T) {
	leader := fake(t, "")
	f := fake(t, leader)
	cl, err := New(Config{Endpoints: []string{f}, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if r, err := cl.Get(ctx, "k"); err != nil || len(r.Kvs) != 1 {
		t.Fatalf("redirect to a reachable leader failed: %v", err)
	}
}
