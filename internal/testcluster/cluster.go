// Package testcluster boots whole kvcoord nodes in-process: real gRPC for
// clients, a simulated network for raft traffic so tests can cut links.
package testcluster

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/client"
	"github.com/monaaoua9/kvcoord/internal/raft"
	"github.com/monaaoua9/kvcoord/internal/server"
	"github.com/monaaoua9/kvcoord/internal/simnet"
)

type Options struct {
	Nodes         int
	Seed          uint64
	SnapshotEvery uint64
	CompactRetain uint64
	AuthTokens    map[string]string
	Verbose       bool
}

type node struct {
	join  bool
	id    raft.NodeID
	addr  string
	store *raft.MemStorage
	srv   *server.Server
}

type Cluster struct {
	t     testing.TB
	opts  Options
	Net   *simnet.Network
	peers map[uint64]string

	mu    sync.Mutex
	nodes map[raft.NodeID]*node
	ids   []raft.NodeID
}

func New(t testing.TB, opts Options) *Cluster {
	t.Helper()
	if opts.Nodes == 0 {
		opts.Nodes = 3
	}
	c := &Cluster{
		t: t, opts: opts, Net: simnet.New(opts.Seed + 1),
		peers: make(map[uint64]string), nodes: make(map[raft.NodeID]*node),
	}
	lis := make(map[raft.NodeID]net.Listener)
	for i := 1; i <= opts.Nodes; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		lis[id] = l
		c.peers[id] = l.Addr().String()
	}
	for id, l := range lis {
		c.nodes[id] = &node{id: id, addr: c.peers[id], store: raft.NewMemStorage()}
		c.boot(c.nodes[id], l)
	}
	t.Cleanup(c.Close)
	return c
}

func (c *Cluster) boot(n *node, l net.Listener) {
	c.t.Helper()
	var logger *slog.Logger
	if c.opts.Verbose {
		logger = slog.New(slog.NewTextHandler(testWriter{c.t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	peers := c.peers
	if n.join {
		peers = map[uint64]string{n.id: n.addr}
	}
	srv, err := server.New(server.Config{
		NodeID: n.id, Peers: peers, Join: n.join,
		TickInterval: 10 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 1,
		SnapshotEvery: c.opts.SnapshotEvery, SnapshotTrailing: 10,
		MinLeaseTTL: 100 * time.Millisecond, SessionTTL: time.Hour,
		CompactRetain: c.opts.CompactRetain, CompactEvery: 100 * time.Millisecond,
		AuthTokens: c.opts.AuthTokens,
		Storage:    n.store, Transport: c.Net.Transport(n.id), Listener: l,
		Logger: logger,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	n.srv = srv
	c.Net.Register(n.id, srv.Node())
	if err := srv.Start(); err != nil {
		c.t.Fatal(err)
	}
}

type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

func (c *Cluster) IDs() []raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]raft.NodeID(nil), c.ids...)
}

func (c *Cluster) Endpoints() []string {
	var eps []string
	for _, id := range c.IDs() {
		eps = append(eps, c.Addr(id))
	}
	return eps
}

func (c *Cluster) Addr(id raft.NodeID) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id].addr
}

func (c *Cluster) Server(id raft.NodeID) *server.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := c.nodes[id]; n != nil {
		return n.srv
	}
	return nil
}

func (c *Cluster) Client() *client.Client {
	c.t.Helper()
	cl, err := client.New(client.Config{Endpoints: c.Endpoints(), RequestTimeout: 2 * time.Second})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

// ClientWithToken is Client for clusters that require authentication.
func (c *Cluster) ClientWithToken(token string) *client.Client {
	c.t.Helper()
	cl, err := client.New(client.Config{Endpoints: c.Endpoints(), RequestTimeout: 2 * time.Second, Token: token})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

// Crash stops a node but keeps its disk.
func (c *Cluster) Crash(id raft.NodeID) {
	c.mu.Lock()
	n := c.nodes[id]
	srv := n.srv
	n.srv = nil
	c.mu.Unlock()
	if srv == nil {
		return
	}
	c.Net.Unregister(id)
	srv.Stop()
}

func (c *Cluster) Restart(id raft.NodeID) {
	c.t.Helper()
	c.mu.Lock()
	n := c.nodes[id]
	c.mu.Unlock()
	if n.srv != nil {
		return
	}
	var l net.Listener
	var err error
	for i := 0; i < 50; i++ {
		if l, err = net.Listen("tcp", n.addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatalf("rebind %s: %v", n.addr, err)
	}
	c.boot(n, l)
}

func (c *Cluster) Close() {
	for _, id := range c.IDs() {
		c.Crash(id)
	}
}

// Leader waits for a node that is leader and ready to serve.
func (c *Cluster) Leader(timeout time.Duration) raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range c.IDs() {
			if s := c.Server(id); s != nil {
				if st := s.Node().Status(); st.State == raft.Leader && st.LeaderReady {
					return id
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return 0
}

func (c *Cluster) String() string { return fmt.Sprintf("cluster(%d)", c.opts.Nodes) }

// AddNode boots an empty node in join mode and returns its id. It is not a
// voter until the test calls MemberAdd.
func (c *Cluster) AddNode() raft.NodeID {
	c.t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	id := c.ids[len(c.ids)-1] + 1
	n := &node{id: id, addr: l.Addr().String(), store: raft.NewMemStorage(), join: true}
	c.nodes[id] = n
	c.ids = append(c.ids, id)
	c.mu.Unlock()
	c.boot(n, l)
	return id
}

// Members lists the voters according to any live node (they converge quickly).
func (c *Cluster) Members() []raft.NodeID {
	for _, id := range c.IDs() {
		if s := c.Server(id); s != nil {
			var out []raft.NodeID
			for _, m := range s.Node().Members() {
				out = append(out, m.ID)
			}
			if len(out) > 0 {
				return out
			}
		}
	}
	return c.IDs()
}
