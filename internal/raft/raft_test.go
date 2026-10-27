package raft_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/internal/raft"
	"github.com/monaaoua9/kvcoord/internal/simnet"
)

// seqFSM just remembers the payloads it was given, in order.
type seqFSM struct {
	mu   sync.Mutex
	data []string
}

func (f *seqFSM) Apply(e raft.Entry) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = append(f.data, string(e.Data))
	return len(f.data)
}

func (f *seqFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint32(len(f.data)))
	for _, s := range f.data {
		binary.Write(&b, binary.BigEndian, uint32(len(s)))
		b.WriteString(s)
	}
	return b.Bytes(), nil
}

func (f *seqFSM) Restore(p []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := bytes.NewReader(p)
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return err
	}
	f.data = f.data[:0]
	for i := uint32(0); i < n; i++ {
		var l uint32
		binary.Read(r, binary.BigEndian, &l)
		buf := make([]byte, l)
		io.ReadFull(r, buf)
		f.data = append(f.data, string(buf))
	}
	return nil
}

func (f *seqFSM) snapshotData() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.data...)
}

type member struct {
	node *raft.Node
	fsm  *seqFSM
	st   *raft.MemStorage
}

type cluster struct {
	t     *testing.T
	net   *simnet.Network
	ids   []raft.NodeID
	mem   map[raft.NodeID]*member
	cfgFn func(*raft.Config)
}

func newCluster(t *testing.T, n int, cfgFn func(*raft.Config)) *cluster {
	t.Helper()
	c := &cluster{t: t, net: simnet.New(uint64(n)), mem: map[raft.NodeID]*member{}, cfgFn: cfgFn}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, raft.NodeID(i))
	}
	for _, id := range c.ids {
		c.start(id, raft.NewMemStorage())
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *cluster) start(id raft.NodeID, st *raft.MemStorage) {
	c.t.Helper()
	fsm := &seqFSM{}
	cfg := raft.Config{
		ID: id, Peers: c.ids,
		ElectionTick: 20, HeartbeatTick: 1, TickInterval: 5 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if c.cfgFn != nil {
		c.cfgFn(&cfg)
	}
	n, err := raft.NewNode(cfg, st, fsm, c.net.Transport(id))
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.Register(id, n)
	n.Start()
	c.mem[id] = &member{node: n, fsm: fsm, st: st}
}

func (c *cluster) stop(id raft.NodeID) *raft.MemStorage {
	m := c.mem[id]
	c.net.Unregister(id)
	m.node.Stop()
	delete(c.mem, id)
	return m.st
}

func (c *cluster) stopAll() {
	for id := range c.mem {
		c.stop(id)
	}
}

func (c *cluster) leader(timeout time.Duration) *member {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found *member
		count := 0
		for _, m := range c.mem {
			if st := m.node.Status(); st.State == raft.Leader && st.LeaderReady {
				found = m
				count++
			}
		}
		if count == 1 {
			return found
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no stable leader")
	return nil
}

func (c *cluster) propose(m *member, s string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := m.node.Propose(ctx, []byte(s))
	return err
}

func (c *cluster) waitApplied(count int, ids ...raft.NodeID) {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if m := c.mem[id]; m != nil && len(m.fsm.snapshotData()) < count {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, id := range ids {
		if m := c.mem[id]; m != nil {
			c.t.Logf("node %d applied %d", id, len(m.fsm.snapshotData()))
		}
	}
	c.t.Fatalf("timed out waiting for %d applied entries", count)
}

func (c *cluster) assertSame(ids ...raft.NodeID) {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.ids
	}
	var ref []string
	for i, id := range ids {
		d := c.mem[id].fsm.snapshotData()
		if i == 0 {
			ref = d
			continue
		}
		if fmt.Sprint(ref) != fmt.Sprint(d) {
			c.t.Fatalf("node %d diverged:\n%v\nvs\n%v", id, ref, d)
		}
	}
}

func TestSingleNode(t *testing.T) {
	c := newCluster(t, 1, nil)
	l := c.leader(2 * time.Second)
	for i := 0; i < 20; i++ {
		if err := c.propose(l, fmt.Sprint("v", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(20)
	idx, err := l.node.ReadIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if idx < 21 {
		t.Fatalf("read index %d too low", idx)
	}
}

func TestReplicationThreeNodes(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	for i := 0; i < 200; i++ {
		if err := c.propose(l, fmt.Sprint("v", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(200)
	c.assertSame()
}

func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, 5, nil)
	l := c.leader(3 * time.Second)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := c.propose(l, fmt.Sprint("g", g, "-", i)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	c.waitApplied(800)
	c.assertSame()
}

func TestNotLeaderError(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	for id, m := range c.mem {
		if m == l {
			continue
		}
		err := c.propose(m, "x")
		var nl *raft.NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("node %d: want NotLeaderError, got %v", id, err)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	for i := 0; i < 10; i++ {
		if err := c.propose(l, fmt.Sprint("a", i)); err != nil {
			t.Fatal(err)
		}
	}
	oldID := l.node.Status().ID
	st := c.stop(oldID)

	l2 := c.leader(3 * time.Second)
	for i := 0; i < 10; i++ {
		if err := c.propose(l2, fmt.Sprint("b", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.start(oldID, st)
	c.waitApplied(20)
	c.assertSame()
}

func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5, nil)
	l := c.leader(3 * time.Second)
	lid := l.node.Status().ID
	var others []raft.NodeID
	for _, id := range c.ids {
		if id != lid {
			others = append(others, id)
		}
	}
	// leader plus one follower on one side, three nodes on the other
	c.net.Partition([]raft.NodeID{lid, others[0]}, others[1:])

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := l.node.Propose(ctx, []byte("lost")); err == nil {
		t.Fatal("minority leader committed")
	}

	deadline := time.Now().Add(3 * time.Second)
	var l2 *member
	for time.Now().Before(deadline) && l2 == nil {
		for _, id := range others[1:] {
			if st := c.mem[id].node.Status(); st.State == raft.Leader && st.LeaderReady {
				l2 = c.mem[id]
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if l2 == nil {
		t.Fatal("majority side never elected a leader")
	}
	for i := 0; i < 5; i++ {
		if err := c.propose(l2, fmt.Sprint("m", i)); err != nil {
			t.Fatal(err)
		}
	}

	c.net.Heal()
	c.waitApplied(5)
	c.assertSame()
	for _, m := range c.mem {
		for _, s := range m.fsm.snapshotData() {
			if s == "lost" {
				t.Fatal("uncommitted entry survived")
			}
		}
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, func(cfg *raft.Config) {
		cfg.SnapshotEvery = 20
		cfg.SnapshotTrailing = 5
	})
	l := c.leader(3 * time.Second)
	lag := raft.NodeID(0)
	for _, id := range c.ids {
		if c.mem[id] != l {
			lag = id
			break
		}
	}
	c.net.Isolate(lag)

	for i := 0; i < 100; i++ {
		if err := c.propose(l, fmt.Sprint("s", i)); err != nil {
			t.Fatal(err)
		}
	}
	// wait for the leader to compact past what the isolated node has
	deadline := time.Now().Add(3 * time.Second)
	for l.node.Status().SnapIndex < 50 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if l.node.Status().SnapIndex < 50 {
		t.Fatal("leader never compacted")
	}

	c.net.Heal()
	c.waitApplied(100)
	c.assertSame()
}

func TestReadIndexAfterPartition(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	if err := c.propose(l, "x"); err != nil {
		t.Fatal(err)
	}
	idx, err := l.node.ReadIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.node.WaitApplied(context.Background(), idx); err != nil {
		t.Fatal(err)
	}

	// cut the leader off: it must not be able to confirm leadership
	c.net.Isolate(l.node.Status().ID)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := l.node.ReadIndex(ctx); err == nil {
		t.Fatal("isolated leader served a read")
	}
}

func TestRestartKeepsState(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	for i := 0; i < 30; i++ {
		if err := c.propose(l, fmt.Sprint("r", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(30)
	for _, id := range c.ids {
		st := c.stop(id)
		c.start(id, st)
	}
	l = c.leader(3 * time.Second)
	if err := c.propose(l, "after"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(31)
	c.assertSame()
}

func TestLossyNetwork(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.net.SetLoss(0.2)
	c.net.SetDuplicate(0.1)
	c.net.SetLatency(0, 4*time.Millisecond)

	done := 0
	deadline := time.Now().Add(15 * time.Second)
	for done < 60 && time.Now().Before(deadline) {
		l := c.leader(5 * time.Second)
		if err := c.propose(l, fmt.Sprint("n", done)); err == nil {
			done++
		}
	}
	if done < 60 {
		t.Fatalf("only %d proposals went through", done)
	}
	c.net.SetLoss(0)
	c.net.SetDuplicate(0)
	c.waitApplied(60)
	c.assertSame()
}
