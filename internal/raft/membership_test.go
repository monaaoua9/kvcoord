package raft_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

func (c *cluster) startJoiner(id raft.NodeID) {
	c.t.Helper()
	fsm := &seqFSM{}
	st := raft.NewMemStorage()
	n, err := raft.NewNode(raft.Config{
		ID: id, Peers: []raft.NodeID{id}, Join: true,
		ElectionTick: 20, HeartbeatTick: 1, TickInterval: 5 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, st, fsm, c.net.Transport(id))
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.Register(id, n)
	n.Start()
	c.mem[id] = &member{node: n, fsm: fsm, st: st}
}

func (c *cluster) confChange(m *member, cc raft.ConfChange) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return m.node.ProposeConfChange(ctx, cc)
}

func ids(ms []raft.Member) []raft.NodeID {
	var out []raft.NodeID
	for _, m := range ms {
		out = append(out, m.ID)
	}
	return out
}

func TestAddVoter(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	for i := 0; i < 20; i++ {
		if err := c.propose(l, "a"); err != nil {
			t.Fatal(err)
		}
	}

	c.startJoiner(4)
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfAdd, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	c.ids = append(c.ids, 4)
	c.waitApplied(20, 4)

	// new node counts toward quorum and sees the same membership
	l = c.leader(3 * time.Second)
	if err := c.propose(l, "after"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(21)
	c.assertSame()
	for _, m := range c.mem {
		if got := len(m.node.Members()); got != 4 {
			t.Fatalf("members = %v", ids(m.node.Members()))
		}
	}
}

func TestRemoveFollowerKeepsQuorum(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)

	var victim raft.NodeID
	for id := range c.mem {
		if id != l.node.Status().ID {
			victim = id
			break
		}
	}
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfRemove, ID: victim}); err != nil {
		t.Fatal(err)
	}
	c.stop(victim)

	// two voters left, both needed; losing neither is fine
	if err := c.propose(c.leader(3*time.Second), "x"); err != nil {
		t.Fatal(err)
	}
	if got := len(l.node.Members()); got != 2 {
		t.Fatalf("members = %v", ids(l.node.Members()))
	}
}

func TestSecondChangeWhilePendingIsRejected(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)

	// cut the leader off so the first change cannot commit
	for id := range c.mem {
		if id != l.node.Status().ID {
			c.net.Isolate(l.node.Status().ID)
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go l.node.ProposeConfChange(ctx, raft.ConfChange{Type: raft.ConfAdd, ID: 9, Addr: "n9"})
	time.Sleep(50 * time.Millisecond)

	err := c.confChange(l, raft.ConfChange{Type: raft.ConfAdd, ID: 10, Addr: "n10"})
	if !errors.Is(err, raft.ErrConfChangePending) {
		t.Fatalf("want ErrConfChangePending, got %v", err)
	}
}

func TestInvalidChangesRejected(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)

	for _, cc := range []raft.ConfChange{
		{Type: raft.ConfAdd, ID: 2, Addr: "dup"},
		{Type: raft.ConfRemove, ID: 77},
	} {
		if err := c.confChange(l, cc); !errors.Is(err, raft.ErrConfChangeInvalid) {
			t.Fatalf("%+v: want ErrConfChangeInvalid, got %v", cc, err)
		}
	}
}

func TestConfigSurvivesRestartAndSnapshot(t *testing.T) {
	c := newCluster(t, 3, func(cfg *raft.Config) { cfg.SnapshotEvery = 10 })
	l := c.leader(3 * time.Second)

	c.startJoiner(4)
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfAdd, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	c.ids = append(c.ids, 4)

	// enough traffic to compact the config entries into a snapshot
	for i := 0; i < 40; i++ {
		if err := c.propose(c.leader(3*time.Second), "k"); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(40)

	st := c.stop(4)
	c.ids = c.ids[:3] // start() builds Peers from ids; 4 restarts from its own storage
	c.ids = append(c.ids, 4)
	c.start(4, st)
	c.waitApplied(40, 4)
	if got := len(c.mem[4].node.Members()); got != 4 {
		t.Fatalf("members after restart = %v", ids(c.mem[4].node.Members()))
	}
}

func learnerOf(m *member, id raft.NodeID) bool {
	for _, x := range m.node.Members() {
		if x.ID == id {
			return x.Learner
		}
	}
	return false
}

func TestLearnerReplicatesButDoesNotVote(t *testing.T) {
	c := newCluster(t, 2, nil)
	l := c.leader(3 * time.Second)

	c.startJoiner(4)
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfAddLearner, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	c.ids = append(c.ids, 4)
	for i := 0; i < 10; i++ {
		if err := c.propose(l, "x"); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(10, 4)
	if !learnerOf(l, 4) {
		t.Fatalf("node 4 should be a learner: %v", l.node.Members())
	}

	// two voters, so quorum is 2; with the other voter gone the leader and the
	// learner are two live nodes but only one vote
	lid := l.node.Status().ID
	for _, id := range []raft.NodeID{1, 2} {
		if id != lid {
			c.stop(id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := l.node.Propose(ctx, []byte("stuck")); err == nil {
		t.Fatal("a learner's ack must not commit anything")
	}
	if st := c.mem[4].node.Status(); st.State == raft.Leader {
		t.Fatal("a learner must never campaign")
	}
}

func TestPromoteNeedsCatchUp(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)

	c.startJoiner(4)
	c.net.Isolate(4)
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfAddLearner, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	c.ids = append(c.ids, 4)
	for i := 0; i < 400; i++ {
		if err := c.propose(l, "x"); err != nil {
			t.Fatal(err)
		}
	}
	err := c.confChange(l, raft.ConfChange{Type: raft.ConfPromote, ID: 4})
	if !errors.Is(err, raft.ErrLearnerLagging) {
		t.Fatalf("want ErrLearnerLagging, got %v", err)
	}

	c.net.Heal()
	c.waitApplied(400, 4)
	deadline := time.Now().Add(3 * time.Second)
	for {
		err = c.confChange(c.leader(3*time.Second), raft.ConfChange{Type: raft.ConfPromote, ID: 4})
		if err == nil {
			break
		}
		if !errors.Is(err, raft.ErrLearnerLagging) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if learnerOf(c.leader(3*time.Second), 4) {
		t.Fatal("still a learner after promotion")
	}
}

func TestLearnerChangesValidated(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)

	// promoting a voter, or a stranger, makes no sense
	for _, id := range []raft.NodeID{1, 42} {
		err := c.confChange(l, raft.ConfChange{Type: raft.ConfPromote, ID: id})
		if !errors.Is(err, raft.ErrConfChangeInvalid) {
			t.Fatalf("promote %d: %v", id, err)
		}
	}

	c.startJoiner(4)
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfAddLearner, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	// a learner can simply be dropped again
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfRemove, ID: 4}); err != nil {
		t.Fatal(err)
	}
	if got := len(l.node.Members()); got != 3 {
		t.Fatalf("members = %v", ids(l.node.Members()))
	}
}

func TestLearnerFlagSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, func(cfg *raft.Config) { cfg.SnapshotEvery = 10 })
	l := c.leader(3 * time.Second)

	c.startJoiner(4)
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfAddLearner, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	c.ids = append(c.ids, 4)
	for i := 0; i < 40; i++ {
		if err := c.propose(c.leader(3*time.Second), "k"); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(40)

	// node 4 restarts from its own storage and still knows it is a learner
	st := c.stop(4)
	c.start(4, st)
	c.waitApplied(40, 4)
	if !learnerOf(c.mem[4], 4) {
		t.Fatalf("learner flag lost: %v", c.mem[4].node.Members())
	}
}

// a short log is below the lag limit even for a node that has received nothing
func TestPromoteRefusedForSilentLearner(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)

	c.startJoiner(4)
	c.net.Isolate(4)
	if err := c.confChange(l, raft.ConfChange{Type: raft.ConfAddLearner, ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := c.propose(l, "x"); err != nil {
			t.Fatal(err)
		}
	}
	err := c.confChange(l, raft.ConfChange{Type: raft.ConfPromote, ID: 4})
	if !errors.Is(err, raft.ErrLearnerLagging) {
		t.Fatalf("want ErrLearnerLagging, got %v", err)
	}
}
