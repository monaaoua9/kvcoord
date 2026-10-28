package raft_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

// Drives a five node cluster through random partitions, crashes, restarts and
// message loss while proposing, and checks the properties Raft promises:
//
//	election safety:       never two leaders in one term
//	durability:            every proposal that was acknowledged is in the final log
//	state machine safety:  once healed, every replica applied the same sequence
func TestRaftInvariantsUnderChaos(t *testing.T) {
	runs := 4
	if v, err := strconv.Atoi(os.Getenv("KV_RAFT_RUNS")); err == nil {
		runs = v
	}
	for seed := 1; seed <= runs; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { invariantRun(t, uint64(seed)) })
	}
}

func invariantRun(t *testing.T, seed uint64) {
	c := newCluster(t, 5, nil)
	rng := rand.New(rand.NewPCG(seed, 42))
	leaders := map[uint64]raft.NodeID{} // term -> the one leader seen in it
	stopped := map[raft.NodeID]*raft.MemStorage{}
	var acked []string

	sample := func() {
		for id, m := range c.mem {
			st := m.node.Status()
			if st.State != raft.Leader {
				continue
			}
			if prev, ok := leaders[st.Term]; ok && prev != id {
				t.Fatalf("seed %d: nodes %d and %d both lead term %d", seed, prev, id, st.Term)
			}
			leaders[st.Term] = id
		}
	}
	var live []raft.NodeID
	pickLive := func() raft.NodeID {
		live = live[:0]
		for id := range c.mem {
			live = append(live, id)
		}
		// map order is random, sort it so the seed means something
		for i := 1; i < len(live); i++ {
			for j := i; j > 0 && live[j] < live[j-1]; j-- {
				live[j], live[j-1] = live[j-1], live[j]
			}
		}
		return live[rng.IntN(len(live))]
	}

	deadline := time.Now().Add(4 * time.Second)
	n := 0
	for time.Now().Before(deadline) {
		sample()
		switch r := rng.IntN(20); {
		case r < 10: // propose through whoever claims to be leader
			for _, m := range c.mem {
				if m.node.Status().State != raft.Leader {
					continue
				}
				v := fmt.Sprintf("s%d-%d", seed, n)
				n++
				ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
				_, err := m.node.Propose(ctx, []byte(v))
				cancel()
				if err == nil {
					acked = append(acked, v)
				}
				break
			}
		case r == 10 && len(stopped) < 2: // crash a node
			id := pickLive()
			stopped[id] = c.stop(id)
		case r == 11 && len(stopped) > 0: // bring one back from its own storage
			for id, st := range stopped {
				c.start(id, st)
				delete(stopped, id)
				break
			}
		case r == 12:
			ids := c.ids
			a := rng.Perm(len(ids))
			cut := 1 + rng.IntN(len(ids)-1)
			var x, y []raft.NodeID
			for i, p := range a {
				if i < cut {
					x = append(x, ids[p])
				} else {
					y = append(y, ids[p])
				}
			}
			c.net.Partition(x, y)
		case r == 13:
			c.net.Heal()
		case r == 14:
			c.net.SetLoss(rng.Float64() * 0.3)
			c.net.SetDuplicate(0.1)
		case r == 15:
			c.net.SetLoss(0)
			c.net.SetDuplicate(0)
		}
		time.Sleep(time.Duration(1+rng.IntN(4)) * time.Millisecond)
	}

	c.net.Heal()
	c.net.SetLoss(0)
	c.net.SetDuplicate(0)
	for id, st := range stopped {
		c.start(id, st)
	}
	l := c.leader(10 * time.Second)
	if err := c.propose(l, "final"); err != nil {
		for i := 0; i < 20 && err != nil; i++ {
			err = c.propose(c.leader(5*time.Second), "final")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	sample()

	c.waitApplied(len(acked) + 1)
	c.assertSame()

	have := map[string]bool{}
	for _, s := range c.mem[c.ids[0]].fsm.snapshotData() {
		have[s] = true
	}
	for _, v := range acked {
		if !have[v] {
			t.Fatalf("seed %d: acknowledged proposal %q is missing from the replicated log", seed, v)
		}
	}
	t.Logf("seed %d: %d acknowledged proposals, %d terms with a leader", seed, len(acked), len(leaders))
}
