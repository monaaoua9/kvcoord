package raft_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

func (c *cluster) transfer(l *member, to raft.NodeID) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return l.node.TransferLeadership(ctx, to)
}

func TestTransferLeadership(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	for i := 0; i < 30; i++ {
		if err := c.propose(l, "x"); err != nil {
			t.Fatal(err)
		}
	}
	old := l.node.Status().ID
	var target raft.NodeID
	for id := range c.mem {
		if id != old {
			target = id
			break
		}
	}
	if err := c.transfer(l, target); err != nil {
		t.Fatal(err)
	}
	nl := c.leader(3 * time.Second)
	if got := nl.node.Status().ID; got != target {
		t.Fatalf("leader is %d, wanted %d", got, target)
	}
	if err := c.propose(nl, "after"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(31)
	c.assertSame()
}

func TestTransferWaitsForCatchUp(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	lid := l.node.Status().ID
	var lag, other raft.NodeID
	for id := range c.mem {
		if id == lid {
			continue
		}
		if lag == 0 {
			lag = id
		} else {
			other = id
		}
	}
	// the target misses a batch while cut off, then must be brought up to date
	// before it is allowed to campaign
	c.net.Cut(lid, lag)
	c.net.Cut(other, lag)
	for i := 0; i < 50; i++ {
		if err := c.propose(l, "x"); err != nil {
			t.Fatal(err)
		}
	}
	c.net.Heal()
	if err := c.transfer(l, lag); err != nil {
		t.Fatal(err)
	}
	nl := c.leader(3 * time.Second)
	if nl.node.Status().ID != lag {
		t.Fatalf("leader is %d, wanted %d", nl.node.Status().ID, lag)
	}
	// nothing committed may be lost
	c.waitApplied(50)
	c.assertSame()
}

func TestTransferInvalidTargets(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	for _, id := range []raft.NodeID{l.node.Status().ID, 99} {
		if err := c.transfer(l, id); !errors.Is(err, raft.ErrTransferInvalid) {
			t.Fatalf("target %d: %v", id, err)
		}
	}
}

func TestTransferToDeadNodeGivesUp(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(3 * time.Second)
	lid := l.node.Status().ID
	var dead raft.NodeID
	for id := range c.mem {
		if id != lid {
			dead = id
			break
		}
	}
	c.stop(dead)
	if err := c.transfer(l, dead); !errors.Is(err, raft.ErrTransferTimedOut) {
		t.Fatalf("want ErrTransferTimedOut, got %v", err)
	}
	// and writes work again afterwards
	if err := c.propose(c.leader(3*time.Second), "x"); err != nil {
		t.Fatal(err)
	}
}
