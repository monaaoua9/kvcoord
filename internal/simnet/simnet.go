// Package simnet is an in-process network with injectable faults, used by the
// raft tests and the chaos/linearizability harness.
package simnet

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

type Receiver interface {
	Step(raft.Message)
}

type link struct{ from, to raft.NodeID }

type Network struct {
	mu      sync.Mutex
	nodes   map[raft.NodeID]Receiver
	cut     map[link]bool
	loss    float64
	dup     float64
	minD    time.Duration
	maxD    time.Duration
	rng     *rand.Rand
	sent    uint64
	dropped uint64
}

func New(seed uint64) *Network {
	return &Network{
		nodes: make(map[raft.NodeID]Receiver),
		cut:   make(map[link]bool),
		rng:   rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

func (n *Network) Register(id raft.NodeID, r Receiver) {
	n.mu.Lock()
	n.nodes[id] = r
	n.mu.Unlock()
}

func (n *Network) Unregister(id raft.NodeID) {
	n.mu.Lock()
	delete(n.nodes, id)
	n.mu.Unlock()
}

func (n *Network) SetLoss(p float64) {
	n.mu.Lock()
	n.loss = p
	n.mu.Unlock()
}

func (n *Network) SetDuplicate(p float64) {
	n.mu.Lock()
	n.dup = p
	n.mu.Unlock()
}

func (n *Network) SetLatency(lo, hi time.Duration) {
	n.mu.Lock()
	n.minD, n.maxD = lo, hi
	n.mu.Unlock()
}

// Cut blocks traffic in both directions between a and b.
func (n *Network) Cut(a, b raft.NodeID) {
	n.mu.Lock()
	n.cut[link{a, b}] = true
	n.cut[link{b, a}] = true
	n.mu.Unlock()
}

func (n *Network) Isolate(id raft.NodeID) {
	n.mu.Lock()
	for other := range n.nodes {
		if other != id {
			n.cut[link{id, other}] = true
			n.cut[link{other, id}] = true
		}
	}
	n.mu.Unlock()
}

// Partition splits the given nodes into groups that cannot talk to each other.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					n.cut[link{a, b}] = true
				}
			}
		}
	}
}

func (n *Network) Heal() {
	n.mu.Lock()
	n.cut = make(map[link]bool)
	n.mu.Unlock()
}

func (n *Network) Stats() (sent, dropped uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sent, n.dropped
}

type endpoint struct {
	net  *Network
	from raft.NodeID
}

func (n *Network) Transport(from raft.NodeID) raft.Transport {
	return &endpoint{net: n, from: from}
}

func (e *endpoint) Send(m raft.Message) {
	n := e.net
	n.mu.Lock()
	n.sent++
	r := n.nodes[m.To]
	if r == nil || n.cut[link{e.from, m.To}] || (n.loss > 0 && n.rng.Float64() < n.loss) {
		n.dropped++
		n.mu.Unlock()
		return
	}
	copies := 1
	if n.dup > 0 && n.rng.Float64() < n.dup {
		copies = 2
	}
	delays := make([]time.Duration, copies)
	for i := range delays {
		if n.maxD > 0 {
			delays[i] = n.minD + time.Duration(n.rng.Int64N(int64(n.maxD-n.minD)+1))
		}
	}
	n.mu.Unlock()

	for _, d := range delays {
		if d == 0 {
			r.Step(m)
			continue
		}
		time.AfterFunc(d, func() {
			n.mu.Lock()
			blocked := n.cut[link{e.from, m.To}]
			cur := n.nodes[m.To]
			n.mu.Unlock()
			if cur != nil && !blocked {
				cur.Step(m)
			}
		})
	}
}
