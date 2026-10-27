package raft

type progState uint8

const (
	probe progState = iota
	replicate
	snapshotting
)

// progress is the leader's view of one follower.
type progress struct {
	match uint64
	next  uint64
	state progState

	probeSent bool
	infl      []uint64 // last index of every in-flight MsgApp that carried entries

	recentActive bool
	ackedRound   uint64
	stuckTicks   int
	snapTicks    int
}

func (p *progress) becomeProbe() {
	p.state = probe
	p.next = p.match + 1
	p.probeSent = false
	p.infl = p.infl[:0]
}

func (p *progress) becomeReplicate() {
	p.state = replicate
	p.next = p.match + 1
	p.probeSent = false
	p.infl = p.infl[:0]
}

func (p *progress) freeTo(idx uint64) {
	i := 0
	for i < len(p.infl) && p.infl[i] <= idx {
		i++
	}
	p.infl = p.infl[i:]
}

func (p *progress) paused(maxInflight int) bool {
	switch p.state {
	case probe:
		return p.probeSent
	case replicate:
		return len(p.infl) >= maxInflight
	}
	return true
}
