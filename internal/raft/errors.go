package raft

import (
	"errors"
	"fmt"
)

var (
	ErrStopped         = errors.New("raft: node stopped")
	ErrLeadershipLost  = errors.New("raft: leadership lost before the proposal was applied")
	ErrProposalDropped = errors.New("raft: proposal was overwritten by another leader")
	ErrBusy            = errors.New("raft: too many uncommitted proposals")
	ErrCompacted       = errors.New("raft: requested index is compacted")
	ErrUnavailable     = errors.New("raft: requested index is beyond the log")
)

type NotLeaderError struct {
	Leader NodeID
}

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "raft: not leader, leader unknown"
	}
	return fmt.Sprintf("raft: not leader, leader is %d", e.Leader)
}

var (
	ErrConfChangePending = errors.New("raft: another membership change has not committed yet")
	ErrConfChangeInvalid = errors.New("raft: membership change does not apply to the current voter set")
	ErrNotReady          = errors.New("raft: leader has not committed an entry in its term yet")
)

var ErrLearnerLagging = errors.New("raft: learner has not caught up with the log yet")

var (
	ErrTransferring     = errors.New("raft: leadership transfer in progress")
	ErrTransferInvalid  = errors.New("raft: transfer target must be another voter")
	ErrTransferTimedOut = errors.New("raft: target did not take over in time")
)
