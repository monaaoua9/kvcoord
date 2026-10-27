// Package raft is a from-scratch implementation of the Raft consensus
// protocol: leader election with PreVote and CheckQuorum, log replication
// with pipelining, snapshots, and ReadIndex linearizable reads.
package raft

type NodeID = uint64

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryNoop
	EntryConfig // Data is a marshalled ConfChange; takes effect when appended, not when committed
)

type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

func (e Entry) size() int { return 24 + len(e.Data) }

type HardState struct {
	Term uint64
	Vote NodeID
}

type Snapshot struct {
	Index   uint64
	Term    uint64
	Members []Member // voter set as of Index
	Data    []byte
}

type MsgType uint8

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgPreVote
	MsgPreVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
	MsgSnapResp
	MsgTimeoutNow // leader tells a caught-up follower to campaign right away
)

var msgNames = [...]string{"", "Vote", "VoteResp", "PreVote", "PreVoteResp", "App", "AppResp", "Heartbeat", "HeartbeatResp", "Snap", "SnapResp", "TimeoutNow"}

func (t MsgType) String() string {
	if int(t) < len(msgNames) {
		return msgNames[t]
	}
	return "Unknown"
}

// Message is the single wire type between nodes. Field meaning depends on Type:
//
//	Vote/PreVote:     LogIndex/LogTerm = candidate's last entry
//	App:              LogIndex/LogTerm = entry preceding Entries
//	AppResp (ok):     LogIndex = highest index known to match
//	AppResp (reject): LogIndex = the rejected prev index, Conflict* = hint
//	SnapResp:         LogIndex = follower's commit index after the install
type Message struct {
	Type          MsgType
	Term          uint64
	From          NodeID
	To            NodeID
	LogIndex      uint64
	LogTerm       uint64
	Commit        uint64
	Round         uint64
	Entries       []Entry
	Reject        bool
	ConflictIndex uint64
	ConflictTerm  uint64
	Snapshot      *Snapshot
	Transfer      bool // vote requests: sent by a node told to take over, skip the leader-lease check
}

type StateType uint8

const (
	Follower StateType = iota
	PreCandidate
	Candidate
	Leader
)

func (s StateType) String() string {
	switch s {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// StateMachine is driven by a single goroutine: Apply, Snapshot and Restore
// are never called concurrently with each other.
type StateMachine interface {
	Apply(e Entry) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// AsyncSnapshotter is an optional upgrade of StateMachine.Snapshot: SnapshotAsync
// is called on the apply goroutine and must be cheap, the function it returns
// does the heavy work and may run concurrently with later Apply calls.
type AsyncSnapshotter interface {
	SnapshotAsync() func() ([]byte, error)
}

type Transport interface {
	// Send must not block. Dropping messages is fine, raft copes with loss.
	Send(m Message)
}

type Status struct {
	ID           NodeID
	Term         uint64
	Leader       NodeID
	State        StateType
	Commit       uint64
	Applied      uint64
	LastIndex    uint64
	FirstIndex   uint64
	SnapIndex    uint64
	Transferring bool
	LeaderReady  bool
	Match        map[NodeID]uint64
}
