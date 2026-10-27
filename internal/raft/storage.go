package raft

// Recovered is what a Storage hands back at startup.
type Recovered struct {
	HardState HardState
	SnapIndex uint64
	SnapTerm  uint64
	Entries   []Entry // everything after SnapIndex
}

// Storage must be safe for concurrent use: the event loop calls Save while
// the apply goroutine calls SaveSnapshot.
type Storage interface {
	Load() (Recovered, error)
	// Save durably writes hs (if non-nil) and ents. If ents starts at or below
	// an index that was written earlier, the older suffix is replaced.
	Save(hs *HardState, ents []Entry) error
	// SaveSnapshot persists s and discards log entries up to s.Index.
	// Snapshots older than the newest one are ignored.
	SaveSnapshot(s Snapshot) error
	LoadSnapshot() (Snapshot, bool, error)
	Close() error
}
