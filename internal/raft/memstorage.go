package raft

import "sync"

// MemStorage keeps everything in memory. Reusing one across node restarts
// simulates a disk that survives the process, which is what the tests want.
type MemStorage struct {
	mu   sync.Mutex
	hs   HardState
	snap Snapshot
	has  bool
	ents []Entry // ents[0].Index == snap.Index+1
}

func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) Load() (Recovered, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Recovered{
		HardState: m.hs,
		SnapIndex: m.snap.Index,
		SnapTerm:  m.snap.Term,
		Entries:   append([]Entry(nil), m.ents...),
	}, nil
}

func (m *MemStorage) Save(hs *HardState, ents []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hs != nil {
		m.hs = *hs
	}
	if len(ents) == 0 {
		return nil
	}
	first := ents[0].Index
	if first <= m.snap.Index {
		skip := m.snap.Index + 1 - first
		if skip >= uint64(len(ents)) {
			return nil
		}
		ents = ents[skip:]
		first = ents[0].Index
	}
	keep := first - m.snap.Index - 1
	if keep > uint64(len(m.ents)) {
		panic("memstorage: gap in log")
	}
	m.ents = append(m.ents[:keep], ents...)
	return nil
}

func (m *MemStorage) SaveSnapshot(s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.has && s.Index <= m.snap.Index {
		return nil
	}
	last := m.snap.Index + uint64(len(m.ents))
	if s.Index >= last {
		m.ents = nil
	} else {
		m.ents = append([]Entry(nil), m.ents[s.Index-m.snap.Index:]...)
	}
	m.snap = s
	m.has = true
	return nil
}

func (m *MemStorage) LoadSnapshot() (Snapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap, m.has, nil
}

func (m *MemStorage) Close() error { return nil }
