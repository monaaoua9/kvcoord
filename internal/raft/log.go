package raft

// raftLog keeps every entry after the last compaction point in memory.
// It is only touched from the node's event loop.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	ents      []Entry // ents[0].Index == snapIndex+1
	stable    uint64  // highest index known to be on disk
}

func newLog(snapIndex, snapTerm uint64, ents []Entry) *raftLog {
	l := &raftLog{snapIndex: snapIndex, snapTerm: snapTerm, ents: ents}
	l.stable = l.lastIndex()
	return l
}

func (l *raftLog) firstIndex() uint64 { return l.snapIndex + 1 }

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.ents)) }

func (l *raftLog) lastTerm() uint64 {
	if len(l.ents) == 0 {
		return l.snapTerm
	}
	return l.ents[len(l.ents)-1].Term
}

func (l *raftLog) term(i uint64) (uint64, error) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, nil
	case i < l.snapIndex:
		return 0, ErrCompacted
	case i > l.lastIndex():
		return 0, ErrUnavailable
	}
	return l.ents[i-l.snapIndex-1].Term, nil
}

// slice returns a copy of up to maxEnts entries starting at lo, stopping early
// once maxBytes is exceeded (always returns at least one entry if any exist).
func (l *raftLog) slice(lo uint64, maxEnts, maxBytes int) ([]Entry, error) {
	if lo <= l.snapIndex {
		return nil, ErrCompacted
	}
	if lo > l.lastIndex() {
		return nil, nil
	}
	src := l.ents[lo-l.snapIndex-1:]
	n, size := 0, 0
	for n < len(src) && n < maxEnts {
		size += src[n].size()
		n++
		if size >= maxBytes {
			break
		}
	}
	out := make([]Entry, n)
	copy(out, src[:n])
	return out, nil
}

func (l *raftLog) append(ents ...Entry) {
	l.ents = append(l.ents, ents...)
}

// truncateFrom drops i and everything after it.
func (l *raftLog) truncateFrom(i uint64) {
	if i > l.lastIndex() {
		return
	}
	l.ents = l.ents[:i-l.snapIndex-1]
	if l.stable >= i {
		l.stable = i - 1
	}
}

func (l *raftLog) unstable() []Entry {
	if l.stable >= l.lastIndex() {
		return nil
	}
	return l.ents[l.stable-l.snapIndex:]
}

func (l *raftLog) markStable() { l.stable = l.lastIndex() }

// compactTo makes i the new base. Callers must make sure i is applied.
func (l *raftLog) compactTo(i uint64) {
	if i <= l.snapIndex || i > l.lastIndex() {
		return
	}
	t, _ := l.term(i)
	rest := l.ents[i-l.snapIndex:]
	l.ents = append(make([]Entry, 0, len(rest)+16), rest...)
	l.snapIndex, l.snapTerm = i, t
}

// restore throws the whole log away in favour of a snapshot.
func (l *raftLog) restore(index, term uint64) {
	l.ents = nil
	l.snapIndex, l.snapTerm = index, term
	l.stable = index
}

func (l *raftLog) isUpToDate(lastIdx, lastTerm uint64) bool {
	lt := l.lastTerm()
	return lastTerm > lt || (lastTerm == lt && lastIdx >= l.lastIndex())
}

// lastIndexOfTerm scans backwards from hi for the newest entry of term t.
func (l *raftLog) lastIndexOfTerm(t, hi uint64) uint64 {
	if hi > l.lastIndex() {
		hi = l.lastIndex()
	}
	for i := hi; i > l.snapIndex; i-- {
		if et, _ := l.term(i); et == t {
			return i
		}
	}
	if l.snapTerm == t {
		return l.snapIndex
	}
	return 0
}
