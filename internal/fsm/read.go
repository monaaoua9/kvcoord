package fsm

import (
	"errors"
	"slices"
	"sort"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/keyrange"
	"github.com/monaaoua9/kvcoord/internal/watch"
)

var ErrCompacted = errors.New("fsm: requested revision has been compacted")

// Range serves a read from local state. For linearizable reads the caller
// must have waited for the ReadIndex first.
func (m *Machine) Range(req *kvpb.RangeRequest) (*kvpb.RangeResponse, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if r := req.Revision; r != 0 && r != m.rev {
		if r > m.rev {
			return nil, m.rev, ErrFutureRev
		}
		if r < m.compactRev {
			return nil, m.rev, ErrCompacted
		}
		return m.rangeAt(req, r), m.rev, nil
	}
	return m.latest(req), m.rev, nil
}

func (m *Machine) latest(req *kvpb.RangeRequest) *kvpb.RangeResponse {
	resp := &kvpb.RangeResponse{}
	limit := int(req.Limit)
	if req.CountOnly {
		resp.Count = uint64(len(m.collect(req.Key, req.RangeEnd, 0)))
		return resp
	}
	want := 0
	if limit > 0 {
		want = limit + 1
	}
	items := m.collect(req.Key, req.RangeEnd, want)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
		resp.More = true
	}
	resp.Count = uint64(len(items))
	resp.Kvs = make([]*kvpb.KeyValue, len(items))
	for i, it := range items {
		resp.Kvs[i] = it.kv(!req.KeysOnly)
	}
	return resp
}

func (m *Machine) Revision() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rev
}

func (m *Machine) Applied() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.applied
}

// ExpiredLeases lists leases whose deadline is at or before now (leader wall clock, ns).
func (m *Machine) ExpiredLeases(now int64) []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []uint64
	for id, l := range m.leases {
		if l.deadline <= now {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

type LeaseInfo struct {
	TTLNs      int64
	DeadlineNs int64
	Keys       [][]byte
}

func (m *Machine) LeaseInfo(id uint64, withKeys bool) (LeaseInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	l := m.leases[id]
	if l == nil {
		return LeaseInfo{}, false
	}
	info := LeaseInfo{TTLNs: l.ttl, DeadlineNs: l.deadline}
	if withKeys {
		for k := range l.keys {
			info.Keys = append(info.Keys, []byte(k))
		}
		slices.SortFunc(info.Keys, func(a, b []byte) int { return slices.Compare(a, b) })
	}
	return info, true
}

type Stats struct {
	Keys     int
	Leases   int
	Locks    int
	Sessions int
	Watchers int
	Revision uint64
	History  int
}

func (m *Machine) Stats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Stats{
		Keys: m.tree.Len(), Leases: len(m.leases), Locks: len(m.locks),
		Sessions: len(m.sessions), Watchers: m.hub.Count(), Revision: m.rev, History: len(m.hist),
	}
}

type WatchSpec struct {
	Key      []byte
	End      []byte
	StartRev uint64
	PrevKV   bool
}

// Watch registers a watcher. History replay and live registration happen
// under one read lock, so no event can slip between them.
func (m *Machine) Watch(spec WatchSpec, buf int) (*watch.Watcher, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var pre []watch.Batch
	if spec.StartRev > 0 && spec.StartRev <= m.rev {
		if spec.StartRev < m.histBase {
			return nil, m.histBase, ErrCompacted
		}
		from := sort.Search(len(m.hist), func(i int) bool { return m.hist[i].rev >= spec.StartRev })
		for _, h := range m.hist[from:] {
			var evs []*kvpb.Event
			for _, ev := range h.events {
				if keyrange.Contains(spec.Key, spec.End, ev.Kv.Key) {
					evs = append(evs, watch.Strip(ev, spec.PrevKV))
				}
			}
			if len(evs) > 0 {
				pre = append(pre, watch.Batch{Revision: h.rev, Events: evs})
			}
		}
	}
	w := m.hub.Add(spec.Key, spec.End, spec.PrevKV, spec.StartRev, buf, pre)
	return w, 0, nil
}

func (m *Machine) CancelWatcher(w *watch.Watcher) { m.hub.Remove(w, "") }
