package fsm

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

const reasonRestored = "SNAPSHOT_RESTORED"

func (m *Machine) Snapshot() ([]byte, error) { return m.SnapshotAsync()() }

// SnapshotAsync captures the state in O(1) on the caller's goroutine (the trees
// are cloned copy-on-write, the small maps are copied) and returns the function
// that does the expensive serialisation. That function may run while Apply keeps
// going: nothing it reads is ever mutated in place.
func (m *Machine) SnapshotAsync() func() ([]byte, error) {
	m.mu.Lock()
	tree, vers := m.tree.Clone(), m.vers.Clone()
	s := &kvpb.FSMSnapshot{Revision: m.rev, Applied: m.applied, ClockNs: m.clock, CompactRev: m.compactRev}

	for _, l := range m.leases {
		s.Leases = append(s.Leases, &kvpb.SnapLease{Id: l.id, TtlNs: l.ttl, DeadlineNs: l.deadline})
	}
	slices.SortFunc(s.Leases, func(a, b *kvpb.SnapLease) int { return cmpU64(a.Id, b.Id) })

	for name, ls := range m.locks {
		s.Locks = append(s.Locks, &kvpb.SnapLock{
			Name: []byte(name), Owner: ls.owner, Token: ls.token, Waiters: slices.Clone(ls.waiters),
		})
	}
	slices.SortFunc(s.Locks, func(a, b *kvpb.SnapLock) int { return strings.Compare(string(a.Name), string(b.Name)) })

	for id, ss := range m.sessions {
		s.Sessions = append(s.Sessions, &kvpb.SnapSession{
			ClientId: id, LastActiveNs: ss.lastActive, AckSeq: ss.ackSeq, Results: maps.Clone(ss.results),
		})
	}
	slices.SortFunc(s.Sessions, func(a, b *kvpb.SnapSession) int { return cmpU64(a.ClientId, b.ClientId) })
	m.mu.Unlock()

	return func() ([]byte, error) {
		tree.Ascend(func(it *item) bool {
			s.Kvs = append(s.Kvs, it.kv(true))
			return true
		})
		// the newest version of a live key is the tree item itself, only older ones are stored
		vers.Ascend(func(kh *keyHist) bool {
			vs := kh.vers
			if n := len(vs); n > 0 && !vs[n-1].tomb {
				vs = vs[:n-1]
			}
			if len(vs) == 0 {
				return true
			}
			h := &kvpb.SnapKeyHistory{Key: []byte(kh.key)}
			for _, v := range vs {
				h.Versions = append(h.Versions, &kvpb.SnapVersion{
					Rev: v.rev, CreateRev: v.createRev, Version: v.ver, Lease: v.lease, Value: v.val, Tombstone: v.tomb,
				})
			}
			s.History = append(s.History, h)
			return true
		})
		return proto.MarshalOptions{Deterministic: true}.Marshal(s)
	}
}

func cmpU64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (m *Machine) Restore(data []byte) error {
	var s kvpb.FSMSnapshot
	if err := proto.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("fsm: bad snapshot: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for k, subs := range m.lockSubs {
		for _, c := range subs {
			c <- LockEvent{Err: kvpb.ErrUnavailable}
		}
		delete(m.lockSubs, k)
	}
	m.hub.CancelAll(reasonRestored)
	m.reset()

	m.rev, m.applied, m.clock = s.Revision, s.Applied, s.ClockNs
	m.histBase = m.rev + 1
	for _, l := range s.Leases {
		m.leases[l.Id] = &lease{
			id: l.Id, ttl: l.TtlNs, deadline: l.DeadlineNs,
			keys: make(map[string]struct{}), locks: make(map[string]struct{}),
		}
	}
	m.compactRev = s.CompactRev
	for _, h := range s.History {
		kh := &keyHist{key: string(h.Key)}
		for _, v := range h.Versions {
			kh.vers = append(kh.vers, version{
				rev: v.Rev, createRev: v.CreateRev, ver: v.Version, lease: v.Lease, val: v.Value, tomb: v.Tombstone,
			})
		}
		m.vers.ReplaceOrInsert(kh)
	}
	for _, kv := range s.Kvs {
		it := &item{
			key: string(kv.Key), val: kv.Value, createRev: kv.CreateRevision,
			modRev: kv.ModRevision, version: kv.Version, lease: kv.Lease,
		}
		m.tree.ReplaceOrInsert(it)
		m.recordPut(it)
		if l := m.leases[it.lease]; l != nil {
			l.keys[it.key] = struct{}{}
		}
	}
	for _, sl := range s.Locks {
		name := string(sl.Name)
		m.locks[name] = &lockState{owner: sl.Owner, token: sl.Token, waiters: slices.Clone(sl.Waiters)}
		for _, id := range append([]uint64{sl.Owner}, sl.Waiters...) {
			if l := m.leases[id]; l != nil {
				l.locks[name] = struct{}{}
			}
		}
	}
	for _, ss := range s.Sessions {
		sess := &session{lastActive: ss.LastActiveNs, ackSeq: ss.AckSeq, results: ss.Results}
		if sess.results == nil {
			sess.results = make(map[uint64]*kvpb.CommandResult)
		}
		m.sessions[ss.ClientId] = sess
	}
	return nil
}
