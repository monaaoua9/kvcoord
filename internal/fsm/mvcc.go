package fsm

import (
	"bytes"
	"errors"
	"sort"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/keyrange"
)

var ErrFutureRev = errors.New("fsm: requested revision is in the future")

// version is one committed state of a key. A tombstone marks a delete.
type version struct {
	rev       uint64
	createRev uint64
	ver       uint64
	lease     uint64
	val       []byte
	tomb      bool
}

type keyHist struct {
	key  string
	vers []version // ascending by rev
}

// record never edits a keyHist that is already in the tree: a snapshot may be
// reading it through a cloned tree. The old slice is only appended to, which the
// old header does not see.
func (m *Machine) record(key string, v version) {
	nk := &keyHist{key: key}
	if old, ok := m.vers.Get(&keyHist{key: key}); ok {
		nk.vers = old.vers
	}
	nk.vers = append(nk.vers, v)
	m.vers.ReplaceOrInsert(nk)
}

func (m *Machine) recordPut(it *item) {
	m.record(it.key, version{rev: it.modRev, createRev: it.createRev, ver: it.version, lease: it.lease, val: it.val})
}

// at returns the state of the key as of rev, nil if it did not exist then.
func (kh *keyHist) at(rev uint64) *version {
	i := sort.Search(len(kh.vers), func(i int) bool { return kh.vers[i].rev > rev }) - 1
	if i < 0 || kh.vers[i].tomb {
		return nil
	}
	return &kh.vers[i]
}

func (v *version) kv(key string, withValue bool) *kvpb.KeyValue {
	kv := &kvpb.KeyValue{
		Key: []byte(key), CreateRevision: v.createRev, ModRevision: v.rev, Version: v.ver, Lease: v.lease,
	}
	if withValue {
		kv.Value = v.val
	}
	return kv
}

func (m *Machine) rangeAt(req *kvpb.RangeRequest, rev uint64) *kvpb.RangeResponse {
	resp := &kvpb.RangeResponse{}
	limit := int(req.Limit)
	var out []*kvpb.KeyValue
	var count uint64

	visit := func(kh *keyHist) bool {
		v := kh.at(rev)
		if v == nil {
			return true
		}
		count++
		if req.CountOnly {
			return true
		}
		if limit > 0 && len(out) == limit {
			resp.More = true
			return false
		}
		out = append(out, v.kv(kh.key, !req.KeysOnly))
		return true
	}

	if len(req.RangeEnd) == 0 {
		if kh, ok := m.vers.Get(&keyHist{key: string(req.Key)}); ok {
			visit(kh)
		}
	} else {
		all := keyrange.IsAll(req.RangeEnd)
		m.vers.AscendGreaterOrEqual(&keyHist{key: string(req.Key)}, func(kh *keyHist) bool {
			if !all && bytes.Compare([]byte(kh.key), req.RangeEnd) >= 0 {
				return false
			}
			return visit(kh)
		})
	}
	resp.Kvs = out
	resp.Count = count
	if !req.CountOnly {
		resp.Count = uint64(len(out))
	}
	return resp
}

// applyCompact is deterministic and consumes no revision. Compacting to a
// revision that is already compacted is a no-op so retries are harmless.
func (m *Machine) applyCompact(req *kvpb.CompactRequest) *kvpb.CommandResult {
	if req.Revision > m.rev {
		return errResult(kvpb.ErrFutureRevision, "revision is in the future")
	}
	if req.Revision > m.compactRev {
		m.compact(req.Revision)
	}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_Compact{Compact: &kvpb.CompactResponse{}}}
}

// compact keeps, per key, the newest version at or below rev (unless it is a
// tombstone) and everything after. It walks every key, so on a huge store it
// holds the apply loop for a moment.
func (m *Machine) compact(rev uint64) {
	var changed []*keyHist // replacements, the originals may be shared with a snapshot
	var empty []*keyHist
	m.vers.Ascend(func(kh *keyHist) bool {
		i := sort.Search(len(kh.vers), func(i int) bool { return kh.vers[i].rev > rev }) - 1
		if i < 0 {
			return true
		}
		if kh.vers[i].tomb {
			i++
		}
		if i > 0 {
			nk := &keyHist{key: kh.key, vers: append([]version(nil), kh.vers[i:]...)}
			if len(nk.vers) == 0 {
				empty = append(empty, kh)
			} else {
				changed = append(changed, nk)
			}
		}
		return true
	})
	for _, nk := range changed {
		m.vers.ReplaceOrInsert(nk)
	}
	for _, kh := range empty {
		m.vers.Delete(kh)
	}
	m.compactRev = rev
}

// CompactRevision is the oldest revision reads can still be served at.
func (m *Machine) CompactRevision() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.compactRev
}
