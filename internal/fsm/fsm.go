// Package fsm is the replicated state machine on top of raft: an ordered KV
// store with revisions, leases, fair locks, client sessions for exactly-once
// writes, and an event history for watches.
//
// Apply is the only writer. Every decision that could differ between replicas
// (time, ids, expiry) is derived from the log entry itself.
package fsm

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/keyrange"
	"github.com/monaaoua9/kvcoord/internal/raft"
	"github.com/monaaoua9/kvcoord/internal/watch"
)

type Options struct {
	// HistoryLimit is how many revisions of events are kept for watch resume.
	HistoryLimit int
}

type item struct {
	key       string
	val       []byte
	createRev uint64
	modRev    uint64
	version   uint64
	lease     uint64
}

func (it *item) kv(withValue bool) *kvpb.KeyValue {
	kv := &kvpb.KeyValue{
		Key:            []byte(it.key),
		CreateRevision: it.createRev,
		ModRevision:    it.modRev,
		Version:        it.version,
		Lease:          it.lease,
	}
	if withValue {
		kv.Value = it.val
	}
	return kv
}

type histEntry struct {
	rev    uint64
	events []*kvpb.Event
}

type Machine struct {
	opts Options
	hub  *watch.Hub

	mu          sync.RWMutex
	tree        *btree.BTreeG[*item]
	vers        *btree.BTreeG[*keyHist]
	compactRev  uint64 // reads below this revision are refused
	sweeping    bool   // a compaction is still being applied to the history
	sweepCursor string // next key the sweep looks at
	rev         uint64
	applied     uint64
	clock       int64
	leases      map[uint64]*lease
	locks       map[string]*lockState
	sessions    map[uint64]*session
	hist        []histEntry
	histBase    uint64 // oldest revision still replayable

	lockSubs map[lockKey][]chan LockEvent

	// per-command scratch, only touched inside Apply
	revBumped bool
	pending   []*kvpb.Event
}

var _ raft.StateMachine = (*Machine)(nil)

func New(opts Options) *Machine {
	if opts.HistoryLimit <= 0 {
		opts.HistoryLimit = 100_000
	}
	m := &Machine{opts: opts, hub: watch.NewHub()}
	m.reset()
	return m
}

func (m *Machine) reset() {
	m.tree = btree.NewG(32, func(a, b *item) bool { return a.key < b.key })
	m.vers = btree.NewG(32, func(a, b *keyHist) bool { return a.key < b.key })
	m.compactRev = 0
	m.sweeping, m.sweepCursor = false, ""
	m.sweeping, m.sweepCursor = false, ""
	m.rev, m.applied, m.clock = 0, 0, 0
	m.leases = make(map[uint64]*lease)
	m.locks = make(map[string]*lockState)
	m.sessions = make(map[uint64]*session)
	m.hist = nil
	m.histBase = 1
	m.lockSubs = make(map[lockKey][]chan LockEvent)
}

func errResult(code, msg string) *kvpb.CommandResult {
	return &kvpb.CommandResult{ErrorCode: code, ErrorMessage: msg}
}

func (m *Machine) Apply(e raft.Entry) any {
	var cmd kvpb.Command
	if err := proto.Unmarshal(e.Data, &cmd); err != nil {
		// can't happen unless the log is corrupt; keep replicas in lockstep anyway
		return errResult(kvpb.ErrInternal, "undecodable command")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = e.Index
	if cmd.TimestampNs > m.clock {
		m.clock = cmd.TimestampNs
	}
	m.revBumped = false
	m.pending = m.pending[:0]

	var res *kvpb.CommandResult
	switch b := cmd.Body.(type) {
	case *kvpb.Command_OpenSession:
		res = m.openSession()
	case *kvpb.Command_Put:
		res = m.withSession(b.Put.Meta, func() *kvpb.CommandResult { return m.applyPut(b.Put) })
	case *kvpb.Command_DeleteRange:
		res = m.withSession(b.DeleteRange.Meta, func() *kvpb.CommandResult { return m.applyDelete(b.DeleteRange) })
	case *kvpb.Command_Txn:
		res = m.withSession(b.Txn.Meta, func() *kvpb.CommandResult { return m.applyTxn(b.Txn) })
	case *kvpb.Command_LeaseGrant:
		res = m.withSession(b.LeaseGrant.Meta, func() *kvpb.CommandResult { return m.leaseGrant(b.LeaseGrant) })
	case *kvpb.Command_LeaseRevoke:
		res = m.leaseRevoke(b.LeaseRevoke.Id)
	case *kvpb.Command_LeaseKeepAlive:
		res = m.leaseKeepAlive(b.LeaseKeepAlive.Id)
	case *kvpb.Command_LeaseExpire:
		res = m.leaseExpire(b.LeaseExpire.Ids, cmd.TimestampNs)
	case *kvpb.Command_RefreshLeases:
		res = m.leaseRefreshAll()
	case *kvpb.Command_Lock:
		res = m.lockAcquire(b.Lock)
	case *kvpb.Command_Unlock:
		res = m.lockRelease(b.Unlock)
	case *kvpb.Command_SessionExpire:
		res = m.sessionExpire(b.SessionExpire.IdleMs)
	case *kvpb.Command_Compact:
		res = m.applyCompact(b.Compact)
	default:
		res = errResult(kvpb.ErrInvalidArgument, "empty command")
	}

	m.compactStep()
	m.flushEvents()
	return res
}

// curRev lazily allocates the revision of this command: commands that change
// nothing must not consume one, otherwise watch history would have holes.
func (m *Machine) curRev() uint64 {
	if !m.revBumped {
		m.rev++
		m.revBumped = true
	}
	return m.rev
}

func (m *Machine) flushEvents() {
	if len(m.pending) == 0 {
		return
	}
	evs := append([]*kvpb.Event(nil), m.pending...)
	m.hist = append(m.hist, histEntry{rev: m.rev, events: evs})
	if over := len(m.hist) - m.opts.HistoryLimit; over > 0 {
		m.hist = append([]histEntry(nil), m.hist[over:]...)
		m.histBase = m.hist[0].rev
	}
	m.hub.Publish(m.rev, evs)
}

// ---- sessions (exactly-once for non-idempotent writes) ----

type session struct {
	lastActive int64
	ackSeq     uint64
	results    map[uint64]*kvpb.CommandResult
}

func (m *Machine) openSession() *kvpb.CommandResult {
	id := m.applied
	m.sessions[id] = &session{lastActive: m.clock, results: make(map[uint64]*kvpb.CommandResult)}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_OpenSession{
		OpenSession: &kvpb.OpenSessionResponse{ClientId: id},
	}}
}

func (m *Machine) withSession(meta *kvpb.RequestMeta, fn func() *kvpb.CommandResult) *kvpb.CommandResult {
	if meta == nil || meta.ClientId == 0 || meta.Seq == 0 {
		return errResult(kvpb.ErrInvalidArgument, "missing request meta")
	}
	s := m.sessions[meta.ClientId]
	if s == nil {
		return errResult(kvpb.ErrSessionExpired, "unknown or expired client session")
	}
	s.lastActive = m.clock
	if meta.AckSeq > s.ackSeq {
		for seq := range s.results {
			if seq <= meta.AckSeq {
				delete(s.results, seq)
			}
		}
		s.ackSeq = meta.AckSeq
	}
	if r, ok := s.results[meta.Seq]; ok {
		return r
	}
	if meta.Seq <= s.ackSeq {
		return errResult(kvpb.ErrStaleRequest, "request already acknowledged")
	}
	r := fn()
	s.results[meta.Seq] = r
	return r
}

func (m *Machine) sessionExpire(idleMs uint64) *kvpb.CommandResult {
	idle := int64(idleMs) * 1_000_000
	for id, s := range m.sessions {
		if m.clock-s.lastActive > idle {
			delete(m.sessions, id)
		}
	}
	return &kvpb.CommandResult{}
}

// ---- kv writes ----

func (m *Machine) get(key string) *item {
	it, _ := m.tree.Get(&item{key: key})
	return it
}

func (m *Machine) applyPut(req *kvpb.PutRequest) *kvpb.CommandResult {
	if len(req.Key) == 0 {
		return errResult(kvpb.ErrInvalidArgument, "empty key")
	}
	if req.Lease != 0 && m.leases[req.Lease] == nil {
		return errResult(kvpb.ErrLeaseNotFound, "lease not found")
	}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_Put{Put: m.put(req)}}
}

func (m *Machine) put(req *kvpb.PutRequest) *kvpb.PutResponse {
	rev := m.curRev()
	key := string(req.Key)
	old := m.get(key)

	it := &item{key: key, val: req.Value, modRev: rev, lease: req.Lease}
	var prev *kvpb.KeyValue
	if old == nil {
		it.createRev, it.version = rev, 1
	} else {
		it.createRev, it.version = old.createRev, old.version+1
		prev = old.kv(true)
		if old.lease != 0 {
			m.detach(old.lease, key)
		}
	}
	m.tree.ReplaceOrInsert(it)
	m.recordPut(it)
	if it.lease != 0 {
		m.leases[it.lease].keys[key] = struct{}{}
	}
	m.pending = append(m.pending, &kvpb.Event{Type: kvpb.Event_PUT, Kv: it.kv(true), PrevKv: prev})

	resp := &kvpb.PutResponse{}
	if req.PrevKv {
		resp.PrevKv = prev
	}
	return resp
}

func (m *Machine) applyDelete(req *kvpb.DeleteRangeRequest) *kvpb.CommandResult {
	if len(req.Key) == 0 {
		return errResult(kvpb.ErrInvalidArgument, "empty key")
	}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_DeleteRange{DeleteRange: m.deleteRange(req)}}
}

func (m *Machine) deleteRange(req *kvpb.DeleteRangeRequest) *kvpb.DeleteRangeResponse {
	victims := m.collect(req.Key, req.RangeEnd, 0)
	resp := &kvpb.DeleteRangeResponse{Deleted: uint64(len(victims))}
	if len(victims) == 0 {
		return resp
	}
	rev := m.curRev()
	for _, it := range victims {
		m.removeItem(it, rev)
		if req.PrevKv {
			resp.PrevKvs = append(resp.PrevKvs, it.kv(true))
		}
	}
	return resp
}

func (m *Machine) removeItem(it *item, rev uint64) {
	m.tree.Delete(it)
	m.record(it.key, version{rev: rev, tomb: true})
	if it.lease != 0 {
		m.detach(it.lease, it.key)
	}
	m.pending = append(m.pending, &kvpb.Event{
		Type:   kvpb.Event_DELETE,
		Kv:     &kvpb.KeyValue{Key: []byte(it.key), ModRevision: rev},
		PrevKv: it.kv(true),
	})
}

func (m *Machine) detach(leaseID uint64, key string) {
	if l := m.leases[leaseID]; l != nil {
		delete(l.keys, key)
	}
}

// collect gathers items in range; limit 0 means no limit.
func (m *Machine) collect(key, end []byte, limit int) []*item {
	var out []*item
	if len(end) == 0 {
		if it := m.get(string(key)); it != nil {
			out = append(out, it)
		}
		return out
	}
	all := keyrange.IsAll(end)
	m.tree.AscendGreaterOrEqual(&item{key: string(key)}, func(it *item) bool {
		if !all && bytes.Compare([]byte(it.key), end) >= 0 {
			return false
		}
		out = append(out, it)
		return limit == 0 || len(out) < limit
	})
	return out
}

// ---- txn ----

func (m *Machine) applyTxn(req *kvpb.TxnRequest) *kvpb.CommandResult {
	ok := true
	for _, c := range req.Compare {
		if !m.compare(c) {
			ok = false
			break
		}
	}
	ops := req.Success
	if !ok {
		ops = req.Failure
	}
	for _, op := range ops {
		if p := op.GetPut(); p != nil {
			if len(p.Key) == 0 {
				return errResult(kvpb.ErrInvalidArgument, "empty key")
			}
			if p.Lease != 0 && m.leases[p.Lease] == nil {
				return errResult(kvpb.ErrLeaseNotFound, "lease not found")
			}
		}
		if d := op.GetDeleteRange(); d != nil && len(d.Key) == 0 {
			return errResult(kvpb.ErrInvalidArgument, "empty key")
		}
		if r := op.GetRange(); r != nil && r.Revision != 0 {
			return errResult(kvpb.ErrInvalidArgument, "revision reads are not allowed inside a txn")
		}
	}

	resp := &kvpb.TxnResponse{Succeeded: ok}
	for _, op := range ops {
		switch r := op.Request.(type) {
		case *kvpb.RequestOp_Range:
			resp.Responses = append(resp.Responses, &kvpb.ResponseOp{
				Response: &kvpb.ResponseOp_Range{Range: m.latest(r.Range)},
			})
		case *kvpb.RequestOp_Put:
			resp.Responses = append(resp.Responses, &kvpb.ResponseOp{
				Response: &kvpb.ResponseOp_Put{Put: m.put(r.Put)},
			})
		case *kvpb.RequestOp_DeleteRange:
			resp.Responses = append(resp.Responses, &kvpb.ResponseOp{
				Response: &kvpb.ResponseOp_DeleteRange{DeleteRange: m.deleteRange(r.DeleteRange)},
			})
		}
	}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_Txn{Txn: resp}}
}

func (m *Machine) compare(c *kvpb.Compare) bool {
	it := m.get(string(c.Key))
	var cmp int
	switch c.Target {
	case kvpb.Compare_VALUE:
		var cur []byte
		if it != nil {
			cur = it.val
		}
		cmp = bytes.Compare(cur, c.GetValue())
	default:
		var cur, want uint64
		switch c.Target {
		case kvpb.Compare_VERSION:
			if it != nil {
				cur = it.version
			}
			want = c.GetVersion()
		case kvpb.Compare_CREATE:
			if it != nil {
				cur = it.createRev
			}
			want = c.GetCreateRevision()
		case kvpb.Compare_MOD:
			if it != nil {
				cur = it.modRev
			}
			want = c.GetModRevision()
		case kvpb.Compare_LEASE:
			if it != nil {
				cur = it.lease
			}
			want = c.GetLease()
		}
		switch {
		case cur < want:
			cmp = -1
		case cur > want:
			cmp = 1
		}
	}
	switch c.Result {
	case kvpb.Compare_EQUAL:
		return cmp == 0
	case kvpb.Compare_NOT_EQUAL:
		return cmp != 0
	case kvpb.Compare_GREATER:
		return cmp > 0
	case kvpb.Compare_LESS:
		return cmp < 0
	}
	return false
}
