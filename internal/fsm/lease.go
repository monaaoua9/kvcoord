package fsm

import (
	"slices"
	"strings"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

type lease struct {
	id       uint64
	ttl      int64 // ns
	deadline int64 // ns, in the leader-supplied FSM clock
	keys     map[string]struct{}
	locks    map[string]struct{} // locks owned or queued on
}

func (m *Machine) leaseGrant(req *kvpb.LeaseGrantRequest) *kvpb.CommandResult {
	if req.TtlMs == 0 {
		return errResult(kvpb.ErrInvalidArgument, "ttl must be positive")
	}
	l := &lease{
		id:    m.applied,
		ttl:   int64(req.TtlMs) * 1_000_000,
		keys:  make(map[string]struct{}),
		locks: make(map[string]struct{}),
	}
	l.deadline = m.clock + l.ttl
	m.leases[l.id] = l
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_LeaseGrant{
		LeaseGrant: &kvpb.LeaseGrantResponse{Id: l.id, TtlMs: req.TtlMs},
	}}
}

func (m *Machine) leaseKeepAlive(id uint64) *kvpb.CommandResult {
	l := m.leases[id]
	resp := &kvpb.LeaseKeepAliveResponse{Id: id}
	if l != nil {
		l.deadline = m.clock + l.ttl
		resp.TtlMs = uint64(l.ttl / 1_000_000)
	}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_LeaseKeepAlive{LeaseKeepAlive: resp}}
}

func (m *Machine) leaseRevoke(id uint64) *kvpb.CommandResult {
	if m.leases[id] == nil {
		return errResult(kvpb.ErrLeaseNotFound, "lease not found")
	}
	m.dropLease(id)
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_LeaseRevoke{LeaseRevoke: &kvpb.LeaseRevokeResponse{}}}
}

// leaseExpire only honours ids whose deadline has really passed as of the
// leader's timestamp, so a stale proposal cannot kill a freshly renewed lease.
func (m *Machine) leaseExpire(ids []uint64, now int64) *kvpb.CommandResult {
	for _, id := range ids {
		if l := m.leases[id]; l != nil && l.deadline <= now {
			m.dropLease(id)
		}
	}
	return &kvpb.CommandResult{}
}

// leaseRefreshAll runs when a new leader takes over: clients could not renew
// during the election, so everybody gets a fresh full TTL.
func (m *Machine) leaseRefreshAll() *kvpb.CommandResult {
	for _, l := range m.leases {
		l.deadline = m.clock + l.ttl
	}
	return &kvpb.CommandResult{}
}

func (m *Machine) dropLease(id uint64) {
	l := m.leases[id]
	delete(m.leases, id)

	var doomed []*item
	for k := range l.keys {
		if it := m.get(k); it != nil {
			doomed = append(doomed, it)
		}
	}
	if len(doomed) > 0 {
		sortItems(doomed)
		rev := m.curRev()
		for _, it := range doomed {
			m.removeItem(it, rev)
		}
	}
	for name := range l.locks {
		m.lockDropLease(name, id)
	}
}

func sortItems(s []*item) {
	// map iteration order differs per replica, events must not
	slices.SortFunc(s, func(a, b *item) int { return strings.Compare(a.key, b.key) })
}
