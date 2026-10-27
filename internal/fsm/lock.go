package fsm

import (
	"slices"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

type lockState struct {
	owner   uint64 // lease id, 0 when free
	token   uint64
	waiters []uint64
}

type lockKey struct {
	name  string
	lease uint64
}

// LockEvent tells a blocked Lock call how its wait ended.
type LockEvent struct {
	Token uint64
	Err   string // empty on grant
}

func (m *Machine) lockAcquire(req *kvpb.LockRequest) *kvpb.CommandResult {
	if len(req.Name) == 0 {
		return errResult(kvpb.ErrInvalidArgument, "empty lock name")
	}
	l := m.leases[req.Lease]
	if l == nil {
		return errResult(kvpb.ErrLeaseNotFound, "lease not found")
	}
	name := string(req.Name)
	ls := m.locks[name]
	if ls == nil {
		ls = &lockState{}
		m.locks[name] = ls
	}

	res := &kvpb.LockResult{}
	switch {
	case ls.owner == 0:
		ls.owner, ls.token = req.Lease, m.applied
		l.locks[name] = struct{}{}
		res.Acquired, res.FencingToken = true, ls.token
	case ls.owner == req.Lease:
		res.Acquired, res.FencingToken = true, ls.token
	case req.Try:
		// leave no trace
	default:
		pos := slices.Index(ls.waiters, req.Lease)
		if pos < 0 {
			ls.waiters = append(ls.waiters, req.Lease)
			pos = len(ls.waiters) - 1
			l.locks[name] = struct{}{}
		}
		res.QueuePos = uint64(pos + 1)
	}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_Lock{Lock: res}}
}

func (m *Machine) lockRelease(req *kvpb.UnlockRequest) *kvpb.CommandResult {
	name := string(req.Name)
	if ls := m.locks[name]; ls != nil {
		switch {
		case ls.owner == req.Lease:
			m.forget(req.Lease, name)
			m.promote(name, ls)
		default:
			if i := slices.Index(ls.waiters, req.Lease); i >= 0 {
				ls.waiters = slices.Delete(ls.waiters, i, i+1)
				m.forget(req.Lease, name)
			}
		}
	}
	return &kvpb.CommandResult{Body: &kvpb.CommandResult_Unlock{Unlock: &kvpb.UnlockResponse{}}}
}

func (m *Machine) forget(leaseID uint64, name string) {
	if l := m.leases[leaseID]; l != nil {
		delete(l.locks, name)
	}
}

// promote hands the lock to the next waiter, or frees it.
func (m *Machine) promote(name string, ls *lockState) {
	if len(ls.waiters) == 0 {
		delete(m.locks, name)
		return
	}
	ls.owner = ls.waiters[0]
	ls.waiters = ls.waiters[1:]
	ls.token = m.applied
	m.notifyLock(name, ls.owner, LockEvent{Token: ls.token})
}

// lockDropLease is called while a lease is being destroyed.
func (m *Machine) lockDropLease(name string, leaseID uint64) {
	ls := m.locks[name]
	if ls == nil {
		return
	}
	m.notifyLock(name, leaseID, LockEvent{Err: kvpb.ErrLeaseExpired})
	if ls.owner == leaseID {
		m.promote(name, ls)
		return
	}
	if i := slices.Index(ls.waiters, leaseID); i >= 0 {
		ls.waiters = slices.Delete(ls.waiters, i, i+1)
	}
}

func (m *Machine) notifyLock(name string, leaseID uint64, ev LockEvent) {
	k := lockKey{name, leaseID}
	subs := m.lockSubs[k]
	if len(subs) == 0 {
		return
	}
	delete(m.lockSubs, k)
	for _, c := range subs {
		c <- ev // buffered with room for exactly one event
	}
}

// SubscribeLock must be called before proposing the Lock command, otherwise a
// grant that lands right after the proposal could be missed.
func (m *Machine) SubscribeLock(name string, leaseID uint64) (<-chan LockEvent, func()) {
	c := make(chan LockEvent, 1)
	k := lockKey{name, leaseID}
	m.mu.Lock()
	m.lockSubs[k] = append(m.lockSubs[k], c)
	m.mu.Unlock()
	return c, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		subs := m.lockSubs[k]
		for i, s := range subs {
			if s == c {
				subs = slices.Delete(subs, i, i+1)
				break
			}
		}
		if len(subs) == 0 {
			delete(m.lockSubs, k)
		} else {
			m.lockSubs[k] = subs
		}
	}
}
