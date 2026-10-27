package raft

import "sync"

type waitResult struct {
	val any
	err error
}

type waiter struct {
	term uint64
	ch   chan waitResult
}

// waitMap connects a proposal (registered by the event loop) with its
// application (resolved by the apply goroutine).
type waitMap struct {
	mu sync.Mutex
	m  map[uint64]*waiter
}

func (w *waitMap) add(index, term uint64) *waiter {
	wt := &waiter{term: term, ch: make(chan waitResult, 1)}
	w.mu.Lock()
	if w.m == nil {
		w.m = make(map[uint64]*waiter)
	}
	w.m[index] = wt
	w.mu.Unlock()
	return wt
}

func (w *waitMap) resolve(index, term uint64, val any) {
	w.mu.Lock()
	wt := w.m[index]
	delete(w.m, index)
	w.mu.Unlock()
	if wt == nil {
		return
	}
	if wt.term != term {
		wt.ch <- waitResult{err: ErrProposalDropped}
		return
	}
	wt.ch <- waitResult{val: val}
}

func (w *waitMap) cancel(index uint64) {
	w.mu.Lock()
	delete(w.m, index)
	w.mu.Unlock()
}

func (w *waitMap) failAll(err error) {
	w.mu.Lock()
	old := w.m
	w.m = nil
	w.mu.Unlock()
	for _, wt := range old {
		wt.ch <- waitResult{err: err}
	}
}

// appliedWaiters lets readers block until the FSM reaches an index.
type appliedWaiters struct {
	mu   sync.Mutex
	cur  uint64
	subs []appliedSub
}

type appliedSub struct {
	idx uint64
	ch  chan struct{}
}

func (a *appliedWaiters) set(idx uint64) {
	a.mu.Lock()
	a.cur = idx
	keep := a.subs[:0]
	for _, s := range a.subs {
		if s.idx <= idx {
			close(s.ch)
		} else {
			keep = append(keep, s)
		}
	}
	a.subs = keep
	a.mu.Unlock()
}

func (a *appliedWaiters) wait(idx uint64) <-chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	ch := make(chan struct{})
	if a.cur >= idx {
		close(ch)
		return ch
	}
	a.subs = append(a.subs, appliedSub{idx, ch})
	return ch
}

func (a *appliedWaiters) get() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cur
}
