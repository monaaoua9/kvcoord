// Package linearize checks recorded operation histories for linearizability
// with the Wing & Gong search, memoised as in Lowe's refinement.
package linearize

import (
	"context"
	"math"
	"sort"
	"sync"
)

// Pending marks an operation whose outcome is unknown (timeout, crash). It may
// have taken effect at any point after its call, or never.
const Pending = math.MaxInt64

type Op struct {
	Client int
	Call   int64
	Return int64 // Pending when unknown
	Input  any
	Output any
}

func (o Op) pending() bool { return o.Return == Pending }

// Model describes a sequential specification. States must be comparable.
type Model struct {
	Init func() any
	// Step applies op to state. For pending ops op.Output must be ignored:
	// any output is acceptable, but the state transition is still defined.
	Step func(state any, op Op) (ok bool, next any)
	// Partition splits a history into independent sub-histories (e.g. per key).
	Partition func(ops []Op) [][]Op
}

type Outcome int

const (
	OK Outcome = iota
	Illegal
	Unknown // search budget exhausted
)

func (o Outcome) String() string { return [...]string{"ok", "illegal", "unknown"}[o] }

type Result struct {
	Outcome Outcome
	// Culprit is the sub-history that has no valid linearization.
	Culprit []Op
}

type node struct {
	op         *Op
	idx        int
	isCall     bool
	match      *node
	prev, next *node
}

func Check(ctx context.Context, m Model, history []Op) Result {
	parts := [][]Op{history}
	if m.Partition != nil {
		parts = m.Partition(history)
	}
	results := make([]Result, len(parts))
	var wg sync.WaitGroup
	for i := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = checkOne(ctx, m, parts[i])
		}()
	}
	wg.Wait()

	final := Result{Outcome: OK}
	for _, r := range results {
		switch {
		case r.Outcome == Illegal:
			return r
		case r.Outcome == Unknown:
			final = r
		}
	}
	return final
}

type cacheKey struct {
	bits  string
	state any
}

func checkOne(ctx context.Context, m Model, ops []Op) Result {
	if len(ops) == 0 {
		return Result{Outcome: OK}
	}
	head, completed := buildList(ops)

	words := (len(ops) + 63) / 64
	lin := make([]uint64, words)
	cache := make(map[cacheKey]struct{})
	type frame struct {
		n     *node
		state any
	}
	var stack []frame
	state := m.Init()
	e := head.next
	steps := 0

	bitsKey := func() string {
		b := make([]byte, 0, words*8)
		for _, w := range lin {
			for s := 0; s < 64; s += 8 {
				b = append(b, byte(w>>s))
			}
		}
		return string(b)
	}

	for {
		if completed == 0 {
			return Result{Outcome: OK}
		}
		if steps++; steps&0xfff == 0 && ctx.Err() != nil {
			return Result{Outcome: Unknown, Culprit: ops}
		}

		if e.isCall {
			ok, next := m.Step(state, *e.op)
			if ok {
				lin[e.idx/64] |= 1 << (e.idx % 64)
				k := cacheKey{bitsKey(), next}
				if _, seen := cache[k]; !seen {
					cache[k] = struct{}{}
					stack = append(stack, frame{e, state})
					state = next
					lift(e)
					if !e.op.pending() {
						completed--
					}
					e = head.next
					continue
				}
				lin[e.idx/64] &^= 1 << (e.idx % 64)
			}
			e = e.next
			continue
		}

		// A return whose call is still in the list means that op had to be
		// linearized by now but could not be: undo the last choice. (Pending
		// returns sit at the very end, so we only get here for them when no
		// completed op is left, which the check above already accepted.)
		if len(stack) == 0 {
			return Result{Outcome: Illegal, Culprit: ops}
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = f.state
		lin[f.n.idx/64] &^= 1 << (f.n.idx % 64)
		unlift(f.n)
		if !f.n.op.pending() {
			completed++
		}
		e = f.n.next
	}
}

// buildList turns ops into a time-ordered list of call/return events. Calls
// sort before returns at equal timestamps, which is the permissive reading.
func buildList(ops []Op) (*node, int) {
	type ev struct {
		t      int64
		isCall bool
		idx    int
	}
	evs := make([]ev, 0, 2*len(ops))
	completed := 0
	for i, o := range ops {
		evs = append(evs, ev{o.Call, true, i}, ev{o.Return, false, i})
		if !o.pending() {
			completed++
		}
	}
	sort.SliceStable(evs, func(a, b int) bool {
		if evs[a].t != evs[b].t {
			return evs[a].t < evs[b].t
		}
		return evs[a].isCall && !evs[b].isCall
	})

	head := &node{}
	prev := head
	calls := make(map[int]*node, len(ops))
	for _, v := range evs {
		n := &node{op: &ops[v.idx], idx: v.idx, isCall: v.isCall, prev: prev}
		prev.next = n
		prev = n
		if v.isCall {
			calls[v.idx] = n
		} else {
			n.match = calls[v.idx]
			calls[v.idx].match = n
		}
	}
	return head, completed
}

func lift(call *node) {
	call.prev.next = call.next
	call.next.prev = call.prev
	ret := call.match
	ret.prev.next = ret.next
	if ret.next != nil {
		ret.next.prev = ret.prev
	}
}

func unlift(call *node) {
	ret := call.match
	ret.prev.next = ret
	if ret.next != nil {
		ret.next.prev = ret
	}
	call.prev.next = call
	call.next.prev = call
}
