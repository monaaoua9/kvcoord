package linearize

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Kind int

const (
	KindPut Kind = iota
	KindGet
	KindDelete
	KindCAS
)

func (k Kind) String() string { return [...]string{"put", "get", "delete", "cas"}[k] }

type KVInput struct {
	Kind         Kind
	Key          string
	Value        string // put/cas: the value to write
	Expect       string // cas
	ExpectAbsent bool   // cas: succeed only if the key does not exist
}

type KVOutput struct {
	Found   bool // get
	Value   string
	Success bool // cas
	Deleted bool // delete: the key existed
}

type kvState struct {
	present bool
	value   string
}

// KVModel is a register per key with get/put/delete/compare-and-swap.
func KVModel() Model {
	return Model{
		Init: func() any { return kvState{} },
		Step: func(s any, op Op) (bool, any) {
			st := s.(kvState)
			in := op.Input.(KVInput)
			switch in.Kind {
			case KindPut:
				return true, kvState{true, in.Value}
			case KindGet:
				if op.pending() {
					return true, st
				}
				out := op.Output.(KVOutput)
				return out.Found == st.present && (!st.present || out.Value == st.value), st
			case KindDelete:
				if op.pending() {
					return true, kvState{}
				}
				return op.Output.(KVOutput).Deleted == st.present, kvState{}
			case KindCAS:
				match := (in.ExpectAbsent && !st.present) || (!in.ExpectAbsent && st.present && st.value == in.Expect)
				next := st
				if match {
					next = kvState{true, in.Value}
				}
				if op.pending() {
					return true, next
				}
				return op.Output.(KVOutput).Success == match, next
			}
			return false, st
		},
		Partition: func(ops []Op) [][]Op {
			byKey := map[string][]Op{}
			for _, o := range ops {
				k := o.Input.(KVInput).Key
				byKey[k] = append(byKey[k], o)
			}
			keys := make([]string, 0, len(byKey))
			for k := range byKey {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := make([][]Op, 0, len(keys))
			for _, k := range keys {
				out = append(out, byKey[k])
			}
			return out
		},
	}
}

// Recorder collects a concurrent history. Timestamps come from one monotonic
// clock: Begin stamps the call before the request leaves, End stamps the
// return after the response arrived.
type Recorder struct {
	start time.Time
	last  atomic.Int64
	mu    sync.Mutex
	ops   []Op
}

func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

// now never returns the same value twice. Coarse OS timers would otherwise give a
// return and the next call the same stamp, and the checker would treat them
// as concurrent and let stale reads through.
func (r *Recorder) now() int64 {
	for {
		t := int64(time.Since(r.start)) + 1
		old := r.last.Load()
		if t <= old {
			t = old + 1
		}
		if r.last.CompareAndSwap(old, t) {
			return t
		}
	}
}

type Handle struct {
	r      *Recorder
	client int
	call   int64
	in     any
}

func (r *Recorder) Begin(client int, in any) *Handle {
	return &Handle{r: r, client: client, call: r.now(), in: in}
}

func (h *Handle) End(out any) {
	op := Op{Client: h.client, Call: h.call, Return: h.r.now(), Input: h.in, Output: out}
	h.r.mu.Lock()
	h.r.ops = append(h.r.ops, op)
	h.r.mu.Unlock()
}

// EndUnknown records an operation whose result we never saw.
func (h *Handle) EndUnknown() {
	op := Op{Client: h.client, Call: h.call, Return: Pending, Input: h.in}
	h.r.mu.Lock()
	h.r.ops = append(h.r.ops, op)
	h.r.mu.Unlock()
}

func (r *Recorder) History() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}
