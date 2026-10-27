// Package watch fans committed key events out to subscribers.
package watch

import (
	"sync"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/keyrange"
)

type Batch struct {
	Revision uint64
	Events   []*kvpb.Event
}

type Watcher struct {
	ID     uint64
	Key    []byte
	End    []byte
	PrevKV bool
	MinRev uint64
	C      chan Batch

	reason string // guarded by hub.mu until C is closed
	hub    *Hub
}

// Reason is only meaningful once C has been closed.
func (w *Watcher) Reason() string {
	w.hub.mu.Lock()
	defer w.hub.mu.Unlock()
	return w.reason
}

type Hub struct {
	mu     sync.Mutex
	nextID uint64
	exact  map[string]map[uint64]*Watcher
	ranges map[uint64]*Watcher
}

func NewHub() *Hub {
	return &Hub{exact: make(map[string]map[uint64]*Watcher), ranges: make(map[uint64]*Watcher)}
}

// Add registers a watcher whose channel can hold buf batches. Callers that
// want to preload history can fill w.C before it becomes visible by passing
// preload.
func (h *Hub) Add(key, end []byte, prevKV bool, minRev uint64, buf int, preload []Batch) *Watcher {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	w := &Watcher{
		ID: h.nextID, Key: key, End: end, PrevKV: prevKV, MinRev: minRev,
		C: make(chan Batch, buf+len(preload)), hub: h,
	}
	for _, b := range preload {
		w.C <- b
	}
	if len(end) == 0 {
		set := h.exact[string(key)]
		if set == nil {
			set = make(map[uint64]*Watcher)
			h.exact[string(key)] = set
		}
		set[w.ID] = w
	} else {
		h.ranges[w.ID] = w
	}
	return w
}

func (h *Hub) Remove(w *Watcher, reason string) {
	h.mu.Lock()
	h.removeLocked(w, reason)
	h.mu.Unlock()
}

func (h *Hub) removeLocked(w *Watcher, reason string) {
	if len(w.End) == 0 {
		set := h.exact[string(w.Key)]
		if _, ok := set[w.ID]; !ok {
			return
		}
		delete(set, w.ID)
		if len(set) == 0 {
			delete(h.exact, string(w.Key))
		}
	} else {
		if _, ok := h.ranges[w.ID]; !ok {
			return
		}
		delete(h.ranges, w.ID)
	}
	w.reason = reason
	close(w.C)
}

func (h *Hub) CancelAll(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, set := range h.exact {
		for _, w := range set {
			h.removeLocked(w, reason)
		}
	}
	for _, w := range h.ranges {
		h.removeLocked(w, reason)
	}
}

func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.ranges)
	for _, s := range h.exact {
		n += len(s)
	}
	return n
}

// Publish never blocks: a watcher that cannot keep up is cancelled and has to
// resume from the last revision it saw.
func (h *Hub) Publish(rev uint64, events []*kvpb.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.exact) == 0 && len(h.ranges) == 0 {
		return
	}

	per := make(map[*Watcher][]*kvpb.Event)
	for _, ev := range events {
		k := ev.Kv.Key
		for _, w := range h.exact[string(k)] {
			per[w] = append(per[w], Strip(ev, w.PrevKV))
		}
		for _, w := range h.ranges {
			if keyrange.Contains(w.Key, w.End, k) {
				per[w] = append(per[w], Strip(ev, w.PrevKV))
			}
		}
	}
	for w, evs := range per {
		if rev < w.MinRev {
			continue
		}
		select {
		case w.C <- Batch{Revision: rev, Events: evs}:
		default:
			h.removeLocked(w, kvpb.ErrWatchOverflow)
		}
	}
}

// Strip drops the previous value unless the watcher asked for it.
func Strip(ev *kvpb.Event, prevKV bool) *kvpb.Event {
	if prevKV || ev.PrevKv == nil {
		return ev
	}
	return &kvpb.Event{Type: ev.Type, Kv: ev.Kv}
}
