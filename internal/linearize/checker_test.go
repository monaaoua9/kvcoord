package linearize

import (
	"context"
	"testing"
	"time"
)

func put(c int, key, v string, call, ret int64) Op {
	return Op{Client: c, Call: call, Return: ret, Input: KVInput{Kind: KindPut, Key: key, Value: v}, Output: KVOutput{}}
}

func get(c int, key string, found bool, v string, call, ret int64) Op {
	return Op{Client: c, Call: call, Return: ret, Input: KVInput{Kind: KindGet, Key: key}, Output: KVOutput{Found: found, Value: v}}
}

func cas(c int, key, expect, v string, ok bool, call, ret int64) Op {
	return Op{Client: c, Call: call, Return: ret, Input: KVInput{Kind: KindCAS, Key: key, Expect: expect, Value: v}, Output: KVOutput{Success: ok}}
}

func check(ops []Op) Outcome {
	return Check(context.Background(), KVModel(), ops).Outcome
}

func TestSequentialHistory(t *testing.T) {
	h := []Op{
		put(1, "k", "a", 1, 2),
		get(1, "k", true, "a", 3, 4),
		put(1, "k", "b", 5, 6),
		get(1, "k", true, "b", 7, 8),
	}
	if check(h) != OK {
		t.Fatal("sequential history rejected")
	}
}

func TestStaleReadRejected(t *testing.T) {
	h := []Op{
		put(1, "k", "a", 1, 2),
		put(1, "k", "b", 3, 4),
		get(2, "k", true, "a", 5, 6), // starts after b was acknowledged
	}
	if check(h) != Illegal {
		t.Fatal("stale read accepted")
	}
}

func TestConcurrentWritesEitherOrder(t *testing.T) {
	for _, final := range []string{"a", "b"} {
		h := []Op{
			put(1, "k", "a", 1, 10),
			put(2, "k", "b", 2, 9),
			get(3, "k", true, final, 11, 12),
		}
		if check(h) != OK {
			t.Fatalf("concurrent writes, read %q rejected", final)
		}
	}
}

func TestReadOverlappingWriteMaySeeEither(t *testing.T) {
	old := []Op{put(1, "k", "a", 1, 2), put(1, "k", "b", 3, 10), get(2, "k", true, "a", 4, 5)}
	newer := []Op{put(1, "k", "a", 1, 2), put(1, "k", "b", 3, 10), get(2, "k", true, "b", 4, 5)}
	if check(old) != OK || check(newer) != OK {
		t.Fatal("a read concurrent with a write may see either value")
	}
}

func TestNewOldInversionRejected(t *testing.T) {
	// two sequential reads must not go back in time even if the write is still running
	h := []Op{
		put(1, "k", "a", 1, 2),
		put(1, "k", "b", 3, 20),
		get(2, "k", true, "b", 4, 5),
		get(2, "k", true, "a", 6, 7),
	}
	if check(h) != Illegal {
		t.Fatal("time-travelling reads accepted")
	}
}

func TestPendingWriteMayOrMayNotApply(t *testing.T) {
	pend := Op{Client: 1, Call: 3, Return: Pending, Input: KVInput{Kind: KindPut, Key: "k", Value: "b"}}
	applied := []Op{put(1, "k", "a", 1, 2), pend, get(2, "k", true, "b", 10, 11)}
	dropped := []Op{put(1, "k", "a", 1, 2), pend, get(2, "k", true, "a", 10, 11)}
	if check(applied) != OK || check(dropped) != OK {
		t.Fatal("unknown-outcome write must be allowed both ways")
	}
	// but it cannot show up and then vanish
	flip := []Op{put(1, "k", "a", 1, 2), pend, get(2, "k", true, "b", 10, 11), get(2, "k", true, "a", 12, 13)}
	if check(flip) != Illegal {
		t.Fatal("pending write applied twice")
	}
}

func TestCASSemantics(t *testing.T) {
	good := []Op{
		put(1, "k", "0", 1, 2),
		cas(1, "k", "0", "1", true, 3, 4),
		cas(2, "k", "0", "2", false, 5, 6),
		get(1, "k", true, "1", 7, 8),
	}
	if check(good) != OK {
		t.Fatal("valid CAS history rejected")
	}
	bad := []Op{
		put(1, "k", "0", 1, 2),
		cas(1, "k", "0", "1", true, 3, 4),
		cas(2, "k", "0", "2", true, 5, 6), // both cannot win
	}
	if check(bad) != Illegal {
		t.Fatal("double CAS success accepted")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Op{
		put(1, "a", "1", 1, 2),
		get(2, "b", false, "", 3, 4),
		put(1, "b", "2", 5, 6),
		get(2, "a", true, "1", 7, 8),
	}
	if check(h) != OK {
		t.Fatal("independent keys rejected")
	}
}

func TestBudgetExhaustionIsUnknown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	var h []Op
	for i := 0; i < 40; i++ {
		h = append(h, put(i, "k", string(rune('a'+i%26)), int64(i), int64(1000+i)))
	}
	h = append(h, get(99, "k", true, "zzz", 2000, 2001)) // impossible, search is huge
	if got := Check(ctx, KVModel(), h).Outcome; got == OK {
		t.Fatal("impossible history accepted")
	}
}

func TestRecorderTimestampsNeverTie(t *testing.T) {
	r := NewRecorder()
	prev := int64(0)
	for i := 0; i < 10000; i++ {
		n := r.now()
		if n <= prev {
			t.Fatalf("clock went %d -> %d", prev, n)
		}
		prev = n
	}
}
