package fsm

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

func (r *rig) compact(rev uint64) *kvpb.CommandResult {
	return r.do(&kvpb.Command{Body: &kvpb.Command_Compact{Compact: &kvpb.CompactRequest{Revision: rev}}})
}

func (r *rig) getAt(k string, rev uint64) (*kvpb.KeyValue, error) {
	resp, _, err := r.m.Range(&kvpb.RangeRequest{Key: []byte(k), Revision: rev})
	if err != nil || len(resp.Kvs) == 0 {
		return nil, err
	}
	return resp.Kvs[0], nil
}

func allAt(t *testing.T, m *Machine, rev uint64) []*kvpb.KeyValue {
	t.Helper()
	resp, _, err := m.Range(&kvpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, Revision: rev})
	if err != nil {
		t.Fatalf("range at %d: %v", rev, err)
	}
	return resp.Kvs
}

func sameKvs(a, b []*kvpb.KeyValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestReadAtRevision(t *testing.T) {
	r := newRig(t)
	c := r.session()          // rev 0, sessions don't consume one
	r.put(c, 1, "a", "v1", 0) // rev 1
	r.put(c, 2, "a", "v2", 0) // rev 2
	r.del(c, 3, "a", "")      // rev 3
	r.put(c, 4, "a", "v4", 0) // rev 4, a fresh incarnation

	for rev, want := range map[uint64]string{1: "v1", 2: "v2", 3: "", 4: "v4"} {
		kv, err := r.getAt("a", rev)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case want == "" && kv != nil:
			t.Fatalf("rev %d: key should not exist, got %v", rev, kv)
		case want != "" && (kv == nil || string(kv.Value) != want):
			t.Fatalf("rev %d: got %v want %s", rev, kv, want)
		}
	}
	kv, _ := r.getAt("a", 2)
	if kv.Version != 2 || kv.CreateRevision != 1 || kv.ModRevision != 2 {
		t.Fatalf("metadata at rev 2: %v", kv)
	}
	kv, _ = r.getAt("a", 4)
	if kv.Version != 1 || kv.CreateRevision != 4 {
		t.Fatalf("recreated key should start over: %v", kv)
	}

	if _, err := r.getAt("a", 99); !errors.Is(err, ErrFutureRev) {
		t.Fatalf("want future revision error, got %v", err)
	}
}

func TestCompaction(t *testing.T) {
	r := newRig(t)
	c := r.session()
	r.put(c, 1, "a", "v1", 0) // 1
	r.put(c, 2, "a", "v2", 0) // 2
	r.put(c, 3, "b", "b1", 0) // 3
	r.del(c, 4, "b", "")      // 4
	r.put(c, 5, "a", "v5", 0) // 5

	if res := r.compact(3); res.ErrorCode != "" {
		t.Fatal(res.ErrorCode)
	}
	if _, err := r.getAt("a", 2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("rev 2 should be compacted, got %v", err)
	}
	// rev 3 is the boundary: still readable, and it still sees the old a and b
	if kv, err := r.getAt("a", 3); err != nil || string(kv.Value) != "v2" {
		t.Fatalf("a at 3: %v %v", kv, err)
	}
	if kv, err := r.getAt("b", 3); err != nil || kv == nil {
		t.Fatalf("b at 3: %v %v", kv, err)
	}

	// a tombstone below the boundary takes the whole key with it
	r.compact(5)
	if _, ok := r.m.vers.Get(&keyHist{key: "b"}); ok {
		t.Fatal("history of the deleted key should be gone")
	}
	if kv, _ := r.getAt("a", 5); string(kv.Value) != "v5" {
		t.Fatalf("latest value lost: %v", kv)
	}

	// retrying is harmless, going into the future is not allowed
	if res := r.compact(3); res.ErrorCode != "" {
		t.Fatalf("recompact: %s", res.ErrorCode)
	}
	if res := r.compact(50); res.ErrorCode != kvpb.ErrFutureRevision {
		t.Fatalf("future compact: %q", res.ErrorCode)
	}
}

func TestRevisionReadNotAllowedInTxn(t *testing.T) {
	r := newRig(t)
	c := r.session()
	res := r.do(&kvpb.Command{Body: &kvpb.Command_Txn{Txn: &kvpb.TxnRequest{
		Meta: &kvpb.RequestMeta{ClientId: c, Seq: 1},
		Success: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_Range{
			Range: &kvpb.RangeRequest{Key: []byte("a"), Revision: 1},
		}}},
	}}})
	if res.ErrorCode != kvpb.ErrInvalidArgument {
		t.Fatalf("got %q", res.ErrorCode)
	}
}

// Random writes, deletes, lease expiries and compactions. After every command
// the full latest state is remembered under its revision; at the end every
// readable revision must reproduce exactly what was remembered, on the live
// machine and on one rebuilt from a snapshot.
func TestHistoryMatchesModel(t *testing.T) {
	r := newRig(t)
	c := r.session()
	rng := rand.New(rand.NewPCG(7, 7))
	states := map[uint64][]*kvpb.KeyValue{}
	seq := uint64(1)
	var compacted uint64

	snapshotNow := func() {
		states[r.m.Revision()] = allAt(t, r.m, 0)
	}
	for i := 0; i < 1500; i++ {
		seq++
		switch n := rng.IntN(10); {
		case n < 5:
			var l uint64
			if len(r.leases) > 0 && rng.IntN(3) == 0 {
				l = r.leases[rng.IntN(len(r.leases))]
			}
			r.put(c, seq, fmt.Sprint("k", rng.IntN(25)), fmt.Sprint(i), l)
		case n < 7:
			end := ""
			if rng.IntN(5) == 0 {
				end = "k5"
			}
			r.del(c, seq, fmt.Sprint("k", rng.IntN(25)), end)
		case n == 7:
			r.leases = append(r.leases, r.grant(c, seq, uint64(500+rng.IntN(3000))))
		case n == 8:
			r.now += int64(rng.IntN(2500)) * 1_000_000
			r.expire(r.m.ExpiredLeases(r.now)...)
		default:
			if rev := r.m.Revision(); rev > compacted+10 {
				compacted += uint64(rng.IntN(int(rev-compacted))) / 2
				r.compact(compacted)
			}
		}
		snapshotNow()
	}

	check := func(m *Machine, label string) {
		for rev := compacted; rev <= r.m.Revision(); rev++ {
			want, ok := states[rev]
			if !ok {
				continue // that revision never existed (rev 0 before the first write)
			}
			if got := allAt(t, m, rev); !sameKvs(got, want) {
				t.Fatalf("%s: state at revision %d differs: got %d keys, want %d", label, rev, len(got), len(want))
			}
		}
		if compacted > 1 {
			if _, _, err := m.Range(&kvpb.RangeRequest{Key: []byte("k1"), Revision: compacted - 1}); !errors.Is(err, ErrCompacted) {
				t.Fatalf("%s: revision below the compaction point must fail, got %v", label, err)
			}
		}
	}
	check(r.m, "live")

	snap, err := r.m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	m2 := New(Options{})
	if err := m2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	check(m2, "restored")
}

// The serialising half of an async snapshot runs while Apply keeps changing the
// store (writes, deletes, lease expiry and compaction). It must still produce
// exactly the bytes a stop-the-world snapshot of that moment would have.
func TestAsyncSnapshotIgnoresLaterWrites(t *testing.T) {
	r := newRig(t)
	c := r.session()
	seq := uint64(1)
	rng := rand.New(rand.NewPCG(3, 3))
	churn := func(n int) {
		for i := 0; i < n; i++ {
			seq++
			switch rng.IntN(6) {
			case 0, 1, 2:
				r.put(c, seq, fmt.Sprint("k", rng.IntN(40)), fmt.Sprint(seq), 0)
			case 3:
				r.del(c, seq, fmt.Sprint("k", rng.IntN(40)), "")
			case 4:
				r.leases = append(r.leases, r.grant(c, seq, uint64(500+rng.IntN(1000))))
			case 5:
				if rev := r.m.Revision(); rev > 20 {
					r.compact(rev - 10)
				}
			}
		}
	}
	churn(600)

	want, err := r.m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	write := r.m.SnapshotAsync()

	done := make(chan struct{})
	go func() {
		defer close(done)
		churn(600)
	}()
	got, err := write()
	if err != nil {
		t.Fatal(err)
	}
	<-done

	if !bytes.Equal(got, want) {
		t.Fatalf("async snapshot differs from the state at capture time (%d vs %d bytes)", len(got), len(want))
	}
}
