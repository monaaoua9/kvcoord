package fsm

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/raft"
)

type rig struct {
	leases []uint64
	t      *testing.T
	m      *Machine
	idx    uint64
	now    int64
}

func newRig(t *testing.T) *rig {
	return &rig{t: t, m: New(Options{HistoryLimit: 50}), now: 1_000_000_000}
}

func (r *rig) do(c *kvpb.Command) *kvpb.CommandResult {
	r.t.Helper()
	r.idx++
	c.TimestampNs = r.now
	data, err := proto.Marshal(c)
	if err != nil {
		r.t.Fatal(err)
	}
	return r.m.Apply(raft.Entry{Index: r.idx, Term: 1, Data: data}).(*kvpb.CommandResult)
}

func (r *rig) session() uint64 {
	res := r.do(&kvpb.Command{Body: &kvpb.Command_OpenSession{OpenSession: &kvpb.OpenSessionRequest{}}})
	return res.GetOpenSession().ClientId
}

func (r *rig) put(cid, seq uint64, k, v string, lease uint64) *kvpb.CommandResult {
	return r.do(&kvpb.Command{Body: &kvpb.Command_Put{Put: &kvpb.PutRequest{
		Meta: &kvpb.RequestMeta{ClientId: cid, Seq: seq}, Key: []byte(k), Value: []byte(v), Lease: lease, PrevKv: true,
	}}})
}

func (r *rig) del(cid, seq uint64, k, end string) *kvpb.CommandResult {
	return r.do(&kvpb.Command{Body: &kvpb.Command_DeleteRange{DeleteRange: &kvpb.DeleteRangeRequest{
		Meta: &kvpb.RequestMeta{ClientId: cid, Seq: seq}, Key: []byte(k), RangeEnd: []byte(end), PrevKv: true,
	}}})
}

func (r *rig) grant(cid, seq, ttlMs uint64) uint64 {
	res := r.do(&kvpb.Command{Body: &kvpb.Command_LeaseGrant{LeaseGrant: &kvpb.LeaseGrantRequest{
		Meta: &kvpb.RequestMeta{ClientId: cid, Seq: seq}, TtlMs: ttlMs,
	}}})
	if res.ErrorCode != "" {
		r.t.Fatalf("grant: %s", res.ErrorCode)
	}
	return res.GetLeaseGrant().Id
}

func (r *rig) lock(name string, lease uint64, try bool) *kvpb.CommandResult {
	return r.do(&kvpb.Command{Body: &kvpb.Command_Lock{Lock: &kvpb.LockRequest{Name: []byte(name), Lease: lease, Try: try}}})
}

func (r *rig) unlock(name string, lease uint64) {
	r.do(&kvpb.Command{Body: &kvpb.Command_Unlock{Unlock: &kvpb.UnlockRequest{Name: []byte(name), Lease: lease}}})
}

func (r *rig) expire(ids ...uint64) {
	r.do(&kvpb.Command{Body: &kvpb.Command_LeaseExpire{LeaseExpire: &kvpb.LeaseExpireRequest{Ids: ids}}})
}

func (r *rig) get(k string) *kvpb.KeyValue {
	resp, _, _ := r.m.Range(&kvpb.RangeRequest{Key: []byte(k)})
	if len(resp.Kvs) == 0 {
		return nil
	}
	return resp.Kvs[0]
}

func TestPutGetRevisions(t *testing.T) {
	r := newRig(t)
	c := r.session()
	r.put(c, 1, "a", "1", 0)
	r.put(c, 2, "b", "2", 0)
	res := r.put(c, 3, "a", "3", 0)

	if prev := res.GetPut().PrevKv; prev == nil || string(prev.Value) != "1" {
		t.Fatalf("prev kv: %v", prev)
	}
	kv := r.get("a")
	if string(kv.Value) != "3" || kv.Version != 2 || kv.CreateRevision != 1 || kv.ModRevision != 3 {
		t.Fatalf("unexpected kv %v", kv)
	}
	if r.m.Revision() != 3 {
		t.Fatalf("revision %d", r.m.Revision())
	}
}

func TestRangeAndDelete(t *testing.T) {
	r := newRig(t)
	c := r.session()
	for i, k := range []string{"a1", "a2", "a3", "b1", "c1"} {
		r.put(c, uint64(i+1), k, k, 0)
	}
	resp, _, _ := r.m.Range(&kvpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("b")})
	if len(resp.Kvs) != 3 {
		t.Fatalf("prefix range returned %d", len(resp.Kvs))
	}
	resp, _, _ = r.m.Range(&kvpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, Limit: 2})
	if len(resp.Kvs) != 2 || !resp.More {
		t.Fatalf("limit: %d more=%v", len(resp.Kvs), resp.More)
	}
	resp, _, _ = r.m.Range(&kvpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, CountOnly: true})
	if resp.Count != 5 || len(resp.Kvs) != 0 {
		t.Fatalf("count %d", resp.Count)
	}

	revBefore := r.m.Revision()
	res := r.del(c, 6, "zzz", "")
	if res.GetDeleteRange().Deleted != 0 || r.m.Revision() != revBefore {
		t.Fatal("deleting nothing must not bump the revision")
	}
	res = r.del(c, 7, "a", "b")
	if res.GetDeleteRange().Deleted != 3 || r.get("a2") != nil || r.get("b1") == nil {
		t.Fatal("range delete went wrong")
	}
	if r.m.Revision() != revBefore+1 {
		t.Fatal("one delete command must be one revision")
	}
}

func TestTxnCompareAndSwap(t *testing.T) {
	r := newRig(t)
	c := r.session()
	r.put(c, 1, "k", "old", 0)

	cas := func(seq uint64, expect, next string) bool {
		res := r.do(&kvpb.Command{Body: &kvpb.Command_Txn{Txn: &kvpb.TxnRequest{
			Meta: &kvpb.RequestMeta{ClientId: c, Seq: seq},
			Compare: []*kvpb.Compare{{
				Result: kvpb.Compare_EQUAL, Target: kvpb.Compare_VALUE, Key: []byte("k"),
				TargetUnion: &kvpb.Compare_Value{Value: []byte(expect)},
			}},
			Success: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_Put{Put: &kvpb.PutRequest{Key: []byte("k"), Value: []byte(next)}}}},
			Failure: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_Range{Range: &kvpb.RangeRequest{Key: []byte("k")}}}},
		}}})
		return res.GetTxn().Succeeded
	}
	if !cas(2, "old", "new") {
		t.Fatal("first cas should win")
	}
	if cas(3, "old", "newer") {
		t.Fatal("second cas must fail")
	}
	if string(r.get("k").Value) != "new" {
		t.Fatal("value changed by failed cas")
	}
}

func TestTxnMissingKeyCompare(t *testing.T) {
	r := newRig(t)
	c := r.session()
	res := r.do(&kvpb.Command{Body: &kvpb.Command_Txn{Txn: &kvpb.TxnRequest{
		Meta: &kvpb.RequestMeta{ClientId: c, Seq: 1},
		Compare: []*kvpb.Compare{{
			Result: kvpb.Compare_EQUAL, Target: kvpb.Compare_CREATE, Key: []byte("fresh"),
			TargetUnion: &kvpb.Compare_CreateRevision{CreateRevision: 0},
		}},
		Success: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_Put{Put: &kvpb.PutRequest{Key: []byte("fresh"), Value: []byte("1")}}}},
	}}})
	if !res.GetTxn().Succeeded || r.get("fresh") == nil {
		t.Fatal("create-if-absent failed")
	}
}

func TestSessionDedupe(t *testing.T) {
	r := newRig(t)
	c := r.session()
	r.put(c, 1, "k", "v1", 0)
	again := r.put(c, 1, "k", "SHOULD-NOT-APPLY", 0)
	if string(r.get("k").Value) != "v1" || r.get("k").Version != 1 {
		t.Fatal("retried request was applied twice")
	}
	if again.GetPut() == nil {
		t.Fatal("retry should return the cached response")
	}

	// ack_seq lets the server forget; a replay below it is refused
	r.do(&kvpb.Command{Body: &kvpb.Command_Put{Put: &kvpb.PutRequest{
		Meta: &kvpb.RequestMeta{ClientId: c, Seq: 2, AckSeq: 1}, Key: []byte("k2"), Value: []byte("x"),
	}}})
	stale := r.put(c, 1, "k", "v", 0)
	if stale.ErrorCode != kvpb.ErrStaleRequest {
		t.Fatalf("want STALE_REQUEST, got %q", stale.ErrorCode)
	}
	if got := r.put(999, 1, "k", "v", 0); got.ErrorCode != kvpb.ErrSessionExpired {
		t.Fatalf("want SESSION_EXPIRED, got %q", got.ErrorCode)
	}
}

func TestSessionExpiry(t *testing.T) {
	r := newRig(t)
	c := r.session()
	r.now += int64(time.Minute)
	r.do(&kvpb.Command{Body: &kvpb.Command_SessionExpire{SessionExpire: &kvpb.SessionExpireRequest{IdleMs: 1000}}})
	if res := r.put(c, 1, "k", "v", 0); res.ErrorCode != kvpb.ErrSessionExpired {
		t.Fatalf("session should be gone, got %q", res.ErrorCode)
	}
}

func TestLeaseExpiryDeletesKeys(t *testing.T) {
	r := newRig(t)
	c := r.session()
	l := r.grant(c, 1, 1000)
	r.put(c, 2, "e1", "v", l)
	r.put(c, 3, "e2", "v", l)
	r.put(c, 4, "keep", "v", 0)

	r.now += int64(500 * time.Millisecond)
	r.expire(l) // too early: deadline not reached
	if r.get("e1") == nil {
		t.Fatal("lease expired early")
	}

	// keepalive pushes the deadline out
	r.do(&kvpb.Command{Body: &kvpb.Command_LeaseKeepAlive{LeaseKeepAlive: &kvpb.LeaseKeepAliveRequest{Id: l}}})
	r.now += int64(700 * time.Millisecond)
	r.expire(l)
	if r.get("e1") == nil {
		t.Fatal("keepalive was ignored")
	}

	r.now += int64(time.Second)
	if ids := r.m.ExpiredLeases(r.now); len(ids) != 1 || ids[0] != l {
		t.Fatalf("expired set %v", ids)
	}
	r.expire(l)
	if r.get("e1") != nil || r.get("e2") != nil || r.get("keep") == nil {
		t.Fatal("expiry removed the wrong keys")
	}
	if res := r.put(c, 5, "x", "v", l); res.ErrorCode != kvpb.ErrLeaseNotFound {
		t.Fatalf("put on dead lease: %q", res.ErrorCode)
	}
}

func TestLeaseRevokeEmitsOneRevision(t *testing.T) {
	r := newRig(t)
	c := r.session()
	l := r.grant(c, 1, 5000)
	for i := 0; i < 5; i++ {
		r.put(c, uint64(2+i), fmt.Sprint("k", i), "v", l)
	}
	before := r.m.Revision()
	r.do(&kvpb.Command{Body: &kvpb.Command_LeaseRevoke{LeaseRevoke: &kvpb.LeaseRevokeRequest{Id: l}}})
	if r.m.Revision() != before+1 {
		t.Fatalf("revoke used %d revisions", r.m.Revision()-before)
	}
}

func TestOverwriteMovesKeyBetweenLeases(t *testing.T) {
	r := newRig(t)
	c := r.session()
	l1 := r.grant(c, 1, 1000)
	r.put(c, 2, "k", "v", l1)
	r.put(c, 3, "k", "v2", 0) // detaches from l1
	r.now += int64(5 * time.Second)
	r.expire(l1)
	if r.get("k") == nil {
		t.Fatal("key was deleted by a lease it no longer belongs to")
	}
}

func TestLockFairnessAndFencing(t *testing.T) {
	r := newRig(t)
	c := r.session()
	a, b, d := r.grant(c, 1, 5000), r.grant(c, 2, 5000), r.grant(c, 3, 5000)

	ra := r.lock("L", a, false).GetLock()
	if !ra.Acquired || ra.FencingToken == 0 {
		t.Fatal("a should own the lock")
	}
	if again := r.lock("L", a, false).GetLock(); !again.Acquired || again.FencingToken != ra.FencingToken {
		t.Fatal("re-acquire by owner must be idempotent")
	}

	sb, _ := r.m.SubscribeLock("L", b)
	sd, cancelD := r.m.SubscribeLock("L", d)
	defer cancelD()
	if r.lock("L", b, false).GetLock().QueuePos != 1 || r.lock("L", d, false).GetLock().QueuePos != 2 {
		t.Fatal("waiters should queue in order")
	}
	if try := r.lock("L", r.grant(c, 4, 5000), true).GetLock(); try.Acquired || try.QueuePos != 0 {
		t.Fatal("try-lock must not queue")
	}

	r.unlock("L", a)
	ev := <-sb
	if ev.Err != "" || ev.Token <= ra.FencingToken {
		t.Fatalf("b grant %+v, token must exceed %d", ev, ra.FencingToken)
	}
	select {
	case <-sd:
		t.Fatal("d jumped the queue")
	default:
	}

	// the holder dies: d is next, and gets a larger token again
	r.now += int64(time.Minute)
	r.expire(b)
	ev2 := <-sd
	if ev2.Err != "" || ev2.Token <= ev.Token {
		t.Fatalf("d grant %+v", ev2)
	}
}

func TestLockWaiterLeaseDeathNotifies(t *testing.T) {
	r := newRig(t)
	c := r.session()
	a, b := r.grant(c, 1, 5000), r.grant(c, 2, 5000)
	r.lock("L", a, false)
	sb, _ := r.m.SubscribeLock("L", b)
	r.lock("L", b, false)

	r.now += int64(time.Minute)
	r.expire(b)
	if ev := <-sb; ev.Err != kvpb.ErrLeaseExpired {
		t.Fatalf("got %+v", ev)
	}
	r.unlock("L", a)
	if got := r.lock("L", a, true).GetLock(); !got.Acquired {
		t.Fatal("lock should be free once the only waiter died")
	}
}

func TestWatchLiveAndHistory(t *testing.T) {
	r := newRig(t)
	c := r.session()
	r.put(c, 1, "w/a", "1", 0)
	r.put(c, 2, "w/b", "2", 0)
	r.put(c, 3, "other", "x", 0)
	r.del(c, 4, "w/a", "")

	w, _, err := r.m.Watch(WatchSpec{Key: []byte("w/"), End: []byte("w0"), StartRev: 1, PrevKV: true}, 16)
	if err != nil {
		t.Fatal(err)
	}
	r.put(c, 5, "w/c", "3", 0)

	var revs []uint64
	var types []kvpb.Event_Type
	for i := 0; i < 4; i++ {
		select {
		case b := <-w.C:
			revs = append(revs, b.Revision)
			types = append(types, b.Events[0].Type)
		case <-time.After(time.Second):
			t.Fatalf("only got %d batches", i)
		}
	}
	want := []uint64{1, 2, 4, 5}
	for i := range want {
		if revs[i] != want[i] {
			t.Fatalf("revisions %v want %v", revs, want)
		}
	}
	if types[2] != kvpb.Event_DELETE {
		t.Fatalf("types %v", types)
	}
}

func TestWatchCompactedHistory(t *testing.T) {
	r := newRig(t)
	c := r.session()
	for i := 0; i < 120; i++ {
		r.put(c, uint64(i+1), "k", fmt.Sprint(i), 0)
	}
	_, compact, err := r.m.Watch(WatchSpec{Key: []byte("k"), StartRev: 1}, 4)
	if err != ErrCompacted || compact <= 1 {
		t.Fatalf("err=%v compact=%d", err, compact)
	}
	if _, _, err := r.m.Watch(WatchSpec{Key: []byte("k"), StartRev: compact}, 100); err != nil {
		t.Fatal(err)
	}
}

func TestWatchOverflowCancels(t *testing.T) {
	r := newRig(t)
	c := r.session()
	w, _, _ := r.m.Watch(WatchSpec{Key: []byte("k")}, 2)
	for i := 0; i < 10; i++ {
		r.put(c, uint64(i+1), "k", "v", 0)
	}
	n := 0
	for range w.C {
		n++
	}
	if w.Reason() != kvpb.ErrWatchOverflow || n != 2 {
		t.Fatalf("reason=%q n=%d", w.Reason(), n)
	}
}

func TestSnapshotRestoreIsExact(t *testing.T) {
	a := newRig(t)
	c := a.session()
	seq := uint64(1)

	step := func(r *rig, rng *rand.Rand) {
		seq++
		switch rng.IntN(7) {
		case 0, 1, 2:
			var l uint64
			if len(r.leases) > 0 && rng.IntN(2) == 0 {
				l = r.leases[rng.IntN(len(r.leases))]
			}
			r.put(c, seq, fmt.Sprint("k", rng.IntN(30)), fmt.Sprint(seq), l)
		case 3:
			r.del(c, seq, fmt.Sprint("k", rng.IntN(30)), "")
		case 4:
			r.leases = append(r.leases, r.grant(c, seq, uint64(1000+rng.IntN(5000))))
		case 5:
			name := fmt.Sprint("L", rng.IntN(3))
			if len(r.leases) > 0 {
				r.lock(name, r.leases[rng.IntN(len(r.leases))], rng.IntN(2) == 0)
			}
		case 6:
			r.now += int64(rng.IntN(3000)) * int64(time.Millisecond)
			r.expire(r.m.ExpiredLeases(r.now)...)
		}
	}

	for i := 0; i < 400; i++ {
		step(a, rand.New(rand.NewPCG(uint64(i), 1)))
	}
	snap, err := a.m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	b := &rig{t: t, m: New(Options{}), idx: a.idx, now: a.now, leases: append([]uint64(nil), a.leases...)}
	if err := b.m.Restore(snap); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		// both machines see the same commands from here on
		seqBefore := seq
		step(a, rand.New(rand.NewPCG(uint64(1000+i), 1)))
		seq = seqBefore
		step(b, rand.New(rand.NewPCG(uint64(1000+i), 1)))
	}
	sa, _ := a.m.Snapshot()
	sb, _ := b.m.Snapshot()
	if !bytes.Equal(sa, sb) {
		t.Fatalf("replicas diverged after restore (%d vs %d bytes)", len(sa), len(sb))
	}
}
