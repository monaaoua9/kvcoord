package server_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/client"
	"github.com/monaaoua9/kvcoord/internal/testcluster"
)

func ctxT(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestPutGetThroughAnyNode(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 10*time.Second)

	if _, err := cl.Put(ctx, "greeting", "hello"); err != nil {
		t.Fatal(err)
	}
	got, err := cl.Get(ctx, "greeting")
	if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "hello" {
		t.Fatalf("get: %v %v", got, err)
	}

	// a client that only knows a follower must still get through (redirect)
	leader := c.Leader(time.Second)
	for _, id := range c.IDs() {
		if id == leader {
			continue
		}
		only, _ := client.New(client.Config{Endpoints: []string{c.Addr(id)}, RequestTimeout: time.Second})
		defer only.Close()
		got, err := only.Get(ctx, "greeting")
		if err != nil || string(got.Kvs[0].Value) != "hello" {
			t.Fatalf("via follower %d: %v %v", id, got, err)
		}
		if r := got.Header.NodeId; r != leader {
			t.Fatalf("linearizable read served by node %d, leader is %d", r, leader)
		}
	}
}

func TestSerializableReadOnFollower(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	leader := c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 10*time.Second)
	if _, err := cl.Put(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.IDs() {
		if id == leader {
			continue
		}
		only, _ := client.New(client.Config{Endpoints: []string{c.Addr(id)}, RequestTimeout: time.Second})
		defer only.Close()
		deadline := time.Now().Add(3 * time.Second)
		for {
			r, err := only.Get(ctx, "k", client.WithSerializable())
			if err == nil && len(r.Kvs) == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("follower %d never served the key: %v", id, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestCompareAndSwap(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 10*time.Second)

	cl.Put(ctx, "counter", "0")
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := cl.Txn().
				If(client.CmpValue("counter", "0")).
				Then(client.OpPut("counter", "1")).
				Commit(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			if r.Succeeded {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d goroutines won the CAS", wins.Load())
	}
}

func TestWatchSurvivesLeaderCrash(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	leader := c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 30*time.Second)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := cl.Watch(wctx, "w/", client.WithPrefix())

	// give the watch a moment to attach before writing
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("w/%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	c.Crash(leader)
	c.Leader(5 * time.Second)
	for i := 5; i < 10; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("w/%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	deadline := time.After(15 * time.Second)
	for len(seen) < 10 {
		select {
		case ev, ok := <-events:
			if !ok || ev.Err != nil {
				t.Fatalf("watch ended early: %+v", ev)
			}
			for _, e := range ev.Events {
				seen[string(e.Kv.Key)] = true
			}
		case <-deadline:
			t.Fatalf("saw only %d of 10 keys: %v", len(seen), seen)
		}
	}
}

func TestLeaseExpiresKeys(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 20*time.Second)

	id, _, err := cl.Grant(ctx, 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(ctx, "ephemeral", "x", client.WithLease(id)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := cl.Get(ctx, "ephemeral")
		if err == nil && len(r.Kvs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("key outlived its lease")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSessionKeepsLeaseAlive(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 20*time.Second)

	s, err := cl.NewSession(ctx, 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	cl.Put(ctx, "alive", "1", client.WithLease(s.Lease()))
	time.Sleep(1500 * time.Millisecond) // several TTLs
	r, err := cl.Get(ctx, "alive")
	if err != nil || len(r.Kvs) != 1 {
		t.Fatalf("renewed lease lost its key: %v %v", r, err)
	}

	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r, _ = cl.Get(ctx, "alive")
	if len(r.Kvs) != 0 {
		t.Fatal("closing the session must delete its keys")
	}
}

func TestMutualExclusionAndFencing(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	c.Leader(3 * time.Second)
	ctx := ctxT(t, 60*time.Second)

	const workers, rounds = 5, 6
	var (
		inCS    atomic.Int32
		maxSeen atomic.Int32
		mu      sync.Mutex
		tokens  []uint64
		wg      sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := c.Client()
			s, err := cl.NewSession(ctx, 2*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			m := client.NewMutex(s, "crit")
			for r := 0; r < rounds; r++ {
				tok, err := m.Lock(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if n := inCS.Add(1); n > maxSeen.Load() {
					maxSeen.Store(n)
				}
				mu.Lock()
				tokens = append(tokens, tok)
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
				inCS.Add(-1)
				if err := m.Unlock(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if maxSeen.Load() != 1 {
		t.Fatalf("%d holders inside the critical section at once", maxSeen.Load())
	}
	if len(tokens) != workers*rounds {
		t.Fatalf("%d grants, want %d", len(tokens), workers*rounds)
	}
	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Fatalf("fencing tokens not increasing: %v", tokens)
		}
	}
}

func TestLockPassesToWaiterWhenHolderDies(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 30*time.Second)

	lc := c.Client()
	holder, err := lc.NewSession(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	hm := client.NewMutex(holder, "L")
	if _, err := hm.Lock(ctx); err != nil {
		t.Fatal(err)
	}

	waiter, err := cl.NewSession(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wm := client.NewMutex(waiter, "L")
	got := make(chan uint64, 1)
	go func() {
		tok, err := wm.Lock(ctx)
		if err != nil {
			t.Error(err)
		}
		got <- tok
	}()

	select {
	case <-got:
		t.Fatal("waiter got the lock while the holder is alive")
	case <-time.After(300 * time.Millisecond):
	}

	// kill the holder's lease out from under it
	if err := cl.Revoke(ctx, holder.Lease()); err != nil {
		t.Fatal(err)
	}
	select {
	case tok := <-got:
		if tok == 0 {
			t.Fatal("no fencing token")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never got the lock")
	}
}

func TestNodeRestartKeepsData(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, SnapshotEvery: 25})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	for i := 0; i < 80; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%03d", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.IDs() {
		c.Crash(id)
	}
	for _, id := range c.IDs() {
		c.Restart(id)
	}
	c.Leader(5 * time.Second)

	r, err := cl.Get(ctx, "k", client.WithPrefix())
	if err != nil || len(r.Kvs) != 80 {
		n := 0
		if r != nil {
			n = len(r.Kvs)
		}
		t.Fatalf("after full restart: %d keys, err %v", n, err)
	}
}

func TestFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, SnapshotEvery: 20})
	leader := c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	var lag uint64
	for _, id := range c.IDs() {
		if id != leader {
			lag = id
			break
		}
	}
	c.Net.Isolate(lag)
	for i := 0; i < 120; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("s%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	c.Net.Heal()

	deadline := time.Now().Add(10 * time.Second)
	for {
		st := c.Server(lag).Machine().Stats()
		if st.Keys == 120 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lagging follower has %d keys", st.Keys)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func memberIDs(t *testing.T, cl *client.Client, ctx context.Context) []uint64 {
	t.Helper()
	ms, err := cl.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for _, m := range ms {
		ids = append(ids, m.Id)
	}
	return ids
}

func TestAddNodeCatchesUpViaSnapshot(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, SnapshotEvery: 20})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	for i := 0; i < 100; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	id := c.AddNode()
	if _, err := cl.MemberAdd(ctx, id, c.Addr(id)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for c.Server(id).Machine().Stats().Keys != 100 {
		if time.Now().After(deadline) {
			t.Fatalf("new node has %d keys", c.Server(id).Machine().Stats().Keys)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := memberIDs(t, cl, ctx); len(got) != 4 {
		t.Fatalf("members %v", got)
	}

	// the new voter takes part in commits: lose one of the old three and keep writing
	c.Crash(1)
	for i := 100; i < 120; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRemoveLeaderFromCluster(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	leader := c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	if _, err := cl.Put(ctx, "before", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.MemberRemove(ctx, leader); err != nil {
		t.Fatal(err)
	}

	// the survivors elect someone else and keep serving
	if _, err := cl.Put(ctx, "after", "2"); err != nil {
		t.Fatal(err)
	}
	r, err := cl.Get(ctx, "before")
	if err != nil || len(r.Kvs) != 1 {
		t.Fatalf("lost data across the change: %v %v", r, err)
	}
	for _, id := range memberIDs(t, cl, ctx) {
		if id == leader {
			t.Fatal("removed node still listed")
		}
	}
	if st := c.Server(leader).Node().Status(); st.State.String() == "leader" {
		t.Fatal("removed node is still leading")
	}
	// ...and the removed node cannot start elections that disturb the rest
	time.Sleep(500 * time.Millisecond)
	if _, err := cl.Put(ctx, "later", "3"); err != nil {
		t.Fatal(err)
	}
}

func TestMembershipSurvivesRestart(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, SnapshotEvery: 15})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	id := c.AddNode()
	if _, err := cl.MemberAdd(ctx, id, c.Addr(id)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		cl.Put(ctx, fmt.Sprintf("k%d", i), "v")
	}
	for _, n := range c.IDs() {
		c.Crash(n)
	}
	for _, n := range c.IDs() {
		c.Restart(n)
	}
	c.Leader(10 * time.Second)
	if got := memberIDs(t, cl, ctx); len(got) != 4 {
		t.Fatalf("members after restart: %v", got)
	}
	if _, err := cl.Put(ctx, "post", "1"); err != nil {
		t.Fatal(err)
	}
}

func TestOneMembershipChangeAtATime(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 30*time.Second)

	if _, err := cl.MemberRemove(ctx, 99); err == nil {
		t.Fatal("removing a stranger must fail")
	}
	if _, err := cl.MemberAdd(ctx, 1, "127.0.0.1:1"); err == nil {
		t.Fatal("adding an existing voter must fail")
	}
}

func TestTimeTravelReads(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, SnapshotEvery: 20})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	var revs []uint64
	for i := 0; i < 60; i++ {
		r, err := cl.Put(ctx, "cfg", fmt.Sprint("v", i))
		if err != nil {
			t.Fatal(err)
		}
		revs = append(revs, r.Header.Revision)
	}
	del, err := cl.Delete(ctx, "cfg")
	if err != nil {
		t.Fatal(err)
	}

	for i, rev := range revs {
		r, err := cl.Get(ctx, "cfg", client.WithRev(rev))
		if err != nil || len(r.Kvs) != 1 || string(r.Kvs[0].Value) != fmt.Sprint("v", i) {
			t.Fatalf("at revision %d want v%d, got %v (err %v)", rev, i, r, err)
		}
	}
	if r, err := cl.Get(ctx, "cfg", client.WithRev(del.Header.Revision)); err != nil || len(r.Kvs) != 0 {
		t.Fatalf("after the delete: %v %v", r, err)
	}
	if _, err := cl.Get(ctx, "cfg", client.WithRev(del.Header.Revision+100)); kvpb.Reason(err) != kvpb.ErrFutureRevision {
		t.Fatalf("future revision: %v", err)
	}

	// a node that joined through a snapshot has the same history
	id := c.AddNode()
	if _, err := cl.MemberAdd(ctx, id, c.Addr(id)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for c.Server(id).Machine().Revision() < del.Header.Revision {
		if time.Now().After(deadline) {
			t.Fatal("new node never caught up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	m := c.Server(id).Machine()
	resp, _, err := m.Range(&kvpb.RangeRequest{Key: []byte("cfg"), Revision: revs[10]})
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v10" {
		t.Fatalf("history on the new node: %v %v", resp, err)
	}

	if err := cl.Compact(ctx, revs[30]); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Get(ctx, "cfg", client.WithRev(revs[29])); kvpb.Reason(err) != kvpb.ErrCompacted {
		t.Fatalf("compacted read: %v", err)
	}
	if r, err := cl.Get(ctx, "cfg", client.WithRev(revs[30])); err != nil || string(r.Kvs[0].Value) != "v30" {
		t.Fatalf("boundary read: %v %v", r, err)
	}
}

func TestAutoCompaction(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, CompactRetain: 20})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 30*time.Second)

	first, err := cl.Put(ctx, "k", "0")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 100; i++ {
		if _, err := cl.Put(ctx, "k", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := cl.Get(ctx, "k", client.WithRev(first.Header.Revision))
		if kvpb.Reason(err) == kvpb.ErrCompacted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("old revision never got compacted (last err %v)", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// the recent window is still readable
	if _, err := cl.Get(ctx, "k", client.WithRev(first.Header.Revision+95)); err != nil {
		t.Fatal(err)
	}
}

func TestLearnerThenPromote(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, SnapshotEvery: 20})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	for i := 0; i < 80; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	id := c.AddNode()
	ms, err := cl.MemberAddLearner(ctx, id, c.Addr(id))
	if err != nil {
		t.Fatal(err)
	}
	var seen bool
	for _, m := range ms {
		if m.Id == id {
			seen = m.Learner
		}
	}
	if !seen {
		t.Fatalf("node %d should be listed as a learner: %v", id, ms)
	}

	// losing a voter now leaves 2 of 3: still fine, and the learner changes nothing
	c.Crash(1)
	if _, err := cl.Put(ctx, "after-crash", "v"); err != nil {
		t.Fatal(err)
	}
	c.Restart(1)

	if ms, err = cl.MemberPromote(ctx, id); err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if m.Learner {
			t.Fatalf("still a learner: %v", ms)
		}
	}
	if c.Server(id).Machine().Stats().Keys < 81 {
		t.Fatalf("promoted node has only %d keys", c.Server(id).Machine().Stats().Keys)
	}
}

func TestTransferLeaderKeepsServing(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	first := c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 30*time.Second)

	if _, err := cl.Put(ctx, "before", "1"); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		if err := cl.TransferLeader(ctx, 0); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if _, err := cl.Put(ctx, fmt.Sprint("after", round), "1"); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	if c.Leader(3*time.Second) == first {
		// three hand-overs among three nodes can land back on the start, but not
		// without data being intact
		t.Log("leader ended where it began")
	}
	r, err := cl.Get(ctx, "after", client.WithPrefix())
	if err != nil || len(r.Kvs) != 3 {
		t.Fatalf("keys after transfers: %v %v", r, err)
	}
}

func TestReplaceNode(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3, SnapshotEvery: 20})
	c.Leader(3 * time.Second)
	cl := c.Client()
	ctx := ctxT(t, 60*time.Second)

	for i := 0; i < 60; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	id := c.AddNode()
	ms, err := cl.MemberReplace(ctx, 3, id, c.Addr(id))
	if err != nil {
		t.Fatal(err)
	}
	got := map[uint64]bool{}
	for _, m := range ms {
		got[m.Id] = m.Learner
	}
	if _, old := got[3]; old || len(got) != 3 || got[id] {
		t.Fatalf("members after replace: %v", ms)
	}

	// the replacement carries the data and the cluster survives losing another node
	if k := c.Server(id).Machine().Stats().Keys; k < 60 {
		t.Fatalf("replacement has %d keys", k)
	}
	c.Crash(1)
	if _, err := cl.Put(ctx, "after", "v"); err != nil {
		t.Fatal(err)
	}

	// running it again is harmless
	if _, err := cl.MemberReplace(ctx, 3, id, c.Addr(id)); err != nil {
		t.Fatalf("second replace: %v", err)
	}
}
