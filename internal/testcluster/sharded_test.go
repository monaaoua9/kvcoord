package testcluster_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/client"
	"github.com/monaaoua9/kvcoord/internal/testcluster"
)

func newShards(t *testing.T, n int) (*client.Sharded, []*testcluster.Cluster) {
	t.Helper()
	var cs []*testcluster.Cluster
	var eps [][]string
	for i := 0; i < n; i++ {
		c := testcluster.New(t, testcluster.Options{Nodes: 3, Seed: uint64(i + 1)})
		c.Leader(5 * time.Second)
		cs = append(cs, c)
		eps = append(eps, c.Endpoints())
	}
	sc, err := client.NewSharded(eps, client.Config{RequestTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.Close)
	return sc, cs
}

func TestShardedRoutingAndFanOut(t *testing.T) {
	sc, cs := newShards(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const n = 90
	for i := 0; i < n; i++ {
		if _, err := sc.Put(ctx, fmt.Sprintf("user/%03d", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}

	// every key sits on exactly one shard, and that shard is the one For() names
	total := 0
	for i, c := range cs {
		keys := 0
		for _, id := range c.IDs() {
			if s := c.Server(id); s != nil {
				keys = s.Machine().Stats().Keys
				break
			}
		}
		if keys == 0 || keys == n {
			t.Fatalf("shard %d holds %d of %d keys, the spread is broken", i, keys, n)
		}
		total += keys
	}
	if total != n {
		t.Fatalf("shards hold %d keys in total, want %d", total, n)
	}
	r, err := sc.For("user/007").Get(ctx, "user/007")
	if err != nil || len(r.Kvs) != 1 {
		t.Fatalf("key is not on the shard that owns it: %v %v", r, err)
	}

	// a prefix read gathers everything, in order, and honours the limit
	all, err := sc.Get(ctx, "user/", client.WithPrefix())
	if err != nil || len(all.Kvs) != n {
		t.Fatalf("prefix get: %d keys, %v", len(all.Kvs), err)
	}
	for i := 1; i < len(all.Kvs); i++ {
		if string(all.Kvs[i-1].Key) >= string(all.Kvs[i].Key) {
			t.Fatal("merged result is not sorted")
		}
	}
	lim, _ := sc.Get(ctx, "user/", client.WithPrefix(), client.WithLimit(10))
	if len(lim.Kvs) != 10 || !lim.More || string(lim.Kvs[0].Key) != "user/000" {
		t.Fatalf("limit: %d keys more=%v", len(lim.Kvs), lim.More)
	}
	cnt, _ := sc.Get(ctx, "user/", client.WithPrefix(), client.WithCountOnly())
	if cnt.Count != n {
		t.Fatalf("count %d", cnt.Count)
	}

	del, err := sc.Delete(ctx, "user/0", client.WithPrefix())
	if err != nil || del.Deleted != 90 {
		t.Fatalf("range delete: %v %v", del, err)
	}
}

func TestShardedTxn(t *testing.T) {
	sc, _ := newShards(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := sc.Put(ctx, "cfg", "blue"); err != nil {
		t.Fatal(err)
	}
	r, err := sc.Txn().If(client.CmpValue("cfg", "blue")).Then(client.OpPut("cfg", "green")).Commit(ctx)
	if err != nil || !r.Succeeded {
		t.Fatalf("single-shard txn: %v %v", r, err)
	}

	// find two keys that live on different shards
	other := ""
	for i := 0; i < 100 && other == ""; i++ {
		if k := fmt.Sprint("probe", i); sc.ShardOf(k) != sc.ShardOf("cfg") {
			other = k
		}
	}
	_, err = sc.Txn().If(client.CmpValue("cfg", "green")).Then(client.OpPut(other, "x")).Commit(ctx)
	if !errors.Is(err, client.ErrCrossShard) {
		t.Fatalf("cross-shard txn: %v", err)
	}
	if g, _ := sc.Get(ctx, other); len(g.Kvs) != 0 {
		t.Fatal("a rejected transaction must not have written anything")
	}
}

func TestShardedWatchAndOneShardDown(t *testing.T) {
	sc, cs := newShards(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wctx, stop := context.WithCancel(ctx)
	defer stop()
	ch := sc.Watch(wctx, "w/", client.WithPrefix())
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 20; i++ {
		if _, err := sc.Put(ctx, fmt.Sprintf("w/%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	seen := 0
	for seen < 20 {
		select {
		case ev := <-ch:
			if ev.Err != nil {
				t.Fatal(ev.Err)
			}
			seen += len(ev.Events)
		case <-time.After(10 * time.Second):
			t.Fatalf("watch delivered only %d of 20 events", seen)
		}
	}

	// shards fail independently: break shard 0 entirely, shard 1 keeps working
	for _, id := range cs[0].IDs() {
		cs[0].Crash(id)
	}
	live := ""
	for i := 0; live == ""; i++ {
		if k := fmt.Sprint("live", i); sc.ShardOf(k) == 1 {
			live = k
		}
	}
	if _, err := sc.Put(ctx, live, "still works"); err != nil {
		t.Fatalf("healthy shard failed while another was down: %v", err)
	}
}
