package testcluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/client"
	"github.com/monaaoua9/kvcoord/internal/linearize"
	"github.com/monaaoua9/kvcoord/internal/testcluster"
)

// Serializable reads are allowed to be stale, so a history that contains
// one must be flagged by the checker. This keeps the harness honest: if it
// ever stops catching this, a green chaos run means nothing.
func TestCheckerCatchesStaleFollowerRead(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{Nodes: 3})
	leader := c.Leader(3 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl := c.Client()
	rec := linearize.NewRecorder()

	put := func(v string) {
		h := rec.Begin(1, linearize.KVInput{Kind: linearize.KindPut, Key: "k", Value: v})
		if _, err := cl.Put(ctx, "k", v); err != nil {
			t.Fatal(err)
		}
		h.End(linearize.KVOutput{})
	}
	put("old")

	var victim uint64
	for _, id := range c.IDs() {
		if id != leader {
			victim = id
		}
	}
	// wait until the victim has replicated "old", then cut it off
	only, _ := client.New(client.Config{Endpoints: []string{c.Addr(victim)}, RequestTimeout: time.Second})
	defer only.Close()
	for {
		if r, err := only.Get(ctx, "k", client.WithSerializable()); err == nil && len(r.Kvs) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Net.Isolate(victim)
	put("new")

	h := rec.Begin(2, linearize.KVInput{Kind: linearize.KindGet, Key: "k"})
	r, err := only.Get(ctx, "k", client.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	h.End(linearize.KVOutput{Found: true, Value: string(r.Kvs[0].Value)})

	if string(r.Kvs[0].Value) != "old" {
		t.Fatalf("expected a stale read from the isolated follower, got %q", r.Kvs[0].Value)
	}
	res := linearize.Check(ctx, linearize.KVModel(), rec.History())
	if res.Outcome != linearize.Illegal {
		t.Fatalf("checker accepted a stale read: %v", res.Outcome)
	}
}
