package testcluster_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/client"
	"github.com/monaaoua9/kvcoord/internal/linearize"
	"github.com/monaaoua9/kvcoord/internal/raft"
	"github.com/monaaoua9/kvcoord/internal/testcluster"
)

func envDuration(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

type nemesis struct {
	churn bool
	c     *testcluster.Cluster
	rng   *rand.Rand
	down  map[raft.NodeID]bool
	maxD  int
	log   func(string, ...any)
}

func (n *nemesis) pickUp() raft.NodeID {
	var up []raft.NodeID
	for _, id := range n.c.Members() {
		if !n.down[id] {
			up = append(up, id)
		}
	}
	return up[n.rng.IntN(len(up))]
}

func (n *nemesis) step(ctx context.Context) {
	sleep := func(lo, hi int) {
		d := time.Duration(lo+n.rng.IntN(hi-lo+1)) * time.Millisecond
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
	}
	ids := n.c.Members()
	choice := n.rng.IntN(6)
	if n.churn && choice == 5 {
		n.reshape(ctx)
		return
	}
	switch choice {
	case 0: // split the cluster into two random halves
		perm := n.rng.Perm(len(ids))
		cut := 1 + n.rng.IntN(len(ids)-1)
		var a, b []raft.NodeID
		for i, p := range perm {
			if i < cut {
				a = append(a, ids[p])
			} else {
				b = append(b, ids[p])
			}
		}
		n.log("partition %v | %v", a, b)
		n.c.Net.Partition(a, b)
		sleep(300, 900)
		n.c.Net.Heal()
	case 1: // cut off whoever is leading
		for _, id := range ids {
			if s := n.c.Server(id); s != nil && s.Node().IsLeader() {
				n.log("isolate leader %d", id)
				n.c.Net.Isolate(id)
				sleep(400, 1000)
				n.c.Net.Heal()
				return
			}
		}
	case 2, 3: // crash a node, bring it back later
		if len(n.down) >= n.maxD {
			for id := range n.down {
				n.log("restart %d", id)
				n.c.Restart(id)
				delete(n.down, id)
				break
			}
			return
		}
		id := n.pickUp()
		n.log("crash %d", id)
		n.c.Crash(id)
		n.down[id] = true
		sleep(200, 700)
		n.log("restart %d", id)
		n.c.Restart(id)
		delete(n.down, id)
	case 4: // flaky links
		n.log("flaky network")
		n.c.Net.SetLoss(0.05 + n.rng.Float64()*0.2)
		n.c.Net.SetDuplicate(0.1)
		n.c.Net.SetLatency(0, time.Duration(n.rng.IntN(20))*time.Millisecond)
		sleep(400, 1000)
		n.c.Net.SetLoss(0)
		n.c.Net.SetDuplicate(0)
		n.c.Net.SetLatency(0, 0)
	default: // hand leadership over, or just sit quiet
		if n.rng.IntN(3) > 0 {
			for _, id := range ids {
				if s := n.c.Server(id); s != nil && s.Node().IsLeader() {
					n.log("transfer leadership away from %d", id)
					tctx, cancel := context.WithTimeout(ctx, time.Second)
					s.StepDown(tctx, 0)
					cancel()
					return
				}
			}
		}
		sleep(200, 500)
	}
}

func (n *nemesis) healAll() {
	n.c.Net.Heal()
	n.c.Net.SetLoss(0)
	n.c.Net.SetDuplicate(0)
	n.c.Net.SetLatency(0, 0)
	for _, id := range n.c.Members() {
		n.c.Restart(id)
	}
	n.down = map[raft.NodeID]bool{}
}

// reshape adds a fresh node or removes an existing voter, never dropping
// below three or going above five.
func (n *nemesis) reshape(ctx context.Context) {
	members := n.c.Members()
	cl := n.c.Client()
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if len(members) < 5 && (len(members) == 3 || n.rng.IntN(2) == 0) {
		id := n.c.AddNode()
		n.log("add node %d", id)
		if _, err := cl.MemberAdd(cctx, id, n.c.Addr(id)); err != nil {
			n.log("add node %d failed: %v", id, err)
		}
		return
	}
	if len(members) > 3 {
		id := members[n.rng.IntN(len(members))]
		if n.down[id] {
			return
		}
		n.log("remove node %d", id)
		if _, err := cl.MemberRemove(cctx, id); err != nil {
			n.log("remove node %d failed: %v", id, err)
			return
		}
		n.c.Crash(id)
	}
}

func runChaos(t *testing.T, nodes int, seed uint64, opTimeout time.Duration, churn bool) {
	dur := envDuration("KV_CHAOS_DURATION", 6*time.Second)
	workers := envInt("KV_CHAOS_WORKERS", 6)
	const keys = 4

	c := testcluster.New(t, testcluster.Options{Nodes: nodes, Seed: seed, SnapshotEvery: 40})
	c.Leader(5 * time.Second)

	rec := linearize.NewRecorder()
	ctx, stop := context.WithTimeout(context.Background(), dur)
	defer stop()

	var total, unknown atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := c.Client()
			rng := rand.New(rand.NewPCG(seed, uint64(w)+100))
			last := map[string]string{} // what this worker last saw per key
			seq := 0
			for ctx.Err() == nil {
				key := fmt.Sprintf("k%d", rng.IntN(keys))
				opCtx, cancel := context.WithTimeout(context.Background(), opTimeout)
				total.Add(1)

				switch r := rng.IntN(100); {
				case r < 40:
					h := rec.Begin(w, linearize.KVInput{Kind: linearize.KindGet, Key: key})
					resp, err := cl.Get(opCtx, key)
					if err != nil {
						break // a failed read changes nothing, leave it out of the history
					}
					out := linearize.KVOutput{}
					if len(resp.Kvs) == 1 {
						out = linearize.KVOutput{Found: true, Value: string(resp.Kvs[0].Value)}
						last[key] = out.Value
					}
					h.End(out)

				case r < 75:
					seq++
					val := fmt.Sprintf("w%d-%d", w, seq)
					h := rec.Begin(w, linearize.KVInput{Kind: linearize.KindPut, Key: key, Value: val})
					if _, err := cl.Put(opCtx, key, val); err != nil {
						unknown.Add(1)
						h.EndUnknown()
						break
					}
					last[key] = val
					h.End(linearize.KVOutput{})

				case r < 85:
					h := rec.Begin(w, linearize.KVInput{Kind: linearize.KindDelete, Key: key})
					resp, err := cl.Delete(opCtx, key)
					if err != nil {
						unknown.Add(1)
						h.EndUnknown()
						break
					}
					delete(last, key)
					h.End(linearize.KVOutput{Deleted: resp.Deleted > 0})

				default:
					seq++
					val := fmt.Sprintf("w%d-%d", w, seq)
					in := linearize.KVInput{Kind: linearize.KindCAS, Key: key, Value: val}
					var cmp *kvpb.Compare
					if exp, ok := last[key]; ok && rng.IntN(5) > 0 {
						in.Expect = exp
						cmp = client.CmpValue(key, exp)
					} else {
						in.ExpectAbsent = true
						cmp = client.CmpCreateRevision(key, kvpb.Compare_EQUAL, 0)
					}
					h := rec.Begin(w, in)
					resp, err := cl.Txn().If(cmp).Then(client.OpPut(key, val)).Commit(opCtx)
					if err != nil {
						unknown.Add(1)
						h.EndUnknown()
						break
					}
					if resp.Succeeded {
						last[key] = val
					}
					h.End(linearize.KVOutput{Success: resp.Succeeded})
				}
				cancel()
				time.Sleep(time.Duration(rng.IntN(8)) * time.Millisecond)
			}
		}()
	}

	nem := &nemesis{churn: churn, c: c, rng: rand.New(rand.NewPCG(seed, 999)), down: map[raft.NodeID]bool{}, maxD: (nodes - 1) / 2, log: t.Logf}
	for ctx.Err() == nil {
		nem.step(ctx)
	}
	wg.Wait()
	nem.healAll()
	c.Leader(10 * time.Second)

	history := rec.History()
	t.Logf("seed %d: %d ops issued, %d recorded, %d with unknown outcome", seed, total.Load(), len(history), unknown.Load())

	chk, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	res := linearize.Check(chk, linearize.KVModel(), history)
	switch res.Outcome {
	case linearize.Illegal:
		dump(t, res.Culprit)
		t.Fatalf("history is NOT linearizable (seed %d)", seed)
	case linearize.Unknown:
		t.Logf("checker ran out of budget, history inconclusive")
	}

	assertReplicasIdentical(t, c)
}

func dump(t *testing.T, ops []linearize.Op) {
	var b bytes.Buffer
	for _, o := range ops {
		ret := fmt.Sprint(o.Return)
		if o.Return == linearize.Pending {
			ret = "?"
		}
		fmt.Fprintf(&b, "  c%d [%d, %s] %+v -> %+v\n", o.Client, o.Call, ret, o.Input, o.Output)
	}
	t.Logf("offending sub-history:\n%s", b.String())
}

// assertReplicasIdentical waits for every node to apply the same prefix and
// then compares full state-machine snapshots byte for byte.
func assertReplicasIdentical(t *testing.T, c *testcluster.Cluster) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var snaps [][]byte
	for {
		snaps = snaps[:0]
		var applied []uint64
		for _, id := range c.Members() {
			m := c.Server(id).Machine()
			applied = append(applied, m.Applied())
			s, err := m.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			snaps = append(snaps, s)
		}
		same := true
		for i := 1; i < len(applied); i++ {
			if applied[i] != applied[0] || !bytes.Equal(snaps[i], snaps[0]) {
				same = false
			}
		}
		if same {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replicas never converged, applied indexes %v", applied)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestChaosLinearizable3(t *testing.T) {
	for i := 0; i < envInt("KV_CHAOS_RUNS", 1); i++ {
		runChaos(t, 3, uint64(i+1), 1500*time.Millisecond, false)
	}
}

func TestChaosLinearizable5(t *testing.T) {
	for i := 0; i < envInt("KV_CHAOS_RUNS", 1); i++ {
		runChaos(t, 5, uint64(100+i), 1500*time.Millisecond, false)
	}
}

// Short client deadlines make requests give up mid-flight, so a good share of
// writes end with an unknown outcome and the retry/dedupe paths get exercised.
func TestChaosAmbiguousWrites(t *testing.T) {
	for i := 0; i < envInt("KV_CHAOS_RUNS", 1); i++ {
		runChaos(t, 3, uint64(200+i), 60*time.Millisecond, false)
	}
}

// Nodes join and leave the cluster while clients keep hammering it.
func TestChaosMembershipChurn(t *testing.T) {
	for i := 0; i < envInt("KV_CHAOS_RUNS", 1); i++ {
		runChaos(t, 3, uint64(300+i), 800*time.Millisecond, true)
	}
}
