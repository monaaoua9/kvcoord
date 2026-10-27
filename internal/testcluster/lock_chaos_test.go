package testcluster_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/client"
	"github.com/monaaoua9/kvcoord/internal/raft"
	"github.com/monaaoua9/kvcoord/internal/testcluster"
)

// Workers fight over one lock while the nemesis breaks the cluster. Leases are
// long enough that none expires, so two holders at once or a fencing token that
// goes backwards are real bugs, not clock noise.
func TestChaosLockMutualExclusion(t *testing.T) {
	const seed = 77
	dur := envDuration("KV_CHAOS_DURATION", 6*time.Second)
	c := testcluster.New(t, testcluster.Options{Nodes: 3, Seed: seed, SnapshotEvery: 40})
	c.Leader(5 * time.Second)

	ctx, stop := context.WithTimeout(context.Background(), dur)
	defer stop()

	var (
		inside     atomic.Int32
		violations atomic.Int32
		grants     atomic.Int64
		mu         sync.Mutex
		maxTok     uint64
		maxWorker  = -1
	)
	checkToken := func(w int, tok uint64) {
		mu.Lock()
		defer mu.Unlock()
		// the same grant can be handed back to the same worker after an
		// ambiguous failure, anything else must be strictly newer
		if tok < maxTok || (tok == maxTok && w != maxWorker) {
			violations.Add(1)
			t.Errorf("worker %d got token %d after %d (held by %d)", w, tok, maxTok, maxWorker)
		}
		maxTok, maxWorker = tok, w
	}

	var sessions []*client.Session
	var smu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := c.Client()
			sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			sess, err := cl.NewSession(sctx, 30*time.Second)
			cancel()
			if err != nil {
				return
			}
			smu.Lock()
			sessions = append(sessions, sess)
			smu.Unlock()
			mx := client.NewMutex(sess, "the-lock")
			rng := rand.New(rand.NewPCG(seed, uint64(w)))

			for ctx.Err() == nil {
				lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				tok, err := mx.Lock(lctx)
				cancel()
				if err != nil {
					continue
				}
				if inside.Add(1) != 1 {
					violations.Add(1)
					t.Errorf("worker %d entered while somebody else was inside", w)
				}
				grants.Add(1)
				checkToken(w, tok)
				time.Sleep(time.Duration(rng.IntN(6)) * time.Millisecond)
				inside.Add(-1)

				uctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				mx.Unlock(uctx)
				cancel()
			}
		}()
	}

	nem := &nemesis{c: c, rng: rand.New(rand.NewPCG(seed, 999)), down: map[raft.NodeID]bool{}, maxD: 1, log: t.Logf}
	for ctx.Err() == nil {
		nem.step(ctx)
	}
	wg.Wait()
	nem.healAll()
	c.Leader(10 * time.Second)

	for _, s := range sessions {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.Close(cctx)
		cancel()
	}
	t.Logf("%d grants, max token %d, %d violations", grants.Load(), maxTok, violations.Load())
	if grants.Load() < 20 {
		t.Fatalf("only %d grants, the test did not exercise anything", grants.Load())
	}
	assertReplicasIdentical(t, c)
}
