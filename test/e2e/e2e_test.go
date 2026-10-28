// Package e2e runs the real kvd binary: actual processes, WAL on disk, raft
// over gRPC, and hard kills instead of polite shutdowns.
package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monaaoua9/kvcoord/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var kvdBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kvd-e2e")
	if err != nil {
		panic(err)
	}
	kvdBin = filepath.Join(dir, "kvd")
	if strings.HasSuffix(os.Args[0], ".exe") {
		kvdBin += ".exe"
	}
	out, err := exec.Command("go", "build", "-o", kvdBin, "github.com/monaaoua9/kvcoord/cmd/kvd").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build kvd: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type proc struct {
	id   uint64
	cmd  *exec.Cmd
	data string
}

type cluster struct {
	t     *testing.T
	peers string
	addrs []string
	procs []*proc
	extra []string // more kvd flags
	env   []string
}

func freeAddrs(t *testing.T, n int) []string {
	var out []string
	var ls []net.Listener
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
		out = append(out, l.Addr().String())
	}
	for _, l := range ls {
		l.Close()
	}
	return out
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{t: t, addrs: freeAddrs(t, n)}
	var parts []string
	for i, a := range c.addrs {
		parts = append(parts, fmt.Sprintf("%d=%s", i+1, a))
	}
	c.peers = strings.Join(parts, ",")
	for i := range c.addrs {
		c.procs = append(c.procs, &proc{id: uint64(i + 1), data: t.TempDir()})
	}
	t.Cleanup(func() {
		for i := range c.procs {
			c.kill(i)
		}
	})
	return c
}

func (c *cluster) start(i int) {
	p := c.procs[i]
	args := []string{
		"-id", fmt.Sprint(p.id), "-peers", c.peers, "-data-dir", p.data,
		"-tick", "20ms", "-snapshot-every", "50", "-min-lease-ttl", "100ms", "-log-level", logLevel(),
	}
	cmd := exec.Command(kvdBin, append(args, c.extra...)...)
	cmd.Env = append(os.Environ(), c.env...)
	if os.Getenv("KV_E2E_VERBOSE") != "" {
	}
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	p.cmd = cmd
}

func (c *cluster) kill(i int) {
	p := c.procs[i]
	if p.cmd == nil {
		return
	}
	p.cmd.Process.Kill() // SIGKILL: no flush, no goodbye
	p.cmd.Wait()
	p.cmd = nil
}

func (c *cluster) client() *client.Client {
	cl, err := client.New(client.Config{Endpoints: c.addrs, RequestTimeout: 2 * time.Second})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

func (c *cluster) waitReady(cl *client.Client) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		if _, err := cl.Get(ctx, "probe"); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("cluster never became ready")
}

func TestClusterSurvivesHardKills(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	c := newCluster(t, 3)
	for i := range c.procs {
		c.start(i)
	}
	cl := c.client()
	c.waitReady(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	put := func(from, to int) {
		for i := from; i < to; i++ {
			if _, err := cl.Put(ctx, fmt.Sprintf("key-%04d", i), fmt.Sprintf("val-%d", i)); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
	}
	expect := func(n int) {
		t.Helper()
		r, err := cl.Get(ctx, "key-", client.WithPrefix())
		if err != nil || len(r.Kvs) != n {
			got := -1
			if r != nil {
				got = len(r.Kvs)
			}
			t.Fatalf("want %d keys, got %d (err %v)", n, got, err)
		}
		for i, kv := range r.Kvs {
			if want := fmt.Sprintf("val-%d", i); string(kv.Value) != want {
				t.Fatalf("key %s = %q, want %q", kv.Key, kv.Value, want)
			}
		}
	}

	put(0, 200) // enough to cross several snapshots
	expect(200)

	// kill one node, keep writing, bring it back: it has to catch up
	c.kill(2)
	put(200, 300)
	c.start(2)
	put(300, 320)

	// now take everything down at once and bring it back from disk alone
	c.kill(0)
	c.kill(1)
	c.kill(2)
	for i := range c.procs {
		c.start(i)
	}
	c.waitReady(cl)
	expect(320)

	// writes still work after the full restart, and survive another kill
	put(320, 340)
	c.kill(0)
	put(340, 350)
	expect(350)
}

func TestLockAcrossProcessRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	c := newCluster(t, 3)
	for i := range c.procs {
		c.start(i)
	}
	cl := c.client()
	c.waitReady(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	holder, err := cl.NewSession(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m := client.NewMutex(holder, "job")
	tok1, err := m.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// bounce the whole cluster; the lease and lock are in the replicated log
	for i := range c.procs {
		c.kill(i)
	}
	for i := range c.procs {
		c.start(i)
	}
	c.waitReady(cl)

	other, err := cl.NewSession(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m2 := client.NewMutex(other, "job")
	if ok, _, err := m2.TryLock(ctx); err != nil || ok {
		t.Fatalf("lock was free after restart while its holder is alive: ok=%v err=%v", ok, err)
	}
	if err := m.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	tok2, err := m2.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tok2 <= tok1 {
		t.Fatalf("fencing token went backwards across restart: %d -> %d", tok1, tok2)
	}
}

func TestJoinNewProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	c := newCluster(t, 3)
	for i := range c.procs {
		c.start(i)
	}
	cl := c.client()
	c.waitReady(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 120; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("j-%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	addr := freeAddrs(t, 1)[0]
	joiner := &proc{id: 4, data: t.TempDir()}
	c.procs = append(c.procs, joiner)
	startJoiner := func() {
		cmd := exec.Command(kvdBin, "-id", "4", "-join", "-peers", "4="+addr, "-data-dir", joiner.data,
			"-tick", "20ms", "-snapshot-every", "50", "-min-lease-ttl", "100ms", "-log-level", logLevel())
		if os.Getenv("KV_E2E_VERBOSE") != "" {
			cmd.Stderr = os.Stderr
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		joiner.cmd = cmd
	}
	startJoiner()

	// learner first, promoted once it has the log, so losing a node right after is safe
	if _, err := cl.MemberAdd(ctx, 4, addr); err != nil {
		t.Fatal(err)
	}

	// with 4 voters we can lose one; take out an original node and keep going
	c.kill(0)
	for i := 120; i < 160; i++ {
		if _, err := cl.Put(ctx, fmt.Sprintf("j-%03d", i), "v"); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// the joiner must remember the cluster after a hard restart
	c.kill(3)
	startJoiner()
	c.start(0)
	c.waitReady(cl)
	ms, err := cl.Members(ctx)
	if err != nil || len(ms) != 4 {
		t.Fatalf("members = %v, err %v", ms, err)
	}
	r, err := cl.Get(ctx, "j-", client.WithPrefix())
	if err != nil || len(r.Kvs) != 160 {
		t.Fatalf("want 160 keys, got %v (err %v)", r, err)
	}
}

func logLevel() string {
	if v := os.Getenv("KV_E2E_LOGLEVEL"); v != "" {
		return v
	}
	return "warn"
}

func TestAuthenticatedCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	const (
		reader = "e2e-read-token-0000"
		writer = "e2e-write-token-000"
		peer   = "e2e-peer-token-0000"
	)
	tokens := filepath.Join(t.TempDir(), "tokens")
	os.WriteFile(tokens, []byte(reader+" read\n"+writer+" write\n"+peer+" peer\n"), 0o600)

	c := newCluster(t, 3)
	c.extra = []string{"-auth-tokens-file", tokens}
	c.env = []string{"KVD_PEER_TOKEN=" + peer}
	for i := range c.procs {
		c.start(i)
	}

	mk := func(tok string) *client.Client {
		cl, err := client.New(client.Config{Endpoints: c.addrs, RequestTimeout: 2 * time.Second, Token: tok})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cl.Close() })
		return cl
	}
	w, r, anon := mk(writer), mk(reader), mk("")

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for {
		// the nodes can only elect a leader if their peer tokens are accepted
		if _, err := w.Put(ctx, "k", "v"); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("authenticated cluster never became writable")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if resp, err := r.Get(ctx, "k"); err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("reader: %v", err)
	}
	if _, err := r.Put(ctx, "k", "x"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reader write: %v", err)
	}
	if _, err := anon.Get(ctx, "k"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("anonymous read: %v", err)
	}
}
