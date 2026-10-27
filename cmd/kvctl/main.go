package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/client"
	"github.com/monaaoua9/kvcoord/internal/tlsutil"
)

const usage = `usage: kvctl [global flags] <command> [args]

commands:
  put KEY VALUE [-lease ID]
  get KEY [-prefix] [-serializable] [-limit N] [-rev N]
  compact REV                   drop history below REV
  del KEY [-prefix]
  cas KEY EXPECTED NEW          set NEW only if the current value is EXPECTED
  watch KEY [-prefix] [-rev N]
  lease grant TTL | keepalive ID | revoke ID | ttl ID
  lock NAME [-ttl D] -- CMD...  run CMD while holding the lock (KV_FENCING_TOKEN is set)
  member list | add ID ADDR | add-learner ID ADDR | promote ID | transfer [ID] | remove ID
  status

global flags:
`

func main() {
	endpoints := flag.String("endpoints", envOr("KVCTL_ENDPOINTS", "127.0.0.1:7001"), "comma separated host:port list")
	timeout := flag.Duration("timeout", 10*time.Second, "command timeout (watch and lock ignore it)")
	tlsCert := flag.String("tls-cert", "", "client certificate")
	tlsKey := flag.String("tls-key", "", "client key")
	tlsCA := flag.String("tls-ca", "", "CA bundle, turns on TLS")
	tlsName := flag.String("tls-server-name", "", "expected server name")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	var creds credentials.TransportCredentials
	if *tlsCA != "" {
		var err error
		if creds, err = tlsutil.Client(*tlsCert, *tlsKey, *tlsCA, *tlsName); err != nil {
			die(err)
		}
	}
	cl, err := client.New(client.Config{Endpoints: strings.Split(*endpoints, ","), Creds: creds})
	if err != nil {
		die(err)
	}
	defer cl.Close()

	cmd, rest := args[0], args[1:]
	ctx, cancel := context.WithCancel(context.Background())
	if cmd != "watch" && cmd != "lock" {
		ctx, cancel = context.WithTimeout(context.Background(), *timeout)
	}
	defer cancel()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	go func() { <-sigc; cancel() }()

	switch cmd {
	case "put":
		fs := flag.NewFlagSet("put", flag.ExitOnError)
		lease := fs.Uint64("lease", 0, "attach to lease")
		pos := parse(fs, rest, 2)
		_, err = cl.Put(ctx, pos[0], pos[1], client.WithLease(*lease))
		check(err)
		fmt.Println("OK")

	case "get":
		fs := flag.NewFlagSet("get", flag.ExitOnError)
		prefix := fs.Bool("prefix", false, "match every key with this prefix")
		ser := fs.Bool("serializable", false, "allow a stale read from any node")
		limit := fs.Uint64("limit", 0, "max keys")
		rev := fs.Uint64("rev", 0, "read the store as of this revision")
		pos := parse(fs, rest, 1)
		var opts []client.Option
		if *prefix {
			opts = append(opts, client.WithPrefix())
		}
		if *ser {
			opts = append(opts, client.WithSerializable())
		}
		opts = append(opts, client.WithLimit(*limit))
		if *rev != 0 {
			opts = append(opts, client.WithRev(*rev))
		}
		resp, err := cl.Get(ctx, pos[0], opts...)
		check(err)
		for _, kv := range resp.Kvs {
			fmt.Printf("%s\n%s\n", kv.Key, kv.Value)
		}

	case "compact":
		pos := parse(flag.NewFlagSet("compact", flag.ExitOnError), rest, 1)
		r, err := strconv.ParseUint(pos[0], 10, 64)
		check(err)
		check(cl.Compact(ctx, r))
		fmt.Println("OK")

	case "del":
		fs := flag.NewFlagSet("del", flag.ExitOnError)
		prefix := fs.Bool("prefix", false, "delete every key with this prefix")
		pos := parse(fs, rest, 1)
		var opts []client.Option
		if *prefix {
			opts = append(opts, client.WithPrefix())
		}
		resp, err := cl.Delete(ctx, pos[0], opts...)
		check(err)
		fmt.Println(resp.Deleted)

	case "cas":
		pos := parse(flag.NewFlagSet("cas", flag.ExitOnError), rest, 3)
		resp, err := cl.Txn().
			If(client.CmpValue(pos[0], pos[1])).
			Then(client.OpPut(pos[0], pos[2])).
			Commit(ctx)
		check(err)
		if !resp.Succeeded {
			fmt.Println("value did not match")
			os.Exit(1)
		}
		fmt.Println("OK")

	case "watch":
		fs := flag.NewFlagSet("watch", flag.ExitOnError)
		prefix := fs.Bool("prefix", false, "watch a prefix")
		rev := fs.Uint64("rev", 0, "start revision")
		pos := parse(fs, rest, 1)
		opts := []client.Option{client.WithStartRevision(*rev), client.WithPrevKV()}
		if *prefix {
			opts = append(opts, client.WithPrefix())
		}
		for ev := range cl.Watch(ctx, pos[0], opts...) {
			if ev.Err != nil {
				die(ev.Err)
			}
			for _, e := range ev.Events {
				fmt.Printf("%s rev=%d %s %s\n", e.Type, e.Kv.ModRevision, e.Kv.Key, e.Kv.Value)
			}
		}

	case "member":
		memberCmd(ctx, cl, rest)

	case "lease":
		leaseCmd(ctx, cl, rest)

	case "lock":
		lockCmd(ctx, cl, rest)

	case "status":
		for _, ep := range cl.Endpoints() {
			st, err := cl.Status(ctx, ep)
			if err != nil {
				fmt.Printf("%-22s unreachable: %v\n", ep, err)
				continue
			}
			fmt.Printf("%-22s node=%d %-9s leader=%d term=%d commit=%d applied=%d keys=%d leases=%d locks=%d\n",
				ep, st.Header.NodeId, st.State, st.LeaderId, st.Header.Term, st.CommitIndex, st.AppliedIndex, st.Keys, st.Leases, st.Locks)
		}

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func leaseCmd(ctx context.Context, cl *client.Client, args []string) {
	if len(args) < 2 {
		die(fmt.Errorf("lease: want grant|keepalive|revoke|ttl"))
	}
	switch args[0] {
	case "grant":
		ttl, err := time.ParseDuration(args[1])
		check(err)
		id, granted, err := cl.Grant(ctx, ttl)
		check(err)
		fmt.Printf("lease %d granted, ttl %s\n", id, granted)
	case "keepalive":
		ttl, err := cl.KeepAliveOnce(ctx, mustID(args[1]))
		check(err)
		if ttl == 0 {
			die(client.ErrLeaseLost)
		}
		fmt.Println("ttl", ttl)
	case "revoke":
		check(cl.Revoke(ctx, mustID(args[1])))
		fmt.Println("OK")
	case "ttl":
		r, err := cl.TimeToLive(ctx, mustID(args[1]), true)
		check(err)
		if r.RemainingMs < 0 {
			die(client.ErrLeaseLost)
		}
		fmt.Printf("remaining %dms of %dms, %d keys\n", r.RemainingMs, r.GrantedTtlMs, len(r.Keys))
	default:
		die(fmt.Errorf("lease: unknown subcommand %q", args[0]))
	}
}

func lockCmd(ctx context.Context, cl *client.Client, args []string) {
	fs := flag.NewFlagSet("lock", flag.ExitOnError)
	ttl := fs.Duration("ttl", 10*time.Second, "session TTL, the lock is lost if we cannot renew it")
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) < 3 || rest[1] != "--" {
		die(fmt.Errorf("lock: usage: lock NAME [-ttl D] -- CMD..."))
	}
	name, argv := rest[0], rest[2:]

	sess, err := cl.NewSession(ctx, *ttl)
	check(err)
	defer sess.Close(context.Background())
	mu := client.NewMutex(sess, name)
	tok, err := mu.Lock(ctx)
	check(err)

	c := exec.CommandContext(ctx, argv[0], argv[1:]...)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	c.Env = append(os.Environ(), "KV_FENCING_TOKEN="+strconv.FormatUint(tok, 10))
	if err := c.Start(); err != nil {
		die(err)
	}
	finished := make(chan struct{})
	go func() {
		// the lease is gone so the lock is not ours anymore: stop the command
		select {
		case <-sess.Done():
			fmt.Fprintln(os.Stderr, "kvctl: lost the session, killing command")
			c.Process.Kill()
		case <-finished:
		case <-ctx.Done():
		}
	}()
	err = c.Wait()
	close(finished)
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		die(err)
	}
}

// parse lets flags appear before or after the positional arguments.
func parse(fs *flag.FlagSet, args []string, want int) []string {
	var pos, flags []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			pos = append(pos, a)
			continue
		}
		flags = append(flags, a)
		if f := fs.Lookup(strings.TrimLeft(a, "-")); f != nil && !isBool(f) && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	fs.Parse(flags)
	if len(pos) != want {
		fmt.Fprintf(os.Stderr, "kvctl %s: want %d argument(s), got %d\n", fs.Name(), want, len(pos))
		os.Exit(2)
	}
	return pos
}

func isBool(f *flag.Flag) bool {
	b, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok && b.IsBoolFlag()
}

func mustID(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		die(fmt.Errorf("bad lease id %q", s))
	}
	return v
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func check(err error) {
	if err == nil {
		return
	}
	if r := kvpb.Reason(err); r != "" {
		die(fmt.Errorf("%s: %w", r, err))
	}
	die(err)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "kvctl:", err)
	os.Exit(1)
}

func memberCmd(ctx context.Context, cl *client.Client, args []string) {
	var (
		ms  []*kvpb.Member
		err error
	)
	switch {
	case len(args) == 1 && args[0] == "list":
		ms, err = cl.Members(ctx)
	case len(args) == 3 && args[0] == "add":
		// learner first, promoted once it has caught up
		ms, err = cl.MemberAdd(ctx, mustID(args[1]), args[2])
	case len(args) == 3 && args[0] == "add-learner":
		ms, err = cl.MemberAddLearner(ctx, mustID(args[1]), args[2])
	case len(args) >= 1 && args[0] == "transfer":
		var target uint64
		if len(args) == 2 {
			target = mustID(args[1])
		}
		check(cl.TransferLeader(ctx, target))
		fmt.Println("OK")
		return
	case len(args) == 2 && args[0] == "promote":
		ms, err = cl.MemberPromote(ctx, mustID(args[1]))
	case len(args) == 2 && args[0] == "remove":
		ms, err = cl.MemberRemove(ctx, mustID(args[1]))
	default:
		die(fmt.Errorf("member: want list | add ID ADDR | add-learner ID ADDR | promote ID | transfer [ID] | remove ID"))
	}
	check(err)
	for _, m := range ms {
		role := ""
		if m.IsLeader {
			role = " (leader)"
		}
		if m.Learner {
			role = " (learner)"
		}
		fmt.Printf("%d %s%s\n", m.Id, m.Addr, role)
	}
}
