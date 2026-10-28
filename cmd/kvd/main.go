package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/monaaoua9/kvcoord/internal/server"
	"github.com/monaaoua9/kvcoord/internal/tlsutil"
)

func parsePeers(s string) (map[uint64]string, error) {
	peers := map[uint64]string{}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", part)
		}
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("bad peer id %q", id)
		}
		peers[n] = addr
	}
	return peers, nil
}

func main() {
	var (
		id          = flag.Uint64("id", 0, "node id (non-zero, unique)")
		peersFlag   = flag.String("peers", "", "all members: 1=host:port,2=host:port,...")
		listen      = flag.String("listen", "", "bind address, defaults to the address in -peers")
		dataDir     = flag.String("data-dir", "", "directory for the WAL and snapshots")
		snapEvery   = flag.Uint64("snapshot-every", 10000, "applied entries between snapshots")
		tick        = flag.Duration("tick", 100*time.Millisecond, "raft tick interval")
		electionTk  = flag.Int("election-ticks", 10, "ticks without a leader before campaigning")
		minTTL      = flag.Duration("min-lease-ttl", time.Second, "smallest lease TTL accepted")
		metricsAddr = flag.String("metrics-addr", "", "serve /metrics and /healthz here, empty disables")
		compactKeep = flag.Uint64("compact-retain", 100000, "revisions of history kept readable, older ones are compacted; 0 disables")
		logLevel    = flag.String("log-level", "info", "debug|info|warn|error")
		logJSON     = flag.Bool("log-json", false, "log as JSON")
		tlsCert     = flag.String("tls-cert", "", "server certificate (PEM)")
		tlsKey      = flag.String("tls-key", "", "server key (PEM)")
		tlsCA       = flag.String("tls-ca", "", "CA bundle, turns on mutual TLS")
		tlsName     = flag.String("tls-server-name", "", "expected server name when dialing peers")
		join        = flag.Bool("join", false, "start empty and wait to be added with kvctl member add; -peers needs only this node")
		authFile    = flag.String("auth-tokens-file", "", "lines of 'token role' (read, write, admin, peer); turns authentication on")
		peerToken   = flag.String("peer-token", os.Getenv("KVD_PEER_TOKEN"), "token this node presents to the other nodes, needs role peer there")
		noSync      = flag.Bool("unsafe-no-fsync", false, "skip fsync: data loss on power failure, benchmarks only")
	)
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		fatal(err)
	}
	hopts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, hopts)
	if *logJSON {
		h = slog.NewJSONHandler(os.Stderr, hopts)
	}
	logger := slog.New(h)

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		fatal(err)
	}

	var tokens map[string]string
	if *authFile != "" {
		if tokens, err = server.LoadTokenFile(*authFile); err != nil {
			fatal(err)
		}
		if *peerToken == "" {
			fatal(fmt.Errorf("-auth-tokens-file needs -peer-token (or KVD_PEER_TOKEN) so the nodes can talk to each other"))
		}
	}

	cfg := server.Config{
		NodeID: *id, Peers: peers, Join: *join, Listen: *listen, DataDir: *dataDir, NoSync: *noSync,
		TickInterval: *tick, ElectionTick: *electionTk, SnapshotEvery: *snapEvery,
		MinLeaseTTL: *minTTL, CompactRetain: *compactKeep, Logger: logger,
		AuthTokens: tokens, PeerToken: *peerToken,
	}
	if *tlsCert != "" {
		var srvCreds, peerCreds credentials.TransportCredentials
		if srvCreds, err = tlsutil.Server(*tlsCert, *tlsKey, *tlsCA); err != nil {
			fatal(err)
		}
		if peerCreds, err = tlsutil.Client(*tlsCert, *tlsKey, *tlsCA, *tlsName); err != nil {
			fatal(err)
		}
		cfg.ServerCreds, cfg.PeerCreds = srvCreds, peerCreds
	}

	srv, err := server.New(cfg)
	if err != nil {
		fatal(err)
	}
	if err := srv.Start(); err != nil {
		fatal(err)
	}

	var httpSrv *http.Server
	if *metricsAddr != "" {
		httpSrv = &http.Server{Addr: *metricsAddr, Handler: srv.HTTPHandler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
				logger.Error("metrics listener", "err", err)
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	// a leader that just disappears costs clients an election timeout; hand over first
	ctx, cancelHand := context.WithTimeout(context.Background(), 3*time.Second)
	if err := srv.StepDown(ctx, 0); err == nil {
		logger.Info("handed leadership over")
	}
	cancelHand()
	if httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		httpSrv.Shutdown(ctx)
		cancel()
	}
	srv.Stop()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "kvd:", err)
	os.Exit(1)
}
