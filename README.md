# kvcoord

[![ci](https://github.com/monaaoua9/kvcoord/actions/workflows/ci.yml/badge.svg)](https://github.com/monaaoua9/kvcoord/actions/workflows/ci.yml)

A distributed, strongly consistent key-value and coordination store written in Go.
Raft consensus, write-ahead log and snapshots, leases, distributed locks with
fencing tokens, gRPC watch streams, and linearizable reads that are checked by a
linearizability checker under fault injection.

Everything on the consensus path is implemented in this repository; the only
runtime dependencies are gRPC, protobuf and a B-tree.

## What is in it

* **Raft from scratch** (`internal/raft`): leader election with PreVote and
  CheckQuorum, term management, pipelined log replication with flow control,
  fast log backtracking, snapshot transfer, ReadIndex reads.
* **Durability** (`internal/wal`): CRC-protected segmented WAL, hard state,
  atomic snapshot files, torn-write recovery, log compaction.
* **State machine** (`internal/fsm`): ordered KV with revisions, compare-and-swap
  transactions, prefix ranges, leases, locks with FIFO queues, exactly-once
  client sessions, bounded event history.
* **Leases and locks**: TTL leases that delete their keys atomically, locks
  owned by leases, automatic hand-off to the next waiter, monotonically
  increasing fencing tokens.
* **Watches**: gRPC bidirectional streaming, historical replay from a revision,
  automatic resume in the client, bounded queues with explicit overflow.
* **Operations**: dynamic membership (single-server changes, non-voting learners,
  catch-up-gated promotion), leader transfer with a graceful hand-off on SIGTERM,
  MVCC time-travel reads with compaction, snapshots that do not stall apply.
* **Linearizability**: reads via ReadIndex, a Wing-Gong checker, and chaos tests
  that partition, crash and degrade a live cluster while recording histories. A second chaos test
  checks lock mutual exclusion and fencing-token order under the same faults, and a
  third adds and removes nodes while clients run.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the write pipeline, the
determinism rules, the durability design and the failure model, and
[docs/DESIGN.md](docs/DESIGN.md) for a shorter list of the decisions behind them.

## Layout

| path | what |
|---|---|
| `cmd/kvd` | the server (one process per node) |
| `cmd/kvctl` | command line client: kv, leases, locks, watch, membership |
| `cmd/kvbench` | load generator |
| `client` | Go client: retries, sessions, watch resume, `Sharded` |
| `api/proto` | the gRPC and log-entry definitions |
| `internal/raft` | consensus |
| `internal/wal` | write-ahead log and snapshot files |
| `internal/fsm` | the replicated state machine |
| `internal/server`, `internal/transport` | gRPC services and node-to-node transport |
| `internal/linearize`, `internal/testcluster` | checker, fault-injecting in-process cluster, chaos tests |
| `test/e2e` | real processes with hard kills |
| `docs` | architecture, design decisions, benchmarks |

Requires Go 1.27 (see `go.mod`) and, to regenerate the protobuf code, `protoc`.

## Build and test

```
go build -o bin/ ./cmd/...
go test ./...                 # unit + in-process cluster tests
go test -race ./...           # what CI runs
make chaos                    # longer nemesis runs (minutes)
make e2e                      # real kvd processes, SIGKILL, restarts
```

## Run a three node cluster

```
bin/kvd -id=1 -peers=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data-dir=./data/1
bin/kvd -id=2 -peers=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data-dir=./data/2
bin/kvd -id=3 -peers=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data-dir=./data/3
```

or `make cluster` (Docker Compose, with `/metrics` on ports 9101-9103).

```
export KVCTL_ENDPOINTS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003

kvctl status
kvctl put config/mode blue
kvctl get config/ -prefix
kvctl get config/mode -rev 12            # the value as of revision 12
kvctl compact 12                       # forget history below revision 12
kvctl cas config/mode blue green
kvctl watch config/ -prefix            # streams changes, resumes across failovers

kvctl lease grant 10s                  # lease 42 granted
kvctl put session/abc 1 -lease 42      # deleted when the lease expires
kvctl lock deploy -- ./run-migration.sh   # one holder at a time, $KV_FENCING_TOKEN is set
```

## Go client

One cluster is one Raft group. To go beyond one node's memory, `client.NewSharded` routes keys
across several clusters (static hash sharding, no cross-shard transactions; see the
architecture doc).

```go
cl, _ := client.New(client.Config{Endpoints: []string{"10.0.0.1:7001", "10.0.0.2:7001", "10.0.0.3:7001"}})
defer cl.Close()

cl.Put(ctx, "jobs/leader", "worker-7")                 // exactly-once, even across retries
resp, _ := cl.Get(ctx, "jobs/leader")                  // linearizable
cl.Txn().If(client.CmpValue("jobs/leader", "worker-7")).
	Then(client.OpPut("jobs/leader", "worker-8")).Commit(ctx)

sess, _ := cl.NewSession(ctx, 10*time.Second)          // lease kept alive in the background
mu := client.NewMutex(sess, "deploy")
token, _ := mu.Lock(ctx)                               // pass `token` to the resource you protect
defer mu.Unlock(ctx)

for ev := range cl.Watch(ctx, "jobs/", client.WithPrefix()) { ... }
```

Errors carry stable reason codes (`kvpb.Reason(err)`): `NOT_LEADER`, `LEASE_NOT_FOUND`,
`SESSION_EXPIRED`, `COMPACTED`, `WATCH_OVERFLOW`, ... Clients switch on those, never on messages.

## Performance

`kvbench` against three real `kvd` processes on one Windows 11 laptop (loopback, fsync on,
128 B values, 64 clients, 8 s per run). One machine means no real network or independent disks,
so read these as a floor for the code path, not as cluster capacity.

| workload | ops/s | p50 | p99 |
|---|---|---|---|
| put | 6,561 | 8.8 ms | 33 ms |
| linearizable get | 21,785 | 2.5 ms | 8.7 ms |
| 50/50 mixed | 7,850 | 7.6 ms | 25 ms |
| serializable get (any node) | 38,788 | 1.4 ms | 4.9 ms |

The same workload against the `deploy/docker-compose.yml` cluster (three containers, separate network
namespaces and volumes, Docker Desktop on the same machine, client on the host through published
ports) is slower, mostly Docker Desktop's port forwarding and VM disk:

| workload | ops/s | p50 | p99 |
|---|---|---|---|
| put | 2,040 | 27.8 ms | 83.5 ms |
| linearizable get | 6,400 | 8.8 ms | 29.3 ms |
| 50/50 mixed | 1,960 | 31.0 ms | 70.3 ms |
| serializable get | 9,918 | 5.3 ms | 22.6 ms |

Still one physical machine: there is no real network latency or independent disk between nodes.

The Docker table above is from an earlier run and wasn't repeated. Commands and caveats for the loopback numbers: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

Run your own: `go run ./cmd/kvbench -endpoints=host1:7001,host2:7001,host3:7001 -mix=mixed -c 64`.

## Operating notes

* Run 3 or 5 nodes. To grow or shrink a live cluster, one node at a time:
  ```
  bin/kvd -id=4 -join -peers=4=127.0.0.1:7004 -data-dir=./data/4
  kvctl member add 4 127.0.0.1:7004
  kvctl member list
  kvctl member remove 1
  ```
  `member replace OLD NEW ADDR` does add, promote and remove in one go, and can be rerun.
  `member add` brings the node in as a non-voting learner and promotes it once it has caught up,
  so a failure right after the add cannot cost you the quorum. `add-learner` and `promote` split the two steps.
* `kvd` flags worth knowing: `-compact-retain` (how many revisions stay readable), `-snapshot-every`, `-tick`, `-election-ticks`,
  `-min-lease-ttl`, `-metrics-addr`, and `-tls-cert/-tls-key/-tls-ca` (a CA turns on mutual TLS
  for clients and for node-to-node traffic).
* Authentication is opt-in: `-auth-tokens-file` with `token role` lines (read, write, admin, peer)
  plus `-peer-token`; clients use `kvctl -token` / `KVCTL_TOKEN` or `client.Config.Token`. Use TLS
  with it. See the architecture doc for what each role may call.
* `/metrics` (Prometheus text), `/healthz`, `/readyz` on `-metrics-addr`.
* Lock and lease expiry use wall clocks on the leader. That can only affect *liveness*
  (a lease may live a little longer or shorter than its TTL). For safety, make the protected
  resource check the fencing token.
* `-unsafe-no-fsync` exists for benchmarks. Do not use it with data you care about.

## License

MIT, see [LICENSE](LICENSE).
