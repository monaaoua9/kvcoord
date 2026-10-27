# Architecture

kvcoord is a replicated, strongly consistent key-value and coordination store.
A cluster of 3 or 5 nodes keeps one ordered keyspace; every write goes through a
Raft log that was written from scratch for this project (no Raft library).

```
                         clients (client/ , kvctl)
                                   │ gRPC
        ┌──────────────────────────▼──────────────────────────┐
        │ internal/server   KV · Lease · Lock · Watch · Cluster │
        │   - turns requests into Commands (proto)              │
        │   - leader-only duties: lease expiry, session reaping │
        └───────┬───────────────────────────────┬──────────────┘
      Propose / ReadIndex                  Range / Watch (local reads)
        ┌───────▼───────────┐          ┌────────▼──────────────────┐
        │ internal/raft     │  Apply   │ internal/fsm              │
        │ election, log     ├─────────►│ btree KV, revisions,      │
        │ replication,      │ Snapshot │ leases, locks, sessions,  │
        │ ReadIndex,        │ Restore  │ watch history ─► watch.Hub│
        │ snapshots         │          └───────────────────────────┘
        └──┬─────────────┬──┘
   Storage │             │ Transport
        ┌──▼──────────┐ ┌▼───────────────────┐
        │ internal/wal│ │ internal/transport │  gRPC client streams,
        │ segments,   │ │ (internal/simnet   │  snapshot chunking
        │ CRC, fsync, │ │  in tests)         │
        │ snapshots   │ └────────────────────┘
        └─────────────┘
```

## Package map

| Path | Responsibility |
|---|---|
| `api/proto`, `api/kvpb` | Public API, internal command/snapshot formats, stable error codes |
| `internal/raft` | Consensus: PreVote, CheckQuorum, pipelined replication, fast log backtracking, InstallSnapshot, ReadIndex |
| `internal/wal` | Durable `raft.Storage`: checksummed segmented log, hard state, snapshot files, torn-write recovery |
| `internal/fsm` | The replicated state machine and its determinism rules |
| `internal/watch` | Fan-out of committed events with bounded per-watcher queues |
| `internal/server` | gRPC services, proposal and read paths, leader housekeeping, `/metrics` |
| `internal/transport` | Node-to-node raft messages over gRPC streams |
| `internal/simnet` | In-process network with partitions, loss, duplication and delay |
| `internal/linearize` | Wing-Gong linearizability checker and history recorder |
| `internal/testcluster` | Whole nodes in-process, chaos (nemesis) tests |
| `client` | Go client: leader discovery, retries, sessions, watches, mutex |
| `cmd/kvd`, `cmd/kvctl` | Server binary and CLI |
| `test/e2e` | Real processes, real disks, `SIGKILL` |

## Write pipeline

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader (server + raft)
    participant F as Followers
    participant W as WAL
    participant M as FSM
    C->>L: Put(key, value, meta{client_id, seq, ack_seq})
    L->>L: Command{timestamp, put}; marshal
    L->>L: raft.Propose: append at index i, term t, register waiter(i, t)
    par replicate
        L->>W: Save(entries) + fsync        (one fsync per batch)
        L->>F: AppendEntries(prev, entries, commit)
        F->>F: log-matching check, truncate conflicts
        F->>W: Save(entries) + fsync
        F-->>L: AppendResp(match)
    end
    L->>L: quorum match >= i and term(i) == current term → commit = i
    L->>M: Apply(entry i)  (apply goroutine, in order)
    M->>M: session dedupe → revision++ → btree update → events → Hub.Publish
    M-->>L: CommandResult
    L->>L: resolve waiter(i, t)  (fails if another leader owns index i)
    L-->>C: PutResponse{header.revision}
    L->>F: next AppendEntries / heartbeat carries the new commit index
```

Details that matter:

* **Batching.** The raft event loop drains everything queued (proposals, network
  messages) before it persists and sends, so one `fsync` covers a whole batch
  and an `AppendEntries` carries many entries.
* **Persist before ack.** A follower sends its response, and a node sends a
  vote, only after `Storage.Save` has returned. Leaders count their own log
  towards the quorum only after their own `Save`.
* **Commit rule.** A leader commits an index only if a quorum holds it *and*
  the entry belongs to the leader's current term. Every new leader appends a
  no-op first so older entries get committed indirectly.
* **Waiters.** `Propose` blocks on a waiter keyed by `(index, term)`. If the
  slot is applied with a different term, the proposal was overwritten and the
  caller gets `ErrProposalDropped`; if leadership is lost, `ErrLeadershipLost`.
  Both mean "outcome unknown" and are retried by the client under the same
  `(client_id, seq)`.
* **Back-pressure.** At most `MaxUncommitted` proposals in flight, a bounded
  inbound message queue (excess is dropped, Raft tolerates loss), and the
  apply channel is non-blocking from the event loop so a slow state machine
  can never stall heartbeats.

## Read pipeline (linearizable)

Reads use **ReadIndex**, not leader leases, so correctness does not depend on
clocks.

1. The leader records `readIndex = commitIndex` (valid only once it has
   committed an entry from its own term).
2. It starts a heartbeat *round*; reads that arrive during the same loop
   iteration share one round.
3. When a quorum has acknowledged that round, the leader was still the leader
   after the read arrived.
4. The server waits until the FSM has applied `readIndex`, then reads locally.

`serializable` reads skip this and may be stale; they can be served by any node.

## Exactly-once writes

Retries are unavoidable (timeouts, leader changes), so duplicates are removed in
the state machine:

* A client opens a **session** (a replicated command; the id is the log index).
* Every mutating request carries `(client_id, seq, ack_seq)`. The FSM stores the
  result of each `(client_id, seq)` and returns it again on a replay. `ack_seq`
  tells the server it may forget everything at or below it.
* Sessions that stay idle for `SessionTTL` are reaped by a leader-proposed
  command, using the replicated clock, so all replicas agree.

If a session expired while a request was in flight the client reports
`ErrSessionLost` instead of guessing.

## Determinism rules for the FSM

Every replica must compute exactly the same thing from the same log:

* Time comes only from `Command.timestamp_ns`, set by the leader; the FSM clock
  is the running maximum, so it never goes backwards even if leaders' wall
  clocks disagree.
* Ids (sessions, leases) are log indexes. Lock fencing tokens are log indexes.
* Anything that iterates a Go map and produces ordered output sorts first.
* Expiry is not a timer inside the FSM. The leader notices an expired lease and
  *proposes* `LeaseExpire{ids}`; the FSM re-checks `deadline <= command time`,
  so a stale proposal cannot kill a lease that was just renewed.
* A new leader proposes `RefreshLeases` once it is ready, giving every lease a
  full TTL, because clients could not renew during the election.
* A test applies random command streams to two machines, one restored from the
  other's snapshot mid-way, and requires byte-identical snapshots at the end.

## Durability

`internal/wal` implements `raft.Storage`.

* **Record format:** `crc32c | length | type | payload`. Types: hard state
  (term, vote), entry, snapshot marker.
* **Segments** of 64 MiB by default. Every segment starts with the current hard
  state and snapshot marker, so earlier segments can be deleted independently.
* **Overwrites** are implicit: writing index `i` again means "drop everything
  from `i` and append". Recovery replays in order and applies that rule, so
  there is no separate truncate record to get wrong.
* **Torn writes.** On open, a bad or short record at the tail of the *last*
  segment is truncated away. A bad record anywhere else is reported as
  corruption and the node refuses to start. A test cuts the file at every byte
  offset and checks that recovery always yields a clean prefix.
* **Snapshots** are written to a temp file, `fsync`ed, renamed, and the
  directory synced; then a marker is appended and fully covered segments are
  removed. The newest snapshot that passes its checksum wins on recovery.
* **InstallSnapshot** is streamed in 1 MiB chunks by the transport and
  reassembled on the receiver, so the gRPC message limit does not cap state size.

## Leases, locks and fencing

* A lease has a TTL; keys can be attached to it and are deleted atomically (one
  revision) when it is revoked or expires.
* A **lock** is owned by a lease and has a FIFO wait queue. Releasing, revoking
  or expiring the owner's lease hands the lock to the next waiter, and dropping
  a waiter's lease removes it from the queue. `Lock` blocks on the server until
  the grant is applied (also correct across leader changes, because the grant
  event comes from the local FSM).
* **Fencing token:** every grant carries the log index that granted it, which
  strictly increases per lock. Lock expiry is time based, so a paused holder can
  wake up believing it still owns the lock; resources that check the token and
  reject stale ones stay safe regardless of clock skew or GC pauses.

## Watches

* Events are produced by the FSM under its write lock and published to the
  `watch.Hub` with the revision of the command; one command → one batch.
* Each watcher has a bounded queue. If a consumer cannot keep up it is
  cancelled with `WATCH_OVERFLOW` instead of growing memory without bound; the
  client library resumes from the last revision it delivered.
* `start_revision` replays from an in-memory history ring (default 100k
  revisions). Replay and live registration happen under one read lock so no
  event can fall between them. Asking for compacted history yields
  `COMPACTED` with the oldest available revision.
* After a snapshot restore, existing watchers are cancelled (their state moved
  under them) and resume or report `COMPACTED`.

## Failure behaviour

| Event | Result |
|---|---|
| Leader crash | Followers time out (randomised 10–20 ticks), PreVote avoids term inflation, new leader commits a no-op, clients follow `NOT_LEADER` hints |
| Minority partition | Minority side cannot commit; its leader steps down (CheckQuorum) and cannot answer ReadIndex |
| Disruptive rejoin | PreVote plus the "heard from leader recently" rule keeps a returning node from forcing elections |
| Follower far behind | Leader has compacted past it → InstallSnapshot, then normal replication |
| Torn WAL tail | Truncated on open, node rejoins and is brought up to date by the leader |
| Slow watcher | Cancelled with `WATCH_OVERFLOW`, client resumes |
| Lagging state machine | Back-pressure on proposals; heartbeats keep flowing |

## Membership

The voter set is part of the replicated log. A cluster started with `-peers`
writes one config entry per member at term 1 on a fresh log, so every node
begins with the same history. After that:

* `ConfChange{Add|Remove}` is proposed through the leader as a normal entry of
  type `EntryConfig`.
* A node switches to the new configuration when the entry is *appended*, not
  committed (Raft thesis, section 4.1). Truncating a conflicting suffix
  recomputes the config from the snapshot base plus the surviving entries.
* Only one change may be in flight: the leader refuses a new one until its own
  noop and the previous change are committed (`CONF_CHANGE_PENDING`). Single-server
  steps keep any two quorums overlapping, which is why no joint consensus is needed.
* Snapshots carry the member list (file format `KVS3`, and `snap_members` on the
  wire), so a node restored from a snapshot knows the config at that index.
* A leader that commits its own removal steps down; a removed node stops
  campaigning.
* Learners: `ConfAddLearner` makes a member that receives the log but is left out
  of every quorum (commit, elections, ReadIndex, CheckQuorum) and never campaigns.
  `ConfPromote` turns it into a voter, refused with `LEARNER_NOT_CAUGHT_UP` while
  it trails by more than 256 entries. Without this a blank node that counts for
  quorum can stall the cluster the moment another node fails.
* A new node starts with `kvd -join -peers=<self>` and waits to be contacted.
  `kvctl member add ID ADDR` adds it as a learner and promotes it when it has
  caught up; `add-learner` and `promote` do the two steps by hand. `remove` and
  `list` do the rest.

## Sharding

There is no coordinator. `client.NewSharded` takes the endpoints of N independent
clusters and sends key `k` to shard `fnv64(k) mod N`. Each shard keeps every single-cluster
guarantee for the keys it owns (linearizability, exactly-once writes, revisions, watches).

* Point put, get, delete and single-key watch go to one shard.
* Range reads, range deletes and prefix watches fan out to all shards. Results are
  merged and sorted, but they are not one snapshot: each shard answers at its own
  revision, and a range delete that fails on one shard may already have deleted on others.
* A transaction whose keys span shards fails with `ErrCrossShard` before anything is
  sent. Revision numbers only mean something within a shard.
* Locks and sessions live on one shard: use `sc.For(name)` to open the session and
  mutex on the shard that owns the lock name.
* A shard that is down only affects its own keys.

## Leader transfer

`TransferLeadership(target)` stops accepting proposals (`ErrTransferring`), brings
the target up to the last log index, then sends `TimeoutNow`. The target starts a real
election immediately, skipping PreVote, and its vote requests carry `transfer` so voters
do not ignore them because of the leader lease. If the target does not win within an
election timeout the old leader resumes (`ErrTransferTimedOut`). `kvd` does this on
SIGTERM, so a rolling restart costs clients one short election instead of a timeout;
`kvctl member transfer [ID]` triggers it by hand and the chaos nemesis uses it too.

## Revisions and time-travel reads

Every command that changes data takes one revision (commands that change nothing
take none, so watch history has no holes). Besides the live tree, each key keeps
its older versions, tombstones included, so `Range` with `revision = R` returns
the store exactly as it was after revision R.

* `R` above the current revision fails with `FUTURE_REVISION`, `R` below the
  compaction point with `COMPACTED`. Revision reads are not allowed inside a txn.
* `Compact(R)` is a normal log entry, so every replica compacts the same way. Per
  key it keeps the newest version at or below `R` (dropped if it is a tombstone)
  and everything newer, so `R` itself stays readable. Repeating a compaction is a
  no-op, which makes retries after an ambiguous failure safe.
* The leader proposes a compaction every few seconds so that only the last
  `-compact-retain` revisions (default 100000) stay readable; 0 turns that off.
* Snapshots store only the versions older than the live one (the live one is the
  tree item) plus the compaction point, so a node that catches up by snapshot has
  the same history.

## How it is verified

* `internal/raft`: election, replication, concurrent proposals, failover,
  minority leader cannot commit, snapshot catch-up, ReadIndex under partition,
  restart from storage, 20% loss + duplication + reordering.
* `internal/wal`: round trip, overwrite semantics, corruption detection,
  compaction, every-byte truncation recovery.
* `internal/fsm`: KV/Txn/lease/lock/session/watch semantics and snapshot
  equivalence on random command streams.
* `internal/linearize` + `internal/testcluster`: concurrent clients run
  put/get/delete/CAS against 3- and 5-node clusters while a nemesis partitions,
  isolates leaders, crashes and restarts nodes, and degrades the network. The
  recorded history is checked for linearizability per key, and afterwards all
  replicas must hold byte-identical state. A second run uses very short client
  deadlines so many writes have unknown outcomes. A sanity test proves the
  checker rejects a stale follower read, and removing the ReadIndex wait from the
  server makes the chaos test fail.
* Membership: learners replicate without voting (a leader plus a learner cannot
  commit with two voters), promotion waits for catch-up, the learner flag
  survives snapshots and restarts. Also: add a node that catches up through a snapshot, remove the leader,
  config surviving a restart, one change at a time; plus a chaos run where nodes
  join and leave (3 to 5 voters) under the same linearizability check.
* Sharding: keys land on exactly one shard and that shard is the one `For` names,
  merged prefix reads are sorted and honour limits, cross-shard transactions are
  rejected without side effects, and a shard that is down does not stop the others.
* Leader transfer: target catches up first, invalid and dead targets, a chaos nemesis
  action. Locks: a chaos test with five workers asserts one holder at a time and
  strictly increasing fencing tokens; breaking exclusion in the state machine makes
  it fail.
* MVCC: a model test replays 1500 random writes, deletes, lease expiries and
  compactions, remembers the full state after each revision and checks that every
  readable revision reproduces it, on the live machine and on one restored from a
  snapshot. Server tests cover time-travel reads, a node joining via snapshot and
  automatic compaction.
* `test/e2e`: the real binary, WAL on disk, raft over gRPC, `SIGKILL` of single
  nodes and of the whole cluster.

## Non-goals and known limits

* **One membership change at a time.** No joint consensus, so replacing a node
  is two changes (add, then remove). A learner that is promoted must be within
  256 entries of the leader; the client retries until it is.
* **Sharding is client-side and static.** Each cluster is one Raft group bounded by
  one node's memory; `client.Sharded` spreads keys over several clusters by hash. The
  shard count cannot change without moving keys by hand, there is no cross-shard
  transaction or snapshot read, and no rebalancing.
* **No MVCC reads.** Revisions order writes and drive watches, but only the
  latest value of a key is stored.
* **Writes and linearizable reads are served by the leader**; followers answer
  `NOT_LEADER` with a hint, or serve `serializable` reads.
* Snapshots no longer stall apply for their whole duration: the state machine
  clones its B-trees copy-on-write (O(1)) and a worker serialises and fsyncs the
  clone while applying continues. Capture still copies leases, locks and session
  caches, and compaction still walks every key. Snapshots are full, not incremental.
