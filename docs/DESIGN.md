# kvcoord: design decisions

The reasoning behind the main choices. Package map, write/read pipelines and failure table: [ARCHITECTURE.md](ARCHITECTURE.md).

## Scope

A replicated, strongly consistent key-value and coordination store: ordered keys, revisions, transactions, leases, locks with fencing tokens, watches. Three or five nodes, one Raft group. The Raft implementation is written from scratch (no Raft library).

## Decisions

**Linearizable reads use ReadIndex, not leader leases.** The leader records its commit index, confirms leadership with a heartbeat round (reads arriving together share one round), waits for the FSM to apply that index and reads locally. Correctness doesn't depend on clocks. The cost is a quorum round trip per read batch; `serializable` reads skip it and can be served by any node, possibly stale.

**Exactly-once writes live in the state machine.** Retries are unavoidable. A client opens a replicated session and every write carries `(client_id, seq, ack_seq)`. The FSM stores the result per `(client_id, seq)` and returns it on replay; `ack_seq` lets it forget old ones. If the session expired mid-request the client gets `ErrSessionLost` rather than a guess.

**The FSM is deterministic.** Time comes from the command (the leader stamps it), never from `time.Now()` inside `Apply`; the replicated clock never moves back. Expiry of leases and sessions is a leader-proposed command, so all replicas agree on when it happened.

**Locks return fencing tokens.** A lock held by a paused or partitioned client is not safe on its own. Every grant carries the log index that granted it, strictly increasing per lock, so the protected resource can reject a stale holder whatever the clocks or GC pauses did.

**PreVote and CheckQuorum.** A partitioned node can't inflate terms and disrupt the cluster when it returns, and a leader cut off from the majority steps down and stops answering ReadIndex.

**WAL in checksummed segments.** `crc32c | length | type | payload`; every segment starts with the current hard state, so older segments can be deleted independently. A bad tail record in the last segment is truncated; a bad record anywhere else is corruption and the node refuses to start. A test cuts the file at every byte offset.

**Snapshots don't stall apply.** The state machine clones its B-trees copy-on-write (O(1)) and a worker serialises and fsyncs the clone while applying continues. Snapshots are still full, not incremental.

**Sharding is the client's job.** One Raft group per cluster; `client.Sharded` hashes keys across clusters. Simple, but the shard count is static and there are no cross-shard transactions.

## How it's checked

Raft tests on a simulated network (including 20% loss, duplication and reordering), a WAL test that cuts the file at every byte offset, and a chaos run where concurrent clients do put/get/delete/CAS against 3- and 5-node clusters while a nemesis partitions, crashes and restarts nodes. The recorded history is checked for linearizability per key and the replicas must end byte-identical. Removing the ReadIndex wait from the server makes that test fail, which is how I know it can detect a stale read. Fuzzing covers the WAL framing, snapshot files and arbitrary command bytes; it found one real bug (a corrupt length field could make recovery allocate gigabytes). Full list: "How it is verified" in ARCHITECTURE.md.

## Known limits

- Membership changes are one at a time (no joint consensus).
- Old revisions live in memory until compaction, so a hot key with a long retention window costs memory.
- All writes and linearizable reads go through the leader.
- Everything runs on one machine in tests, so real network latency and independent disks are not exercised.
