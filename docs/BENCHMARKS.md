# kvcoord: benchmarks

Measured 2026-10-29.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Go 1.27.0 |

Three `kvd` processes and the load generator all run on this one laptop, over loopback, with fsync on. That means no real network latency and no independent disks, so read the numbers as a floor for the code path, not as cluster capacity.

Reproduce:

```
go build -o bin/kvd ./cmd/kvd && go build -o bin/kvbench ./cmd/kvbench
P=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
for i in 1 2 3; do bin/kvd -id=$i -peers=$P -data-dir=./data/$i & done
bin/kvbench -mix=put -c 64 -d 8s        # also: get, mixed, get-serializable
```

64 concurrent clients, 128-byte values, 10,000-key space, 8 s per run, one run each, 0 errors in every run.

| workload | ops/s | p50 | p90 | p99 |
|---|---:|---:|---:|---:|
| put | 6,500 | 9.0 ms | 13.1 ms | 24.1 ms |
| linearizable get | 16,758 | 3.3 ms | 6.0 ms | 11.3 ms |
| 50/50 mixed | 6,953 | 7.9 ms | 11.1 ms | 27.3 ms |
| serializable get (any node) | 15,004 | 3.3 ms | 7.4 ms | 17.3 ms |

## Reading these numbers

- Writes are bound by one fsync per batch on the leader and followers. Proposals are group-committed, so 64 clients share the disk cost.
- A linearizable get costs a heartbeat round, shared by reads that arrive together. On loopback that's cheap, which is why it's only about 3× faster than a put here. Across real machines the round trip would show.
- Serializable gets didn't beat linearizable ones in this run, even though they skip the round. The client in `kvbench` spreads over all three nodes and everything shares four cores, so the machine is probably the limit. I didn't dig further.
- The README has numbers from an earlier run on the same laptop (put ~3,955 ops/s) and a Docker Compose variant. They're lower than this run; I haven't tracked down why. Use this file for the current code.
- The mixed run had a 387 ms max latency, far above its p99 (27 ms). One outlier, cause not investigated.
