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

64 concurrent clients, 128-byte values, 10,000-key space, 8 s per run, 0 errors in every run. The four workloads ran in turn on one cluster, three rounds; each cell is the median of the three rounds with min–max in brackets.

| workload | ops/s | p50 | p99 |
|---|---:|---:|---:|
| put | 6,561 (6,480 – 7,035) | 8.8 ms | 33 ms (19 – 35) |
| linearizable get | 21,785 (16,308 – 23,300) | 2.5 ms | 8.7 ms (8.3 – 10.3) |
| 50/50 mixed | 7,850 (6,879 – 8,019) | 7.6 ms | 25 ms (25 – 32) |
| serializable get (any node) | 38,788 (25,491 – 42,677) | 1.4 ms | 4.9 ms (4.3 – 7.8) |

## Reading these numbers

- Writes are bound by one fsync per batch on the leader and followers. Proposals are group-committed, so 64 clients share the disk cost.
- A linearizable get costs a heartbeat round, shared by reads that arrive together. On loopback that's cheap, which is why it's only about 3× faster than a put here. Across real machines the round trip would show.
- Serializable gets skip the round and are about 1.8× faster than linearizable ones in the median.
- Reads vary the most between rounds (serializable: 25k to 43k). The load generator and all three nodes share four cores, so how the OS schedules them matters.
- The README also has a Docker Compose run from earlier, with separate network namespaces and volumes. I didn't repeat it.

## A bug these runs found

My first full pass showed `put` dropping from about 7,400 ops/s to about 180 ops/s after roughly 100,000 writes, with p50 jumping to 350 ms. It wasn't noise: it reproduced on a fresh cluster every time, and automatic compaction was not the cause (the same cliff appeared with `-compact-retain=0`).

The cause was the watch-resume history in the state machine. Once it reached `HistoryLimit` (100,000 revisions), every apply copied the whole slice to drop the oldest entry. The fix reslices instead of copying, with the same limit and the same oldest-revision bookkeeping. After the fix, four consecutive 8 s put runs on one cluster gave 7.7k, 7.3k, 6.8k and 5.9k ops/s. The numbers above are from the fixed code. The slow downward drift in that last test is not explained; I haven't looked into it.
