# Ideas backlog — Pebble engine perf

Free-form scratch. Append new ideas as bullets; mark tried ones with
status (kept / discarded / crashed) so we don't repeat them.

## To try (priority order)

- [ ] **P1.1** Memtable size 64 MiB → 256 MiB (`MemTableSize` in `options.go`).
- [ ] **P1.2** `L0CompactionThreshold` sweep: 2 → 4, 8.
- [ ] **P1.3** `MaxConcurrentCompactions` upper bound: 8 → 12 (gate on GOMAXPROCS).
- [ ] **P1.4** Enable bloom filters on L0 (FilterPolicy + FilterType).
- [ ] **P1.5** Mixed compression: Snappy at L0, zstd at L6.
- [ ] **P2.6** Per-record-type per-level options (grants vs resources).
- [ ] **P2.7** Codec codegen via `cmd/protoc-gen-batonstore` — replaces reflection path. Big change; may need human approval.
- [ ] **P3.8** Pool tuple encoder buffer (`AppendTupleString`) — kill per-record slice alloc.
- [ ] **P3.9** Larger SST block size (32 KiB → 64 KiB) — amortize header overhead.

## Tried — see jsonl for verdicts

(populated by the loop)

## Follow-up / human review

- Split-batch in PutGrantRecords (commit 63c0869b) breaks cross-batch atomicity:
  if priBatch commits but idxBatch fails, primary records exist without
  by_entitlement / by_principal index entries. Fresh-sync replays the
  whole sync from the connector so it's OK there, but incremental Put
  paths (mid-sync upserts) might leak. RFC stack-6 grant expansion path
  could be a concrete victim. Consider:
    - Apply split only when IsFreshSync() is true; keep one-batch atomic
      semantics outside fresh-sync.
    - Or: document the contract change.

## Closed axes (do NOT retry — multiple attempts confirm dead)

- **Parallel engine.Close + WriteEnvelope** (tried at #19, #28, #45 — three baselines).
  Mechanism is theoretically safe (CheckpointTo creates self-contained dir), but
  goroutine + channel coordination overhead exceeds the engine.Close wallclock
  savings (~30-50 ms). At smaller scales the overhead dominates and regresses
  10-15%. Not a clean win at any size.
- **Parallelize large heap allocations across goroutines** (#47 priBatch/idxBatch,
  #48 priBatch sub-shards). Three different attempts. Go's heap allocator
  serializes large (>32 KB) allocations through the central heap-arena mutex;
  OS mmap underneath has kernel-level locks. Concurrent 150 MB-class allocs
  from N goroutines queue serially, plus goroutine scheduling adds overhead
  proportional to N. Stick to single-goroutine allocation for the big buffers.
- **FlushSplitBytes axis** (tried 2 MiB → 16 MiB at #21, #31; 2 MiB → 64 MiB at #37).
  Pebble doesn't honor very large hints, or bigger SSTs lose write parallelism.
  All flat-to-mildly-negative across multiple baselines.
- **Tournament tree / prefix-skip merge optimizations** (#39, #40). The naive
  4-way bytes.Compare scan is already optimally branch-predictable and SIMD-tight;
  wrapping with anything in Go costs more than it saves at k=4.
- **Parallel reads for WriteEnvelope** (#43 bulk-pre-read; #46 streaming with bounded
  lookahead). Two different failure modes: #43 didn't actually overlap reads with
  writes (3 serial phases); #46 did overlap but per-file os.ReadFile allocated
  ~530 MB of one-shot buffers vs io.Copy's reused 32 KB buffer. Pebble checkpoint
  files are page-cache-hot anyway — io.Copy pulls them at memory speed, so serial
  reading is already efficient. Closed axis.
- **Background WAL fsync** (WALBytesPerSync=4MiB, #38). On this hardware fsync
  isn't a meaningful bottleneck; spreading it via background syncs doesn't help.
- **MemTableSize > 64 MiB** (#1 256 MiB, #16 128 MiB). Larger memtable lets entire
  100k workload fit in memory → no during-write flushes → forced serial flush at
  EndSync. 100k workload regresses ~30%.
- **L0CompactionThreshold ≠ 8** axis fully mapped (2/4/6/16). 8 is the knee.
- **CompactionConcurrencyRange** (#7). With L0=8 compactor isn't the bottleneck.
- **DisableAutomaticCompactions** (#20). With L0=8 it's already idle.
- **proto.MarshalAppend with SetDeferred + cached size** (#23). proto.Size
  double-traversal eats the memcpy savings.
- **appendEscaped bytes.IndexByte fast path** (#22). Tuple encoder is on the
  smaller goroutine; max(A,B) wallclock means optimizing B doesn't help when B<A.
