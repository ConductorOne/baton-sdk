# Autoresearch: Pebble engine perf

Driven by `docs/rfcs/0004-storage-engine-v4/autoresearch-pebble-perf.md` — read that doc first for full context, baselines, and rationale. This file is the operational summary.

## Objective

Optimize end-to-end write+pack and unpack+read throughput of the v3 Pebble storage engine, focused on the 1M-grant workload where LSM-internal choices (memtable size, L0 thresholds, compaction concurrency, bloom filters, compression presets, codec hot path) actually move the needle.

Baseline (commit `9676f153`, Linux arm64, `-benchtime=2x`):
- `pebble_writepack_1m_ms` ≈ **4240 ms** (primary)
- `pebble_writepack_100k_ms` ≈ 352 ms
- `pebble_readpaginated_100k_ms` ≈ 142 ms

## Metrics

- **Primary**: `pebble_writepack_1m_ms` (ms, lower is better)
- **Secondary**:
  - `pebble_writepack_100k_ms` (ms, lower)
  - `pebble_writepack_10k_ms` (ms, lower)
  - `pebble_writepack_1k_ms` (ms, lower)
  - `pebble_writepack_100_ms` (ms, lower)
  - `pebble_writepack_1m_bytes_op` (bytes, lower — memory pressure)
  - `pebble_writepack_1m_allocs_op` (allocs/op, lower)
  - `pebble_readpaginated_100k_ms` (ms, lower)
  - `pebble_readpaginated_1k_ms` (ms, lower)
  - `pebble_writegrant_solo_ns_op` (ns/op, lower — engine startup cost)
  - `codec_direct_ns_op` (ns/op, lower)
  - `codec_reflect_ns_op` (ns/op, lower)
  - `sqlite_writepack_1k_ms` (ms, lower — regression sentinel, must not get >5% worse)

## How to Run

`./autoresearch.sh` — outputs `METRIC name=value` lines plus diagnostic stdout. ~5–6 min/iteration with the default scales (100..1M).

Fast iteration mode (~1.5 min, skips 1M):
```
BATONSDK_BENCH_SCALES="100,1000,10000,100000" ./autoresearch.sh
```
The full 1M run is required before a final `keep` — restore the default scales before the confirmation run.

`./autoresearch.checks.sh` is invoked automatically after each successful bench by the harness. It runs engine + SQLite + compactor + equivalence tests, lints, and asserts no `go.mod`/`go.sum`/proto drift.

## Files in Scope

- `pkg/dotc1z/engine/pebble/options.go` — Pebble.Options (memtable, L0, cache, block size, compression, bloom)
- `pkg/dotc1z/engine/pebble/engine.go` — lifecycle, Quiesce, Save, fresh-sync hooks
- `pkg/dotc1z/engine/pebble/grants.go` — `PutGrantRecord(s)` batch shape & durability
- `pkg/dotc1z/engine/pebble/resources.go`, `entitlements.go`, `resource_types.go` — same pattern, write paths
- `pkg/dotc1z/engine/pebble/keys.go` — key encoding (smaller keys → less memory)
- `pkg/dotc1z/engine/pebble/paginate.go` — pagination cursor + range iteration (read path)
- `pkg/dotc1z/engine/pebble/codec/tuple.go` — tuple encoder hot path
- `pkg/dotc1z/engine/pebble/codec/reflect.go` — reflection codec (codegen candidate)
- `pkg/dotc1z/engine/pebble/codec/syncid.go` — KSUID encoding
- `pkg/synccompactor/pebble/compactor.go` — IngestAndExcise driver
- `pkg/synccompactor/pebble/bucket_plans.go` — bucket layout

## Off Limits

- `pb/c1/storage/v3/*` — wire format frozen for v4 (generated + proto)
- `pb/c1/reader/v2/*`, `pb/c1/connector/v2/*` — external surface
- `proto/c1/storage/v3/` — proto IDL frozen
- `pkg/dotc1z/c1file.go`, `pkg/dotc1z/grants.go`, `pkg/dotc1z/resources.go`, … — SQLite engine path (regression sentinel only)
- `docs/rfcs/`, `.github/workflows/`, `.golangci.yml`
- `go.mod`, `go.sum`, `vendor/` — no new dependencies
- `cmd/protoc-gen-batonstore` codegen — if reached, escalate to human

## Constraints (enforced by `autoresearch.checks.sh`)

1. Engine + compactor + equivalence + envelope tests pass: `go test -tags=batonsdkv2 -count=1 ./pkg/dotc1z/engine/pebble/... ./pkg/dotc1z/engine/equivalence/... ./pkg/synccompactor/pebble/... ./pkg/dotc1z/format/v3/...`
2. SQLite engine tests pass: `go test -tags=baton_lambda_support -short -count=1 ./pkg/dotc1z/`
3. Lint clean: `golangci-lint run --timeout=3m --build-tags=batonsdkv2` over engine + compactor.
4. `go.mod` / `go.sum` unmodified (no new deps).
5. `proto/c1/storage/v3/` unmodified.
6. WritePack bench asserts paginated total — corruption fails naturally.
7. `sqlite_writepack_1k_ms` may not regress by more than 5% (manual check via secondary metric; large regressions → discard).

## Priority Ideas (from RFC §9)

### Priority 1
1. Larger memtable (64 → 256 MiB) — absorbs more write burst before L0 flush.
2. `L0CompactionThreshold` sweep (currently 2 → try 4, 8) — write throughput vs read amp tradeoff.
3. `MaxConcurrentCompactions` upper bound — capped at 8; try 12 on big-core hosts.
4. Bloom filters on L0 — read-side win, currently disabled.
5. Mixed compression: Snappy at L0, zstd at L6 — cuts compaction CPU.

### Priority 2
6. Per-record-type compaction tuning (grants vs resources differ).
7. Codec codegen replacing `codec/reflect.go` — ~5× microbench, 5–10% e2e estimated.

### Cleanup
8. Pool tuple encoder buffer (`AppendTupleString` per-record alloc).
9. Larger block size to amortize header overhead.

## Known Dead Ends (do not retry)

- Per-record `db.Set` instead of batched `pebble.Batch` — slower.
- `DisableWAL: true` — saves <5%, loses durability across Open/Close.
- Shared block cache across engines — wins in C1 prod, no-op in bench.

## What's Been Tried

Maintained in `autoresearch.ideas.md` and the `autoresearch.jsonl` log. Resuming agents: read both before mutating code.

Already-applied wins (the status quo baseline):
- Fat-batch `PutGrantRecords` (one batch per N grants) — ~10× at 1M.
- Fresh-sync `pebble.NoSync`, single Flush+LogData(Sync) at EndFreshSync.
- Read-before-write index cleanup unconditional (12% cost, correctness-critical).
- 256 MiB block cache.
- `CompactionConcurrencyRange` capped at `(2, min(8, GOMAXPROCS/4))`.

### Wins kept by this loop (cumulative -52.3% from 4292 → 2046 ms at 1M)

In order applied (compounds multiplicatively):

1. **`L0CompactionThreshold` 2 → 8** (-15.8%). The default 2 over-eagerly compacted during the 1M write burst, stealing CPU from writers. 8 lets ~8 L0 sub-levels accumulate before compaction kicks in; L0StopWritesThreshold=20 still bounds the worst case. Knee mapped: 2/4/6 worse; 16 flat vs 8.
2. **Scratch byte buffers + `proto.MarshalAppend`** (-5.6%). Reused `keyBuf` / `idx1Buf` / `idx2Buf` / `valBuf` across the loop; Pebble's `batch.Set` is documented as safe to modify args after return (it copies into batch buffer). Added `appendGrantKey` / `appendGrantBy*IndexKey` variants taking `dst []byte`. Allocs 9.0M→4.0M.
3. **Hoisted `resolveSyncBytes`** (-4.9%). Cache last-resolved (string, bytes) pair across loop iterations; falls back to per-record resolve when string differs. Adapter typically stamps a uniform sync_id, so the cache hits 1M times.
4. **Split `priBatch` / `idxBatch`** (-12.8%). Primary writes (sorted by external_id by construction) go to one batch; index writes (unsorted) go to another. pdqsort early-exits the priBatch's flushable-batch promotion sort; only the idxBatch pays full O(N log N) on 2/3 the entries. Cross-batch atomicity is fine for fresh-sync (replays from connector).
5. **`NewBatchWithSize(len*600)` / `(len*140)`** (-6.1%). Pre-size the batches so they don't grow-by-2x internally; saves ~10 reallocations and up to 2x peak slack. bytes_op -23%.
6. **Skip read-before-write Get for fresh-sync first call** (-14.5%). New engine flag `freshGrantsEmpty` is true between `MarkFreshSync` and the first `PutGrantRecords` commit. While true, the 1M `e.db.Get` calls are skipped — they'd all return ErrNotFound anyway (db.Get doesn't see in-batch writes; the keyspace is empty). Across-call dup detection preserved by clearing the flag after first commit.
7. **Parallel-build the two batches for batches ≥ 256** (-8.8%). When skipGet is true, the two batches have no shared mutable state. Two goroutines build them concurrently; each has its own scratch buffers and sync_id cache. Threshold of 256 records avoids goroutine setup overhead on tiny calls (solo write regression bounded to +11%).

### Major dead ends (do NOT retry)

- **MemTableSize >64 MiB at any size** (-1% primary, +30%+ at 100k). Larger memtable lets the entire 100k workload fit in memory → no during-write flushes → forced end-of-sync serial flush. The 64→256 MiB attempt regressed 100k by 32%; 128 MiB by 34%. Memtable should be sized so the workload triggers ≥3 flushes during writes.
- **Chunking PutGrantRecords commits** (+83% at 1M). Splitting one big batch into 16Ki-grant chunks force memtable rotation per chunk → many L0 files → compaction storm. The single-big-batch path takes Pebble's optimized flushable-batch promotion (sort once, swap in as memtable atomically).
- **Bloom filters on all levels** (+2.9%). Fresh-sync workloads have unique external_ids; the Get-before-Put population is 100% misses, and the bench's read path is range iteration not point Gets. Filters add construction CPU during flushes with no payoff. They MIGHT help in C1 prod where ReaderCache does point Gets across syncs, but that's not measured here.
- **`zstd.SpeedDefault` → `SpeedFastest` + `WithEncoderConcurrency(0)`** (flat). The c1z pack tar wraps Pebble SSTs that are already Snappy-compressed internally; outer zstd is nearly incompressible regardless of level.
- **`L0CompactionThreshold` ≠ 8** — axis fully mapped. 2/4/6 worse, 16 flat with all other wins.
- **`CompactionConcurrencyRange` (2, GOMAXPROCS/2 capped 8)** (flat). With L0=8 the compactor isn't the bottleneck; adding lanes makes no difference.
- **`LBaseMaxBytes` 256 → 512 MiB** (-1.6% within noise). L1 consolidation doesn't matter at our workload size.
- **`FlushSplitBytes` 2 → 16 MiB** (-1.1% within noise). Per-SST overhead is small.
- **`DisableAutomaticCompactions: true`** (-1% within noise). With L0=8 already limiting compaction-during-writes, disabling shifts work later but saves no wallclock.
- **`SetDeferred` for primary key+value** (+2.3%). `proto.Size` traversal cost exceeds the `batch.Set` memcpy savings; no net win.
- **`appendEscaped` bytes.IndexByte fast path** (+1.7% within noise). The tuple encoder lives on the smaller goroutine (idxBatch); parallel wallclock = max(A,B), so optimizing B doesn't reduce max when B<A.

### Open ideas for future work (not pursued in this loop)

- **Codec codegen via `cmd/protoc-gen-batonstore`** replacing proto reflection for the per-record marshal. The priBatch goroutine is now the long pole; cutting its proto.Marshal cost would directly drop primary. Big refactor (touches generated code surface).
- **Apply scratch-buffer + dual-batch + skipGet + parallel pattern to `PutResources` / `PutEntitlements` / `PutResourceTypes`**. Transferable production win; not measured by this bench so not pursued by the loop, but high-value follow-up.
- **3+ way parallel split of the priBatch path** via `batch.Apply` concatenation. The Apply does an extra memcpy; uncertain net win.
- **`SetDeferred` + cached marshal size** could eliminate the per-record memcpy if we can avoid the double-traverse of proto.Size+MarshalAppend. Would require dropping into proto/protoreflect lower-level APIs.

### Production safety follow-up (see `autoresearch.ideas.md`)

The split-batch change (commit 63c0869b onward) breaks cross-batch atomicity:
if priBatch commits but idxBatch fails, primary records exist without their
by_entitlement / by_principal index entries. Fresh-sync replays the whole sync
on crash so it's safe there. Incremental upserts (mid-sync mutate) might leak.
Human-review item: either gate the split behind IsFreshSync() or document the
contract change.

## Stop Conditions

- Primary plateau for 20 consecutive iterations.
- 3 consecutive `checks_failed` from the same root cause → thrashing, change direction.
- >2× improvement → confirm with a clean rerun, commit, continue with next idea.
- All priority-1 ideas exhausted → move to priority-2, then write a summary.

## Resume Protocol

1. Read this file + RFC `autoresearch-pebble-perf.md`.
2. Read `autoresearch.ideas.md` (prune stale entries).
3. Skim `autoresearch.jsonl` for recent `keep`/`discard` patterns.
4. Run `./autoresearch.sh` once to re-establish baseline on the current commit.
5. Pick the highest-priority untried idea.
