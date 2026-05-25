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
