# Autoresearch: Pebble engine read perf

Starts from the autoresearch/pebble-perf-20260525 branch (WritePack
session, −69.7 % cumulative win at 1 M grants). That session is archived
under `docs/rfcs/0004-storage-engine-v4/autoresearch-archive/writepack/`.

## Objective

Optimize the Pebble engine's **paginated read** path:
`store.ListGrants` iterated over an entire sync's grants in 10 k-grant
pages. The bench (`BenchmarkRegisteredPebbleUnpackReadGrants`) measures
end-to-end per-iteration wallclock for:

1. `dotc1z.NewStore(ctx, path, WithReadOnly(true))` — opens the `.c1z`
   file, unpacks the zstd-tar payload into a tmp directory,
   `pebble.Open`s the Pebble engine pointing at that directory.
2. `store.SetCurrentSync(ctx, syncID)`.
3. Walk grants via `ListGrants` in pages of 10 000 until exhausted.
4. `store.Close(ctx)`.

Current baseline at 100 k grants is **~129 ms** (3.0× faster than
SQLite). At 1 M grants it's likely ~1.5 s; we'll measure as part of the
baseline.

## Primary metric

**`pebble_readpaginated_1m_ms`** — wallclock for the 1 M-grant paginated
read benchmark. Lower is better.

Picked 1 M (not 100 k) because:

- LSM-vs-B-tree differences scale with N. Optimizing 1 M-read tends to
  generalise downward; optimizing 100 k can overfit to small-workload
  noise.
- The 1 M write-side bench was where the WritePack session found its
  big wins; same likely true for reads.

Bench script will run the 100, 1 k, 10 k, 100 k, 1 M scales so we can
sanity-check that improvements at 1 M aren't regressions elsewhere.

## Secondary metrics

| Metric | Direction | Notes |
|---|---|---|
| `pebble_readpaginated_100k_ms` | lower | secondary scale to confirm |
| `pebble_readpaginated_10k_ms` | lower | secondary scale |
| `pebble_readpaginated_1k_ms` | lower | secondary scale |
| `pebble_readpaginated_100_ms` | lower | secondary scale |
| `pebble_readpaginated_1m_bytes_op` | lower | memory pressure |
| `pebble_readpaginated_1m_allocs_op` | lower | GC pressure |
| `sqlite_readpaginated_1k_ms` | unchanged | regression sentinel; SQLite engine must not slow down while tuning Pebble |
| `pebble_writepack_1m_ms` | unchanged | write-side regression sentinel; the WritePack session's wins must hold |

## How to Run

`./autoresearch.sh` — outputs `METRIC name=value` lines + diagnostic bench
output. Runtime ~5–6 min per iteration (read sweep + write sentinel +
sqlite sentinel).

Fast iteration mode (skips 1 M, keeps the smaller scales):

```
BATONSDK_READ_SCALES="100,1000,10000,100000" ./autoresearch.sh
```

The full 1 M run is required before a final `keep` confirmation —
revert the env override.

`./autoresearch.checks.sh` runs after every passing benchmark by the
harness. Same correctness gate as the WritePack session: engine +
adapter + compactor + equivalence + envelope + SQLite tests, golangci
lint, and no `go.mod`/`go.sum`/proto drift.

## Files in Scope

Read-path files most likely to benefit:

- `pkg/dotc1z/engine/pebble/register.go` — `OpenStore` / `unpackExisting`
   (the bulk of the per-iter wallclock at large scales is here)
- `pkg/dotc1z/format/v3/envelope.go` — `ReadEnvelope` + `ExtractZstdTar`
   (decode + extract the payload)
- `pkg/dotc1z/engine/pebble/paginate.go` — pagination cursor decode +
   range iteration
- `pkg/dotc1z/engine/pebble/grants.go` — `ListGrants` adapter +
   `IterateGrantsBySync`
- `pkg/dotc1z/engine/pebble/adapter_reader.go` — adapter surface for
   reads
- `pkg/dotc1z/engine/pebble/adapter.go` — `ListGrants` / `SetCurrentSync`
- `pkg/dotc1z/engine/pebble/translate_v2.go` — `V3GrantToV2`
   (hot per-record on the read path)
- `pkg/dotc1z/engine/pebble/options.go` — Pebble.Options affecting
   reads (cache, bloom filters, block size on read amp)
- `pkg/dotc1z/engine/pebble/codec/*.go` — tuple encode/decode + KSUID
   syncid (used per grant for index iteration)

## Off Limits

Same as the WritePack session:

- `pb/c1/storage/v3/*` — wire format frozen for v4
- `pb/c1/reader/v2/*`, `pb/c1/connector/v2/*` — external surface
- `proto/c1/storage/v3/` — proto IDL frozen
- SQLite engine path (`pkg/dotc1z/c1file.go` etc.) — regression sentinel only
- `docs/rfcs/`, `.github/workflows/`, `.golangci.yml`
- `go.mod`, `go.sum`, `vendor/` — no new dependencies

## Constraints (enforced by `autoresearch.checks.sh`)

1. Engine + adapter + compactor + equivalence + envelope tests pass.
2. SQLite engine tests pass (regression guard).
3. Lint clean.
4. `go.mod` / `go.sum` unmodified.
5. `proto/c1/storage/v3/` unmodified.
6. Paginated total assertion in the bench (`paginated ListGrants total = %d, want %d`)
   — silent regressions or skipped reads fail naturally.
7. `pebble_writepack_1m_ms` must stay within 5 % of the WritePack session's
   ending baseline (1251 ms). Read-side changes that regress writes by
   more than that are discards.

## What's Been Tried (this session)

Maintained in `autoresearch.ideas.md` and `autoresearch.jsonl`. Resuming
agents: read both. The WritePack session's archived ideas + closed-axis
catalogue under `docs/rfcs/0004-storage-engine-v4/autoresearch-archive/writepack/`
is required reading — many of those closed axes apply to reads too
(e.g. heap-arena contention on parallel large allocs, modern bytes.Compare
SIMD invalidates simple prefix-skip wrappers).

### Probable open targets (priority guesses, profile-confirm before pursuing)

1. **`ExtractZstdTar`** — single-threaded zstd decode + tar walk + per-file
   `os.OpenFile` + `io.Copy` into the destination. Per-iter cost scales
   with payload size; for 1 M-grant `.c1z` of ~500 MB this dominates.
   Possible attacks: parallel zstd decode (`klauspost/compress/zstd`'s
   `WithDecoderConcurrency`), parallel file writes via worker pool,
   buffer pooling.
2. **Pebble cache** — current default 256 MiB. For 100 k workload
   (50 MB) the cache fits everything; for 1 M (500 MB) it doesn't. Tune
   or warm the cache.
3. **`V3GrantToV2` translation** on each grant — analogous to
   `V2GrantToV3` arena win from the WritePack session. May or may not
   transfer; reads return one grant at a time via the iterator.
4. **`pebble.Iterator` options** — `LowerBound`/`UpperBound` for the
   grant primary keyspace (we set these). Bloom filters? KeyPrefix?
5. **Pagination cursor decode** — `paginate.go` decodes the cursor on
   every page boundary. Tiny cost at 10 pages but worth profiling.
6. **Engine.Open at iter start** — replays WAL (none for a checkpointed
   .c1z), reads manifest, loads SST metadata. May have idle time we can
   skip.

## Stop Conditions

- Primary plateau for 20 consecutive iterations.
- 3 consecutive `checks_failed` from the same root cause → thrashing.
- >2× improvement → confirm with a clean rerun + summary.
- All priority-1 ideas exhausted → move to priority-2 or finalize.

## Resume Protocol

1. Read this file + the archived WritePack `autoresearch.md`.
2. Read `autoresearch.ideas.md` (this session) + the archived
   `autoresearch.ideas.md` (do-not-retry catalogue from WritePack).
3. Skim `autoresearch.jsonl` for recent `keep`/`discard` patterns.
4. Run `./autoresearch.sh` once to re-establish a baseline on the
   current commit.
5. Pick the highest-priority untried idea.
