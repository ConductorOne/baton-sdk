# Autoresearch plan: Pebble engine perf

Design doc for an autonomous experiment loop that optimizes the v3
Pebble storage engine's read + write throughput. Answers every question
the `autoresearch-create` skill asks, plus a concrete walkthrough of
how each artifact (`autoresearch.md`, `autoresearch.sh`,
`autoresearch.checks.sh`, `autoresearch.config.json`) would look for
this codebase.

> Not a runbook to start the loop today — that's a separate `/autoresearch`
> invocation. This file just makes the loop reproducible: anyone with
> commit access can copy the snippets in §6–9 into a fresh worktree and
> kick it off.

## 1. Goal

Optimize the Pebble engine's end-to-end write+pack and unpack+read
throughput on a representative grant workload. Current state (commit
`1627b047`, Linux arm64, `-benchtime=2x`):

| Workload | SQLite | Pebble | Pebble vs SQLite |
|---|---:|---:|---:|
| WritePack 100 grants | 14.2 ms | 7.1 ms | 1.93× faster |
| WritePack 1k | 30.2 ms | 10.7 ms | 2.82× faster |
| WritePack 10k | 218.6 ms | 42.9 ms | 5.10× faster |
| WritePack 100k | 3.17 s | 352 ms | 9.00× faster |
| WritePack 1M | 55.59 s | 4.24 s | **12.95× faster** |
| Read 100 | 2.13 ms | 2.17 ms | parity |
| Read 1k | 5.97 ms | 3.73 ms | 1.60× faster |
| Read 10k | 42.8 ms | 15.6 ms | 2.75× faster |
| Read 100k | 386 ms | 142 ms | 2.72× faster |

The interesting headroom is at the larger scales (≥10k grants) where
Pebble's per-grant cost has flatlined; SQLite continues to climb. The
loop should focus on the 100k and 1M Pebble numbers.

## 2. Primary + secondary metrics

| | Metric | Unit | Direction |
|---|---|---|---|
| **Primary** | `pebble_writepack_1m_ms` | milliseconds | lower is better |
| Secondary | `pebble_writepack_100k_ms` | ms | lower |
| Secondary | `pebble_readpaginated_100k_ms` | ms | lower |
| Secondary | `pebble_writepack_1m_bytes_op` | bytes/op | lower (memory pressure) |
| Secondary | `pebble_writepack_1m_allocs_op` | int | lower |
| Secondary | `pebble_writegrant_solo_ns_op` | ns/op | lower (startup cost) |
| Secondary | `pebble_compact_per_sync_ms` | ms | lower |
| Secondary | `sqlite_writepack_1k_ms` | ms | unchanged (SQLite shouldn't regress while we tune Pebble) |

Why 1M as the primary: Pebble's strength is amortizing fixed costs.
Smaller workloads are dominated by startup + go-build cache; 1M is where
LSM-internal choices (compaction concurrency, memtable size, block size,
L0 thresholds, compression presets, bloom filters) actually move the
needle.

Why include the SQLite secondary: any change that touches shared code
(`pkg/dotc1z/*.go`) could accidentally regress SQLite. Discard runs that
make Pebble faster at the cost of SQLite slowing > ~5%.

## 3. Files in scope

Tunable / safe to mutate during the loop:

```
pkg/dotc1z/engine/pebble/options.go            # Pebble.Options (memtable size, L0 thresholds, compaction range, cache, block size)
pkg/dotc1z/engine/pebble/engine.go             # lifecycle, Quiesce, Save, MarkFreshSync / EndFreshSync
pkg/dotc1z/engine/pebble/grants.go             # PutGrantRecord / PutGrantRecords; the batch shape, durability mode
pkg/dotc1z/engine/pebble/resources.go          # same pattern
pkg/dotc1z/engine/pebble/entitlements.go       # same pattern
pkg/dotc1z/engine/pebble/resource_types.go     # same pattern
pkg/dotc1z/engine/pebble/keys.go               # key encoding; small encoding changes might shrink keys → less memory
pkg/dotc1z/engine/pebble/paginate.go           # pagination cursor decode/encode + range iteration
pkg/dotc1z/engine/pebble/codec/tuple.go        # tuple encoder hot path (called per indexed field per record)
pkg/dotc1z/engine/pebble/codec/reflect.go      # reflection codec — codegen replacement is on the table as a perf option
pkg/dotc1z/engine/pebble/codec/syncid.go       # KSUID encoding
pkg/synccompactor/pebble/compactor.go          # IngestAndExcise driver; SST writer choice
pkg/synccompactor/pebble/bucket_plans.go       # bucket layout for compaction
```

## 4. Off limits

```
pb/c1/storage/v3/*.pb.go      # generated; mutate via .proto only
pb/c1/storage/v3/*.proto      # wire format frozen for v4
pb/c1/reader/v2/*.pb.go       # external surface
pb/c1/connector/v2/*.pb.go    # external surface
pkg/dotc1z/c1file.go          # SQLite engine surface — DO NOT touch from this loop
pkg/dotc1z/grants.go          # SQLite path
pkg/dotc1z/resources.go etc.  # SQLite path
docs/rfcs/                    # RFC text is frozen
.github/workflows/            # CI config
.golangci.yml                 # lint config
go.mod, go.sum                # dependency surface
vendor/                       # generated
```

Touching anything under "off limits" should be a manual decision the
loop escalates to the human, not an automatic mutation.

## 5. Constraints

Hard requirements every iteration must satisfy. `autoresearch.checks.sh`
verifies them:

1. **All engine tests pass** —
   `go test -tags=batonsdkv2 -count=1 ./pkg/dotc1z/engine/pebble/... ./pkg/synccompactor/pebble/...`.
   30 engine + 5 compactor + 4 equivalence + 1 tuple property + 4 codec
   benchmarks + 11 pagination + 6 mutation = 60+ test cases.
2. **All SQLite tests pass** —
   `go test -tags=baton_lambda_support -short -count=1 ./pkg/dotc1z/`.
   Catches accidental regressions to the SQLite engine.
3. **Lint clean** —
   `golangci-lint run --timeout=3m --build-tags=batonsdkv2`.
4. **No new dependencies** — `go.sum` unchanged.
5. **No changes to proto wire format** — checks `git diff proto/` is empty.
6. **WritePack 1M output is correct** — the bench asserts
   `paginated total = 1_000_000` after the write; a regression that
   skips writes or corrupts indexes would fail this naturally.
7. **No SQLite regression > 5%** on the 1k WritePack secondary metric.

## 6. How to run — `autoresearch.sh`

The benchmark script. Outputs `METRIC name=value` lines that
`run_experiment` parses, plus diagnostic stdout the agent can use to
localize regressions.

```bash
#!/usr/bin/env bash
set -euo pipefail

# Pin the Go test cache so successive iterations don't re-pay download/
# compile cost. The bench itself is the only thing that runs.
export GOCACHE="${GOCACHE:-$HOME/.cache/go-build}"
export CGO_ENABLED=0

BENCH_DIR="./pkg/dotc1z/engine/pebble"
COMMON_FLAGS=(-tags=batonsdkv2 -run='^$' -benchmem -benchtime=2x -timeout=20m)
WRITE_BENCH='BenchmarkRegistered(Pebble|SQLite)WritePack$'
READ_BENCH='BenchmarkRegisteredPebbleUnpackReadGrants$'

# 1) Write+pack sweep. Scales chosen to cover the LSM-vs-B-tree crossover.
BATONSDK_BENCH_SCALES="100,1000,10000,100000,1000000" \
  go test "${COMMON_FLAGS[@]}" -bench "$WRITE_BENCH" "$BENCH_DIR" \
  > /tmp/autoresearch-write.txt 2>&1

# 2) Paginated read sweep, capped at 100k (1M reads through pagination
#    take ~5s and aren't on the critical perf path being tuned).
BATONSDK_BENCH_SCALES="100,1000,10000,100000" \
  go test "${COMMON_FLAGS[@]}" -bench "$READ_BENCH" "$BENCH_DIR" \
  > /tmp/autoresearch-read.txt 2>&1

# 3) Codec hot-path microbench. Picks up direct vs reflection-codec
#    regressions instantly. Cheap, run every iteration.
go test "${COMMON_FLAGS[@]}" \
  -bench='BenchmarkCodec' \
  ./pkg/dotc1z/engine/pebble/microtests/ \
  > /tmp/autoresearch-codec.txt 2>&1

# 4) Solo write (engine cold-start cost). Catches options bloat.
go test "${COMMON_FLAGS[@]}" \
  -bench='BenchmarkRegisteredPebbleWriteGrant$' \
  "$BENCH_DIR" \
  > /tmp/autoresearch-solo.txt 2>&1

# --- Extract the numbers we care about ---

# bench_value <pkg-path-file> <line-prefix> <column>
# Returns the value at column N (ns/op is col 3, bytes is col 5, allocs col 7)
bench_value() {
  local file=$1 prefix=$2 col=$3
  awk -v p="$prefix" -v c="$col" '$1 ~ "^"p"$" { print $c; exit }' "$file"
}

# Pebble write+pack at each scale
pwrite_100=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredPebbleWritePack/grants=100-' 3)
pwrite_1k=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredPebbleWritePack/grants=1000-' 3)
pwrite_10k=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredPebbleWritePack/grants=10000-' 3)
pwrite_100k=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredPebbleWritePack/grants=100000-' 3)
pwrite_1m=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredPebbleWritePack/grants=1000000-' 3)
pwrite_1m_bytes=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredPebbleWritePack/grants=1000000-' 5)
pwrite_1m_allocs=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredPebbleWritePack/grants=1000000-' 7)

# SQLite write+pack at 1k as a regression sentinel
swrite_1k=$(bench_value /tmp/autoresearch-write.txt 'BenchmarkRegisteredSQLiteWritePack/grants=1000-' 3)

# Pebble paginated read
pread_1k=$(bench_value /tmp/autoresearch-read.txt 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=1000-' 3)
pread_100k=$(bench_value /tmp/autoresearch-read.txt 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=100000-' 3)

# Codec direct vs reflect
codec_direct=$(bench_value /tmp/autoresearch-codec.txt 'BenchmarkCodecDirect-' 3)
codec_reflect=$(bench_value /tmp/autoresearch-codec.txt 'BenchmarkCodecReflect-' 3)

# Solo write (engine startup cost)
solo_write=$(bench_value /tmp/autoresearch-solo.txt 'BenchmarkRegisteredPebbleWriteGrant-' 3)

# Convert ns/op to ms for human readability (autoresearch supports both)
ns_to_ms() { awk -v v="$1" 'BEGIN { printf "%.3f", v / 1000000 }'; }

# --- Emit structured metrics for the loop ---
echo "METRIC pebble_writepack_1m_ms=$(ns_to_ms "$pwrite_1m")"
echo "METRIC pebble_writepack_100k_ms=$(ns_to_ms "$pwrite_100k")"
echo "METRIC pebble_writepack_10k_ms=$(ns_to_ms "$pwrite_10k")"
echo "METRIC pebble_writepack_1k_ms=$(ns_to_ms "$pwrite_1k")"
echo "METRIC pebble_writepack_100_ms=$(ns_to_ms "$pwrite_100")"
echo "METRIC pebble_writepack_1m_bytes_op=$pwrite_1m_bytes"
echo "METRIC pebble_writepack_1m_allocs_op=$pwrite_1m_allocs"
echo "METRIC pebble_readpaginated_100k_ms=$(ns_to_ms "$pread_100k")"
echo "METRIC pebble_readpaginated_1k_ms=$(ns_to_ms "$pread_1k")"
echo "METRIC pebble_writegrant_solo_ns_op=$solo_write"
echo "METRIC codec_direct_ns_op=$codec_direct"
echo "METRIC codec_reflect_ns_op=$codec_reflect"
echo "METRIC sqlite_writepack_1k_ms=$(ns_to_ms "$swrite_1k")"

# --- Diagnostic data the agent uses to localize regressions ---
echo "=== Write+Pack details ==="
grep -E 'BenchmarkRegistered(Pebble|SQLite)WritePack' /tmp/autoresearch-write.txt
echo
echo "=== Read details ==="
grep 'BenchmarkRegisteredPebbleUnpackReadGrants' /tmp/autoresearch-read.txt
echo
echo "=== Codec ==="
grep 'BenchmarkCodec' /tmp/autoresearch-codec.txt
echo
echo "=== Solo write ==="
grep 'BenchmarkRegisteredPebbleWriteGrant' /tmp/autoresearch-solo.txt
```

Runtime budget per iteration:
- Write sweep (100…1M, both engines, 2 iterations): ~3.5 min
- Read sweep (100…100k, Pebble only, 2 iterations): ~1.5 min
- Codec micros + solo: <30 s
- **Total ≈ 5–6 min per iteration.**

The script always exits 0; the bench failures show as zero-valued
metrics which `log_experiment` will surface to the agent. Test
correctness lives in `autoresearch.checks.sh` (§7).

### Fast variant for iterating on ideas

When the agent is in a tight inner loop on a single experiment idea, it
can override the scales to skip the 1M run:

```bash
BATONSDK_BENCH_SCALES="100,1000,10000,100000" ./autoresearch.sh
```

Drops total iteration to ~1.5 min. The full 1M run is required before
`log_experiment` records a `keep` — the agent should set
`BATONSDK_BENCH_SCALES` back to default before the final confirmation
run.

## 7. Correctness gate — `autoresearch.checks.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

export CGO_ENABLED=0

# 1) Engine + adapter + compactor + equivalence tests.
go test -tags=batonsdkv2 -count=1 -timeout=5m \
  ./pkg/dotc1z/engine/pebble/... \
  ./pkg/dotc1z/engine/equivalence/... \
  ./pkg/synccompactor/pebble/... \
  ./pkg/dotc1z/format/v3/... 2>&1 | tail -50

# 2) SQLite engine regression guard.
go test -tags=baton_lambda_support -short -count=1 -timeout=5m \
  ./pkg/dotc1z/ 2>&1 | tail -50

# 3) Lint (pinned version, matches CI).
golangci-lint run --timeout=3m --build-tags=batonsdkv2 \
  ./pkg/dotc1z/engine/... \
  ./pkg/synccompactor/pebble/... 2>&1 | tail -20

# 4) No new deps / go.sum drift.
if ! git diff --quiet go.mod go.sum; then
  echo "FAIL: go.mod or go.sum modified — new dependency introduced"
  exit 1
fi

# 5) No proto changes.
if ! git diff --quiet proto/c1/storage/v3/; then
  echo "FAIL: proto wire format changed — out of scope for perf loop"
  exit 1
fi
```

## 8. Loop config — `autoresearch.config.json`

```json
{
  "maxIterations": 200,
  "workingDir": "/data/squire/src/baton-sdk"
}
```

200 iterations × 5–6 min ≈ 17–20 hours of autonomous work. Stop early
if the primary metric stabilizes for 20 consecutive iterations.

## 9. What's been tried (seed for the agent)

A short list of ideas the agent should NOT re-discover. Updated by the
agent as the loop runs.

### Already-applied wins (status quo baseline)

- **Fat-batch `PutGrantRecords`** — bundle N grants into one
  `pebble.Batch`, commit once. Was per-grant batch+commit. **~10×
  faster at 1M grants.**
- **Fresh-sync `pebble.NoSync`** — between `MarkFreshSync` and
  `EndFreshSync`, batch commits use `pebble.NoSync`. `EndFreshSync`
  does one `Flush + LogData(Sync)` to harden everything. Matches
  SQLite's `PRAGMA synchronous=NORMAL` semantics. **Single biggest
  write-side win.**
- **Read-before-write index cleanup runs unconditionally** —
  freshSync does NOT skip the Get. Skipping leaks orphan index
  entries when a connector emits the same external_id twice. ~12%
  perf cost vs the unsafe skip; worth it.
- **256 MiB block cache (default preset)** — vs Pebble's 8 MiB
  default. ~20% faster reads at 100k.
- **CompactionConcurrencyRange capped** — `(2, min(8, GOMAXPROCS/4))`.
  Unbounded compaction starves the workload on many-core hosts.

### Known dead ends — do NOT try again

- **Per-record `db.Set` instead of `db.NewBatch` + `batch.Set` + `Commit`**
  — slower because each Set allocates its own internal batch.
- **Disabling WAL (`DisableWAL: true`)** — saves <5% perf but loses
  durability across `pebble.Open`/`Close` (we lose un-flushed
  writes). Documented in micro-test
  `/tmp/baton-rfc-microtests/checkpoint_test.go`.
- **Single block cache shared across all engines** — actually a win
  in C1's ReaderCache scenario, but our bench opens one engine per
  iteration so it doesn't show. Skip.

### Open ideas worth trying

1. **Larger memtable** — currently 64 MiB. 256 MiB might absorb more
   of the 1M-grant write burst before triggering L0 flush. Tradeoff:
   memory pressure under multi-engine workloads.
2. **`L0CompactionThreshold` tuning** — currently 2. Higher (4 or 8)
   buys more write throughput at the cost of read amplification.
   Worth a sweep.
3. **`MaxConcurrentCompactions` upper bound** — capped at 8 today;
   on a 16-core machine 12 might be the sweet spot.
4. **Bloom filters on L0** — currently disabled (deferred from RFC
   §8 question 3). Add and measure read-side win.
5. **Snappy at L0, zstd at L6** — currently zstd-everywhere via
   `DBCompressionGood`. Snappy at L0 cuts compaction CPU; the L6
   stable layer keeps the on-disk size compact.
6. **Per-record-type compaction tuning** — grants are write-heavy,
   small per-record; resources are read-heavy, large per-record.
   Pebble's per-level options can differ.
7. **`Reflective` codec → codegen** — replace the
   `pkg/dotc1z/engine/pebble/codec/reflect.go` path with codegen
   emitted by `cmd/protoc-gen-batonstore`. ~5× microbench delta;
   ~50µs/grant total → maybe 5–10% e2e savings.
8. **Tuple encoder allocation** — `AppendTupleString` currently
   allocates a new slice per record. Pool the encoder buffer.
9. **`KeyPrefixCompression`** — keys share the `v3 | type | sync_id_bytes`
   prefix; Pebble's block prefix compression already covers this,
   but increasing block size might amortize header overhead.

The agent should treat (1)–(5) as priority-1, (6)–(7) as priority-2,
and (8)–(9) as cleanup work after the bigger wins are in.

## 10. Validation of existing test structure

The autoresearch loop is only as honest as the tests that gate `keep`
decisions. Inventory + verdict of every test under our PR surface:

### Engine package — `pkg/dotc1z/engine/pebble/`

| File | Tests | Verdict |
|---|---|---|
| `engine_test.go` | Open/Close, DBSizeBytes, Put/Get/Iterate/Delete grants, Quiesce, CheckpointTo, empty syncID fallback | **Robust.** Covers the lifecycle path the bench drives. |
| `records_test.go` | ResourceType / Resource / Entitlement / Asset / SyncRun round-trips + by_parent / by_resource indexes | **Robust.** Multi-record-type coverage. |
| `adapter_test.go` | Sync lifecycle (StartNewSync, EndSync, ResumeSync, CheckpointSync), Put*/List*/Get*/Delete*, LatestFinishedSyncID | **Robust.** Mirrors what the bench exercises end-to-end. |
| `adapter_reader_test.go` | GetEntitlement/Resource/ResourceType, ListGrantsForEntitlement / ResourceType, GetSync, ListSyncs, GetLatestFinishedSync, Stats, GrantStats, IfNewer skip-stale | **Robust.** Covers syncer-required reader gRPC surface. |
| `paginate_test.go` | ListGrants pagination at page sizes 10/50/100/250/251/default, by-principal paged, ListResources/Entitlements/RTs paged, malformed token | **Robust.** This is the exact code path the bench's `benchmarkRegisteredUnpackReadGrants` exercises. |
| `mutation_test.go` | Grant/Resource/Entitlement overwrite cleans indexes; fresh-sync duplicate external_id cleans indexes; delete-then-put returns to clean state | **Robust.** Catches the index-integrity bug class the bench wouldn't notice on its own (a corrupt-but-not-crashing engine would pass the bench while serving wrong data). |
| `translate_v2_test.go` | v2 ↔ v3 round-trips for grant/resource/RT/entitlement, nil safety, GrantSources roundtrip, unknown trait safety | **Robust.** Translation layer is symmetric. |
| `engine_stub.go` | (compile-only sentinel error declarations) | n/a |
| `microtests/tuple_test.go` | Prefix-free property of tuple encoder over 9 hand-picked + 200 random tuples (40k+ pairwise comparisons) | **Robust** — encodes the property tests would normally find. |
| `microtests/codec_perf_test.go` | Direct vs reflection codec benchmark | **Diagnostic only**, not pass/fail. The bench in `autoresearch.sh` already runs these. |

### Compactor — `pkg/synccompactor/pebble/`

| File | Tests | Verdict |
|---|---|---|
| `compactor_test.go` | Basic roundtrip, replaces-existing, isolates-other-syncs, empty source, bad inputs | **Robust.** The isolate-other-syncs case is the load-bearing safety property. |

### Equivalence — `pkg/dotc1z/engine/equivalence/`

| File | Tests | Verdict |
|---|---|---|
| `equivalence_test.go` | Pebble vs in-memory ref under 200 puts + 25 deletes, empty workload, single grant, put overwrite | **Robust** as a Pebble-correctness reference but not a SQLite equivalence test (SQLite parity is Stack 6 work). |

### Envelope — `pkg/dotc1z/format/v3/`

| File | Tests | Verdict |
|---|---|---|
| `envelope_test.go` | Roundtrip, bad magic, truncated header, descriptor closure verification | **Robust.** The autoresearch loop doesn't exercise envelope code (the bench's `EndSync` doesn't go through the v3 envelope yet), so this is a no-op gate for it. |

### SQLite engine — `pkg/dotc1z/`

| File | Coverage | Verdict |
|---|---|---|
| `c1file_test.go`, `c1file_concurrent_test.go`, `grants_test.go`, etc. | Inherits from main; covers SQLite write/read/sync lifecycle + the Windows-fragile `TestC1ZCachedViewSyncRunInvalidation` (fixed in commit `1627b047`). | **Robust for the regression sentinel.** Step 7 of the checks gate runs `go test -short` on the SQLite path. |

### What the existing tests **don't** cover (gaps the loop should NOT touch but should flag)

1. **Concurrent writers across goroutines** — the engine's `writeMu`
   serializes writers, but no test exercises concurrent
   `PutGrantRecords` calls from N goroutines. Would catch a race
   that a single-threaded bench misses.
2. **Crash recovery after `MarkFreshSync` mid-sync** — kill the
   process between PutGrantRecords calls and verify Pebble's WAL
   replay puts the engine in a consistent state. The micro-test
   exists at `/tmp/baton-rfc-microtests/checkpoint_test.go` but
   isn't ported.
3. **`IngestAndExcise` under concurrent reads** — the compactor's
   atomic-per-bucket claim is unproven beyond the basic happy path.
   A concurrent-reader test would harden it.

These are out of scope for the autoresearch perf loop — they're
correctness work for a separate session.

## 11. Stop conditions

The loop should stop (or escalate to a human) when:

- The primary metric plateaus for 20 consecutive iterations.
- `autoresearch.checks.sh` fails three iterations in a row from the
  same root cause — likely the agent has tried something structurally
  broken and is thrashing.
- A run achieves > 2× improvement on the primary metric — celebrate +
  confirm via a clean rerun + commit.
- The agent reaches a dead end on all priority-1 ideas — switch to
  priority-2 or stop and write up findings.

## 12. Resume protocol

If the loop resumes after a context limit / crash:

1. Read `autoresearch.md` (this file).
2. Read `autoresearch.ideas.md` (free-form scratch maintained by the
   agent). Prune stale entries.
3. Read `autoresearch.jsonl` (loop's own state, one line per
   experiment with `description`, `asi`, primary + secondary metrics,
   and `keep`/`discard` verdict).
4. Run `./autoresearch.sh` once to re-establish a baseline reading on
   the current commit.
5. Continue with the highest-priority idea from §9 or `autoresearch.ideas.md`.

## 13. Out-of-scope (loop won't touch)

- **Stack 6 grant expansion** — separate research spike, separate RFC.
- **Stack 7 C1 platform integration** — different repo.
- **`cmd/baton-c1z migrate`** — v1→v3 c1z migration. Operational tool,
  not a perf concern.
- **`protoc-gen-batonstore` codegen plugin** — listed as priority-2 in
  §9 but if the loop reaches it, it should be a deliberate human-
  approved branch (changes a lot of generated code).
- **SQLite optimizations** — the loop preserves SQLite as a regression
  sentinel; it doesn't try to make SQLite faster.

## 14. Final note for the agent driving the loop

Spending 200 iterations on Pebble perf is worthwhile only if the
output is reproducible. Every commit the loop makes should:

- Include the bench delta in the commit message.
- Reference which idea from §9 it implements (e.g. "(9.2)
  L0CompactionThreshold=4: writepack_1m 4.24s → 3.82s, -9.9%").
- Include a brief rationale in the body (why this works, what it
  trades away).

The dashboard view (`ctrl+shift+t` per the autoresearch UI) is the
human-facing summary; the commit log is what survives after the loop
ends and someone has to maintain the code.
