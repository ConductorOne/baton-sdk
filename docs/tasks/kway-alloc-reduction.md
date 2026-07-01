# Task: Reduce k-way compactor allocations by ~90%

## Context

The Pebble k-way merge compactor (`pkg/synccompactor/pebble/kway.go`,
`overlay.go`) is faster than the SQLite compactor but allocates 30–40x
more objects. Profiling the `syncs=500` same-size case at fan-in 50
shows ~30.8M allocations per op, of which ~96% come from four
mechanical patterns in the merge hot loops. Fixing them should bring
allocations down to roughly 3M/op (~3–4x SQLite) without changing the
merge algorithm, on-disk run-file format, or output.

Reference profiles (already captured, in repo root):

- `.profiles/same500-kway.fanin50.cpu.pprof`
- `.profiles/same500-kway.fanin50.mem.pprof`

Profile breakdown (alloc_objects, 30,855,941 total):

| Source | Allocs (cum) | Share |
|---|---|---|
| `encodeIndexKeysByFamilyFromRaw` (fresh scratch + blob buffers per record) | 13.1M | 42% |
| Run-record I/O header arrays escaping through `io.Reader`/`io.Writer` | ~9.2M | 30% |
| `sortIndexRunChunks` (one heap slice per index key) | 5.9M | 19% |
| `directStream.next` (per-record key + value copies) | 3.2M | 10.5% |

GC is NOT the current wall-clock bottleneck (CPU profile is
syscall-dominated; GC ~5%). The goal is allocation-count and GC-pressure
reduction, holding throughput at least neutral.

## Reproduction / verification

```bash
# Benchmark (compares sqlite vs pebble_kway vs pebble_overlay):
go test -run '^$' -bench 'BenchmarkCompactorSQLiteVsPebbleKWay/syncs=500/pebble_kway' \
  -benchmem -benchtime 1x \
  -cpuprofile /tmp/kway.cpu.pprof -memprofile /tmp/kway.mem.pprof \
  ./pkg/synccompactor

# Inspect:
go tool pprof -top -sample_index=alloc_objects /tmp/kway.mem.pprof
go tool pprof -list='<func>' -sample_index=alloc_objects /tmp/kway.mem.pprof
```

Fixture generation dominates benchmark setup time. The bench supports
prebuilt fixtures via `BATON_WRITE_COMPACTION_COMPARE_FIXTURES_DIR`
(write once) and `BATON_COMPACTION_COMPARE_FIXTURES_DIR` (reuse); see
`pkg/synccompactor/compactor_compare_bench_test.go`.

Correctness gates (must pass after every item):

```bash
go test ./pkg/synccompactor/... ./pkg/dotc1z/...
```

In particular `pkg/synccompactor/pebble/kway_test.go`,
`merge_stats_test.go`, and the cross-engine parity tests in
`pkg/dotc1z`. `make lint` must also pass.

---

## Item 1 — Reuse index-key scratch; emit keys directly to writers (~13M allocs, 42%)

**Where:** `pkg/synccompactor/pebble/overlay.go` →
`encodeIndexKeysByFamilyFromRaw` (~line 346); callers in
`pkg/synccompactor/pebble/kway.go` → `materializeRunFileBucket`
(~line 1150), `materializeSourceBucketToPebble` (~line 618), and the
overlay whole-bucket fast path in `overlay.go` (~line 321).

**Problem:** every winner record does `var scratch rawIndexScratch`
and `var out indexBlobs`. All scratch buffers (`key`, `tail1`,
`tail2`) and all per-family blob buffers grow from nil cap, so every
record pays fresh allocations in:

- the blob appends (`out[idx] = append(...)` — 6.4M flat),
- `codec.DecodeTupleStringTo` (2.7M) and `codec.appendEscaped` (2.4M),
- `enginepkg.Append*IndexKeyRawBytes` (1.3M+).

Note the raw field scanners (`scanResourceParentBytes`,
`scanEntitlementResourceBytes`, `scanGrantIndexFieldsBytes`,
`scanResourceRefBytes`, `scanEntitlementRefBytes`,
`scanPrincipalRefBytes`) already return borrowed sub-slices of the
Pebble value — they are zero-alloc and must stay that way. The
allocations are entirely in the scratch/blob layer above them.

**Fix (two parts):**

1. Hoist one `rawIndexScratch` per bucket-materialize loop and pass
   it through (`forEachIndexKeyFromRaw` already accepts a
   `*rawIndexScratch`; the bug is that `encodeIndexKeysByFamilyFromRaw`
   constructs a fresh one instead of taking one from the caller).
2. Eliminate the `indexBlobs` round-trip entirely. The blob format is
   just length-prefixed keys, which `indexRunWriter.writeBlob`
   immediately re-parses and re-writes length-prefixed to its
   `bufio.Writer`. Instead, pass the `*indexWriterSet` (or an emit
   closure that routes on `indexID(key)`) into the per-winner callback
   and write each key directly via `writeLengthPrefixedBytes` to the
   right family writer. Delete `indexBlobs`, `writeBlobs`, and
   `writeBlob` once no callers remain.

**Watch out:** the `stats *mergeStatsAccumulator` parameter piggybacks
per-RT grouping on this scan and must only be passed for winners —
preserve that contract (see comment above `forEachIndexKeyFromRaw`).

**Acceptance:** `codec.DecodeTupleStringTo`, `codec.appendEscaped`,
and the blob-append lines disappear from the alloc_objects top.

## Item 2 — Stop header arrays escaping through interface I/O (~9.2M allocs, 30%)

**Where:** `pkg/synccompactor/pebble/kway.go` → `writeRunRecord`
(~line 714, 2.7M), `readRunRecordInto` (~line 744, 2.4M),
`writeLengthPrefixedBytes` (~line 1305, 2.4M),
`readLengthPrefixedBytes` (~line 1319, 1.7M).

**Problem:** each function declares a small stack array
(`var header [runHeaderSize]byte` / `var lenBuf [4]byte`) and passes a
slice of it to `io.ReadFull(r, ...)` or `w.Write(...)` where `r`/`w`
are interface types. Escape analysis can't prove the callee doesn't
retain the slice, so the array is heap-allocated **per call** — one
allocation per record/key read or written.

**Fix:** give the buffer a home that lives for the whole merge instead
of the call frame. Options (pick one, apply consistently):

- Wrap readers/writers in small concrete structs (`runWriter{w
  *countingWriter, hdr [20]byte}` / `runReader{r *bufio.Reader, hdr
  [20]byte}`) and make these functions methods on them, using
  `o.hdr[:]`; one allocation per file open, amortized to ~0.
- Or thread an explicit `hdr []byte` scratch parameter from the
  per-bucket loops.

Apply to all four functions, including the `readLengthPrefixedBytes`
call sites in `sortIndexRunChunks` and `mergeSortedIndexChunksToSST`,
and `writeLengthPrefixedBytes` in `writeSortedIndexChunk` and the item
1 direct-emit path.

**Acceptance:** flat allocations in all four functions drop to ~0;
run-file bytes on disk are identical (format unchanged).

## Item 3 — Arena-sort the index chunks (~5.9M allocs, 19%)

**Where:** `pkg/synccompactor/pebble/kway.go` → `sortIndexRunChunks`
(~line 1336).

**Problem:** `keys = append(keys, append([]byte(nil), scratch...))`
heap-copies every index key (4.2M flat) and builds a 100k-entry
`[][]byte` per chunk that the GC scans pointer-by-pointer.

**Fix:** read each chunk into one contiguous arena `[]byte` (reused
across chunks) and collect `[]struct{ off, len uint32 }` views;
sort the views with `bytes.Compare(arena[a.off:a.off+a.len], ...)`.
Write the sorted chunk by slicing the arena. Bonus: better sort cache
locality. Keep the dedupe-on-write behavior in
`writeSortedIndexChunk` (adapt it to take arena+views or feed it
slices).

Arena sizing: keys are bounded (Pebble keys, typically < 1 KiB);
`indexSortChunkKeys` (100k) × average key size ≈ tens of MB — grow
the arena geometrically and reuse it across chunks and across
`indexRunWriter` instances if convenient.

**Acceptance:** `sortIndexRunChunks` flat allocations drop from ~4.2M
to O(chunks); chunk files byte-identical to before.

## Item 4 — Double-buffer key/value in `directStream.next` (~3.2M allocs, 10.5%)

**Where:** `pkg/synccompactor/pebble/kway.go` → `directStream.next`
(~line 415) and `rewritePrimaryKeyForDest` (~line 902).

**Problem:** every source record allocates twice: a fresh rewritten
key (`make` in `rewritePrimaryKeyForDest`, 1.6M) and a fresh value
copy (`append([]byte(nil), s.iter.Value()...)`, 1.7M).

**Fix:** each `directStream` owns reusable `keyBuf`/`valBuf` scratch;
`next()` copies into them (`rewritePrimaryKeyForDestInto` already
exists for the key half) and returns a `runRecord` referencing the
stream-owned buffers.

**Safety argument (verify, then encode in a comment):** the merge
loops (`mergeSourceBucketToRunSection`,
`materializeSourceBucketToPebble`) hold at most one live `runRecord`
per stream in the heap. A stream's buffers are only overwritten by
that stream's own `next()`, which is called only after the current
group — including any winner that references those buffers — has been
fully consumed (written via `writeRunRecord` / `primaryWriter.Set` and
index-emitted). Confirm there is no path where a popped record
outlives its stream's subsequent `next()`; pay attention to
`lastPrimaryKey` (it already defensively copies via
`append(lastPrimaryKey[:0], ...)` — keep that).

**Acceptance:** `directStream.next` and `rewritePrimaryKeyForDest`
flat allocations drop to ~0 (amortized growth only).

## Item 5 — Verify the raw scan layer stays zero-copy; lock it in

**Where:** `pkg/synccompactor/pebble/overlay.go` scan helpers
(`scanResourceParentBytes` ~line 512, `scanEntitlementResourceBytes`
~line 539, `scanGrantIndexFieldsBytes` ~line 566, plus
`scanResourceRefBytes` / `scanEntitlementRefBytes` /
`scanPrincipalRefBytes`).

These return borrowed sub-slices of the raw Pebble value and allocate
nothing today. After items 1–4 land:

1. Re-profile and confirm none of the scan helpers (nor
   `codec.DecodeTupleStringTo` / `codec.appendEscaped` /
   `protowire.*`) appear in the alloc_objects top.
2. The borrowed slices are views into `iter.Value()` (k-way direct
   path) or stream-owned `valBuf` (after item 4) — confirm no emitted
   index key or stats-accumulator path retains them past the record's
   lifetime. `mergeStatsAccumulator` grouping must copy when it stores
   (check `groupResource` / grant grouping in
   `pkg/synccompactor/pebble/merge_stats.go`).
3. Add a focused allocation-regression benchmark, e.g.
   `BenchmarkForEachIndexKeyFromRaw` over a canned grant record
   asserting ~0 allocs/op with warm scratch (use `b.ReportAllocs()`
   and a `testing.AllocsPerRun` guard test so CI catches regressions).

## Sequencing & expected outcome

Recommended order: 2 → 1 → 4 → 3 → 5 (item 2 is mechanical and
de-risks the I/O layer that item 1's direct-emit builds on).

After each item, re-run the syncs=500 kway bench with `-memprofile`
and record allocs/op in the PR description. Expected cumulative result:
~30.8M → ~3M allocs/op (~90% reduction) with neutral-or-better ns/op.

The overlay compactor (`overlay.go`) shares
`encodeIndexKeysByFamilyFromRaw`, `indexWriterSet`, and the
length-prefixed I/O helpers, so items 1–3 improve
`pebble_overlay` benchmarks too — report both.

Do not change: run-file/chunk on-disk formats, winner-selection
semantics (`runRecordIsNewer`), stats-accumulator winner-only
contract, or any public API.
