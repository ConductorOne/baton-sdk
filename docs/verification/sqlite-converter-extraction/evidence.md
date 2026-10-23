# SQLite converter extraction: evidence

Revision under test: `647d89d3` and the evidence-only commit on
`sqlite-converter-extraction`.

Risk route: HIGH. Wrong converted rows are silent and durable. The current
stage uses reference parity while both converters exist, committed fixture
goldens, close/reopen behavior tests, planted violations, race detection, and
repository gates.

## Implementation-obligation addendum

- `Open` owns the source file only while sniffing or decoding it. A framed
  source transfers its decoded temporary directory to `File`; `File.Close`
  closes the read-only pool before removing that directory. Bare SQLite files
  are never removed. Open failures close every acquired handle and remove only
  package-created temporary files. The source-byte before/after assertion in
  `TestFixtureGoldenDigests` is the oracle.
- `openSQLite` applies `mode=ro`, `query_only(ON)`, and
  `trusted_schema(OFF)` to every source connection. `Open` runs only catalog
  reads and `PRAGMA table_xinfo`; no migration or data statement exists in the
  package. Schema rejection tests compare complete source-file hashes.
- `ToPebbleOpen` owns the destination until successful return. Every error
  after `dotc1z.NewStore` closes the destination and removes `outPath`; success
  transfers close ownership to the caller. Its default destination temporary
  directory is independent of the decoded source directory, so the caller may
  close the source first. `TestConversionErrorsRemoveDestination` injects
  malformed protobuf bytes after destination open.
- `BulkSyncImport` is aborted unless `Finish` succeeds. A successful `Finish`
  transfers the imported keyspace to the destination engine. Stage errors
  retain destination cleanup ownership.
- Each grant lane owns one `BulkGrantShard`, one reader goroutine, one decoder
  goroutine, and one channel. The reader closes the channel; the decoder closes
  the shard; the parent waits for every goroutine. The first error cancels the
  shared scan context while readers drain or exit. Race evidence runs the
  parallel finished and edge-key fixtures.
- The empty-source path marks the fresh Pebble store dirty before transferring
  it to the caller, so caller `Close` materializes an empty v3 c1z.
- `BulkSyncImport.ComputedStats` owns converted record counts. The converter
  adds the asset count and stashes the sidecar before `EndSync`.
- `EndSync` establishes Pebble indexes, digests, stats, and durability. The
  metadata overlay then restores source identity, lineage, timestamps,
  verification provenance, and unfinished state. `ToPebbleOpen` returns only
  after the overlay is persisted in the open store.
- A caller that mutates the converted sync clears inherited verification,
  reseals, and restores the conversion's source timestamps through
  `pebble.AsEngine` before closing. The handle test checks the added row,
  timestamps, cleared verification, recomputed stats, and digest count after
  reopen.
- Optional source columns are resolved once at `Open`. Every query uses either
  the present column or its documented default. Required query columns fail at
  `Open`, naming their table and column.

## Planted violations

- Expansion reattachment: removed the `reattachExpansion` call and ran
  `go test ./pkg/dotc1z/sqlite -run
  '^TestFixtureReferenceParity/(finished_full.c1z|edge_keys.c1z)$' -count=1`.
  Both fixture cases failed on grant value and expansion-index differences.
  The call was restored.
- Unfinished metadata: removed `ClearEndedAt` and ran
  `go test ./pkg/dotc1z/sqlite -run
  '^TestFixtureGoldenDigests/unfinished_partial.c1z$' -count=1`. The golden
  digest failed. The branch was restored.
- Slim hydration: removed `hydrateGrant` and ran
  `go test ./pkg/dotc1z/sqlite -run '^TestSlimGrantsAreHydrated$' -count=1`.
  The output contained one grant instead of three. Hydration was restored.
- Golden digest: changed one hex digit in `finished_full.c1z.golden` and ran
  its `TestFixtureGoldenDigests` case. The expected and actual digests differed.
  The golden was restored.
- Import boundary: added a build-tagged non-test file in `pkg/dotc1z` that
  imported `pkg/dotc1z/sqlite` and ran
  `go test ./pkg/dotc1z -run '^TestDotc1zDoesNotImportSQLiteReader$' -count=1`.
  The test named the violating file and import. The file was removed.
- Required query column: removed `v1_assets.sync_id` after dropping its index
  and ran the `query base column` case in
  `TestOpenRejectsUnsupportedOrIncompleteSchemaWithoutMutation`. `Open`
  rejected the source and named `v1_assets.sync_id`.
- CI review closure: `TestToPebbleOpenSurvivesSourceClose` closes the decoded
  source before the destination and reopens the saved output. Schema cases
  replace `v1_grants` with an rtree virtual table and replace
  `v1_sync_runs` with a table lacking `id`; `Open` rejects both and names the
  violated condition.
- Portable golden timestamps: the first Linux and Windows CI run failed every
  non-empty golden because SQLite wall-clock timestamps map to
  location-dependent protobuf instants. The digest now clears source
  `started_at`, `ended_at`, and record `discovered_at` values. The golden test
  passes with both `TZ=UTC` and `TZ=America/New_York`; exact timestamp behavior
  remains covered by reference parity and the non-UTC child tests.

## Benchmark

Command:

`go test ./pkg/dotc1z/sqlite -run '^$' -bench
'^BenchmarkToPebble(Reference|Extracted)$' -benchmem -benchtime=3x -count=5`

Apple M1 medians from five isolated samples over the same committed edge-key
source:

- Reference: 373,235,778 ns/op; 33,300,330 B/op; 10,488 allocs/op.
- Extracted: 359,879,681 ns/op; 34,871,530 B/op; 9,638 allocs/op.
- Delta: -3.58% ns/op; +4.72% B/op; -8.11% allocs/op.

The elapsed-time delta is below the plan's 10% finding threshold. Both
converters retain the same O(rows) table scans, O(lanes × samples) boundary
sampling, and spill-sort cost. The extracted resource scan removes the
reference converter's whole-table SQLite sort.

Follow-up profiling found that the edge-key source has three resource types,
five resources, one entitlement, and one grant, but the spill sorters allocated
their full chunk capacities. Geometric growth removed that fixed allocation but
an isolated full-arena benchmark showed that a fresh 128 MiB arena then
allocated 255 MiB and copied 127 MiB while growing.

All spill sorters now use one allocation policy: start at 1 MiB and, when that
fills, allocate the configured chunk once and copy at most 1 MiB. The chunk
limit, sorting, merge, and full-arena recycling behavior are unchanged.
`BenchmarkSpillArenaFirstFill` exercises `spillSorter.add` without sorting,
filesystem, Pebble, or converter work. Apple M1 medians from five isolated
three-iteration samples are:

- 1 MiB payload: fixed 3.591 ms and 134,218,477 B/op; promoted 0.029 ms and
  1,049,288 B/op. Both report 6 allocs/op.
- 8 MiB payload: fixed 4.131 ms and 134,221,128 B/op; promoted 4.076 ms and
  135,269,704 B/op. Allocation count changes from 6 to 7.
- 127.94 MiB payload: fixed 8.258 ms and 134,267,208 B/op; promoted 7.817 ms
  and 135,315,784 B/op. Allocation count changes from 6 to 7.

The threshold-crossing and full-fill timings are within run-to-run variance;
the measured allocation cost is exactly one additional 1 MiB arena. The focused
`TestSpillSorterArenaPromotesOnce` pins the single promotion and configured
chunk ceiling. Restoring geometric growth as a planted violation failed that
test with a 2 MiB arena where 16 MiB was required.
`TestBulkImportChunkBytesStaysWithinBudget` reserves the actual 1 MiB growth
overlap per expected concurrent grant shard. With four sort slots, the derived
chunk sizes are 93 MiB for one shard, about 44 MiB for four shards, and about
26 MiB for eight shards. The test pins those calculations and the total-byte
budget.

`BenchmarkRegisteredPebbleWritePack` measures the same `BuildGrantDigests`
implementation used by compaction and reports `EndSync` separately from grant
writes. Apple M1 medians from three isolated three-iteration samples with the
single-promotion policy are:

- 100 grants: 78.33 ms/EndSync; 10,263,306 B/op; 7,264 allocs/op.
- 1,000 grants: 74.33 ms/EndSync; 11,909,778 B/op; 9,171 allocs/op.
- 10,000 grants: 84.33 ms/EndSync; 17,039,384 B/op; 9,208 allocs/op.

These scales remain dominated by fixed Pebble and filesystem work. The
measurements do not claim a whale-scale runtime result or isolate compaction
orchestration outside `BuildGrantDigests`; the isolated full-fill benchmark
covers the allocation-policy behavior once a sorter reaches its chunk ceiling.

For entitlements, `EXPLAIN QUERY PLAN` reported
`SCAN v1_entitlements USING INDEX idx_entitlements_external_sync_v1` with
`ORDER BY external_id` and `SCAN v1_entitlements` without it. The ordered query
scans the `(external_id, sync_id)` index and filters by its second column; its
order is discarded by `BulkSyncImport`'s destination-identity spill sort. The
converter now omits that source ordering. Fixture parity and golden tests cover
the unchanged destination output.

The grant bulk merge sees each duplicate-resolved primary row once in
entitlement-first order and can derive the hash-index row without unmarshaling
the grant. It can therefore remove the later full grant-keyspace scan. It
cannot remove ordering work: the hash index is ordered by
`(entitlement, hash(principal), principal)`, not the primary
`(entitlement, principal)` order. A fused implementation must retain an
external ordering stage for large entitlement partitions and preserve the
pending marker, exact digest publication, zero-grant roots, and post-import
mutation behavior. No digest-build change is included in this PR.

## Structural coverage

`go test ./pkg/dotc1z/sqlite -coverprofile=/tmp/sqlite-converter-final.cover`
at `51f35aef` reported 83.2% statement coverage. Covered normal paths include
every table, empty and non-empty
sources, finished and unfinished sources, optional columns and sessions table,
both resolve modes, serial and parallel grant scans, slim and expanded grants,
rowid overflow, bare and framed inputs, decoder limits, hostile schema,
pre-open errors, and post-open cleanup.

The uncovered conversion branches are destination capability failures,
destination close/save failures, context cancellation between row steps,
reader/shard errors after cancellation, and SQLite row/close failures. The
uncovered open/schema branches are source open/read/seek/close failures,
temporary-file create/sync/remove failures, connection-setup failures, and
catalog row/close failures. The remaining record branches are nil timestamps,
malformed optional cached stats/provenance, and defensive missing-reference
inputs. They are not used to claim bounded closure. Malformed source record
bytes, decoder limits, schema rejection, and destination cleanup have
executable failure instruments. Raw filesystem failures, SQLite driver
failures, and Pebble save failure after `Close` starts remain outside the local
fixture harness; Pebble has no abort-after-save-failure API, so this inherited
reference-converter limitation cannot be closed in this package.

## Reference-test disposition

- `to_pebble_test.go`: `TestFixtureReferenceParity`,
  `TestFixtureGoldenDigests`, `TestResolveSyncID`,
  `TestUnfinishedConversionPreservesResumeState`,
  `TestFinishedConversionLeavesSessionsBehind`,
  `TestToPebbleOpenMatchesToPebbleAndAllowsFurtherWrites`, and conversion
  error tests cover round trip, all resolve variants, empty input, newest/full
  selection, resources-only, lineage, resume, sessions, and option errors.
- `to_pebble_equivalence_test.go`: `TestFixtureReferenceParity` and
  `TestParallelAndSerialConversionMatch`.
- `to_pebble_discovered_at_test.go`: golden bytes and
  `TestToPebbleDiscoveredAtIsAbsoluteInstant`; multi-sync resolution covers
  cutoff and abandoned-sync selection.
- `to_pebble_localtime_test.go`: the non-UTC child runs started, ended,
  discovered, and discarded timestamp cases; `localWallClock` assertions are
  the direct `localizeSQLiteTimestamp` cases.
- `to_pebble_unconvertible_test.go`: `TestUnknownLegacySyncTypesListButDoNotAutoResolve`.
- `resolve_convert_sync_id_test.go`: `TestResolveSyncID` and the multi-sync
  fixture cover mixed selection and the unfinished cutoff.
- `discarded_syncs_test.go`: multi-sync reference parity compares every
  `DiscardedSync`; the non-UTC case checks localized timestamps; sole-sync
  fixtures produce no discarded rows. `TestConvertOpenWarnsAboutDiscardedSyncs`
  belongs to `dotc1z.NewStore`'s in-place compatibility conversion and is
  outside the extracted API.
- `rowid_overflow_security_test.go`: `TestRowIDSpanOverflowUsesSingleGrantRange`.
- `legacy_acceptance_security_test.go`: `old_schema.c1z` golden and reference
  parity cover old-schema reads, defaults, conversion, and selected-sync
  isolation. Trigger and view rejection are additional hostile-schema cases.
- `legacy_diff_sync_rows_test.go`: unknown legacy type listing, explicit-ID
  conversion through fixture parity, and loud auto-resolve failure.
- `sqlite_open_dsn_test.go`: `TestSQLiteDSNPortablePathShapes`.
- `to_pebble_bench_test.go`: reference and extracted benchmarks above.

## Acceptance criteria

1. **verified to stated coverage** — `pkg/dotc1z/sqlite` exposes `Open`,
   `Close`, `SyncRuns`, `ResolveSyncID`, `ToPebbleOpen`, and `ToPebble`.
   `go doc github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite` contains no
   `C1File` reference.
2. **verified to stated coverage** — `TestDotc1zDoesNotImportSQLiteReader`
   passes; its planted import failed.
3. **verified to stated coverage** — reference parity passes for every
   framed non-slim fixture, and the slim fixture asserts the documented
   persisted-row deviation and exact hydrated identities. The bare database
   has extracted bare-versus-framed digest equality because the reference
   decoder rejects unenveloped SQLite. Expansion, unfinished metadata, and
   hydration planted violations failed.
4. **verified to stated coverage** — all nine fixtures have committed goldens;
   `TestFixtureGoldenDigests` passes without `fixturegen`; the corrupted golden
   failed; UTC and non-UTC runs produce the same digests.
5. **verified to stated coverage** — `TestToPebbleOpenMatchesToPebbleAndAllowsFurtherWrites`
   covers close equivalence and a lifecycle-correct post-conversion grant
   write, including source timestamp restoration, cleared verification,
   recomputed stats, and grant digest count.
   `TestToPebbleOpenSurvivesSourceClose` covers independent source and
   destination temporary-storage ownership.
6. **verified to stated coverage** — every named reference test has a port or
   disposition in the section above.
7. **verified to stated coverage** — `go run ./cmd/baton to-pebble -f
   pkg/dotc1z/sqlite/testdata/finished_full.c1z --out <temporary path>`
   succeeded. The same fixture and converter path are checked against its
   golden by `TestFixtureGoldenDigests`.
8. **verified to stated coverage** — benchmark median is -3.58% ns/op and
   -8.11% allocs/op, below the 10% finding threshold.
9. **verified to stated coverage** — the only `pkg/dotc1z` change is
   `import_boundary_test.go`; the new package is under its own subdirectory.
10. **verified to stated coverage** — all repository gates pass locally; all
    four native Windows test shards and the Windows aggregate check pass in PR
    run `39332212347`.
11. **verified to stated coverage** — this file records one state for every
    acceptance criterion and names the executable evidence and exclusions.

## Repository gates

Local final-gate run:

- `go build ./...`: passed.
- `go vet ./...`: passed.
- `golangci-lint run ./pkg/dotc1z/... ./cmd/baton/...`: passed with zero
  issues.
- `go test -race ./pkg/dotc1z/sqlite/`: passed.
- `go test ./pkg/dotc1z/... ./cmd/baton/...`: passed.

The commands unset `GOMODCACHE`, `GOCACHE`, `GOPATH`, and `GOFLAGS` before
invoking Go. PR run `39332212347` passed all checks, including four native
Windows shards, Linux tests, lint, and the race-shard audit.
