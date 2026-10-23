# SQLite engine removal: implementation and verification plan

Status: frozen for implementation (PR B of the SQLite removal; PR A is
`docs/verification/sqlite-converter-extraction/`). Post-freeze changes go in
"Change orders" at the end.

## Goal

Delete the v1 (.c1z over SQLite) storage engine from the SDK. After this PR
the only code in the module that can open a v1 file is `pkg/dotc1z/sqlite`,
the read-only reader and converter PR A added, and the only way to reach it
is to import it. Connector binaries do not import it, so `modernc.org/sqlite`
and `goqu` leave their dependency graph. c1 imports it and keeps reading v1
uploads from old connectors indefinitely.

Scope is "remove the functionality", not "relocate it behind a hook" and not
"clean up everything that mentions SQLite":

- No converter hook in `pkg/dotc1z`. A v1 file opened through `NewStore`
  fails with a typed error unconditionally. `pkg/dotc1z` never converts.
- No `baton` CLI read fallback. `baton <cmd> -f old.c1z` fails with the
  typed error naming `baton to-pebble`; `baton to-pebble` is unchanged.
- No change to `pkg/sync` fallback branches that SQLite used to take
  (`storeCaps` nil paths, buffering fallbacks), except the one documented
  contract named under "Design: `pkg/sync`".
- No comment or doc sweep in files this PR does not otherwise edit.

## Constraints

- No change to `pkg/dotc1z/engine/pebble` non-test code, `pkg/dotc1z/sqlite`
  non-test code except registering nothing (it registers nothing today), or
  `pkg/sync` beyond the one case below.
- `pkg/dotc1z` (non-test) still must not import `pkg/dotc1z/sqlite`
  (`import_boundary_test.go` stays).
- The committed fixtures and goldens in `pkg/dotc1z/sqlite/testdata/` are
  never regenerated. Tests that need a v1 input read a committed fixture.
- No version bump. Buf breaking check must stay green, so
  `proto/c1/connectorapi/baton/v1/baton.proto` is not edited; a task that
  still carries `storage_engine: "sqlite"` fails at `NewStore` with the
  existing `ErrEngineNotAvailable`.
- Public repo: no production identifiers.

## Verified facts

### Connector exposure

- Service mode (`pkg/tasks/c1api/full_sync.go:302`) creates a fresh temp c1z
  per task; cross-restart task resumption is not implemented (`:284-292`).
  The only v1 file that can appear is the previous-sync spare, opened via
  `WithOptionalPreviousSyncC1ZPath`, which degrades on any open error
  (`pkg/sync/syncer.go:4426-4435`).
- Local mode (`pkg/tasks/local/syncer.go:124`) opens the `--file` path for
  writing. An existing v1 file fails every run until the operator converts or
  removes it. Accepted; Pebble has been the default since #1087 (2026-08-21).
- The strict `WithPreviousSyncC1ZPath` path documents that a SQLite input is
  a valid file treated as a cold input (`syncer.go:4127-4128`); today that is
  reached through the `!AsEngine` branch after a successful SQLite open.

### Pebble ignores these options

`pebbleDriver.OpenStore` reads `TmpDir`, `DecoderOptions`, `ReadOnly`,
`EncoderConcurrency`, `SyncLimit`, `SkipCleanup`, `DisableGrantDigestIndex`,
`PayloadEncoding`, `DecoderPool`, `MaxDecoded*` (`pebble_store.go:120-140`).
`Pragmas`, `V2GrantsWriter`, `skipVacuum`, `bulkLoad` are read only by the
SQLite constructor. External non-test users of `WithPragma`: three calls in
`pkg/synccompactor/compactor.go:305-309`; of `WithV2GrantsWriter`,
`WithSkipVacuum`, `WithBulkLoad`: none.

### Files

`pkg/dotc1z` non-test files to delete: `assets.go`, `bulk_by_ids.go`,
`c1file.go`, `c1file_attached.go`, `c1file_store.go`, `clone_sync.go`,
`convert_open.go`, `copy_isolate_sync.go`, `entitlements.go`, `grants.go`,
`grants_expandable_query.go`, `grants_for_entitlements.go`,
`grants_hydrate.go`, `internal_grant_options.go`, `resouce_types.go`,
`resources.go`, `session_store.go`, `sql_helpers.go`, `sqlite_open.go`,
`sqlite_schema_guard.go`, `store.go`, `streaming.go`, `sync_runs.go`,
`to_pebble.go`.

Declared in a deleted file and still used by a staying file (relocate to
`options.go`): `C1ZOption`, `c1zOptions` and its fields, `WithTmpDir`,
`WithDecoderOptions`, `WithReadOnly`, `WithEncoderConcurrency`,
`WithSkipCleanup`, `WithSyncLimit`, `WithEngine`, `WithGrantDigestIndex`,
`WithPayloadEncoding`. Deleted with the engine: `pragma`, `WithPragma`,
`WithSkipVacuum`, `WithBulkLoad`, `WithV2GrantsWriter`, `StorePragma`,
`StoreOptions.Pragmas`, `StoreOptions.V2GrantsWriter`, `cleanupDbDir`,
`ErrReadOnly`, `ErrDbNotOpen`, `NewC1ZFile`, `NewC1File`, `C1File`,
`AsSQLiteStore`, `sqliteDriver`, `convertExistingV1C1ZFile`,
`pebbleOpenOptionsFromC1Z`, the `(*C1File)` methods in
`rollback_expansion.go` with `clearOp` and `canonicalGrantSources`, the
`(*C1File)(nil)` assertions in `pebble_store.go:56-57`,
`c1zstore.EngineSQLite`, `decodeBatchCursor` and its test.

`pkg/synccompactor`: delete `attached/`, `CompactorTypeAttached` and
`CompactorType`, `WithCompactorType`, `ErrEnginePolicyConflict`,
`resolvedEngine`, the `default:` SQLite merge arm in `Compact`
(`compactor.go:462-492`), `doOneCompaction`, the three `WithPragma` calls,
`resolveSQLiteCompactionSyncID`, `sqliteSyncRunByID`,
`convertSQLiteInputToPebble`, the `convertedInputs` handling in
`compactPebble` and `compactPebbleFold`, and the `unsupported_engine`
incremental-expansion outcome. `inferEngineFromInputs` becomes a v1
rejection check.

Elsewhere: `cmd/baton/optimize.go:39-50` (`AsSQLiteStore` branch),
`pkg/field/defaults.go:347,352` (`sqlite` value), `pkg/sync/syncer.go`
previous-sync strict path, `.github/workflows/ci.yaml:161` (`sqlite` matrix
leg), `.github/workflows/nightly.yaml:143-146` (`differential` leg),
`Makefile` (`differential-check`, `incremental-soak`, `prodscale-topebble`,
`DIFFERENTIAL_TIME`, `NIGHTLY_DIFFERENTIAL_TIME`, the
`SQLiteExternalPrincipalResumeDegradesWithoutFailure` entry in
`chaos-check`), `.golangci.yml:101-106` (`NewC1ZFile` deprecation
exclusion), `go.mod` (`goqu` and its `replace`; `mattn/go-sqlite3` indirect
if tidy drops it).

Committed v1 fixtures outside `pkg/dotc1z/sqlite/testdata`:
`pkg/sync/testdata/sync-in-progress.c1z` (unfinished sync with a v0 token;
`token_test.go:446`), `pkg/sync/expand/testdata/sync.*.{unexpanded,expanded}`
(`36zfOAoW…` pair used by `expand_correctness_test.go` with no Pebble
counterpart; the other two pairs used only by SQLite benchmarks).

### Test dispositions

Full per-test tables are in the three inventory reports that produced this
plan; the rules they apply:

- DELETE: tests the SQLite engine, its internals, its writer, the reference
  converter, `attached`, convert-open, or a SQLite-vs-Pebble comparison.
- TRIM: engine matrices lose their `EngineSQLite` row. Where the SQLite row
  was the only thing a comparison test compared against
  (`cross_engine_parity_test.go` comparative subtests,
  `TestSyncMetaStatsV2CrossEngineEquality`, `TestFullPipelineDifferentialFuzz`,
  `TestTopologicalMergeMatchesSQLiteGroundTruth`), the test is deleted; a
  one-arm parity test asserts nothing.
- RETARGET: `NewC1ZFile` used as a generic opener becomes `NewStore`; typed
  `*dotc1z.C1File` helpers become `c1zstore.Store`. Methods needed that are
  not on `c1zstore.Store` (`RollbackExpansion`, `SetSupportsDiff`,
  `ListSyncRuns`) are reached through the local interface the production
  caller already uses or a test-local one.
- Fallback-path tests in `pkg/sync` that needed a store without a
  capability (`TestIngestInvariantSkipLogTelemetry`,
  `TestIngestInvariantSkipLogEscalatesForTypeScopedStores`) get a wrapper
  store that hides the capability; tests whose scenario cannot exist without
  a SQLite store (`TestChaosConnectorSQLiteExternalPrincipalResumeDegradesWithoutFailure`,
  `TestCleanupContextDeadlineExceeded`, `TestSQLiteSyncDoesNotRecordTimingStats`)
  are deleted. Fake stores that report `Engine: string(c1zstore.EngineSQLite)`
  to take the token attach path report a literal non-Pebble string instead.
- `pkg/dotc1z/sqlite` tests: delete `fixtures_generate_test.go`,
  `TestFixtureReferenceParity`, `toReferenceDiscarded`,
  `BenchmarkToPebbleReference`, and the reference half of
  `TestSlimGrantsAreHydrated` (`converter_test.go:169-197`). Everything
  else runs against committed fixtures and stays.
- Hostile-v1 security tests in `pkg/dotc1z` that asserted the schema guard
  rejected a file now assert something stronger: `NewStore` returns
  `ErrV1Unsupported` before decoding the payload, so no SQLite code runs on
  the bytes. One test keeps the hostile fixture builder for that assertion;
  the schema-guard cases themselves live in
  `sqlite/converter_test.go:295`.

## Design

### `pkg/dotc1z`

```go
// ErrV1Unsupported is returned by NewStore for a v1 (SQLite) .c1z. The SDK
// no longer reads or writes that format; convert the file with
// `baton to-pebble` or read it through pkg/dotc1z/sqlite.
var ErrV1Unsupported = errors.New("dotc1z: v1 (sqlite) c1z files are not supported; convert with `baton to-pebble`")
```

`selectStoreDriver`'s `C1ZFormatV1` arm returns
`fmt.Errorf("select-store-driver: %s: %w", path, ErrV1Unsupported)` after
reading the header and before any other I/O. The registry's static map holds
only `pebbleDriver`. `requireEngineDriver(EngineSQLite)` is unreachable
because the constant is gone; an unknown string still fails with
`ErrEngineNotAvailable`.

`options.go` holds the relocated option declarations with their existing doc
comments, minus the SQLite sentences.

### `pkg/synccompactor`

`inferEngineFromInputs` reads each input header: v3 → continue; v1 →
`fmt.Errorf("compactor: input %s: %w", path, dotc1z.ErrV1Unsupported)`
before any store is opened or any output created; unreadable → the existing
error. It returns no engine; `Compact` always produces Pebble.
`WithEngine(e)` with `e != EnginePebble` is an error at `Compact`. The
fold/overlay/rebuild selection is unchanged. `inferEngineFromInputs` is
renamed `rejectUnsupportedInputs`.

### `pkg/sync`

`NewSyncer`'s strict previous-sync branch adds
`case errors.Is(err, dotc1z.ErrV1Unsupported):` with the same warn-and-cold
outcome as the existing `!AsEngine` branch, so the documented contract
("SQLite inputs are valid c1z files but are treated as cold inputs") holds.
The optional branch already degrades on any error.

### `cmd/baton`

`optimize.go` keeps only the "not applicable" branch. `to_pebble.go` is
unchanged. No read fallback.

### Fixtures

`pkg/sync/testdata/sync-in-progress.c1z` is converted once with
`sqlite.(*File).ToPebble(..., ResolveNewest)` into
`sync-in-progress.pebble.c1z`; the v1 file is deleted. `token_test.go`
asserts the converted file still lists the unfinished sync with its v0
token. `pkg/sync/expand/testdata/sync.36zfOAoW….{unexpanded,expanded}` are
converted the same way to `.pebble.c1z` siblings and the v1 pair deleted;
the four other v1 fixtures are deleted with their benchmarks.

## Verification

Risk route: the production change is a deletion whose failure mode is loud
(typed error at open). Consumer distance is the escape axis: c1 is another
repo, and connectors upgrade on their own schedule. The silent risk is in
the tests: a retargeted or trimmed test can pass while asserting nothing,
and the reader c1 depends on forever loses its reference oracle in this PR.
Routed HIGH on consumer distance plus silent test weakening; machinery kept
proportional (plan, instruments below, evidence, gates).

### Oracles

1. **Dependency boundary.** `go list -deps ./pkg/connectorrunner
   ./pkg/connectorbuilder ./pkg/cli` contains neither `modernc.org/sqlite`,
   `github.com/doug-martin/goqu/v9`, nor
   `github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite`. Encoded as
   `TestConnectorBinariesDoNotLinkSQLite` in `pkg/connectorrunner`, running
   `go list` from the module root.
2. **v1 rejection before decode.** `NewStore` on a copy of every fixture in
   `pkg/dotc1z/sqlite/testdata/*.c1z`, read-only and writable, with and
   without `WithEngine(EnginePebble)`, returns `ErrV1Unsupported`, leaves
   the file byte-identical, and creates nothing under `WithTmpDir`.
3. **Compactor rejects v1 inputs before work.** `Compact` with one v1 input
   among Pebble inputs returns `ErrV1Unsupported`, and the output path and
   tmp dir hold no new files.
4. **Reader goldens.** `TestFixtureGoldenDigests` passes unchanged; the
   goldens are the reference converter's output and now pin the only v1
   reader.
5. **Converted fixtures.** `token_test.go` finds the v0 token in the Pebble
   fixture; `expand_correctness_test.go` `Small` passes on the Pebble pair.

### Instruments

- Planted violation for oracle 1: add `_ ".../pkg/dotc1z/sqlite"` to a
  `pkg/connectorrunner` file; the test must fail. Record and revert.
- Planted violation for oracle 2: temporarily make the `C1ZFormatV1` arm
  fall through to `requireEngineDriver(EnginePebble)`; the test must fail.
- `import_boundary_test.go` still passes.
- `go test -race` on `pkg/dotc1z/...`, `pkg/synccompactor/...`, `pkg/sync`.
- Structural check that no retargeted test became vacuous: every TRIM test
  that had an engine loop still has at least one `t.Run` arm, and every
  deleted comparison test is named in evidence with the reason.

### Repository gates

`go build ./...`, `go vet ./...`, `golangci-lint run ./...`, `go mod tidy`
and `go mod vendor` produce no diff after commit, full `go test ./...`, and
the Windows CI shards.

## Out of scope

- `pkg/sync` capability-fallback branches.
- `pkg/tasks/local` auto-rename of an existing v1 `--file`.
- `baton` CLI read fallback for v1 files.
- Comment and doc mentions of SQLite in untouched files; RFCs; prior
  verification evidence.
- Splitting `pkg/dotc1z/sqlite` into its own module.
- `cmd/baton/dump_db.go`.
- c1: switching sanitize and the upload path to `sqlite.Open`/`ToPebble`
  (in progress separately).

## Acceptance

- No file under `pkg/` outside `pkg/dotc1z/sqlite` imports
  `modernc.org/sqlite` or `goqu`; `go.mod` has no `goqu` requirement or
  `replace`.
- `rg 'C1File|NewC1ZFile|AsSQLiteStore|EngineSQLite|WithPragma'` over
  non-test, non-vendor Go files returns only `pkg/dotc1z/sqlite` internals
  and comments in untouched files.
- Oracles 1-5 pass; both planted violations were observed to fail.
- Every deleted test file or function is listed in `evidence.md` under
  DELETE/TRIM/RETARGET with the rule it matched.
- Repository gates green, including Windows.
- `evidence.md` records each criterion using the checklist vocabulary.

## Change orders

1. **SQLite-as-oracle tests, split by what the oracle was.** The "one-arm
   parity test asserts nothing" rule treated every SQLite-vs-Pebble test
   the same. They are three kinds:
   - Oracle is the source-batched expander, run on SQLite only because
     SQLite is an unsorted store (`TestTopologicalMergeMatchesSQLiteGroundTruth`,
     `TestTopologicalMergeDifferentialRandomStore`, the engine matrices in
     `topological_merge_resume_test.go`,
     `TestTopologicalMergeUntouchedBaseGrantNotRewritten`). In production
     Pebble is the only store and always takes the projection branch of
     `RunSingleStep`, so the current implementation is the definition of
     expansion correctness, not the legacy algorithm. These tests are
     RETARGETED, not deleted: ground truth is `Expander.Run` on Pebble
     (projection); candidates are `streaming/pebble` and
     `streaming/pebble/small-page`. The mock-store sweep
     `TestTopologicalMergeDifferentialRandom` never used SQLite and keeps
     running the legacy algorithm in memory.
   - Oracle is the other engine (`TestFullPipelineDifferentialFuzz`,
     `whale_graph_compare_test.go`, `cross_engine_parity_test.go`,
     `TestSyncMetaStatsV2CrossEngineEquality`, compactor mixed-input and
     recency tests, `TestFixtureReferenceParity`). Deleted as planned; no
     SQLite writer exists to produce the reference. The pipeline fuzz was
     the only end-to-end artifact differential (translate round-trips,
     deferred `by_principal` build, reopen, bare-id gets, stats sidecar).
     `evidence.md` records that gap. Follow-up: a model-based pipeline fuzz
     reusing the seed generator and `comparePipelineArtifacts`.
   - Oracle is full-vs-incremental expansion and only the `sqliteParityCase`
     type names mention SQLite (`FuzzIncrementalVsFullExpansion`,
     `TestIncrementalDifferentialRandom`,
     `TestIncrementalExhaustiveFourNodeGraphs`,
     `TestCompactorIncrementalDifferentialRandom`). Unchanged.

   Rejected alternative: moving the SQLite engine into a nested module to
   keep it as a live oracle. That keeps the dual-implementation maintenance
   the removal exists to shed, and a frozen oracle patched to agree with the
   code under test stops being one.

2. **Hostile v1 fixture builder dropped.** The plan kept `hostileV1Fixture`
   for one "rejected before decode" test. Oracle 2 runs over copies of the
   committed fixtures in `pkg/dotc1z/sqlite/testdata` instead, so
   `pkg/dotc1z` tests no longer import `modernc.org/sqlite`. The trigger
   fixture proved nothing extra: no SQLite code runs on the bytes, which
   oracle 1 establishes by dependency.

3. **Retargeted rather than deleted:** `pkg/dotc1z/sql_helpers_test.go`
   (pure `PutResources`/`ListResources`/parent-filter surface) became
   `resources_test.go` on `NewStore`; `pkg/synccompactor/compactor_test.go`
   sync-type-union and run-duration tests moved to Pebble inputs.
   `compactor_bench_test.go` keeps its Pebble benchmarks only.
