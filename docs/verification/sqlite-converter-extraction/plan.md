# SQLite converter extraction: implementation and verification plan

Status: frozen for implementation (PR A of the SQLite removal). Post-freeze
changes go in "Change orders" at the end.

## Goal

Move v1 (.c1z over SQLite) reading and v1 → Pebble conversion out of
`pkg/dotc1z` into a new package that depends on `pkg/dotc1z` and never the
reverse. After this PR the SDK still contains the SQLite engine; this PR only
adds the replacement beside it, proves equivalence while the reference still
exists, and freezes fixtures and golden digests so later PRs can delete the
reference without losing the oracle.

The new package is the single home for v1 reading. `cmd/baton to-pebble`
switches to it in this PR. c1's sanitize path and the SDK's convert-open and
compactor paths switch in later PRs.

## Constraints

- Preserve every conversion behavior `C1File.ToPebble` has today: finished
  and unfinished syncs; `connector_sessions` copy and `sync_token`
  preservation for unfinished sources; `started_at`/`ended_at` localization;
  `parent_sync_id`/`supports_diff` lineage; ingest-invariant verification
  provenance for finished sources; `DiscardedSyncs` reporting; both resolve
  behaviors (`newest`, `full_finished`); empty-source → empty Pebble c1z;
  parallel grant scan with sampled external-id boundaries; stats sidecar via
  `BulkSyncImport.ComputedStats`.
- The source file is never written. No schema migrations run on it. Old
  schema generations (see "Verified facts") are read tolerantly.
- `pkg/dotc1z` (non-test) must not import the new package. Enforced by a
  test.
- The new package must not need any `pkg/dotc1z` unexported symbol. Anything
  it needs is copied in; the originals are deleted in PR B.
- No `goqu` in the new package. Its queries are a dozen fixed `SELECT`s with
  bound parameters; `database/sql` is enough and this removes a forked
  dependency (`go.mod:194`) once PR B lands.
- No version bump. No changes to `pkg/dotc1z` behavior, the Pebble engine, or
  `pkg/sync`.
- Public repo: fixtures carry synthetic data only.

## Verified facts

### What `ToPebble` does today (`pkg/dotc1z/to_pebble.go`)

- Resolves `syncID == ""` by behavior: `newest` ranks finished-or-resumable
  ahead of abandoned unfinished (`resolveConvertSyncID`, cutoff
  `unfinishedSyncMaxAge = 7d`, `sync_runs.go:247`); `full_finished` uses
  `LatestFinishedSyncID(SyncTypeFull)` and returns `codes.NotFound` when none.
  No sync runs at all → `convertEmptyToPebble` (fresh store marked dirty via
  `pebble.MarkStoreDirty`, then `Close`).
- Opens the destination with `dotc1z.NewStore(outPath, WithEngine(pebble),
  WithTmpDir)`; `outPath` must not exist.
- `StartNewSyncWithID(sync.Type, syncID, sync.ParentSyncID)` on the engine
  (`engine/pebble/adapter.go:85`) so the destination keeps the source id.
- `StartBulkSyncImport(destSyncID, tmpDir, lanes)`; resource types, resources,
  entitlements streamed in key order with `discovered_at` carried; grants in
  `lanes` external-id ranges from `sampleGrantBoundaries`, each a
  reader+decoder goroutine pair feeding a `BulkGrantShard`; `Finish`.
- `copyAssets` through `dest.PutAsset` after `Finish`; `copySessions` via
  `Engine.SessionSetMany(..., sessions.WithSyncID(destSyncID))` only when
  `sync.EndedAt == nil`.
- `StashComputedSyncStats`; `EndSync`; then `GetSyncRunRecord` and overlay:
  `SetSupportsDiff`, `SetStartedAt(localized)`, finished → `SetEndedAt` and
  verification fields when `sync.IsVerified()`; unfinished → `ClearEndedAt`,
  `SetSyncToken`; `PutSyncRunRecord`; `Close`.
- Error after destination open: `Close` and `os.Remove(outPath)`.
- Logs: `to-pebble: destination finalize timings` (debug),
  `to-pebble: conversion complete` (info, with discarded-sync fields).

### `pkg/dotc1z` unexported symbols the converter uses

`validateSQLiteSchema` (`sqlite_schema_guard.go`, called from `C1File.init`
at `c1file.go:1013` before any DDL), `getSync`, `ListSyncRuns`,
`LatestFinishedSyncID`,
`parseIngestInvariantVerification`, `parseStats` (`sync_runs.go`); table
descriptors and names (`v1_resource_types`, `v1_resources`,
`v1_entitlements`, `v1_grants`, `v1_assets`, `v1_sync_runs`,
`v1_connector_sessions`, each `v<ver>_<name>`); `openSQLite`/`sqliteDSN`
(`sqlite_open.go`, Windows path handling covered by
`sqlite_open_dsn_test.go`); `reattachExpansion` (`grants.go:722`);
`localizeSQLiteTimestamp[Ptr]` (`sql_helpers.go:70`);
`unfinishedSyncCutoff`; `decompressC1z`/`cleanupDbDir` (`file.go`);
`discoveredAtTimestamp`, `rawGrantRow`, `sampleGrantBoundaries`,
`convertGrantScanLanes`, session batch bounds (`to_pebble.go`).

Exported and reusable as-is: `dotc1z.NewStore`, `WithEngine`, `WithTmpDir`,
`NewC1ZFileDecoder`, `ReadHeaderFormat`, `DecoderOption`s;
`pebble.AsEngine`, `pebble.MarkStoreDirty`, `Engine.StartBulkSyncImport`,
`BulkSyncImport.*`, `Engine.StashComputedSyncStats`,
`Engine.GetSyncRunRecord`/`PutSyncRunRecord`, `Engine.SessionSetMany`,
`Engine.StartNewSyncWithID`; `c1zstore.SyncRun`; `connectorstore.SyncType`.

### Schema generations a v1 file may carry

`NewC1File` migrates the decompressed copy on every open, including read-only
opens, so today's converter always sees the latest columns. The new package
does not migrate, so it must read these shapes directly:

- `v1_grants`: `expansion blob` and `needs_expansion` added later
  (`grants.go:85,92`). When `expansion` is absent or NULL the
  `GrantExpandable` annotation is still inside `data`; when present and
  non-NULL it was stripped from `data` and `reattachExpansion` restores it.
  Reading `expansion` only when the column exists, and treating NULL as
  empty, yields the same grant in every generation.
- `v1_sync_runs`: `sync_type`, `parent_sync_id`, `linked_sync_id`,
  `supports_diff`, `grants_backfilled`, `stats`, and the ingest-invariant
  columns were all added by migration (`sync_runs.go:89-167`). Missing →
  defaults: `full`, `""`, `""`, false, 0, NULL, unverified.
- `discovered_at`, `started_at`, `ended_at`, `sync_token`, `external_id`,
  `data`, `content_type`, `key`, `value` are in every generation's base
  schema.

Column presence is read once per table with `PRAGMA table_xinfo`, which the
schema guard already issues.

### Consumers today

- `cmd/baton/to_pebble.go:68-81`: `NewC1ZFile(ReadOnly)` + `ToPebble` with
  `full_finished`. Switches in this PR.
- `cmd/baton/dump_db.go:66`: `NewC1ZFileDecoder` only. Unchanged.
- `pkg/dotc1z/convert_open.go`: `selectStoreDriver` converts a writable v1
  file in place on explicit `WithEngine(pebble)`. Unchanged in this PR.
- `pkg/synccompactor/compactor_pebble.go:1044-1077`: temp-file conversion of
  v1 inputs. Unchanged in this PR.
- c1 `sanitize_activity.go:1064`: `NewC1ZFile` + `ToPebble`. c1 bump later.
- c1 `sync/c1z.go:1185` → `sync_baton.go:142-163`: convert-open, then
  `CloneSync`, `NewStore`, `SetCurrentSync`, expansion pass. The handle
  returned by `ToPebbleOpen` lets c1 drop two pack/unpack cycles and the
  clone; that change is c1's, not this PR's.

### Existing tests that are the reference

`pkg/dotc1z/to_pebble_test.go` (17 cases), `to_pebble_equivalence_test.go`
(keyspace dump oracle `dumpSyncKeyspace`), `to_pebble_discovered_at_test.go`,
`to_pebble_localtime_test.go`, `to_pebble_unconvertible_test.go`,
`resolve_convert_sync_id_test.go`, `discarded_syncs_test.go`,
`rowid_overflow_security_test.go`, `legacy_acceptance_security_test.go`,
`legacy_diff_sync_rows_test.go`, `sqlite_open_dsn_test.go`,
`to_pebble_bench_test.go`.

## Design

### Package

`pkg/dotc1z/sqlite`. Imports `pkg/dotc1z`, `pkg/dotc1z/engine/pebble`,
`pkg/dotc1z/c1zstore`, `pkg/connectorstore`, `pkg/types/sessions`,
`pkg/uotel`, the protos, `database/sql`, and `modernc.org/sqlite` (blank).
Nothing else in the SDK imports it except `cmd/baton`.

### API

```go
// Open reads a v1 .c1z (zstd envelope over SQLite) or a bare SQLite
// database, read-only. A .c1z is decoded into tmpDir and removed on Close.
func Open(ctx context.Context, path string, opts ...OpenOption) (*File, error)
func WithTmpDir(dir string) OpenOption
func WithDecoderOptions(opts ...dotc1z.DecoderOption) OpenOption

func (f *File) Close(ctx context.Context) error

// SyncRuns lists every sync_runs row, oldest first, with the same field
// mapping C1File.ListSyncRuns uses.
func (f *File) SyncRuns(ctx context.Context) ([]*c1zstore.SyncRun, error)

// ResolveSyncID applies a ResolveBehavior. Returns codes.NotFound when no
// sync qualifies. ErrNoSyncRuns when the table is empty.
func (f *File) ResolveSyncID(ctx context.Context, b ResolveBehavior) (string, error)

type ResolveBehavior string
const (
    ResolveNewest       ResolveBehavior = "newest"
    ResolveFullFinished ResolveBehavior = "full_finished"
)

// ToPebbleOpen converts srcSyncID ("" resolves per WithResolveBehavior)
// into a new Pebble store at outPath, which must not exist, and returns it
// open with the sync ended (or, for an unfinished source, resumable) and
// source metadata preserved. Close writes the .c1z. On error the
// destination is closed and outPath removed.
func (f *File) ToPebbleOpen(ctx context.Context, outPath, srcSyncID string, opts ...ConvertOption) (c1zstore.Store, *ConvertStats, error)

// ToPebble is ToPebbleOpen followed by Close.
func (f *File) ToPebble(ctx context.Context, outPath, srcSyncID string, opts ...ConvertOption) (*ConvertStats, error)

func WithResolveBehavior(b ResolveBehavior) ConvertOption
func WithBatchSize(n int) ConvertOption
func WithConvertTmpDir(dir string) ConvertOption
func WithParallelism(n int) ConvertOption
```

`ConvertStats`, `ConvertStageStats`, `DiscardedSync` keep today's field
names so `cmd/baton/to_pebble.go`'s printer is a type-path change.

For an empty source (`ErrNoSyncRuns` and `srcSyncID == ""`), `ToPebbleOpen`
returns the fresh store marked dirty so `Close` writes an empty v3 c1z, as
`convertEmptyToPebble` does today. `ConvertStats.SourceSyncID` is `""`.

### Internals

Files, each a direct port of the named `pkg/dotc1z` code with `goqu`
replaced by `database/sql` and `*C1File` replaced by `*File`:

- `open.go`: header sniff (`dotc1z.ReadHeaderFormat` for v1; the 16-byte
  `SQLite format 3\x00` magic for a bare db), decode via
  `dotc1z.NewC1ZFileDecoder` into `tmpDir`, `openSQLite` with the same DSN
  builder and `SetMaxOpenConns(1)`, schema guard, column-presence probe.
- `schema_guard.go`: `validateSQLiteSchema` verbatim.
- `sync_runs.go`: row → `c1zstore.SyncRun` mapping tolerant of absent
  columns; `SyncRuns`, `getSync`, `latestFinishedSyncID`,
  `resolveNewest` (same `ORDER BY` as `resolveConvertSyncID`), cutoff
  constant equal to `engine/pebble/sync_runs.go:19`.
- `convert.go`: `ToPebbleOpen`/`ToPebble`, stage functions, grant lanes,
  `sampleGrantBoundaries`, assets, sessions, metadata overlay. The grant
  `SELECT` lists `expansion` only when the column exists.
- `timestamps.go`: `localizeSQLiteTimestamp[Ptr]`, `discoveredAtTimestamp`.
- `expansion.go`: `reattachExpansion`.

Span names use the `sqlite.` prefix. Log line text is unchanged.

### Dependency rule

`pkg/dotc1z/import_boundary_test.go`: parse every non-test `.go` file in
`pkg/dotc1z` with `go/parser` (imports only) and fail if any imports
`pkg/dotc1z/sqlite`. Same shape as `write_mu_holders_test.go`.

### `cmd/baton/to_pebble.go`

Replace `dotc1z.NewC1ZFile` + `store.ToPebble` with `sqlite.Open` +
`f.ToPebble(..., sqlite.WithResolveBehavior(sqlite.ResolveFullFinished))`.
Flags unchanged. Drop the `//nolint:staticcheck` line.

### Fixtures and golden digests (`pkg/dotc1z/sqlite/testdata/`)

Generated once in this PR by `//go:build fixturegen` test code that uses the
still-present SQLite writer, committed, and never regenerated after PR B
removes the writer. Each fixture is small (KBs). Each gets a `.golden`
file holding the SHA-256 of `dumpSyncKeyspace` over the output of the
current `C1File.ToPebble`, plus the overlaid sync-run record fields, so the
reference converter's behavior is pinned independently of its code.
`dumpSyncKeyspace` and `normalizeRecordValue` are copied from
`to_pebble_equivalence_test.go` into the new package's test helpers; the
parity and golden tests run the dump with normalization off.

Fixtures:

1. `finished_full.c1z`: one finished full sync; resource types, resources
   with parents, entitlements, grants with and without `GrantExpandable`,
   two assets, `supports_diff=1`, ingest-invariant verification set.
2. `unfinished_partial.c1z`: one unfinished partial with `parent_sync_id`,
   a non-empty `sync_token`, and `connector_sessions` rows; `started_at`
   recent.
3. `multi_sync.c1z`: finished full, then a newer finished partial, then an
   unfinished sync with `started_at` older than the cutoff. `newest` must
   pick the partial; `full_finished` the full.
4. `empty.c1z`: schema only, no sync runs.
5. `duplicate_identity_grants.c1z`: two grant rows with distinct
   `external_id` and the same (entitlement, principal) identity, to exercise
   the fold-at-`Finish` path.
6. `old_schema.c1z`: fixture 1 with `expansion`, `needs_expansion`,
   `supports_diff`, `grants_backfilled`, `stats`, `linked_sync_id`, and the
   ingest-invariant columns dropped (`ALTER TABLE ... DROP COLUMN` in the
   generator, after dropping the partial indexes that reference them) and
   the `GrantExpandable` annotation left inside `data`.
7. `edge_keys.c1z`: the seed from `TestToPebbleBulkImportEquivalence`
   (prefix ids, colons in ids, unicode, dangling references).
8. `bare.db`: fixture 1's decompressed database, for the bare-SQLite open.

If `docs/verification/syncer-on-ledger/tools/build-legacy-artifact.sh`
produces an artifact from a pinned old SDK in CI-free time, add it as
`fixture 9` with its own golden; it is the only fixture not shaped by
today's writer.

## Verification

Risk route: HIGH. A dropped or altered record is silent (no reader of the
converted file knows what the source held) and durable (convert-open
replaces the source; c1 uploads the result). Remediation is re-convert from
the original upload where it still exists, migrate where it does not.

### Oracles

1. **Reference parity (this PR only).** For every fixture and for every
   writer-generated source the ported tests build, convert with
   `dotc1z.(*C1File).ToPebble` and with `sqlite.(*File).ToPebble`, open both
   outputs, and require byte-equal `dumpSyncKeyspace` with no normalization
   (same sync id, same `discovered_at`), equal `GetSyncRunRecord` fields,
   equal assets, equal session rows, equal `ConvertStats` row counts and
   `DiscardedSyncs`. This is the only window in which the reference exists;
   PR B deletes it.
2. **Golden digests (forever).** `sqlite.(*File).ToPebble` over each fixture
   hashes to its `.golden`. Any later change to the converter, the bulk
   importer, or the engine's key codec that alters output fails here, by
   design; a deliberate change regenerates goldens in the same PR with the
   diff explained.
3. **Handle equivalence.** `ToPebbleOpen` then `Close` produces a file whose
   `dumpSyncKeyspace` equals `ToPebble`'s. `ToPebbleOpen` on fixture 1, then
   `SetCurrentSync`, then one `PutGrant`, then `Close`, reopens with the
   added grant and the preserved `started_at`/`ended_at`.
4. **Ported behavioral tests.** Every test in the reference list runs against
   the new API with the same assertions; tests that reach `*C1File`
   internals are rewritten against fixtures.

### Cases

- Resolve: `""` + `newest` on fixtures 2, 3; `""` + `full_finished` on 3
  and `codes.NotFound` on 2; explicit id present and absent; unknown
  behavior string errors before any destination is created; fixture 4
  with `""` → empty c1z, with explicit id → error.
- Unfinished source: `ended_at` nil, `sync_token` preserved, sessions
  present under the destination id, `started_at` localized; resume via
  `StartOrResumeSync` on the reopened file succeeds (port of
  `TestToPebbleConvertedUnfinishedSyncResumes`).
- Finished source: sessions not copied (port of
  `TestToPebbleLeavesFinishedSyncSessionsBehind`); verification fields
  copied only when `IsVerified()`.
- Old schema (fixture 6): grants carry `GrantExpandable`; sync run fields
  default; parity with fixture 1's output except `supports_diff` and
  verification fields, which the old schema cannot hold.
- Bare `.db` open (fixture 8) equals fixture 1's output.
- Local time: run the three `*IsAbsoluteInstant` tests under a non-UTC
  `TZ`.
- Security: schema guard rejects a fixture with a trigger and one with a
  view (`legacy_acceptance_security_test.go` ports); rowid span overflow
  (`rowid_overflow_security_test.go` port) returns no boundaries and
  single-lane scan completes; zstd decode limits honored via
  `WithDecoderOptions` (one test with a tiny `WithDecoderMaxDecodedSize`
  must fail to open fixture 1).
- Error path: output path exists → error, nothing created; failure injected
  after destination open (bad `tmpDir` for the bulk import) → `outPath`
  absent afterwards.
- Windows: `sqlite_open_dsn_test.go` port; CI Windows shards run the
  package.
- Parallelism: `WithParallelism(1)` and default on fixture 1 produce equal
  keyspaces.

### Instruments

- Planted-violation check for oracle 1: temporarily skip `reattachExpansion`
  in the new package and confirm the parity test fails on fixtures 1 and 7;
  temporarily drop the `ClearEndedAt` branch and confirm fixture 2 fails.
  Record both in evidence, then revert.
- Planted-violation check for oracle 2: corrupt one byte of a `.golden` and
  confirm the test fails.
- Dependency rule: temporarily add the import to a `pkg/dotc1z` file and
  confirm `import_boundary_test.go` fails.
- `go test -race ./pkg/dotc1z/sqlite/` with the parallel grant scan on
  fixture 1 and the edge-keys fixture.
- `to_pebble_bench_test.go` ported; report old vs new ns/op and allocs/op
  on the same generated source. Expected: within noise, same algorithm.
  Any regression over 10% in either is a finding.

### Repository gates

`go build ./...`, `go vet ./...`, `golangci-lint run ./pkg/dotc1z/...
./cmd/baton/...`, full `go test ./pkg/dotc1z/... ./cmd/baton/...`, and the
Windows CI shards.

## Out of scope (later PRs)

- PR B: delete the SQLite engine, `attached` compactor, `convert_open.go`,
  `sqliteDriver`; `selectStoreDriver` returns a typed error for v1 files
  unless a converter hook is registered; compactor stops converting inputs;
  `baton-demo-test` drops the `sqlite` leg; fixtures from this PR become
  the only v1 test inputs; `goqu` and `modernc.org/sqlite` leave the main
  module's import graph except via this package.
- Convert-open hook in `pkg/dotc1z` that this package registers on import.
- Splitting `pkg/dotc1z/sqlite` into its own Go module.
- c1: switch sanitize to `sqlite.Open`+`ToPebble`; switch the upload path
  to `ToPebbleOpen` and drop `CloneSync`; delete dead legacy branches in
  `sync_file.go` and `manager.go`; move tests off the SQLite writer.
- `cmd/baton/dump_db.go`.
- Any change to `BulkSyncImport`, the engine, or `pkg/sync`.

## Acceptance

- `pkg/dotc1z/sqlite` exists with the API above; `go doc` on the package
  shows no reference to `*C1File`.
- `import_boundary_test.go` passes and its planted violation fails.
- Oracle 1 passes over all fixtures and all ported writer-generated sources;
  both planted violations were observed to fail and are recorded in
  `evidence.md`.
- Every fixture has a `.golden`; oracle 2 passes; the corrupted-golden check
  was observed to fail.
- Oracle 3 passes.
- Every test in the reference list has a named port or a written reason it
  does not apply.
- `cmd/baton to-pebble` uses the new package; `go run ./cmd/baton to-pebble
  -f testdata/finished_full.c1z --out /tmp/x.c1z` succeeds and the output's
  digest equals the golden.
- Benchmarks reported; no regression over 10%.
- `pkg/dotc1z` diff is limited to `import_boundary_test.go`.
- Repository gates green, including Windows.
- `evidence.md` beside this plan records each criterion's state using the
  checklist vocabulary (verified to stated coverage / failed / explicitly
  excluded / deferred).

## Change orders

None.
