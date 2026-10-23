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

### Source schema contract

`NewC1File` runs `Schema()` and `Migrations()` on the decompressed copy on
every open, including read-only opens, so today's converter always sees the
current columns and indexes. The new package never writes the source, so
this section defines exactly what it reads and what it assumes. All table
names are `v1_<name>`; the version has been `1` since the first commit
(`b5eef37e`, 2023-03-03).

**Required: the 2023-03-03 base schema.** Every column and index below has
existed in every v1 file ever written by this SDK. `Open` checks the
required columns with the same `PRAGMA table_xinfo` probe and fails with
an error naming the table and column when one is missing, so a file older
than the public package, or not written by it, fails loudly instead of
converting partially.

| Table | Columns read | Index relied on | Why |
| --- | --- | --- | --- |
| `v1_sync_runs` | `sync_id`, `started_at`, `ended_at`, `sync_token` | unique `(sync_id)` | point lookup; table is tiny |
| `v1_resource_types` | `data`, `discovered_at` | unique `(external_id, sync_id)` | no-duplicate guarantee the importer's strictly-increasing SST writer requires; ordered scan for `ORDER BY external_id` |
| `v1_resources` | `data`, `discovered_at` | unique `(external_id, sync_id)` | no-duplicate guarantee (importer spill-sorts; order not needed) |
| `v1_entitlements` | `data`, `discovered_at` | unique `(external_id, sync_id)` | no-duplicate guarantee; ordered scan |
| `v1_grants` | `data`, `discovered_at`, `id`, `entitlement_id`, `resource_type_id`, `resource_id`, `principal_resource_type_id`, `principal_resource_id` | unique `(external_id, sync_id)`; rowid | lane range scans `external_id >= ? AND < ?`; `MIN/MAX(id)` and the boundary sampler's `id >= ? ORDER BY id LIMIT 1` are rowid seeks; the five identity columns hydrate slim rows (see below) |
| `v1_assets` | `external_id`, `content_type`, `data` | rowid | `ORDER BY id` |

**Optional: columns added since.** Presence is probed once per table with
`PRAGMA table_xinfo` (the schema guard already issues it). Absent means the
default.

| Table | Column | Added | Converter default when absent |
| --- | --- | --- | --- |
| `v1_grants` | `expansion blob` | `grants.go:85` | treat as NULL (see rule below) |
| `v1_grants` | `needs_expansion` | `grants.go:92` | not read; the importer derives it from the `GrantExpandable` annotation |
| `v1_sync_runs` | `sync_type` | `sync_runs.go:89` | `full` |
| `v1_sync_runs` | `parent_sync_id` | `:103` | `""` |
| `v1_sync_runs` | `linked_sync_id` | `:122` | not read |
| `v1_sync_runs` | `supports_diff` | `:130` | `false` |
| `v1_sync_runs` | `grants_backfilled` | `:140` | not read |
| `v1_sync_runs` | `stats` | `:150` | NULL (`DiscardedSyncs` carries no stats) |
| `v1_sync_runs` | `ingest_invariant_generation/coverage/mode` | `:167` | unverified |

**Optional: the sessions table.** `v1_connector_sessions` (`sync_id`,
`key`, `value`; unique `(sync_id, key)`) first appeared 2025-10-16
(`2faa1192`). Absent table → zero sessions copied; `ConvertStats.Sessions`
is zero.

**Grant expansion rule.** `g = Unmarshal(data); if expansion is non-NULL,
reattachExpansion(g, expansion)`. This is correct in all three states a row
can be in: no `expansion` column (annotation inside `data`); column present
and NULL (non-expandable, or expandable and not yet backfilled, annotation
inside `data`); column present and non-NULL (writer or backfill stripped it
from `data`, `grants.go:887`). It is also exactly what `convertGrants` does
today; `grants_backfilled` is never consulted.

**Slim grant rule (new; fixes a latent drop).** `WithV2GrantsWriter`
(#781, opt-in, default false) writes grants with `Entitlement` and
`Principal` set to nil in `data` (`slimGrantForWrite`, `grants.go:630`);
the SQLite reader rebuilds identity-only stubs from the row's
`entitlement_id`, `resource_type_id`, `resource_id`,
`principal_resource_type_id`, `principal_resource_id` (`hydrateGrants`,
`grants_hydrate.go:21-60`). `convertGrants` today never hydrates, so a slim
row reaches `grantIdentityFromRecord` with nil refs and is dropped with a
counted warning (`bulk_import.go:423-430`). The new converter selects the
five identity columns with `data` and, after unmarshal, applies
`hydrateGrants`'s exact stubs (`Entitlement{Id, Resource{Id{rt, rid}}}`,
`Principal{Id{rt, rid}}`) to whichever of the two is nil. Full rows are
untouched. No SDK syncer path sets the flag; c1 hard-disables it
(`c1z.go:1178`) after a per-tenant experiment, so slim v1 files are rare
but may exist in storage.

**Why no migration of any kind is needed.** Audited `git log -p` over every
non-test file in `pkg/dotc1z` since `b5eef37e`:

- Every `ALTER TABLE` ever committed is an `ADD COLUMN` that is still in
  the code today and is listed in the optional table above. No table was
  ever renamed, dropped and recreated, or had a column removed or
  retyped. The table version has been `1` throughout.
- The only data migration ever committed is the expansion backfill
  (`backfillGrantExpansionColumn`, #666 and #701), covered by the grant
  expansion rule.
- The only table ever removed is `v1_sync_stats` (#874 → #889, two days in
  2026-05). The converter never reads it; the schema guard permits
  unknown ordinary tables.
- `InitTables` also runs `PRAGMA optimize` after a migration; `init` sets
  connection pragmas (`wal_checkpoint`, `journal_mode`, `synchronous`,
  `locking_mode`). None of these changes rows.
- The timestamp text layout (`2006-01-02 15:04:05.999999999`, zone-less)
  is unchanged since the first commit, so `localizeSQLiteTimestamp` reads
  every file's `discovered_at`/`started_at`/`ended_at` the same way.
- The slim-grant shape (#781) is read-side hydration, above, not a
  migration.

**Indexes added since 2023 are not used.** `(entitlement_id, sync_id, id)`
and the two partial indexes on `expansion`/`needs_expansion` serve reader
queries, not the conversion scans. A file written before they existed
converts at the same speed as one written today.

**One scan order is unnecessary.** `convertResources` orders by
`(resource_type_id, external_id)`, which no index covers, so SQLite sorts
the whole table; the importer then spill-sorts resources anyway
(`bulk_import.go:169-173`). The new package drops that `ORDER BY`. The
parity oracle proves the output is unchanged; the benchmark shows the
saving. Resource types keep their `ORDER BY external_id` (required by the
strictly-increasing SST writer); entitlements and grants keep theirs (free
via the unique index; grants need it for lane ranges).

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
9. `slim_grants.c1z`: fixture 1's records written with
   `WithV2GrantsWriter(true)`, plus one grant carrying
   `InsertResourceGrants` so the file holds both slim and full rows
   (`unsafeForSlim`). Its `.golden` is generated from the **new**
   converter, since the reference drops the slim rows; see oracle 1.

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
   PR B deletes it. One documented deviation: on fixture 9 the reference
   drops every slim row and the new converter keeps them. The parity test
   asserts that instead: reference `ConvertStats.Grants.Rows` equals the
   full-row count and the new converter's equals the total; the kept
   grants' `Entitlement`/`Principal` equal what
   `dotc1z.(*C1File).ListGrants` returns for the same source.
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
- Slim grants (fixture 9): every grant present in the output; slim rows
  carry identity-only stubs, the `InsertResourceGrants` row carries its
  full `Entitlement`/`Principal`; the bulk import's missing-refs counter
  is zero.
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
  temporarily drop the `ClearEndedAt` branch and confirm fixture 2 fails;
  temporarily skip hydration and confirm fixture 9 fails. Record all three
  in evidence, then revert.
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

1. **Correction: validate every base column used by conversion queries.**
   Source: implementation of the fixed `SELECT` statements exposed omissions
   in the Source schema contract table. Motivation: `sync_id` predicates,
   `external_id` ordering and lane bounds, and asset `id` ordering are required
   by conversion but were absent from the required-column list. Contract delta:
   `Open` also requires `sync_id` on every converted table, `external_id` on
   resource types, entitlements, and grants, and `id` plus `sync_id` on assets.
   The owning boundary remains `Open` and its `PRAGMA table_xinfo` probe.
   Affected criteria: the old-schema fixture and required-column rejection
   tests cover the expanded check; no conversion behavior changes. Verification
   delta: plant one missing query-only column and require `Open` to fail naming
   its table and column. PR placement: this PR.
2. **Correction: canonicalize destination-stamped asset discovery times in
   parity and golden digests.** Source: running the reference and extracted
   converters over `finished_full.c1z` showed that `copyAssets` calls
   `PutAsset`, which stamps `AssetRecord.discovered_at` at conversion time.
   Motivation: sequential correct conversions and every later golden run
   otherwise differ by construction. Contract delta: none; asset timestamp
   behavior remains unchanged. The parity and golden oracles zero only
   `AssetRecord.discovered_at`; they still compare asset keys, sync IDs,
   content types, bytes, row counts, and byte counts exactly. The owning
   boundary is the test digest helper. Affected criteria: oracle 1 and oracle
   2. Verification delta: the asset assertions remain independent of the
   normalized timestamp, and the corrupted-golden planted violation still
   validates the digest oracle. PR placement: this PR.
3. **Correction: canonicalize destination-stamped stats sidecar write times in
   parity and golden digests.** Source: the next parity comparison showed that
   `EndSync` stamps `SyncStatsRecord.written_at` independently in each
   conversion. Motivation: as with asset discovery time, no source value
   exists to preserve and sequential correct conversions cannot match it.
   Contract delta: none; only the oracle zeros `written_at`. The owning
   boundary is the test digest helper. Affected criteria: oracle 1 and oracle
   2. Verification delta: parity still byte-compares every stats count and
   grouping, and the golden digest includes that canonical stats record. PR
   placement: this PR.
4. **Correction: measure the slim-grant deviation from persisted output, not
   reference `ConvertStats.Grants.Rows`.** Source: fixture 9 showed that the
   reference converter increments its stage count from scanned batch length
   even when `BulkGrantShard` drops rows with nil references. Motivation: the
   planned assertion that reference stats equal the full-row count is false;
   reference stats report all three scanned rows while only one full row is
   persisted. Contract delta: none. The owning boundary is fixture 9's parity
   test, which now asserts reference output contains one grant, extracted
   output contains all three, extracted stats report three, and every extracted
   grant has hydrated references. Affected criterion: oracle 1's documented
   slim-grant deviation. Verification delta: persisted output replaces the
   non-discriminating reference counter as the oracle. PR placement: this PR.
5. **Correction: inject post-open cleanup failure with malformed source record
   bytes instead of an invalid conversion temp directory.** Source:
   implementation inspection showed that `WithConvertTmpDir` configures
   `dotc1z.NewStore` and `StartBulkSyncImport`; an invalid directory fails in
   `NewStore`, before the destination-open cleanup boundary. Motivation: the
   planned injection cannot reach the state it claims to test. Contract delta:
   none. The owning boundary remains `ToPebbleOpen`'s deferred destination
   close-and-remove path. Affected criterion: the error-path case. Verification
   delta: a schema-valid source with malformed resource-type protobuf bytes
   opens the destination, fails in `convertResourceTypes`, and must leave
   `outPath` absent. The invalid-temp-directory case remains as a pre-open error
   check. PR placement: this PR.
6. **Correction: post-`ToPebbleOpen` record mutations must clear inherited
   verification and reseal before `Close`.** Source: focused implementation
   review found that the planned `SetCurrentSync` → `PutGrant` → `Close`
   sequence bypasses the Pebble store lifecycle: stats and grant digests are
   seal-time derivations, and `Close` saves rather than calling `EndSync`.
   Motivation: the original oracle would require and bless stale derived
   metadata. Contract delta: callers that mutate converted sync records clear
   ingest-invariant provenance and call `EndSync` before `Close`, matching every
   other `c1zstore.Store` writer lifecycle. The owning boundary is the caller
   after `ToPebbleOpen` transfers store ownership. Affected criteria: oracle 3
   and the `ToPebbleOpen` API contract. Verification delta: the handle test
   clears provenance, writes one grant, calls `EndSync`, closes and reopens,
   then checks the grant, source timestamps, cleared provenance, recomputed
   stats, and grant-generation digest count. PR placement: this PR.
7. **Correction: resealing after `ToPebbleOpen` requires explicit source
   timestamp restoration.** Source: independent implementation review found
   that `c1zstore.Store.EndSync` stamps a new `ended_at`, so change order 6's
   source-timestamp assertion cannot follow from `ClearIngestInvariantVerification`
   and `EndSync` alone. Motivation: hiding a direct engine metadata write in the
   test would leave the public lifecycle contract incomplete. Contract delta:
   after resealing, callers that must retain the conversion's source timestamps
   restore them on the sync record through `pebble.AsEngine` before `Close`.
   The owning boundary remains the caller after `ToPebbleOpen` transfers store
   ownership. Affected criteria: oracle 3 and the `ToPebbleOpen` API contract.
   Verification delta: the API documentation names the restoration step, and
   the handle test performs it before checking exact source timestamps after
   reopen. PR placement: this PR.
8. **Correction: canonicalize source timestamps in golden digests.**
   Source: Linux and Windows CI showed that correct conversion of SQLite
   wall-clock timestamps produces location-dependent protobuf instants, as the
   reference converter does. Motivation: goldens generated in one local time
   zone cannot contain the protobuf instants for SQLite `started_at`,
   `ended_at`, or record `discovered_at` values and remain portable. Contract
   delta: none; only the golden digest clears those fields. The owning boundary
   is the test digest helper. Affected criteria: oracle 2 and Windows
   verification. Verification delta: reference parity still compares exact
   timestamps within each environment, while the non-UTC child tests
   independently assert started, ended, discovered, and discarded timestamp
   behavior. Goldens continue to cover every non-timestamp field. PR placement:
   this PR.
9. **Correction: separate destination temporary storage from the decoded
   source directory.** Source: CI review found that `ToPebbleOpen` defaulted
   the destination temp directory to `File.tempDir`, which `File.Close`
   removes. Motivation: ownership transfer permits callers to close the source
   before the returned destination, so shared temporary storage could delete
   live destination state. Contract delta: without `WithConvertTmpDir`, the
   destination uses the system temporary directory rather than the source
   decode directory. The owning boundary is `ToPebbleOpen` destination
   creation. Affected criterion: oracle 3. Verification delta: a handle test
   closes the source first, then closes and reopens the destination. PR
   placement: this PR.
10. **Correction: reject virtual SDK-owned tables at `Open`.** Source: CI
    review found that `sqlite_schema.type = 'table'` includes virtual tables,
    while the reference schema guard also checks `PRAGMA table_list`.
    Motivation: the extracted guard must retain ordinary-table enforcement and
    must not accept file-authored virtual-table behavior. Contract delta:
    `Open` rejects a present SDK-owned table whose `table_list` type is not
    `table`. The owning boundary is `validateSQLiteSchema`. Affected criteria:
    read-only hostile-schema acceptance and reference-test disposition.
    Verification delta: a source with virtual `v1_grants` is rejected before
    conversion. PR placement: this PR.
11. **Correction: require `v1_sync_runs.id` at `Open`.** Source: CI review
    found that `SyncRuns` selects and orders by `id`, but change order 1's
    required-column expansion omitted it. Motivation: every base column used by
    a query must fail at `Open`, not on the later method call. Contract delta:
    `v1_sync_runs.id` joins the required 2023 base columns. The owning boundary
    is `requiredColumns`. Affected criterion: required-column enforcement.
    Verification delta: a replacement `v1_sync_runs` table without `id` is
    rejected and named. PR placement: this PR.
12. **Performance: remove the entitlement source ordering.** Source:
    post-implementation profiling found that SQLite satisfies
    `WHERE sync_id = ? ORDER BY external_id` by scanning the entire
    `(external_id, sync_id)` index, while `BulkSyncImport` then spill-sorts the
    rows by destination identity. Motivation: the source ordering neither
    restricts the scan by `sync_id` nor supplies the destination order.
    Contract delta: none; entitlement output remains canonical after the bulk
    import. Affected criterion: output parity and benchmark evidence.
    Verification delta: `EXPLAIN QUERY PLAN` records the removed index scan,
    and fixture parity and golden tests cover output invariance. PR placement:
    this PR.
13. **Performance: allocate spill arenas from observed input.** Source:
    post-implementation allocation profiling found that each spill sorter
    allocated its full 46–128 MiB chunk for a source containing one grant.
    Motivation: chunk size is a flush and memory ceiling, not a useful initial
    allocation for small imports. Contract delta: spill arenas start at no more
    than 1 MiB, grow geometrically, and retain the existing chunk limit, sorting,
    merge, and recycling behavior. The owning boundary is `spillSorter.add`.
    Affected criterion: benchmark evidence. Verification delta: a focused test
    pins initial capacity, growth, and the chunk ceiling; converter benchmarks
    record bytes and allocations before and after the change. PR placement:
    this PR.
14. **Correction: limit adaptive arenas and account for growth overlap.**
    Source: CI review found that change order 13 also changed open-time
    id-index migration and grant-layer sorters without cost-curve evidence, and
    that geometric growth briefly retains both old and new arenas. Motivation:
    the measured converter benefit does not justify changing unrelated
    open-time and layer paths or exceeding the bulk import's stated memory
    budget. Contract delta: adaptive freelists are used only by bulk import and
    grant-digest construction; other sorters retain their prior full-capacity
    allocation. `bulkImportChunkBytes` reserves one additional arena in its
    budget for growth overlap. Affected criterion: benchmark evidence.
    Verification delta: the focused budget test includes the growth allowance,
    and the converter benchmark is repeated after narrowing the scope. PR
    placement: this PR.
15. **Correction: budget concurrent growth and measure digest scaling.**
    Source: CI review found that grant shards can grow one adaptive arena per
    lane concurrently, while change order 14 reserved only one transient
    arena. It also found that `BuildGrantDigests` is shared with compaction but
    had only converter-level cost evidence. Motivation: the memory ceiling must
    hold under the import's documented concurrency, and a shared-path change
    needs a grant-count cost curve. Contract delta:
    `bulkImportChunkBytes` reserves one growth arena per expected shard.
    Affected criterion: benchmark evidence. Verification delta:
    `TestBulkImportChunkBytesStaysWithinBudget` checks the concurrent-growth
    allowance, and `BenchmarkRegisteredPebbleWritePack` records `EndSync`,
    bytes, and allocations at 100, 1,000, and 10,000 grants before and after
    adaptive digest arenas. PR placement: this PR.
16. **Correction: use one bounded spill-arena allocation policy.** Source:
    an isolated 128 MiB first-fill benchmark showed that geometric growth
    allocates 255 MiB and copies 127 MiB per fresh arena, while fixed allocation
    wastes almost the full chunk for small inputs. Motivation: spill sorting
    should not select between unrelated fixed and adaptive algorithms by call
    site. Contract delta: every spill sorter starts at no more than 1 MiB; if
    that arena fills, it allocates the configured chunk once and copies at most
    1 MiB. Full arenas retain the existing recycling, chunk limit, sorting, and
    merge behavior. A single entry larger than the configured chunk still gets
    an arena large enough for that entry. The owning boundary is
    `spillSorter.add`. Affected criterion: benchmark evidence. Verification
    delta: focused benchmarks cover small, threshold-crossing, and full first
    fills without Pebble or filesystem work; existing spill-merge, bulk-import
    budget, converter, digest, compaction, race, and repository gates are
    rerun. PR placement: this PR.
17. **Correction: reserve the actual spill-promotion overlap.** Source: CI
    review found that change order 16 reduced each promotion's retained old
    arena to 1 MiB but left `bulkImportChunkBytes` reserving one full chunk per
    concurrent shard. Motivation: the excess reservation reduces chunk sizes
    and increases spill-merge fan-in without reducing peak memory.
    Contract delta: chunk size is derived from the budget after subtracting one
    1 MiB initial arena per expected shard, then dividing the remainder by the
    full-arena count. The owning boundary is `bulkImportChunkBytes`. Affected
    criterion: benchmark evidence. Verification delta:
    `TestBulkImportChunkBytesStaysWithinBudget` checks the byte-accurate growth
    reserve and exact derived sizes for one, four, and eight shards. PR
    placement: this PR.
