# SQLite engine removal: verification evidence

Plan: `docs/verification/sqlite-engine-removal/plan.md`, committed as
`cedd81bc` before implementation. Three change orders are appended to the
plan; all three keep the HIGH route (consumer distance plus silent test
weakening). Implementation commit: `7723c178` on `sqlite-engine-removal`.

## Oracles

- O1 dependency boundary: `TestConnectorBinariesDoNotLinkSQLite`
  (`pkg/connectorrunner/sqlite_boundary_test.go`) runs `go list -deps` over
  `./pkg/connectorrunner ./pkg/connectorbuilder ./pkg/cli` from the module
  root and asserts none of `modernc.org/sqlite`,
  `github.com/doug-martin/goqu/v9`, `pkg/dotc1z/sqlite` appears. Passes.
- O2 v1 rejection before decode: `TestNewStoreRejectsV1Fixtures`
  (`pkg/dotc1z/v1_rejection_test.go`) opens a copy of each of the eight
  `pkg/dotc1z/sqlite/testdata/*.c1z` fixtures read-only, writable, and
  with `WithEngine(EnginePebble)`; asserts `ErrV1Unsupported`, a
  byte-identical file, and an empty `WithTmpDir`. 24 arms, all pass.
- O3 compactor rejects v1 before work: `TestCompactRejectsV1InputBeforeWork`
  (`pkg/synccompactor/compactor_mode_test.go`) drives `Compact` with one
  Pebble input and a copy of `finished_full.c1z`; asserts
  `ErrV1Unsupported`, an empty output dir, an empty compactor tmp dir, and
  an unchanged input. `TestRejectUnsupportedInputs` covers the header
  classifier (v3 ok, v1 → `ErrV1Unsupported`, garbage → other error).
  Both pass.
- O4 reader goldens: `TestFixtureGoldenDigests` in `pkg/dotc1z/sqlite`
  passes with the committed `.golden` files unchanged.
- O5 converted fixtures: `TestSyncTokenV0FromC1Z` finds the v0 token in
  `pkg/sync/testdata/sync-in-progress.pebble.c1z`; `TestExpandCorrectness/Small`
  loads 240 nodes / 799 edges from the unexpanded Pebble fixture and
  produces 14,981 grants, equal to the expanded fixture. Both pass.

## Instruments

- Planted violation for O1: a blank import of `pkg/dotc1z/sqlite` added to
  `pkg/connectorrunner/runner.go`. The test failed with "connector packages
  must not depend on modernc.org/sqlite". Reverted; `go vet` clean after.
- Planted violation for O2: the `C1ZFormatV1` arm of `selectStoreDriver`
  changed to `return requireEngineDriver(c1zstore.EnginePebble)`. The test
  failed on every fixture (Pebble open of a v1 payload). Reverted.
- `TestDotc1zDoesNotImportSQLiteReader` passes.
- Non-vacuity of trimmed engine matrices: every test that lost its SQLite
  arm still has at least one `t.Run` arm and reports PASS under `-v`
  (`TestLedgerNilStoreFallsBackToPath`, `TestLedgerPublicPathAttachment`,
  `TestSessionBindingSingleSync`, `TestSessionBindingBindFailureFailsSync`,
  `TestCrashBetweenInvariantPassAndSealLeavesNoMarker`,
  `TestRebindingSealedSyncClearsStaleVerification`,
  `TestStoreCapsEngineMatrix`, `TestNewSyncerStoreEngineRouting`: 12 arms).

## Criterion status

- No file under `pkg/` outside `pkg/dotc1z/sqlite` imports
  `modernc.org/sqlite` or `goqu`: verified (`rg`; the only hits are the
  string literals in `sqlite_boundary_test.go`). `go.mod` has no `goqu`
  requirement or `replace`: verified.
- `rg 'C1File|NewC1ZFile|AsSQLiteStore|EngineSQLite|WithPragma'` over
  non-test, non-vendor Go files returns only comments in untouched files
  (`pkg/sync/syncer.go`, `pkg/connectorstore`, `pkg/dotc1z/engine/pebble`,
  `pkg/dotc1z/c1zstore`, `pkg/c1zsanitize/sanitize.go`,
  `pkg/sync/progresslog`, `pkg/dotc1z/pool.go`, `pkg/sync/expand/expander.go`)
  plus `NewC1ZFileDecoder`, which is the zstd decoder and unrelated to
  `C1File`: verified; comment cleanup is explicitly excluded by the plan.
- Oracles 1-5 pass; both planted violations observed to fail: verified.
- Every deleted test file or function listed below under DELETE / TRIM /
  RETARGET: verified against `git status` and a per-file diff of
  `func Test|Benchmark` names between `HEAD~1` and the working tree.
- Repository gates: `go build ./...` clean; `go vet ./...` exit 0;
  `golangci-lint run ./...` 0 issues; `go test ./...` 67 packages ok, 0
  failures; `go mod tidy` and `go mod vendor` produce no diff after commit.
  `go test -race` on `pkg/dotc1z/...`, `pkg/synccompactor/...`,
  `pkg/sync`: all 12 packages ok, no data races reported. Windows CI
  shards: deferred to the PR checks.

## Test disposition

### DELETE (SQLite writer or engine-vs-engine oracle; no SQLite writer exists to produce the reference)

`pkg/dotc1z`: `to_pebble_test.go`, `to_pebble_bench_test.go`,
`to_pebble_discovered_at_test.go`, `to_pebble_equivalence_test.go`,
`to_pebble_localtime_test.go`, `to_pebble_unconvertible_test.go`,
`sync_runs_test.go`, `c1file_test.go`, `grants_v2_writer_test.go`,
`snapshot_test.go`, `convert_open_test.go`, `convert_open_paths_test.go`,
`hostile_boundary_security_test.go`, `copy_isolate_sync_test.go`,
`bulkload_test.go`, `hostile_trigger_security_test.go`,
`c1file_close_cancel_test.go`, `store_conformance_test.go`,
`hostile_index_name_security_test.go`, `grants_listing_bench_test.go`,
`clone_sync_test.go`, `discarded_syncs_test.go`,
`batch_cursor_wrap_security_test.go`, `race_simulation_test.go`,
`race_test.go`, `grant_stats_groupby_test.go`,
`entitlement_stats_groupby_test.go`, `column_validation_test.go`,
`resolve_convert_sync_id_test.go`, `grants_bench_test.go`,
`rollback_expansion_bench_test.go`, `legacy_diff_sync_rows_test.go`,
`legacy_acceptance_security_test.go`, `hostile_schema_guard_security_test.go`,
`rowid_overflow_security_test.go`, `sync_runs_linked_sync_id_test.go`,
`c1file_concurrent_test.go`, `c1file_stats_bench_test.go`,
`clone_sync_bench_test.go`, `sqlite_open_dsn_test.go`, `grants_test.go`,
`streaming_expansion_test.go`, `resource_types_test.go`,
`grants_for_entitlements_test.go`, `cross_engine_parity_test.go`,
`streaming_test.go`; functions `TestNewStoreExistingV1IgnoresRequestedEngine`,
`TestSQLiteStoreOffersNoLedgerCapabilities`,
`TestVerificationUnsupportedSourceFailsClosed`,
`TestSyncMetaStatsV2CrossEngineEquality` (and its sqlite subtest).

`pkg/dotc1z/sqlite`: `fixtures_generate_test.go`, `TestFixtureReferenceParity`,
`BenchmarkToPebbleReference`, the reference half of `TestSlimGrantsAreHydrated`.

`pkg/synccompactor`: `attached/` (all tests), `prodscale_topebble_test.go`,
`compactor_compare_bench_test.go`, `benchmark_test.go`;
`TestCompactExplicitPebbleConvertsAllSQLiteInputs`,
`TestCompactExplicitPebbleConvertsSQLiteEmptySyncIDTiebreaksBySyncID`,
`TestCompactExplicitPebbleConvertsSQLitePartialWithEmptySyncID`,
`TestCompactExplicitPebbleRejectsUnfinishedSQLiteSyncID`,
`TestCompactExplicitSQLiteRejectsPebbleInput`, `TestCompactFoldConvertsSQLitePartial`,
`TestCompactFoldMixedPartialsPreservesRecordRecency`,
`TestCompactFoldPebbleBaseMixedPartials`, `TestCompactMixedInputsAutoSelectsPebble`,
`TestCompactMixedInputsPreservesRecordRecency`,
`TestCompactPebbleBaseMixedPartialsAutoMode`, `TestCompactSQLiteDefaultUnchanged`,
`TestResolveSQLiteCompactionSyncIDReadsMetadataOnly`,
`TestAttachedCompactorFailsWithNoFullSyncInBase`, `TestAttachedCompactorWithTmpDir`,
`TestSyncTypeUnion_AttachedCompactor`, `TestCompactor_IncrementalDegradesGracefullyOnSQLite`,
`BenchmarkCompactSQLite1k`, `BenchmarkCompactSQLite10k`.

`pkg/sync/expand`: `pipeline_fuzz_test.go` (`TestFullPipelineDifferentialFuzz`),
`whale_graph_compare_test.go`, `BenchmarkExpandSmall`,
`BenchmarkExpandSmallMedium`, `BenchmarkExpandSmallPerStep`,
`BenchmarkExpandSmallMediumPerStep`.

`pkg/sync`: `TestChaosConnectorSQLiteExternalPrincipalResumeDegradesWithoutFailure`,
`TestSQLiteSyncDoesNotRecordTimingStats`, `TestCleanupContextDeadlineExceeded`
(relied on the row-by-row SQLite delete path to exhaust the run budget).

`pkg/c1zsanitize`: `TestSanitizeSourceEngineParity`,
`TestSanitizeRejectsSQLiteDestinationBeforeMutation` (a SQLite destination
can no longer be constructed; `Sanitize` keeps the engine check).

`pkg/dotc1z/engine/pebble`: `BenchmarkRegisteredSQLiteWriteGrant`,
`BenchmarkRegisteredSQLiteWritePack`, `BenchmarkRegisteredSQLiteUnpackReadGrants`.

`cmd/baton`: `TestRollbackExpansionNonExpandedRefused` (its premise, the
`supports_diff` marker gating rollback, is SQLite-only;
`TestRollbackExpansionPebbleNotExpanded` covers the Pebble rule).

### TRIM (SQLite arm removed from an engine matrix; Pebble arm kept)

`pkg/sync`: `ledger_attachment_test.go`, `ledger_public_test.go`,
`session_binding_test.go` (two matrices and the `sqlite` bind-failure
case), `ingest_filter_test.go`, `ingest_invariant_verification_test.go`
(two matrices; the SQLite coverage case deleted, the fail-fast I4 case
moved to Pebble with coverage `I5 I10 I4 I7 I3 I8 I9`; the rebind test
keeps only the expansion-only Pebble shape), `store_attach_test.go`,
`store_caps_test.go` (SQLite matrix row and the engine-conditional
principal-sort assertion), `pebble_etag_replay_test.go` (`sqlite-full`
row), `full_sync_bench_test.go` (sqlite arm), `chaos`/mock stores now
report `StoreMetadata{}` (`parallel_scheduler_test.go`,
`stop_checkpoint_test.go`, `external_principal_batch_delete_test.go`,
`resource_ownership_test.go`, `external_principal_cleanup_scale_test.go`).

`pkg/sync/expand`: `topological_merge_resume_test.go` (three matrices),
`topological_merge_differential_test.go` (`TestTopologicalMergeUntouchedBaseGrantNotRewritten`
matrix).

`pkg/provisioner`: `TestProvisionerGrantHydratesResources`.
`pkg/dotc1z/engine/pebble`: `BenchmarkSyncShape_BatonDemo`.
`cmd/baton-crash-harness`: `TestCrashResumeRealConnector` sqlite cell.
`pkg/dotc1z`: `format_test.go`, `metadata_test.go`, `session_store_test.go`,
`artifact_verdict_test.go`, `save_staging_security_test.go`,
`sync_token_stats_test.go`, `resume_checkpoint_parity_test.go`,
`pebble_store_dirty_coverage_test.go`, `source_cache_verification_test.go`,
`malformed_claim_security_test.go` (SQLite halves removed).

### RETARGET (same assertion, Pebble store or converted fixture)

- `pkg/dotc1z/sql_helpers_test.go` → `resources_test.go` on `NewStore`.
- `pkg/sync/expand/topological_merge_sqlite_parity_test.go` →
  `topological_merge_store_parity_test.go`;
  `TestTopologicalMergeMatchesSQLiteGroundTruth` →
  `TestTopologicalMergeStreamingMatchesProjection` (ground = `Expander.Run`
  on Pebble, candidate `streaming/pebble`).
- `TestTopologicalMergeDifferentialRandomStore`: ground = `Expander.Run` on
  Pebble; candidates `streaming/pebble`, `streaming/pebble/small-page`.
- `TestExpandCorrectness`, `BenchmarkExpandFanout10MDisk`,
  `loadPebbleGraph` (moved into `whale_cycles_test.go`): `NewStore` and
  `StartOrResumeSync` on Pebble fixtures.
- `pkg/sync/token_test.go` `TestSyncTokenV0FromC1Z`: Pebble fixture.
- `pkg/sync/ingest_invariants_test.go`, `ingest_invariants_telemetry_test.go`:
  `newInvariantTestStore` returns `c1zstore.Store`; the two skip-telemetry
  tests hide `IngestInvariantStore` through `readerWithoutIngestFacts`
  instead of relying on SQLite lacking it.
- `pkg/sync/pebble_etag_replay_test.go`: `writeRejectedC1Z` →
  `writeV1C1Z` (v1 header over zstd bytes; no SQLite driver). The strict
  `rejected` case became `v1` and asserts `NewSyncer` succeeds with no
  retained replay source, per the plan's strict-path contract.
- `pkg/synccompactor/compactor_test.go`: `TestSyncTypeUnion`,
  `TestCompactorPartialBaseStaysPartial`, `makeEmptySync` on `NewStore`;
  `compactor_mode_test.go`: `TestInferEngineFromInputFormats` →
  `TestRejectUnsupportedInputs` + `TestNewCompactorRejectsNonPebbleEngine`;
  `compactor_bench_test.go` Pebble-only.
- `cmd/baton/rollback_expansion_test.go`, `rollback_feedback_test.go`:
  `openRollbackStore` / `setSupportsDiff` helpers; `buildPreExpansionC1Z`
  takes `finish` because the expansion-only syncer pass requires a
  finished sync on Pebble; `TestRollbackExpansionCommand` asserts a second
  rollback returns `ErrSyncNotExpanded` instead of zero deletes;
  `buildMergedConflictC1Z` hand-places the derived grant because the
  syncer's own expansion hard-fails I5 on that fixture, which is the
  condition the replay under test must survive.
- `cmd/baton/to_pebble_test.go`: source is a copy of
  `pkg/dotc1z/sqlite/testdata/slim_grants.c1z`.
- `pkg/c1zsanitize`: `mustOpen`, `buildFixture`, `buildSyncFixture`,
  `expansion_roundtrip_test.go`, `page_ledger_test.go` on `NewStore`;
  `smallPageReader` no longer claims `ExpansionGrantLister` (the Pebble
  reader carries expansion in `ListGrants`);
  `TestSanitizeMultiSyncIntoPebbleIsRejected` uses `multiSyncReader`, a
  reader double that adds a second distinct sync, because the Pebble
  reader scopes `ListSyncs` to the current sync.
- `pkg/tasks/local/compactor_test.go`: `buildLocalCompactorSQLiteInput` →
  `buildLocalCompactorInput` on `NewStore`.

## Known gap (change order 1)

`TestFullPipelineDifferentialFuzz` was the only end-to-end artifact
differential: translate round-trips, deferred `by_principal` build, reopen,
bare-id gets, and the stats sidecar were compared against a SQLite artifact
built from the same seed. No test in this PR replaces that oracle. The
mock-store sweep `TestTopologicalMergeDifferentialRandom` and the retargeted
`TestTopologicalMergeDifferentialRandomStore` cover expansion algorithms,
not the storage pipeline. Follow-up: a model-based pipeline fuzz reusing the
seed generator and `comparePipelineArtifacts`.

## Findings outside scope

- `c1zsanitize.sortSyncsParentFirst` does not terminate when `ListSyncs`
  returns two runs with the same id (observed while building
  `multiSyncReader`; reproduced by duplicating a sync). Not fixed here.
- `Expander.RunSingleStep` still carries the source-batched legacy branch,
  now reachable only through in-memory test doubles.
