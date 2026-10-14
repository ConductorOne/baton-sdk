# Pebble `rollback-expansion`: implementation and verification plan

Written 2026-10-13 after an investigation of the SQLite implementation and the
Pebble engine. Everything under "Verified facts" was read in code at the cited
location; re-check a citation before relying on it if `main` has moved.

## Goal

`baton rollback-expansion` works on Pebble c1z files with the same flags and
the same output contract it has for SQLite: an isolated `--out` copy of one
sync with expander-derived grants deleted and expander-written `Sources`
cleared, optionally re-expanded with `--replay` and checked with `--validate`.
The input file is never modified.

## Constraints

These are decisions, not open questions.

- **Moderate risk ceiling.** This is a support tool. It must not change any
  code path a connector sync executes. No new ledger operations, no new rawdb
  primitives, no changes to `EndSync`, `SetCurrentSync`, `BeginPass`, or the
  syncer. New engine code is additive and reachable only from the new method.
- **No read-before-write per record.** The classification scan is the only
  read of each grant row. Deletes and clears are staged from the scanned value.
- **No `supports_diff` gate.** Ledgered Pebble syncs never write it
  (`pkg/sync/parallel_syncer.go:409-418`). "Nothing to roll back" is decided
  by the scan: zero classified rows returns `ErrSyncNotExpanded`.
- **Finished is not required, but complete collection is.** The SQLite
  restriction protects a token-held entitlement graph that Pebble does not
  have. An unfinished sync whose pending pass is in `Expanding` or `Sealing`
  (collection done) is handled by `Ledger().Drop()` and a warning. A pending
  pass in `Collecting` is refused: sealing it would stamp `ended_at` on
  partial data and the output would read as a finished sync. A legacy
  token-based unfinished sync (`sync_token != ""`, `ended_at == nil`) is
  refused for the same reason `BeginPass` refuses it (`ledger.go:768`).
- **No version bump.** `pkg/sdk/version.go` changes only at release.
- Writing follows `AGENTS.md`; comments follow `docs/COMMENTS.md`. Failing
  test first for every behavior.

## Verified facts

### What already works on Pebble

- The CLI gates on an interface, not a type:
  `rollbackExpansionStore` (`cmd/baton/rollback_expansion.go:22-25`) with
  `RollbackExpansion` and `GrantSourcesForSync`, type-asserted at lines 105
  and 127 on the store returned by `openReadOnlyC1ZStore`
  (`cmd/baton/store.go:13`). Pebble fails only because nothing implements it.
- Isolation: `FileOps().CopyIsolateSync` is implemented for Pebble in
  `pkg/dotc1z/engine/pebble/adapter_file_ops.go:43` via `cloneSync`.
- Replay: `replayExpansion` (`cmd/baton/rollback_expansion.go:304-336`) runs
  `sync.NewSyncer` with `WithConnectorStore`, `WithSyncID`,
  `WithOnlyExpandGrants`, `WithCompactionMergedStore`. On a finished ledgered
  sync `prepareLedgerState` takes the `ledgerProcessFinished` case and calls
  `ledger.BeginPass` (`pkg/sync/ledger_lifecycle.go:74-81`). No sync token is
  involved; `PrepareExpansionReplayToken` is the SQLite token-path equivalent
  and is irrelevant here.
- No "needs expansion" flag is set by rollback. `WithOnlyExpandGrants` seeds
  `factNeedsExpansion` from config on the ledger path
  (`pkg/sync/initial_actions.go:47-49`); graph load pages
  `PendingExpansionPage` over `by_needs_expansion`, which rollback preserves
  (synth rows never had entries; cleared directs keep their bit through
  `StageGrantPutDeferred`). C1 decides whether a cloned sync needs expansion
  from the same index (`sync_baton.go:168-175`). O3 asserts the index
  survives.
- Replay cannot reuse a stale graph: the entitlement-graph sidecar is bound to
  the grant generation digest and refused on count or hash mismatch
  (`pkg/sync/expansion_graph.go:95-111`). Deleting rows changes the digest.
- `SetCurrentSync` on a finished sync in a writable store is the production
  pattern. C1 does it on every sync: `CloneSync` → `NewStore` →
  `SetCurrentSync(finished id)` → syncer
  (`~/work/c1/pkg/temporal/sync_activity/sync/sync_baton.go:142-163`). The
  SDK does it to rebind for requested expansion
  (`pkg/sync/ledger_sync.go:114`) and in the compactor
  (`pkg/synccompactor/compactor_pebble.go:1121`). `bindCurrentSync` unseals,
  resumes compactions, and clears source-cache row counts; the next seal
  recounts (`pkg/dotc1z/engine/pebble/engine.go:446-467`).
- `EndSync` on a rebound finished sync with no ledger declaration is an
  explicit case: "a finished sync with no declaration is an engine-level
  reseal" (`pkg/dotc1z/engine/pebble/adapter.go:299-314`). It rebuilds
  deferred indexes when armed, repairs digests, recomputes stats and row
  counts, and pauses compactions for the close window.

### Storage shape

- Expander-created grants live in the same primary grant keyspace as connector
  grants. The synth layer is a write path (spill → SST → ingest), not a
  separate durable family (`rawdb/families.go:83-90`,
  `grants_synth_encode.go`).
- `fillSynthGrantRecord` (`grants_synth_encode.go:20-46`) stamps
  `GrantImmutable` and leaves `needs_expansion=false` on every synth row.
  Synth rows therefore have no `by_needs_expansion` entry.
- `Sources` is `map<string, GrantSourceRecord> sources = 9` on
  `v3.GrantRecord` (`proto/c1/storage/v3/records.proto:234-237`). The grant's
  own entitlement is `entitlement.entitlement_id` (`refs.proto:16-20`).
- The classification rule already exists on v3 records:
  `stripExpanderSourcesRaw` in `pkg/dotc1z/engine/pebble/source_cache.go:928`
  derives the self-source key and mirrors SQLite's `RollbackExpansion`. Reuse
  its self-source derivation rather than re-deriving the key format.
- SQLite's rule (`pkg/dotc1z/rollback_expansion.go:339-376`):
  - no `Sources` → untouched;
  - `Sources` without a self-source → expander-derived → delete; if it also
    lacks `GrantImmutable` it is "suspect" and is kept instead when
    `WithPreserveSuspectGrants` is set;
  - `Sources` with a self-source → direct grant the expander touched → keep,
    clear `Sources`.

### Write primitives

- `rawdb.RecordBatch.StageGrantDelete(key, oldVal)`
  (`internal/rawdb/records.go:215-236`) is key-derived: tombstones for
  `by_principal` and `by_needs_expansion` are re-encoded from the primary key,
  then the primary tombstone, source-scope cleanup from `oldVal`, and digest
  invalidation. No `Get`. The `Get` in the public path is
  `stageGrantDeleteIfPresentLocked` (`grants.go:1062-1081`), which exists for
  callers that do not know whether the row exists. Rollback knows.
- `rawdb.RecordBatch.StageGrantPutDeferred(key, val, oldVal, needsExpansion)`
  (`records.go:246-274`) arms the by_principal rebuild marker, maintains
  `by_needs_expansion` inline, stages digest invalidation. This is what
  `PutExpandedGrantRecords` uses (`grants.go:190`).
- Because the clears go through the deferred regime, the seal will excise and
  rebuild `by_principal` regardless. The inline `by_principal` tombstones on
  the deletes are redundant and harmless.
- `Ledger().Drop()` (`ledger.go:387-419`) removes a pending pass and keeps the
  sync; used by compaction and the sanitizer.
- Helpers: `grantIdentityFromRecord` (`identity.go:112`),
  `encodeGrantIdentityKey` (`keys.go:111`), `marshalRecord`
  (`engine.go:1059`), `IterateGrants` (`grants.go:1141`), `withWrite`
  (`engine.go:735`), `requireCurrentSync`.

## Design

### Engine method (new file `pkg/dotc1z/engine/pebble/rollback_expansion.go`)

```go
type RollbackExpansionResult struct {
    GrantsDeleted, SourcesCleared, SuspectConnectorSourced, SuspectPreserved int
}
var ErrNothingToRollBack = errors.New(...)

func (e *Engine) RollbackExpansion(ctx context.Context, syncID string, dryRun bool,
    preserveSuspect bool) (*RollbackExpansionResult, error)
```

The result type and sentinel are package-local to `pebble`. The
`pebbleStore` wrapper (package `dotc1z`) maps them onto the existing
`dotc1z.RollbackResult` and `dotc1z.ErrSyncNotExpanded`, so nothing in
`c1zstore` changes and SQLite callers see no diff.

Steps:

1. `GetSyncRunRecord(syncID)`; error if absent. Refuse when
   `sync_token != ""` and `ended_at == nil` (legacy unfinished).
2. `SetCurrentSync(syncID)`.
3. `Ledger().PendingWork(ctx, 0, 1)`:
   - `LedgerQueueAbsent`: proceed.
   - `Collecting`: refuse with an error naming the phase.
   - `Expanding` or `Sealing`: warn. In write mode `Ledger().Drop()`; in dry
     run do nothing (the CLI opens the input read-only).
4. Scan. Iterate the grant primary range with a resume key, 10k rows per
   round. For each record decode the value (`v3.GrantRecord`), classify with
   the rule above using the self-source derivation from
   `stripExpanderSourcesRaw`, and in write mode stage:
   - delete: `batch.StageGrantDelete(key, val)`;
   - clear: set `Sources` to nil, `marshalRecord`, then
     `batch.StageGrantPutDeferred(key, newVal, val, record.GetNeedsExpansion())`.
   Commit each round's batch under `withWrite` with `requireCurrentSync`.
   The iterator for the round must be closed before the batch commits and
   reopened at the resume key (SQLite does the same with its `external_id`
   cursor; see `classifyRollback`'s comment at
   `pkg/dotc1z/rollback_expansion.go:264-273`).
5. Dry run: same scan, no batch, counts only. The CLI opens the input
   read-only, so `bindCurrentSync` skips the row-count clear.
6. If the scan classified zero rows: return `ErrNothingToRollBack`.
7. Return counts. The caller seals.

Do not call `EndSync` inside the method. The CLI seals when `--replay` is off
and lets the syncer seal when it is on.

### Store wrapper (`pkg/dotc1z/pebble_store.go`)

```go
func (s *pebbleStore) RollbackExpansion(ctx, syncID string, dryRun bool,
    opts ...RollbackOption) (*RollbackResult, error)
```

Reads `preserveSuspect` from the options; ignores `WithSyncTokenRewrite`
(one sentence in the doc comment: ledgered syncs carry no token); marks the
store dirty on a non-dry run; delegates; maps the engine result and sentinel
onto `RollbackResult` and `ErrSyncNotExpanded`.

### CLI (`cmd/baton/rollback_expansion.go`)

- Narrow `rollbackExpansionStore` (line 22) to `RollbackExpansion` only. Use
  the CLI's own `grantSourcesForSync` for the pre-rollback snapshot at line
  139 as well as the post-replay one. `(*C1File).GrantSourcesForSync` is
  already deprecated and stays as is.
- Replace `dotc1z.NewC1ZFile(ctx, outPath)` at line 163 with
  `dotc1z.NewStore(ctx, outPath)`; drop the `nolint` comment.
- Retype `replayExpansion` and `grantSourcesForSync` from `*dotc1z.C1File`
  to `c1zstore.Store`. `StreamGrants` is on `connectorstore`
  (`pkg/connectorstore/streaming.go:34`); `sync.WithConnectorStore` already
  accepts the interface.
- When `--replay` is off and `store.Metadata().Engine == EnginePebble`, call
  `store.EndSync(ctx)` before `Close` so the output is resealed (indexes,
  digests, stats, row counts). SQLite keeps its current behavior.
- `TestRollbackExpansionUnfinishedRefused` encodes the SQLite-only finished
  requirement. Keep it for SQLite; add Pebble counterparts for the
  `Collecting` refusal and the `Expanding`/`Sealing` warn-and-proceed path.

## Verification

Risk model: moderate. Silent and durable, but blast radius is `--out`, the
input is read-only, and the tool is operator-run. No sync path changes, so no
sync-path verification is owed beyond the existing suites passing.

### Oracles

- **O1 Pre-expansion differential.** Build a fixture, snapshot the grant set
  before expansion, expand via the real syncer (`expandViaSyncer`,
  `cmd/baton/rollback_expansion_test.go:71`), roll back, compare the grant
  set (ids, `Sources`, `needs_expansion`, annotations) to the snapshot. This
  is the correctness oracle for classification.
- **O2 Round trip.** Roll back, replay, and run the existing
  `compareGrantSources` against the pre-rollback snapshot. This is what
  `--validate` does; it is the oracle for "replay reproduces expansion".
- **O3 Index agreement.** After rollback + `EndSync`, for every grant in the
  primary keyspace there is exactly one `by_principal` entry and, iff
  `needs_expansion`, one `by_needs_expansion` entry; no orphan entries. There
  are existing index-agreement helpers in the pebble test package (search
  `countKeys`, `ResourceByParentLowerBound`-style bounds, and the deferred
  index tests) to adapt.
- **O4 Dry-run parity.** Dry-run counts equal write-mode counts on the same
  input.
- **O5 Stats parity.** After rollback + replay, the sync's stats (grant
  count and per-type counts as `baton stats` reports them) equal those of
  the same fixture expanded once without a rollback. Guards the stats
  sidecar and the `LedgerSyncStats` overlay through `Drop`/`BeginPass`.

### Cases

- Default fixture (the existing `buildPreExpansionC1Z` shape) on Pebble:
  rollback, replay, validate.
- Suspect grant (Sources, no self-source, no `GrantImmutable`): deleted by
  default, preserved with `--preserve-suspect-grants`, counted either way.
- Connector-set Sources with a self-source only: cleared, not deleted.
- Not expanded: `ErrSyncNotExpanded` and no mutation of `--out`.
- No grants: early return, no error.
- Unfinished sync, pass in `Expanding`: write mode drops the pass, warns,
  proceeds; dry run warns and counts without writing.
- Unfinished sync, pass in `Collecting`: refused, `--out` not created.
- Legacy unfinished sync (`sync_token` set, no `ended_at`): refused.
- Multi-round: more than 10k grants so the resume key is exercised across at
  least two batches; counts and O3 hold.
- Compacted input (`record.Compacted` set, ledger archive from another sync):
  `BeginPass` allows this (`ledger.go:787`); replay with
  `WithCompactionMergedStore` succeeds. Build via the compactor or by setting
  the flag on the record, whichever the existing compactor tests already do.
- SQLite regression: every existing test in `cmd/baton/rollback_expansion_test.go`
  and `rollback_feedback_test.go` still passes unchanged except the retype.

### Instruments

- The engine method's scan must not call `db.Get`. Assert with the engine's
  existing read counters if present, or by a `testSeams` hook; otherwise
  by code review of the method against the constraint.
- Benchmark: `BenchmarkRollbackExpansionPebble` at 100k grants with a
  majority expander-derived, reporting rows/sec and allocs/op. The SQLite
  counterpart is `pkg/dotc1z/rollback_expansion_bench_test.go`.

## Out of scope

- A Pebble-native "was expanded" marker. Decide separately whether
  `BeginExpanding` should also write `supports_diff`.
- Crash-resumable rollback. `--out` is a clone; the CLI already deletes a
  partial output on failure and the operator reruns.
- Removing the SQLite implementation.

## Acceptance

- `baton rollback-expansion --dry-run`, `--out`, `--out --replay`, and
  `--out --replay --validate` succeed on a Pebble c1z produced by the current
  syncer, and `--validate` reports zero divergences.
- O1 through O5 pass. All cases above have a test. Existing SQLite tests pass.
- `go test ./pkg/dotc1z/... ./cmd/baton/... ./pkg/sync/...` and
  `golangci-lint run` are clean.
- The diff touches no file under `pkg/sync/` or `pkg/dotc1z/c1zstore/`, and
  modifies no function in `pkg/dotc1z/engine/pebble/` that existed before
  this change. New code is the new engine file, the `pebbleStore` wrapper,
  the CLI retype, and tests.
