# Pebble `rollback-expansion`: verification evidence

Plan: `docs/verification/pebble-rollback-expansion/plan.md`, committed before
implementation. Change orders CO1 through CO8 are appended to the plan. All
stay MODERATE: none changes a sync path, adds a ledger op, or adds a rawdb
primitive. The diff touches no file under `pkg/sync/` or
`pkg/dotc1z/c1zstore/`, and modifies no pre-existing function in
`pkg/dotc1z/engine/pebble/`.

## Oracles

- O1 pre-expansion differential: `TestRollbackExpansionPebbleRestoresPreExpansionGrants`
  compares ids, Sources, sorted annotation bytes, and `needs_expansion`
  against the snapshot taken before `expandViaSyncer`.
- O2 round trip: `TestRollbackExpansionPebbleReplayValidates` runs
  `--replay --validate` and compares the full grant snapshot against the
  input. `TestRollbackExpansionPebbleValidateDetectsDivergence` shows
  `--validate` fails on connector-set self-sources that replay cannot
  restore.
- O3 index agreement: `requireGrantIndexAgreement` checks the exact key sets
  of `by_principal` and `by_needs_expansion` against the primary rows. It
  runs after every engine write test.
- O4 dry-run parity: `TestRollbackExpansionClassifiesBySources` and
  `TestRollbackExpansionMultiRound` compare dry-run and write results.
  The CLI form compares dry-run deletes to the rows the write removed.
- O5 stats parity: `TestRollbackExpansionPebbleReplayValidates` compares
  `Stats` after rollback and replay with the expanded input.
  `TestRollbackExpansionPebbleRestoresPreExpansionGrants` checks that a
  rollback without replay recomputes the grant count.

## Criterion status

- Default fixture, rollback, replay, validate: verified
  (`TestRollbackExpansionPebbleReplayValidates`, `...ReplayRederives`).
- Suspect grant deleted by default, preserved with
  `--preserve-suspect-grants`, counted either way: verified at both levels
  (`TestRollbackExpansionPreservesSuspectGrants`,
  `TestRollbackExpansionPebbleSuspectGrants`).
- Deleting a suspect grant that carries a source scope poisons that scope,
  so a later sync using `--out` as its previous sync refetches the scope
  instead of replaying it without the deleted grant. Preserved suspects and
  cleared grants poison nothing, and cleared grants keep their scope stamp.
  Verified (`TestRollbackExpansionSuspectDeletePoisonsItsScope`).
- Self-source-only Sources cleared, not deleted: verified
  (`TestRollbackExpansionClassifiesBySources`, `selfOnly` row).
- Not expanded returns `ErrSyncNotExpanded`, with no `--out`: verified
  (`TestRollbackExpansionPebbleNotExpanded`,
  `TestRollbackExpansionNothingToRollBack`).
- No grants returns zero counts without error: verified
  (`TestRollbackExpansionNoGrants`, `TestRollbackExpansionPebbleNoGrants`).
- Unfinished `Expanding` or `Sealing`: write drops the pass and warns; dry
  run warns and counts: verified at the engine level only
  (`TestRollbackExpansionLedgerPhases`). It is unreachable through the CLI
  because `cloneSync` refuses an unfinished sync (CO1).
- Unfinished `Collecting` or `Absent` refused: verified
  (`TestRollbackExpansionLedgerPhases`); the CLI refuses with no `--out`
  (`TestRollbackExpansionPebbleUnfinishedRefused`).
- Legacy unfinished checkpoint refused: verified
  (`TestRollbackExpansionRefusesUnfinishedLegacyCheckpoint`).
- Finished sync with a pending pass from `BeginPass`: dropped and warned.
  Verified (`TestRollbackExpansionLedgerPhases`, `sealed/begin-pass-true`).
- Multi-round: verified with 20,007 grants and at least two commits
  (`TestRollbackExpansionMultiRound`).
- Compacted input: verified through the fold compactor. The test asserts the
  output is Pebble and has expansion to roll back, then runs
  `--replay --validate` (`TestRollbackExpansionPebbleCompactedInput`).
- SQLite regression: every existing `TestRollback*` test passes. Only the
  shared helpers changed (CO5).
- The input is never modified: verified by an unchanged grant snapshot
  (ids, Sources, annotations, `needs_expansion`) of the input after `--out`
  (`TestRollbackExpansionPebbleRestoresPreExpansionGrants`).
- No point reads: verified by `TestRollbackExpansionIssuesNoPointReads`
  (CO8).

## Implementation-obligation addendum

The one new commit site is `rollbackExpansionRounds`, registered in
`commitPointRegistry` with the `SetRecordCommitTestHook` route.

1. Executable failure: `TestRollbackExpansionCommitFailureLandsNothing`
   fails the first commit, and
   `TestRollbackExpansionRetryAfterLaterRoundFailureConverges` fails the
   second. Both are listed under `rawdbHookFailureCases`.
2. Failure re-converges derived state: each round's batch carries the
   primary rows, the inline `by_needs_expansion` and `by_principal`
   changes, and the digest invalidations, so it lands or does not. The
   deferred `by_principal` marker is armed before the commit; if the commit
   fails, the next seal does an extra rebuild. A failed round also leaves
   the store clean, so `Close` discards the working copy
   (`TestPebbleStoreRollbackExpansionDryRunAndRefusalLeaveFile`).
3. Success reaches the checkpoint: the wrapper marks the store dirty after
   the engine returns success, as every `pebbleStore` write wrapper does.
   `TestPebbleStoreRollbackExpansionPersistsWithoutReseal` closes without
   `EndSync` and reads the rollback back.
4. Partial progress is accounted and retryable: an error returns no counts,
   and the rounds before it stay committed. Classification is idempotent
   over committed rows: deleted rows are gone and cleared rows carry no
   Sources. `TestRollbackExpansionRetryAfterLaterRoundFailureConverges`
   checks that a retry after a second-round failure reaches the same grant
   keyspace as an uninterrupted run.
5. The batch is released exactly once: `defer batch.Close()` inside the
   `withWrite` closure covers success, commit failure, and staging errors.
   The rawdb outstanding-batch ledger asserts this at every test's `Close`.
6. Durability class: `writeOpts(e.opts.durability)`, the engine's record
   write class. The fence is the envelope save at `Close`; the working
   directory is temporary until then.

Other obligations:

- The round iterator is closed before the batch commits: `rollbackExpansionRound`
  joins `iter.Close()` into its return before `rollbackExpansionRounds`
  commits.
- A malformed row aborts its round with nothing committed, in both modes
  (`TestRollbackExpansionMalformedRowAbortsRound`; the corrupt row sorts
  after staged deletes in the same round).
- Cancellation is checked before each round
  (`TestRollbackExpansionCanceledWritesNothing`).
- The Pebble CLI path reseals with `EndSync` when `--replay` is off. That
  rebuilds the deferred indexes, repairs digests, and recomputes stats.

## Mutation adequacy

Each fault was planted alone and reverted.

| Planted fault | Caught by |
| --- | --- |
| Skip `StageGrantDelete` | `ClassifiesBySources`, `MultiRound`, CLI O1, CLI suspect |
| Skip `SetSources(nil)` | `ClassifiesBySources`, `MultiRound`, CLI O1, CLI rederive, CLI validate |
| Resume at unprocessed key + `0x00` | `MultiRound` |
| `needsExpansion=false` on clear | O3 in six engine tests, CLI rederive |
| Write ignores `preserveSuspect` | engine and CLI suspect tests |
| Skip `Ledger().Drop` | `LedgerPhases` expanding, sealing, sealed+`BeginPass` |
| Accept unfinished `Absent`/`Collecting` | `LedgerPhases`, CLI unfinished |
| CLI skips the Pebble reseal | CLI O1 stats check |
| Wrapper skips `MarkDirty` | `TestPebbleStoreRollbackExpansionPersistsWithoutReseal` |

The `MarkDirty` mutant first survived every CLI test, because the reseal
or replay marks the store dirty. The store-level test was added to catch
it.

## Structural coverage

Uncovered branches in `rollback_expansion.go` after the engine tests are
error returns:

- Reachable and now instrumented: cancellation, malformed row.
- Defensive after a step that has already succeeded: the `PendingWork`
  read error, `SetCurrentSync`, and `Drop` errors after the record loaded;
  `requireCurrentSync` inside the closure after `SetCurrentSync`; `NewIter`;
  decode of a row the raw scan accepted; `marshalRecordAppend`; and
  `StageGrant*` key decode of a key from the grant primary range. Each
  returns its error unchanged, with nothing committed in that round.

## Performance

`go test ./pkg/dotc1z/engine/pebble/ -run '^$' -bench BenchmarkRollbackExpansionPebble -benchtime 3x -benchmem`
on Apple M1, 100k grants, nine in ten derived: 0.42 s/op, about 238k
rows/s, 462 MB/op, 1.46M allocs/op. The rollback's own allocations are
dominated by Pebble's range-delete fragmenter and batch growth from
`stageGrantDigestInvalidation`'s per-row `DeleteRange` inside the existing
`StageGrantDelete`. The plan rules out changing that primitive.

For comparison, `BenchmarkRollbackExpansionPageSize/pageSize=10000` on
SQLite rolls back 40k grants in 0.50 s/op, about 80k rows/s.

## Commands

- `go test ./pkg/dotc1z/engine/pebble/ -run 'TestRollbackExpansion|Seam|CommitPoint|WriteMu|ChokePoint' -count=1`: pass.
- `go test ./cmd/baton/ -run 'TestRollback' -count=1`: pass on both engines.
- `go test ./pkg/dotc1z/ -run 'TestPebbleStoreRollbackExpansion|TestPebbleStoreDirtyCoverage' -count=1`: pass.
- `go test -race ./pkg/dotc1z/engine/pebble/ ./pkg/dotc1z/ ./cmd/baton/ -run 'TestRollback|TestPebbleStoreRollbackExpansion' -count=1`: pass.
- `go test ./pkg/dotc1z/... ./cmd/baton/... ./pkg/sync/... -count=1 -timeout 30m`: pass, all 12 packages.
  With the default 10-minute timeout, `pkg/dotc1z` and `pkg/sync` timed
  out when run together on one machine. Alone, `pkg/dotc1z` passes in
  165 s and `pkg/sync` in 593 s under the default timeout. This diff adds
  nothing to `pkg/sync`.
- `golangci-lint run`: 0 issues.
- The Pebble CLI tests were run first against the unchanged CLI. All nine
  failed: the store did not satisfy `rollbackExpansionStore`.
