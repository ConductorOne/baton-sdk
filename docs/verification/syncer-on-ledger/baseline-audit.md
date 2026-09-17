# Baseline behavior audit

Baseline: eb63f1b5. Audited private implementation: 66214bc2, with lifecycle
correction CO-010 at bad31ae1. The requester asked for an audit of implicit
caller contracts after rejecting a change to sync lifecycle semantics.

The public syncer still executes the baseline implementation. This report
identifies differences in the private runtime and requirements missing from
its integration design. It does not claim these regressions are deployed.
That statement describes the audited revision. K2c now adds page invocation
and commit branches to the existing scheduler and transition functions, as
authorized by CO-011. Public attachment still leaves the ledger path off.

## Confirmed differences

| ID | Baseline behavior | Private implementation | Consequence | Criteria |
| --- | --- | --- | --- | --- |
| A1 | Parallel dispatch drains the current operation batch before taking later operations from the action stack. | execute puts every pending root into one worker queue, regardless of operation. | A grant action can start while resource collection is blocked; enumeration may run before its input records exist. | C13–C16,C41 |
| A2 | finishActionLocked counts spawned actions in completedActions and per-operation action counts. Per-resource progress is a separate measure. | execute excludes every spawned action from both completion counters. | Sealed accounting differs; using these totals for warning ratios changes the denominator and can change whether a sync fails. | C20,C23,C30 |
| A3 | parallelActionQueue.transition rejects duplicate child cursors within one response, including a child equal to the parent continuation, before committing the transition. Re-mentions across responses are a separate case. | The row commits first; admission silently collapses duplicate children. | A connector protocol error becomes successful committed work. Across-response dedup does not justify this change. | C05,C13,C14,C39 |
| A4 | Missing ingestion quality is unknown. Sync restores conservative blocked state for resumed legacy history with no quality checkpoint. | ledgerSyncStats emits a non-nil clean quality record when both ingest facts are absent. Empty-token restoration can reach the missing-fact state. | Absence of evidence becomes a positive quality claim. ledgerFactIngestKnown is recorded but not consulted at seal. | C19,C24,C30 |
| A5 | syncParallel joins independent worker errors; errors.Is can find each cause. | execute retains only the first error and discards later results while joining workers. | A concurrent storage failure can disappear behind another worker's error. | C39,C40 |

All five were reproduced by TestLedgerBaselineContractAudit at 6cc6eab4. These were tests
against currently wrong behavior, not planted production mutations. The
baseline sides use unchanged runState and parallelActionQueue helpers,
main's quality representation, and its explicit errors.Join contract. The
phase test uses testing/synctest to hold resource collection open and prove
that grant work starts before release; it does not depend on a sleep.

The phase result establishes the private executor's behavior on mixed roots,
such as a restored stack. A future coordinator could restrict its inputs to
one eligible batch, but no such coordinator exists and the brief did not
require one. The test is therefore an integration blocker, not a claim that
all parallel execution across resource types is forbidden.

## Lifecycle finding already corrected in the brief

CO-010 withdraws the proposed reset of ended_at and stale sync metadata.
WithSyncID selects the existing sync; main returns newSync=false, retains
its metadata, and loads its checkpointed facts and stats. Further requested
processing of a finished collection is not permission to discard that
history. The private beginLedgerRuntime still drops ledger state and seeds
Init for every finished binding. That code is not ready for integration.

The public SDK's service-mode task deliberately supports deferred expansion:
SkipExpandGrants becomes WithDontExpandGrants, after which the collection is
sealed and uploaded. The compactor also invokes same-ID expansion over
existing data. These caller sequences must be represented by tests. The
previous synthetic reset test did not justify changing their lifecycle.

In particular, replacing the ledger must not casually erase the collection's
facts, quality history or accounting. Reusing an old seal-ready marker must
not suppress explicitly requested processing either. The implementation
needs a distinction between completed collection and later processing under
the same identity, while preserving the baseline sync metadata behavior.
This report does not select a new lifecycle representation.

## Integration requirements missing from the brief's detail

These are not classified as reproduced public regressions: public routing
and handlers do not exist yet. They are required before that work is ready.

- Preserve main's error classification: NotFound warnings where main treats
  them as warnings, retry policy and rate-limit waits, and warning-ratio
  checks. The general warning ratio and the persisted list-resource ratio
  have different histories. The latter waits for more than ten completions
  in the current process; importing old totals must not satisfy that guard.
- Preserve run-duration expiry as ErrSyncNotComplete and external stop/error
  behavior. Caller code distinguishes these results when saving an artifact
  and deciding whether to retry. Returning context.Canceled everywhere is
  not equivalent. The original batch error handling is in
  parallel_syncer.go:handleOperationError.
- Preserve transition/progress callbacks and their timing sufficiently for
  callers to track the same phases and stop work. Durable row identity does
  not replace the public Action/progress contract.
- Preserve option distinctions: OnlyExpandGrants changes requested work;
  DontExpandGrants permits collection without expansion; CompactionMergedStore
  selects a different invariant policy. Do not infer one from another or
  from ended_at. Empty-connector post-processing must not trigger source
  collection. Assets remain disabled in ordinary baseline scheduling;
  adding an atomic asset handler does not authorize enabling that phase.
- Preserve supports_diff, graph-sidecar publication, ingest verification,
  cleanup and seal ordering. Existing expansion metadata and principal
  ordering remain requirements of the proposed page adapter. These had
  partial coverage in the brief but still need actual caller comparisons.

## Tests and scope

The opt-in TestLedgerBaselineContractAudit at 6cc6eab4 failed all five
comparisons in an ordinary run and three race repetitions. K2c removes that
historical diagnostic and replaces it with normal tests of the actual
scheduler's page integration:

| Finding | Current regression test | Scope |
| --- | --- | --- |
| A1 | TestLedgerExistingSchedulerOperationBarrier | Real parallelSync with two blocked resource workers and a later grant phase. |
| A2 | TestLedgerExistingSchedulerSpawnedCompletion | Spawned page counts match existing runState completion counts. |
| A3 | TestLedgerExistingSchedulerRejectsDuplicateBeforeCommit | Existing queue rejects duplicate response children before any durable page change. |
| A4 | TestLedgerSealPreservesUnknownIngestQuality | Absent quality stays nil; known clean and blocked states remain represented. |
| A5 | TestLedgerExistingSchedulerPreservesIndependentErrors | Both concurrent causes remain reachable through errors.Is. |

The rejected executor is compiled only as a test fixture for historical
runtime, crash and cost tests. It is not a second production scheduler.
These replacement guards cover the adapter with injected handlers. Public
attachment, production handlers, Init and lifecycle restoration remain
unimplemented; passing them does not establish full caller equivalence.

The audit read all new production runtime files, capability resolution,
storage timing changes and the implementation brief, and compared them with
baseline scheduling, transition, token restoration, quality, stop and seal
behavior. It also inspected caller sequences for uploaded artifacts and
compaction. It does not substitute for end-to-end tests of the still-absent
production handlers, a full caller rollout/version matrix, physical crash
coverage, or the final required performance table.

One reason existing tests missed these differences is that several expected
values came from the same new implementation they exercised. For example,
TestLedgerTakeoverLegacyFixtures computes its expected facts/counters with
decodeLedgerCheckpoint, then tests a path that uses that decoder. It proves
migration consistency but cannot independently prove preservation of main's
quality/accounting interpretation. The interrupted-versus-uninterrupted
ledger differential likewise cannot catch behavior both new arms get wrong.
Baseline comparisons must accompany those tests.
