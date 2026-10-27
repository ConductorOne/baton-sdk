# Action invalidation: verification evidence

Plan: `docs/verification/action-invalidation/plan.md`, frozen at `87abf5ed`,
amended by CO-001 through CO-006.
Commit under verification: the uncommitted stage 1 implementation on top of
`76264ae5`. Revision ids below will be filled in at commit time.

## Status of this file

Stage 1 is implemented. The instruments that exist are I1 (classification
table), I4 (chaos `invalidate-action` effect and Graph-mode token expiry),
I8 (report fold), the engine crash dichotomy, and a process-crash dichotomy
on the public path. I2 (the five-cut sweep over the eight honoring
variants), I3's Lambda cell, I5 (soak), and I6 (operation count) do not
exist; I7 (model) cannot exist in stage 1 (CO-005). Criteria whose required
cells exceed the cells executed are **evidence incomplete**, not verified,
and §6 closure is not claimed.

Status vocabulary (plan §6): not assessed · evidence incomplete ·
verified to stated coverage · failed · explicitly excluded · deferred.

Commands run at the implementation under review:

- `go test -race ./pkg/dotc1z/... ./pkg/pagination/... ./pkg/tasks/c1api/... ./internal/chaosconnector/...`
  passes except `pkg/dotc1z` `TestC1ZConcurrentClose`, a pre-existing race
  in the sqlite `C1File` close path (`c1file.go:846` against
  `c1file.go:1506`); no file in `pkg/dotc1z/` outside `c1zstore/` and
  `engine/` is touched by this change.
- `go test -race -timeout 60m ./pkg/sync/` passes (the default 10 minute
  timeout is too short under `-race`; CI runs without `-race`).
- `golangci-lint run` over the changed packages: 0 issues.

Planted violations run by hand (each restored afterwards; each must fail
the named test and did):

| plant | failed |
|---|---|
| per-item cap `>=` → `>` (`stageRestart`) | `TestPendingWorkRestartPerItemCap` |
| per-sync fold ignores other buckets (`stageRestart`) | `TestPendingWorkRestartPerSyncBudgetFoldsBuckets`, `TestPendingWorkRestartBudgetUnderConcurrentWorkers` |
| invalidation list not appended (`stageRestart`) | `TestPendingWorkRestartCommitsRowAndMovesItem` and three others |
| projection `terminal` ignores `restarted` (`ledgerReportProject`) | `TestLedgerReportProjectRestartedNotTerminal` |
| restart to `""` finishes the action (`publishPendingTransition`) | `TestActionInvalidationRestartsAction` (through the completed-list-resources count against the uninterrupted sync) |
| restore skips the invalidation counters | `TestActionInvalidationRestartPageIsDurable`, `TestActionInvalidationPerItemCapFailsSync` |
| `Aborted` not honored | `TestClassifyActionInvalidationTable` |
| roots honored (CO-006) | `TestClassifyActionInvalidationTable` |
| `GRPCStatus()` drops the detail | `TestChaosGraphTokenExpiry/translated/grpc-server-fault` only |

## Per-criterion status

| id | status | artifact | notes |
|---|---|---|---|
| C01 | verified to stated coverage | `pkg/pagination/invalidate_action_test.go` `TestInvalidateActionFromError` | `status.Code(typed) == FailedPrecondition`; the flattened status carries the detail. |
| C02 | evidence incomplete | same; `pkg/sync/chaos_token_expiry_test.go` `TestChaosGraphTokenExpiry` | Bare, `%w`-wrapped, `errors.Join`ed, flattened, and across a real gRPC server boundary (server-fault transport). The Lambda (D8) cell is not executed; by inspection `pkg/lambda/grpc/util.go` `ErrorResponse` serializes the full `spb.Status` (details included) and `Response.Status` rebuilds it with `status.FromProto`. |
| C03 | verified to stated coverage, reduction named | `pkg/sync/ledger_invalidation_test.go` `TestClassifyActionInvalidationTable`, `TestActionInvalidationNestedConnectorCallMisclassified` | 4992 rows: 13 ops × root/per-type × 16 codes × spawned × {absent, typed, wrapped} × own-list-call (CO-009). End to end: a detail from the grants handler's nested `GetResource` is misclassified, one call, no restart; with the mark check planted out it restarts to the per-item cap. The plan's 702 counts op *variants*; the classifier reads only `Op`, `ResourceTypeID == ""`, and `Spawned`, so per-resource, type-scoped, and child-under-parent collapse structurally. Expected class is a function of the cell. The planted-violation mode is the two implementation plants above rather than a flipped expected class. |
| C04 | evidence incomplete | table absent rows; `TestActionInvalidationMisclassifiedCodeFailsSync` | Classification: absent rows return no error. End to end: ledgered `InvalidArgument` with a detail fails without a restart. Other codes end to end not executed. |
| C05 | verified to stated coverage | table, `spawned=true` rows | Every honoring cell with `Spawned` is the classification error. Engine side: `TestPendingWorkRestartRefusesPayload/spawned`. |
| C06 | evidence incomplete | `pkg/dotc1z/engine/pebble/pending_work_restart_test.go` `TestPendingWorkRestartCommitsRowAndMovesItem`, `TestPendingWorkRestartRefusesPayload` | Zero record keys after a restart; records and children refused with nothing written. Claims and terminal are refused by `requireRestartShape` but not exercised. |
| C07 | verified to stated coverage | `TestPendingWorkRestartCommitsRowAndMovesItem`, `TestPendingWorkRestartCrashDichotomy` | Row at `(identity@T, workID, r)`, `Restarted`, `NextPageToken == R`, hash of `R`; for `R ∈ {"", "B", "A"==T}`. |
| C08 | verified to stated coverage | `TestPendingWorkRestartCommitsRowAndMovesItem` | `require.Equal` on the whole `LedgerWork` against the pre-image with revision `+1`, token `R`, list `[r]`; a continuation keeps the list; a second restart appends. |
| C09 | verified to stated coverage | same; `TestPendingWorkRestartRefusesPayload/bucket-not-incremented` | Counter lands with the page; a bucket that does not count the page is refused. |
| C10 | evidence incomplete | `TestPendingWorkRestartCrashDichotomy` (engine, F1 batch error, F2 before, F3 after unsynced, F4 after flush × `R ∈ {"", "B"}`); `TestActionInvalidationCrashProcess` (syncer, real process exit before and after the restart commit × wal and flushed image) | Both show exactly two images: nothing, or row + moved item + counter (+ facts in the process test). P2's 544 cells: engine cells are op-agnostic by construction (`stageRestart` never reads the op); syncer cells executed are one variant (`SyncResources` per-type), mid-chain, restart `""`, resumer = new process. I2 does not exist. |
| C11 | evidence incomplete | `TestActionInvalidationRestartsAction` (`R ∈ {"", "T1", "T2"}`), `TestActionInvalidationCrashProcess` | Next call carries `R` after commit; carries `T_expired` when the restart did not land. One honoring variant. |
| C12 | evidence incomplete | `TestPendingWorkRestartFencesStaleAttempt` | Stale continuation, finish, and second restart after a restart to `""` get `ErrStaleLedgerWork` and write nothing (row count and record keys unchanged). Takeover by another attempt at the syncer level not executed. |
| C13 | not assessed | — | Needs a child-under-parent variant. |
| C14 | evidence incomplete | `TestActionInvalidationRestartsAction` | Resource identities after the restarted sync equal the uninterrupted sync's; the completed-list-resources count equals too. Not byte-equal artifacts. |
| C15 | not assessed | — | The executed restarts re-list the same first page, so over-retention is not visible. |
| C16 | verified to stated coverage | `TestActionInvalidationPerItemCapFailsSync`; `TestPendingWorkRestartPerItemCap` | `errors.Is` sentinel, `status.Code == FailedPrecondition` through the wrap, `ActionInvalidationCapError.Cap.Invalidations == [1, 3]`, calls `["", "T1", "", "T1", "", "T1"]`, refused page leaves item and counter unchanged. Cap parameterized at 2. |
| C17 | verified to stated coverage | `TestActionInvalidationPerItemCapFailsSync` (second half) | Resume on the same artifact makes one connector call (`["T1"]`) and fails with the sentinel; restored count is 2. |
| C18 | verified to stated coverage | `TestPendingWorkRestartPerSyncBudgetFoldsBuckets`, `TestPendingWorkRestartBudgetUnderConcurrentWorkers` (under `-race`), `TestActionInvalidationPerSyncBudgetFailsSync` | Fold across run ids and workers; exactly `PerSync` commits under four concurrent workers. |
| C19 | verified to stated coverage | `TestActionInvalidationRestartPageIsDurable`, `TestActionInvalidationCrashProcess` | `sync.ingest_known` and `sync.ingest_blocked` present after a cancel right after the restart and after a process exit where the restart landed; `replayBlocked` set in memory. |
| C20 | verified to stated coverage, reduction named | `TestLedgerReportProjectRestartedNotTerminal`, `TestLedgerReportCountsRestartPages` | Projection reads fields 2, 26, and the hash only, so the 32 cells collapse to scrubbed × restarted. |
| C21 | evidence incomplete | `TestLedgerReportCountsRestartPages` | Counts asserted against the known row set (restart 1, terminal 1 then 2, continuation 1), not an independent fold over arbitrary rows. |
| C22 | verified to stated coverage | `TestLedgerScrubRestartToken` | Byte-level needle (`checkpointNeedleHits`) zero after the default seal, positive under retain; `Restarted` kept either way. |
| C23 | verified to stated coverage | same | Scrubbed restarted row keeps `NextPageTokenHash == hash(R)`. |
| C24 | evidence incomplete (sampled) | plan §9 addendum | By inspection: the no-invalidation path gains a nil check and in-memory field checks. No count instrument. |
| C25 | explicitly excluded | — | CO-008: the legacy path returns the handler's error unchanged; the sqlite write path is being removed on its own branch. |
| C26 | evidence incomplete | `TestChaosGraphTokenExpiry` | Direct, client-side gRPC, and server-side gRPC transports agree. Lambda not executed. |
| C27 | not assessed, follow-up | — | CO-005: the model has no pending-work machine. |
| C32 | evidence incomplete | `TestChaosGraphTokenExpiry` | Graph-mode expiry with a jump clock; translated: sync completes, store equals the manifest, one rejection, one restart page, on three transports. Control (untranslated): `InvalidArgument` with the 400 text, no restart page. Executed for `SyncResources` per-type; the `SyncGrants` per-resource cell of P8 is not. |
| C33 | verified to stated coverage | `pkg/tasks/c1api/full_sync_classify_test.go` `TestClassifySyncErrorActionInvalidationNonRetryable` | Both sentinels through the production wrap shape join `ErrTaskNonRetryable` with the connector status intact; a bare connector status and a misclassified detail stay retryable. |
| C28 | deferred | — | stage 2 |
| C29 | deferred | — | stage 2 |
| C30 | deferred | — | stage 3 |
| C31 | deferred | — | stage 3 |

## Gaps before stage 1 closure

- I2: the crash sweep over the eight honoring variants, both page
  positions, both restart tokens, and the four resumers. What exists is
  one variant on the public path and an op-agnostic engine dichotomy.
- I3: the Lambda transport cell.
- I5: the soak.
- C13 and C15 need a child-under-parent variant and a connector whose
  restarted chain lists a different first page.
- The decorrelated implementation read and the independent evidence audit
  have not been done.

## Open questions

OQ-1 through OQ-6 settled in the plan (OQ-1: 5 per item, 100 per sync).
