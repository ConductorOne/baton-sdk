# Action invalidation: verification evidence

Plan: `docs/verification/action-invalidation/plan.md`, frozen at `87abf5ed`,
amended by CO-001 through CO-006.
Commit under verification: the uncommitted stage 1 implementation on top of
`76264ae5`. Revision ids below will be filled in at commit time.


Identifiers cited below are the names at the time each row was written; CO-018 in the plan maps them to the current names.

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
| C02 | evidence incomplete | same; `TestInvalidateActionFromErrorMatchesWire`; `pkg/sync/chaos_token_expiry_test.go` `TestChaosGraphTokenExpiry` | Bare, `%w`-wrapped, `errors.Join`ed, flattened, and across a real gRPC server boundary (server-fault transport). The in-process answer equals the answer after `status.Convert` for eight error shapes, including the detail behind another status (absent both ways, CO-017; `connectorbuilder` now surfaces it before serialization, `TestSurfaceRestartAction` and the four-shape runs of `TestListCallInvalidationClearsHTTPCaches` / `TestActionInvalidationThroughHTTPCache`, CO-019). The Lambda (D8) cell is not executed; by inspection `pkg/lambda/grpc/util.go` `ErrorResponse` serializes the full `spb.Status` (details included) and `Response.Status` rebuilds it with `status.FromProto`. |
| C03 | verified to stated coverage, reduction named | `pkg/sync/ledger_invalidation_test.go` `TestClassifyActionInvalidationTable`, `TestActionInvalidationNestedConnectorCallMisclassified` | 4992 rows: 13 ops × root/per-type × 16 codes × spawned × {absent, typed, wrapped} × own-list-call (CO-009). End to end: a detail from the grants handler's nested `GetResource` is misclassified, one call, no restart, on the typed error and on `NotFound` and `Unimplemented` statuses (the codes that helper otherwise swallows, CO-011); with the mark check planted out it restarts to the per-item cap. The plan's 702 counts op *variants*; the classifier reads only `Op`, `ResourceTypeID == ""`, and `Spawned`, so per-resource, type-scoped, and child-under-parent collapse structurally. Expected class is a function of the cell. The planted-violation mode is the two implementation plants above rather than a flipped expected class. |
| C04 | evidence incomplete | table absent rows; `TestActionInvalidationMisclassifiedCodeFailsSync`; `TestActionInvalidationMisclassifiedNotFoundFailsNotWarns` | Classification: absent rows return no error; every misclassified cell reports `Internal`, is not a warning, and is not retried (CO-010). End to end: ledgered `InvalidArgument` with a detail fails without a restart; `NotFound` with a detail on grants fails the sync rather than finishing the action with a warning (with the status override planted out, the sync ends in "too many warnings"). `Unavailable`/`DeadlineExceeded` end to end not executed. |
| C05 | verified to stated coverage | table, `spawned=true` rows | Every honoring cell with `Spawned` is the classification error. Engine side: `TestPendingWorkRestartRefusesPayload/spawned`. |
| C06 | verified to stated coverage | `pkg/dotc1z/engine/pebble/pending_work_restart_test.go` `TestPendingWorkRestartCommitsRowAndMovesItem`, `TestPendingWorkRestartRefusesPayload` | Zero record keys after a restart; every record buffer (resource types, resources, entitlements, grants, grant deletes, assets), children aligned with scheduling keys, and the terminal mark refused with nothing written; the shape line planted out fails `children-aligned` and `terminal` (CO-012). Facts are allowed on a restart page by design (the ingest-known and ingest-blocked facts ride it). |
| C07 | verified to stated coverage | `TestPendingWorkRestartCommitsRowAndMovesItem`, `TestPendingWorkRestartCrashDichotomy` | Row at `(identity@T, workID, r)`, `Restarted`, `NextPageToken == R`, hash of `R`; for `R ∈ {"", "B", "A"==T}`. |
| C08 | verified to stated coverage | `TestPendingWorkRestartCommitsRowAndMovesItem` | `require.Equal` on the whole `LedgerWork` against the pre-image with revision `+1`, token `R`, list `[r]`; a continuation keeps the list; a second restart appends. |
| C09 | verified to stated coverage | same; `TestPendingWorkRestartRefusesPayload/bucket-not-incremented` | Counter lands with the page; a bucket that does not count the page is refused. |
| C10 | verified to stated coverage | `TestPendingWorkRestartCrashDichotomy` (engine, F1 batch error, F2 before, F3 after unsynced, F4 after flush × `R ∈ {"", "B"}`); `TestActionInvalidationCrashProcess` (syncer, real process exit before and after the restart commit × wal and flushed image); `TestActionInvalidationAbortAtRestartSweep` (syncer, five ops × `R ∈ {"", T_expired}` × commit failed / after commit, resumed by a new syncer on the same file) | All show exactly two images: nothing, or row + moved item + counter (+ facts in the process test). The sweep's twenty cells resume to the clean store, with the count restored (planted: zeroing the restore in `ledger_restore.go` fails all ten after-commit cells). Not executed: the process-exit cut on ops other than `SyncResources`, type-scoped entitlements and grants, and the takeover resumer. |
| C11 | verified to stated coverage | `TestActionInvalidationRestartsAction` (`R ∈ {"", "T1", "T2"}`), `TestActionInvalidationRestartsEveryHonoringOp` (five ops, `R = T_expired`, differential store oracle), `TestActionInvalidationCrashProcess`, `TestActionInvalidationResumeMidRestartedChain` | Next call carries `R` after commit; carries `T_expired` when the restart did not land. Every honoring op through its own handler and list call site (CO-013). A stop after the rerun's first page resumes at the page reached with the invalidation list intact (CO-015). `TestActionInvalidationRestartsChildUnderParent`: the expiry on a child list under a parent, restart to `""`, runs the child again from the start and counts under `SyncResources` (planted: refusing actions with a parent in `honorsActionInvalidation` fails it). Type-scoped entitlements and grants not separately executed. |
| C12 | evidence incomplete | `TestPendingWorkRestartFencesStaleAttempt`, `TestPendingWorkRestartRaceOnOneItem` | Stale continuation, finish, and second restart after a restart to `""` get `ErrStaleLedgerWork` and write nothing (row count and record keys unchanged). Eight concurrent restarts of one item: one commits, the rest are fenced, one invalidation recorded, 20× under `-race` (CO-013). Takeover by another attempt at the syncer level not executed. |
| C13 | verified to stated coverage | `TestActionInvalidationRestartedChainPushesChildrenOnce` (same attempt; across a resume) | Each parent's child list runs once; the store equals a clean run; no pending work remains. The ledger scheduling claim is the only dedupe on this path (`collectLedgerResources` does not record into `childSchedule`); planted: disabling `HasScheduledWork` in `stageWorkTransition` fails both cases. |
| C14 | evidence incomplete | `TestActionInvalidationRestartsAction` | Resource identities after the restarted sync equal the uninterrupted sync's; the completed-list-resources count equals too. Not byte-equal artifacts. |
| C15 | verified to stated coverage | `TestActionInvalidationOverRetention` | A resource listed before the expiry and deleted upstream before the rerun stays in the invalidating run's store; the control run without it has two resources. Documented, not discovered. |
| C16 | verified to stated coverage | `TestActionInvalidationPerItemCapFailsSync`; `TestPendingWorkRestartPerItemCap` | `errors.Is` sentinel, `status.Code == FailedPrecondition` through the wrap, `ActionInvalidationCapError.Cap.Invalidations == [1, 3]`, calls `["", "T1", "", "T1", "", "T1"]`, refused page leaves item and counter unchanged. Cap parameterized at 2. |
| C17 | verified to stated coverage | `TestActionInvalidationPerItemCapFailsSync` (second half) | Resume on the same artifact makes one connector call (`["T1"]`) and fails with the sentinel; restored count is 2. |
| C18 | verified to stated coverage | `TestPendingWorkRestartPerSyncBudgetFoldsBuckets`, `TestPendingWorkRestartBudgetUnderConcurrentWorkers` (under `-race`), `TestActionInvalidationPerSyncBudgetFailsSync`, `TestActionInvalidationPerSyncBudgetAcrossResume` | Fold across run ids and workers; exactly `PerSync` commits under four concurrent workers; a budget of one spent in attempt one refuses the next restart in attempt two on the count restored from the ledger (planted: dropping other buckets in `counterSplit` fails it). |
| C19 | verified to stated coverage | `TestActionInvalidationRestartPageIsDurable`, `TestActionInvalidationCrashProcess` | `sync.ingest_known` and `sync.ingest_blocked` present after a cancel right after the restart and after a process exit where the restart landed; `replayBlocked` set in memory. |
| C20 | verified to stated coverage, reduction named | `TestLedgerReportProjectRestartedNotTerminal`, `TestLedgerReportCountsRestartPages` | Projection reads fields 2, 26, and the hash only, so the 32 cells collapse to scrubbed × restarted. |
| C21 | evidence incomplete | `TestLedgerReportCountsRestartPages`; `TestPendingWorkReferencesCheckRestartedRowContinuation` (reference scan: a restarted row's continuation is checked for `R ∈ {"", "B"}`; planted) | Counts asserted against the known row set (restart 1, terminal 1 then 2, continuation 1), not an independent fold over arbitrary rows. |
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

- I2: the full crash sweep. What exists is the process-exit dichotomy on
  one variant, an op-agnostic engine dichotomy, and an in-process abort
  sweep over the five ops × both restart tokens × both sides of the
  restart commit, each resumed by a new syncer (C10). Not executed:
  process exit on the other ops (the restart batch is op-agnostic, so
  the kill result cannot vary by op), type-scoped entitlements and
  grants, and the takeover resumer (one process holds the pebble
  directory; the fence is tested at the engine).
- The HTTP layer: `TestActionInvalidationThroughHTTPCache` runs the
  reference case with a built connector listing through `uhttp`
  (response cache on) over in-memory gRPC against an `httptest` provider
  that expires its page-two token once. The sync finishes with every
  group; page one reaches the provider twice; one restart counted.
  Disabling the cache clear in `connectorbuilder` reproduces the
  reported failure: five restarts replayed from cache, then the per-item
  cap (CO-017).
- I3: the Lambda transport cell.
- I5: the soak.
- The decorrelated implementation read and the independent evidence audit
  have not been done.

## Open questions

OQ-1 through OQ-6 settled in the plan (OQ-1: 5 per item, 100 per sync).
