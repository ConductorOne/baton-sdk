# Verification evidence

The frozen plan and change-order log remain in [plan.md](plan.md).
The [pre-trim record](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md) preserves every criterion's tests,
planted defects, limits and execution history through fda24282. That commit is
retained on the verification-before-quality-trim branch. No status is promoted
by moving historical detail out of this file.

## Equality normalizations (CO-005)

The canonical cross-run comparison may normalize Attempt, CommittedAt,
TakenOverAt, page/connector/wait durations, retry counts and waits. It may
not remove records, indexes, digest, facts, completion or other committed
accounting. Each normalized measurement is still checked independently by
O5. Record raw artifact digests alongside canonical results without claiming
byte equality. A returned fresh NoSync commit need not survive a crash.

## Instrument coverage and gaps

`tools/cells.py` emits stable cell IDs for P1–P10, CO-002 and C49. CO-021/023 remove
local processing phases from page products: P1 has 4,860 cells and P2 has 297. It records
required cells, not executed cells. P4's repeated resumes are mandatory
subcases. Additional feature crosses specified by individual criteria still
need fixtures; the generated products are not the entire coverage model.

The strict fixture and companion capability recorder cover page writes, lifecycle
mutators and session writes. Public chaos fixtures install the engine write hook.
Raw snapshots, logical canonical comparisons, process-crash tests and durable-only
VFS crash images have executed; their bounded coverage is recorded in the archived execution log. The
full product-to-executed-test manifest remains incomplete. This is not closure
of C10, C37, C38 or C47 over all required cells.

## Criterion index

These are the existing conservative coverage dispositions, not a claim that every
historical test still describes the current implementation. CO-027 replaces the
history walk and request-identity claims; its current evidence is below. Each link
retains the original test/mutant record and uncovered cells.

| Criterion and detailed evidence | Status |
| --- | --- |
| [C01](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c01) | verified to stated coverage |
| [C02](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c02) | not assessed |
| [C03](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c03) | not assessed |
| [C04](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c04) | evidence incomplete |
| [C05](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c05) | evidence incomplete |
| [C06](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c06) | not assessed |
| [C07](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c07) | evidence incomplete |
| [C08](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c08) | evidence incomplete |
| [C09](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c09) | evidence incomplete |
| [C10](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c10) | evidence incomplete |
| [C11](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c11) | evidence incomplete |
| [C12](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c12) | not assessed |
| [C13](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c13) | evidence incomplete |
| [C14](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c14) | evidence incomplete |
| [C15](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c15) | not assessed |
| [C16](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c16) | evidence incomplete |
| [C17](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c17) | evidence incomplete |
| [C18](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c18) | not assessed |
| [C19](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c19) | evidence incomplete |
| [C20](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c20) | evidence incomplete |
| [C21](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c21) | evidence incomplete |
| [C22](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c22) | not assessed |
| [C23](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c23) | evidence incomplete |
| [C24](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c24) | evidence incomplete |
| [C25](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c25) | evidence incomplete |
| [C26](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c26) | evidence incomplete |
| [C27](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c27) | evidence incomplete |
| [C28](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c28) | evidence incomplete |
| [C29](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c29) | evidence incomplete |
| [C30](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c30) | evidence incomplete |
| [C31](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c31) | evidence incomplete |
| [C32](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c32) | evidence incomplete |
| [C33](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c33) | evidence incomplete |
| [C34](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c34) | evidence incomplete |
| [C35](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c35) | evidence incomplete |
| [C36](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c36) | evidence incomplete |
| [C37](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c37) | evidence incomplete |
| [C38](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c38) | evidence incomplete |
| [C39](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c39) | evidence incomplete |
| [C40](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c40) | evidence incomplete |
| [C41](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c41) | not assessed |
| [C42](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c42) | evidence incomplete |
| [C43](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c43) | evidence incomplete |
| [C44](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c44) | evidence incomplete |
| [C45](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c45) | evidence incomplete under CO-011's revised shared-change boundary |
| [C46](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c46) | not assessed |
| [C47](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c47) | evidence incomplete |
| [C48](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c48) | evidence incomplete |
| [C49](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c49) | evidence incomplete |
| [C50](https://github.com/ConductorOne/baton-sdk/blob/fda242827a2e177e7f459d802502fd8f4f8fbe2b/docs/verification/syncer-on-ledger/evidence.md#c50) | evidence incomplete |

## CO-027 consumer integration

The SDK now resumes from bounded pending reads, not completed-history traversal.
The prior walker and page-identity claim map are removed. The existing parallel
scheduler owns continuations and admits committed children in bounded FIFO
windows. Ledger resource-child scheduling evidence lives in an indexed durable
relation. SQLite keeps its existing checkpoint path.

| Claim | Status | Test and defect evidence | Limit |
| --- | --- | --- | --- |
| Resume does not consult completed history | verified to stated coverage | TestPendingSyncResumesWithoutHistoryReads rejects every GetLedgerRow call; failed on prior implementation, passes on pending consumer | Public fixture, not every connector topology |
| Equal request arguments remain separate executions | verified to stated coverage | TestPendingSyncRepeatedTokenReachesEmptyTerminalPage failed on prior consumer; repeated A requests now reach the empty terminal response | No cycle detection or connector termination guarantee |
| Execution memory does not accumulate completed identities or all pending actions | verified to stated coverage | TestPendingSyncBoundsExecutionWindow exercises1001 actions; TestPendingSyncSpilledChildrenKeepOrder exercises800 children with1/4 workers and bounded windows | Count bound, not arbitrary response/token bytes; not an RSS proof |
| Missing queue declaration cannot finish unfinished history | verified to stated coverage | TestPendingWorkSealRejectsMissingDeclaration reproduced successful unsafe seal before guard; public refusal test also passes | Already-finished low-level reseal and durable disposal recovery remain allowed |
| Pending serialization preserves request arguments | verified to stated coverage | TestPendingWorkRejectsInvalidChildToken failed before UTF8 rejection; now invalid input leaves no committed page | Valid UTF8 JSON representation; no alternate byte-token encoding |
| Read failures preserve recovery position | verified to stated coverage | TestPendingRefillFailureLeavesChildrenForColdResume and TestLedgerRestoreFailureDoesNotPublishState cover refill, counter and initial queue reads | Failure injection, no independent mutation qualification for each read |
| Local processing stays outside collection transactions | verified to stated coverage | TestPendingSyncCompletesLocalPhaseWithoutPageHistory and TestPendingLocalCompletionFailureAndStopAccounting cover whole-phase completion and failed completion retry | Existing external-import replay limitation remains outside this change |
| Debug references name exact executions | verified to stated coverage | TestPendingWorkReferencesDistinguishRepeatedArguments removes one child and one continuation while equal-argument rows remain; detects both before/after scrub | Direct missing-row injection; no production deletion path implied |

## Current validation and review

At production revision6ca6a0ac, the full sync suite passes (87.646s), storage tree
passes (parent28.146s/Pebble11.927s), and compactor passes (21.455s). Three focused
integrated race repetitions pass. The full142-cut sweep covers52 commits,
45 connector responses and45 token-expiration cuts; six fixed-seed randomized
scheduler runs pass under race. CI merge-checkout lint reports zero issues.

The storage review found that absent pending state could seal unfinished history;
TestPendingWorkSealRejectsMissingDeclaration reproduced the bug before the guard.
The resumed cost driver then found an extra connector request after cancellation.
TestPendingSyncCancellationBeforeContinuation observes two calls before the fix,
one before close and three total after cold resume with the explicit context
check; three race repetitions pass. The additional skipped-sync report test
checks work ID/revision and zero archived reference mismatches. It disproves a
review finding without a production change: the writer already stamps the IDs.

Independent consumer and lifecycle reviews at84811dfb, with bounded follow-ups at
6ca6a0ac, leave no concrete finding open. The consumer review ran as GPT-6 Sol;
the separate lifecycle delegation was configured as GPT-5.5. No Claude review is
claimed. Builds from the earlier overlapping-checkout incident are excluded.

Canonical equality follows CO-005 above. The report reopen comparison separately
asserts ledger_keys_scanned changing2→1 when the queue declaration is removed;
all other decoded fields compare equal and a read-only reopen leaves bytes unchanged.

The [review guide](README.md#pending-work-revision-cost) records current cost
medians and limitations. The original full C49 matrix, byte attribution and full
product-to-test mapping remain incomplete. Passing sampled tests and bounded
reviews do not establish complete original plan-product closure.

## Code-quality trim

The pre-trim snapshot remains pinned at fda24282. The frozen plan is unchanged;
all50 criterion dispositions and their detailed test/mutant records remain linked.
An independent sync-side quality review and a storage/comment audit led to shared
operation-specific root planners, removal of three redundant read wrappers, direct
page-based report-option staging, and moving a test-only seal helper into tests.
Prototype export scaffolding is removed; report assertions and memory benchmarks
remain. Write wrappers retain dirty tracking and write-hook enforcement.

Full sync tests pass (88.561s), the storage tree passes (parent26.434s/Pebble10.614s),
and focused planning tests pass three race repetitions (31.764s). This mechanical
cleanup does not promote coverage statuses or alter durability policy.
Report-option/seal/retention checks also pass three race repetitions (5.046s);
CI merge-checkout lint reports zero issues after the final cleanup.
