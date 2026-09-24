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

## Current criterion reconciliation

[Current coverage](current-coverage.md) maps all50 criteria to current assertions,
applicable change orders and precise residual scope. It replaces the historical
“not assessed” labels. The full old test/mutant record remains linked there.
C01 retains its verified attachment disposition; C02–C50 remain evidence incomplete
against the complete original products. Current positive tests are not presented
as exhaustive closure. No criterion has been silently dropped.

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

Historical validation at production revision6ca6a0ac: the full sync suite passes (87.646s), storage tree
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

## State ownership refactor

Startup now constructs runtime and scheduler from the same final reads; the
historical counter cache and unused legacy graph payload are removed. An initialized
resume folds counters once and reads facts twice (preflight and final restore).
Attempt accounting owns its cumulative bucket, while live historical totals stay
separate. Captured response observations feed both durable and post-commit live
stats. Lifecycle preparation selects one operation; archived recovery fields keep
the existing flat JSON format, checked by a legacy-layout round trip.

The observation regression rejects a reintroduced post-commit annotation decode
(stored wait7ms versus live0ms). Independent accounting review found a reentrant
store callback deadlock; the new observer test reproduces it before the fix and
passes afterward. Completion persists outside the observation lock and publishes
only completed counters on success. A scheduler fixture now preserves its supplied
same-attempt worker buckets; the full suite caught that fixture's earlier reset.
The lock-order meta test passes without relaxing its resolver or exclusions.

Final full sync passes83.064s; storage tree passes (parent55.702s/Pebble26.595s),
and compactor passes45.037s. Corrected accounting, process-crash and lock-order
checks pass three race repetitions (49.732s). All142 commit/response/expiration
cuts and six fixed-seed scheduler soaks pass (7.113s). CI merge-checkout lint is
clean. Independent lifecycle review found no confirmed defect; accounting review
accepted the callback correction. Shared subprocess setup retains every crash
scenario and assertion. These changes do not close unexecuted original products.

## Current assurance reconciliation

At4a8f3675 the four-package cross-instrumented run completed with3,039 passing
and42 skipped test/subtest events; no failures. The old-SDK artifact consumer ran
explicitly in that run. It resumed an actual eb63f1b5 saved checkpoint containing
ten records, fetched only three remaining pages and reopened forty exact IDs and
payloads. The producer and opt-in reproduction commands are in README.md.

New cases cover uninitialized quality facts/counters through public completion,
all-five-fact reverse commit order, V0/V1/V2 accounting across three cold resumes
and duplicate stop flushes, connector cleanup failure after a confirmed finished
record, and full/partial artifacts in four ledger-retention/failure modes through
both compactor strategies. The quality-domain omission, worker0/run-bucket
collision and lost-fact-merge mutations each fail the corresponding new test.
No production correction was required.

Statement-range inspection prompted additional cases for durable child-scheduling
validation (including empty child responses, missing evidence and read errors),
valid grants following rejected inputs, and invalid pending descriptors. Those
cases pass, including three race repetitions. The scan lists residual zero-hit
changed ranges rather than interpreting them as missing whole features. Most
are individual error/rejection paths; optional asset planning, preserved-graph
paging and diagnostic variants remain sampled. Immediate process cuts at every
auxiliary bypass and every public sidecar-I/O failure are not claimed.

The coverage summarizer now aggregates duplicate source blocks emitted by Go's
cross-package profiles. A zero-hit copy cannot override a positive hit in another
package. Duplicate-block, inconsistent-block and incomplete-execution-stream
checks qualify the corrected instrument. Raw profiles, per-range dispositions
and current execution manifest are retained outside the PR.

The current mapping retains all50 criteria and identifies four residual classes:
unexecuted combinations, individually unforced lower-level errors, incomplete
original cost dimensions, and incomplete exhaustive mutation/branch closure.
The bounded review/test evidence supports a merge recommendation; it does not
satisfy the original exhaustive products in full or waive their recorded limits.


## Default disposal correction (CO-028)

Report-generation failure now saves an explicit unavailable report and still drops
history. Public full/partial fixtures verify absent history, preserved data/stats,
and overlay/fold compaction. Recovery-archive write failure prevents seal; its
retry test verifies eventual deletion and physical token purge. The existing
8-stage fresh/finished disposal crash images remain covered by the focused run.
SQLite resource-type validation and targeted child deduplication are restored to
the baseline implementation; shared planning and accounting helpers remain.

Validation: affected Pebble, public dotc1z and compactor suites passed; focused
disposal/report/compactor tests passed three times under the race detector.
The full sync suite passed after removing the test-only Pebble checkpoint mode;
the existing ledger fixture covers the same multi-page filter assertion.
CI-merge-checkout lint reported zero issues.
