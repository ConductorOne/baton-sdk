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


## Explicit ledger debug selection

`TestLedgerDebugRetentionRequiresExplicitOption` exercises public sync with info/debug
logging crossed with the explicit ledger debug option, asserting saved effective
options and actual history retention. `TestLedgerDebugLoggingDoesNotAuthorizeTokenRetention`
asserts debug logging does not permit raw-token retention. Both tests failed on the
logging-coupled implementation before the predicate changed. Durable token retention
from an unfinished sync still applies on resume; the change only removes log-level
selection of ledger debug mode.
Focused debug/retention and unfinished-resume checks pass three times under race;
CI-merge-checkout sync lint reports zero issues. CO-030 below replaces per-attempt option snapshots and folds prior worker buckets.


## Bounded attempt metadata (CO-030)

First/latest option facts replace per-attempt snapshots. First is preserved across
finished same-ID processing; latest is published with the first successful page of
each runtime attempt. Only those two attempts can be queried from the archive.
Prior worker/run buckets fold into one total before the read-only restore; current
attempt buckets are preserved. This bounds live keys by workers and counter labels,
not retry count. Scope-list sizes and completed page history are not bounded by this
change. Exact point deletes and the replacement total share one synced batch.

- `TestLedgerCounterFoldBoundsAttempts`: 1,000 attempts, three buckets each; at most
  four live buckets, exact sums/maxima/flags. The no-op implementation failed the
  key bound at attempt two. Point deletes reduced the fold/lock test run from tens
  of seconds with overlapping range tombstones to 0.474s on this machine.
- `TestLedgerCounterFoldKeepsCurrentCumulativeBuckets`: repeat folding then overwrite
  current totals; prior totals are not lost and current values are counted once.
- `TestLedgerCounterFoldFailureAndCrash`: cancellation, precommit error and synced
  before/after crash images; retry preserves exact accounting and bucket count.
- `TestLedgerCounterFoldNoOpAndReservedBucket` and
  `TestLedgerCounterFoldUnreadableBucketDoesNotDelete`: no repeated commit, rejected
  accumulator collisions and unreadable old buckets left intact.
- `TestLedgerAttemptMetadataBoundedAcrossResumes`: 128 preparations/page commits,
  first/latest values, bounded facts/buckets, seal and saved-file reopen. The
  stale-latest mutation failed at attempt one.
- `TestLedgerFirstOptionsFollowCommitOrder` and
  `TestLedgerFailedOptionsCommitRetries`: overlapping workers with different page
  facts, and failed commit/retry. Publishing the saved-options flag before commit
  failed the latter test.
- `TestLedgerFinishedRetentionUsesCurrentOptions`: original first options remain
  queryable after same-ID processing with different current diagnostic settings.

Independent bounded review found no blocking issue in atomic folding, exact-key
deletion, current-bucket preservation, or options publication under the commit
mutex. The reviewer independently ran the final counter tests (0.484s). This does
not claim exhaustive failure-combination coverage beyond the listed checks.

Final validation: sync 101.366s, public dotc1z 50.834s, Pebble 25.328s,
compactor 42.279s. Focused race checks passed three repetitions (sync 14.861s,
Pebble 41.222s). CI-merge-checkout lint: zero issues. The baseline SDK artifact
consumer also passed. The superseded range-delete race run was stopped; it is not
counted as passing evidence. First/latest mutation checks ran in the isolated CI
checkout and both failed at the intended assertions; production files were restored.

## CO-031 — plain ending preserves recovery

Status: verified to the focused coverage below; the original exhaustive criterion
products remain evidence incomplete. Applies to C16, C24, C31–C36 and C43.

- `TestLedgerEarlyEndPreservesRecovery` failed on the existing plain-EndSync
  refusal before implementation. Default, retain-token and discard-fact inputs
  preserve records, pending identities/revisions/tokens, facts and unscrubbed
  history across durable images before the end stamp, after it and after flush.
  Injected stamp failure leaves the binding retryable. Retrying saves ledger
  durations and ingest quality; cleanup/start-new then follows normal reset.
- `TestLedgerEarlyEndPublicRecoveryAndReset` stops a real paginated sync, saves
  and reopens its c1z, selects the unfinished run, ends it, saves/reopens again,
  and verifies pending work unchanged. Explicit same-ID continuation requests
  only the remaining page. Force reset starts a different sync and recollects.
  The fixture requests ResourcesOnly because it disables grants/entitlements;
  an initial test mistakenly requested Full on reopening and correctly started
  a different sync. The final fixture matches the original sync type.
- `TestLedgerEarlyEndAfterInterruptedDisposalKeepsStats` checks archived stats
  after a successful-completion attempt deleted history but failed before ending.
- Existing pending-work refusal tests remain on EndSyncWithStats. Existing
  disposal tests continue asserting physical removal. The full suite caught
  a skipped retry of an earlier explicit DropLedger purge; plain ending now
  retains that retry without deleting live recovery state.

Shared ledger-to-SyncStats conversion replaces the syncer's private conversion.
Plain ending reads it while writers are sealed. It preserves the pending-work
format declaration; an ended run with recoverable work is not represented as
compatible with readers that cannot understand that work. No new format or
SQLite behavior is introduced. The deprecated error symbol remains available
for source compatibility but is no longer returned.

Validation: full sync 123.167s; Pebble 19.020s; public dotc1z 73.381s;
compactor 56.186s. New early-end checks pass three race repetitions.
Changed-code golangci-lint 2.9.0: zero issues. Full lint also reports six
pre-existing gosec findings with this older linter and three formatting issues
in the separate uncommitted historical-migration drafts; it is not a clean
full-lint claim. Independent bounded review found no concrete defect and ran
both focused lifecycle tests. No full C1 workflow/DB integration test or
exhaustive derived-index failure product is claimed.

## CO-032 — nil store option compatibility

Verified to stated coverage: `TestLedgerNilStoreFallsBackToPath` failed on all
four engine/option-order cases before removing the nil-store attachment error.
The public constructor and actual path-backed attachment now pass for both
SQLite and Pebble in both option orders. No store and no path still fails.
Actual-store engine and capability refusals remain covered by the existing
attachment matrix; this is not a Pebble checkpoint fallback.

## Combined historical checkpoint and crash recovery

Verified to the stated fixture coverage by
`TestLedgerHistoricalMigrationCombinedCrashes`. The producer executes eb63f1b5
itself, retaining two committed grant pages and two pending continuations. A copy
is completed by that SDK as an independent reference. Current migration must not
refetch the old completed phases or committed grant pages. It compares record,
index and digest families plus normalized sync stats with the old completed file;
all normalized ledger counters are compared with an uninterrupted new migration.
Only timestamps and durations are normalized here, not call counts or errors.

The product is 1/4 workers × WAL/flush images × six individual takeover/grant/
terminal cuts and one three-cut chain: 28 cases, 36 process exits per repetition.
Three ordinary repetitions passed in10.633s; one race repetition passed in12.171s.
Dropping the imported completed-action count caused expected15 vs actual3; the
mutation was reverted and the full product rerun. Independent review found no
blocking flaw in the test or producer. Full sync passed in105.652s. Attachment
and engine-refusal race checks passed three repetitions in3.041s.

This adds combined evidence to C04, C16, C24–C28 and C31 without closing their
original exhaustive products. Seal/archive crash cuts remain separate tests.
The fixture excludes expansion, external import, source-cache replay and session
records; data equality is logical selected-family equality, not file-byte equality.
Explicit saved-ID binding avoids the historical producer's test-clock age policy.
The historical build remains opt-in rather than adding another SDK build to CI.

## Final automated-review dispositions

- Restored nil-store/path fallback (CO-032). Empty/unknown engine and missing
  Pebble ledger capability refusals are required by C01/CO-008, not fallback bugs.
- Removed the inaccurate rawdb comment about assets never being batched and the
  redundant assertion about mutating a returned value copy.
- `sdk.Version` is maintained by the release workflow. Custom-store capability
  requirements and the accepted SDK downgrade constraint are release notes in
  the PR description; no manually chosen SDK version is introduced here.
- Commit publication stays inside scheduler transition ordering. Moving it out
  changes concurrency behavior; the measured collection cost is accepted.
- Context assertions and resource-type counter initialization currently follow
  their established construction order; no reproduced loss/panic was found.
  Per-page asset staging is required by atomic page writes. No speculative
  refactor or new payload-size policy is added.
- Allocation checks enforce scaling with child count, not an absolute allocation
  count across Go versions. Cost drivers remain opt-in. Operation-name literals
  and empty-token hashing remain unchanged; neither review supplied a current
  incorrect result. Expansion timing remains outside the collection report.

Installed golangci-lint2.9.0 reports six pre-existing G115 findings and no new
formatting findings; current-PR CI with its configured linter passed at5d35ecd2.
Final revised-head CI must be checked separately.

CI follow-up: golangci-lint2.13.2 flagged the historical fixture's environment-
supplied `os.ReadFile` path (G703), which local2.9.0 did not report. The test now
uses the existing directory-scoped `os.OpenRoot` read pattern. On CI's merge
423bcbb5 plus this correction, Go1.27.1/golangci-lint2.13.2 reports zero issues
for sync/storage, and the combined migration test passes in4.391s. No production
code changed in this correction.

## CO-033 — service-mode SDK rollback

Verified to the bounded fixture coverage by `TestLedgerServiceModeRollback`.
The child uses public NewConnectorRunner in daemon mode, its actual connector
subprocess wrapper, C1 task manager, heartbeat loop and streaming upload client.
Only endpoint resource data and C1's OAuth/task API are simulated. Each process
must authenticate, send Hello, heartbeat, upload usable Pebble data and finish;
the API's return to polling is awaited before stopping a completed daemon.

Rollback targets bba86699 (v0.30.1) and eb63f1b5 are built with their unchanged
production sources and the same child fixture. The product is two old SDKs ×
single/batched polling × spare off/on × process kill/reported error: 16 cases,
repeated three times. The persistent directory, endpoint, credentials and options
are unchanged across processes. Each old daemon completes the redelivered task
and another task without restarting; a new-SDK daemon then completes a roll-forward
task. Every successful upload is reopened and checked for a finished sync and the
two expected resource IDs/payloads. Ordinary error cases assert PermissionDenied,
retryable FinishTask, no upload and partial-file removal. Crash cases assert a
surviving initialized ledger queue. NoSync resource pages may be absent from that
crash image; the test does not require durability beyond the storage contract.

All 16 cases pass, including three ordinary repetitions. The current runner/API
harness passes race checks against v0.30.1 in22.53s; the old binary itself is not
race-instrumented. Full connectorrunner/c1api suites pass; golangci-lint2.13.2 on
connectorrunner reports zero issues. Independent review confirmed service-mode
fidelity and the success oracle; process cleanup was tightened to wait after kill.
The initial recursive test-subcommand dispatch prototype is excluded from evidence.

Limits: resource-only full sync, one resource type, two resource pages and one
worker. No grant/entitlement/expansion/external-import, upload-failure or arbitrary
crash-cut coverage is claimed here. C1's production queue/retry-budget behavior is
not exercised; the local API performs redelivery. This does not establish arbitrary
SDK-version compatibility or remove the separate C1 vendored-SDK downgrade
constraint for hosts reopening unfinished artifacts directly. No production SDK
behavior changed for this test.

Oracle qualification: in an isolated bba86699 build, suppressing the full-sync
handler's upload while leaving successful FinishTask intact makes the rollback
case fail on the missing upload. The mutation is not present in either passing
rollback binary or production sources. Failed-process cleanup also completed
without leftover connector subprocesses. The task API rejects the wrong polling
method, so the single/batched dimension is asserted, not just configured.

## CO-034 — expand an ended unexpanded upload

`TestLedgerUploadedUnexpandedSync` collects a real three-level group fixture with
WithDontExpandGrants, closes and copies the saved artifact, then reopens that copy
with the same sync ID and WithOnlyExpandGrants. It asserts exact base/expanded
grant IDs after another close/reopen, retained sync identity, and accumulated
expansion accounting. The host connector rejects recollection. The product is
1/4 workers × default/debug retention × normal seal/early EndSync/unfinished
empty queue. All four early-EndSync cases failed before the fix. Normal sealed
uploads passed already. The unfinished control seals its prior pass without
recollection and expands on a subsequent finished rebind.

Lifecycle selection now retains the existing bounded lookup's pending/nonempty
result. Ended-empty bindings initialize current work; pending bindings resume;
unfinished-empty bindings seal. ClearLedgerRows permits an empty declaration but
checks for pending work under its write lock. Its existing atomic clear removes
the old declaration with history, retaining records, facts and counters.

Extended ClearRows failure/crash products cover both normally sealed and early-
ended empty queues at stamped/staged/committed cuts. Before the fix, early-ended
cases failed at the old refusal. The tests assert queue declaration and history
change together while metadata and accounting survive. Pending-work refusal
remains covered. An intermediate nested-lock implementation was stopped and
replaced with the marker's locked helper; only subsequent runs count as evidence.

Full sync94.156s, Pebble25.260s, public dotc1z44.581s and compactor37.899s pass.
Focused lifecycle/expansion/clear race checks pass three repetitions (sync16.607s,
Pebble4.167s). These verify the stated lifecycle and artifact flow, not every
connector topology or original plan product. No expansion algorithm or scheduler
change is included. The separate retry-accounting finding is still outstanding.

Correction review caught the prepared-seal exception: an initialized seal-ready
queue must retry sealing even if ended_at is present. The new public cold-reopen
`TestLedgerPreparedSealSurvivesEarlyEnd` failed before adding seal-ready precedence
and checks terminal facts, retained-token policy, counters and absence of newly
seeded actions. This does not change the unexpanded-upload regression, which
halts before the terminal seal page. CI-merge source with Go1.27.1 and
lint2.13.2 passed the initial correction; the final guard is validated separately.

Final guard validation: full sync89.011s and compactor18.842s pass; focused
lifecycle/upload race checks pass three repetitions in14.704s; queue-clear crash
assertions pass race3x in1.900s. CI-merge Go1.27.1/lint2.13.2 reports zero issues
and the final lifecycle/upload tests pass. Independent follow-up found no new
issue and passed the focused tests in1.367s. The retry-accounting finding remains
separate and unfixed by CO-034.

## CO-035 — fulfill expansion after finishing a prior seal

The copied-upload product now includes prepared, prepared-then-ended and ordinary
prepared-recovery cases, in addition to sealed/early-ended/unfinished inputs:
24 cases across1/4workers and default/debug retention. The expansion-only call
must produce all6expected grants in one invocation. An ordinary recovery control
first seals without expanding; only its subsequent explicit expansion request
expands. The prepared/unfinished expansion-only cases failed on f8cc8663 before
the change. No file-level expanded-status flag was introduced.

An empty recovered queue plus an explicit enabled expansion-only request now
finishes the old seal/report, rebinds the same sync ID, then uses the normal Init
path with a fresh accounting attempt. The old request's saved options are not
replaced by the new request while finishing the old seal. No expansion-complete
graph, final success log or connector cleanup is published for the requested pass
before it runs. An explicit expansion-only call can redo deterministic expansion
that completed before interruption; tests account both actual executions rather
than silently skip the new request to preserve one-pass counts.

`TestLedgerExpansionRequestAcrossSealFailure` checks errors during the old seal
and during rebind: neither reports success or expands, base grants and retained
old policy remain, no completed graph is saved, and cold retry produces6grants.
The successful new pass resets diagnostic retention to its own options and saves
the completed graph. Ordinary discard recovery remains separately tested without
WithOnlyExpandGrants; it does not restart processing.

Full sync88.446s and compactor17.283s pass. Focused expansion/lifecycle race checks
pass3repetitions in30.435s; final boundary/control race checks pass3x in7.350s.
CI-merge Go1.27.1/golangci-lint2.13.2 reports zero issues; focused tests pass2.897s.
The scope is saved-artifact/request orchestration, not a new expansion algorithm
or scheduler. The independent retry-accounting issue remains outstanding.

The bounded independent review at82c27640 found no confirmed bug, checked error
propagation and the terminating handoff, and passed focused tests in2.949s.

### CO-036 — retry accounting correction

Status: verified to stated focused coverage (C22–C24/C43 correction, not closure of their full original products).

`TestLedgerConnectorCallTotalsIncludeRetriedCalls` reproduces two unavailable responses followed by success, then two more pages including a repeated request token. It uses the existing scheduler with worker limits 1/4 (one serial page chain), the guarded Pebble fixture and its write hook. Exact assertions cover connector counts/sums/maxima by method and type, session counts/errors/timeouts/sums/maxima, reported waits, ledger row attempts/errors, one completed action, absence of failed-attempt records/facts/ingest counters, and unchanged counters after close/reopen. No per-retry responses or callbacks are retained; aggregate maps are cleared on page commit or identity change.

Planted defect: retain pre-correction page-local observation ownership. The original three-attempt reproduction failed with one saved/live method call rather than three, 4ms rather than 12ms, and two session operations rather than six. The strengthened test also failed against the unmodified CI merge base. It passes with the correction. Failed page effects are still discarded; only connector observations cross a retry boundary. Wall-wait intervals are recorded when observed, while their accounting is published with the successful page.

Validation: full sync suite 107.962s; focused retry/accounting race selection three repetitions 4.549s; final close/reopen test under race three repetitions 1.300s; final test on the PR merge base plus correction, Go 1.27.1, 0.058s; golangci-lint 2.13.2 over sync/dotc1z, zero issues. The only change after the full suite was adding the close/reopen assertions, covered by the final race and merge-base runs. No storage implementation, SQLite path, scheduler ordering or extra durable write changed.

Limits: handler hook supplies deterministic call timings/annotations; this is not a network transport test. The four-worker setting exercises scheduler configuration, not four simultaneous chains. Calls from a page that never commits, or observations lost on process death before commit, remain best-effort and are not claimed as durable accounting. No new independent reviewer was used for this correction; the original independent reproduction supplies the failure oracle.

## CO-038 — coordinator-owned attempt writes, primitive removal

Status: implemented across 30565886, ec2ad81b, 8ad82edd, e5086896, d5ad8091, 38b64a80, 8c699a0b, f31d8749, d7bb92d0. Both primitive registries carry zero `remove:` entries. Structural review of the final code not yet run (pending CO-037).

Storage: `PutLedgerFacts` is one synced `RecordBatch` of `StageLedgerFactValue` under the write lock, in-flight stamped, bound-sync required, empty map or name refused. `TestLedgerPutFactsIsOneUnit` injects a record-commit failure and asserts neither fact lands; `TestLedgerPutFactsRequiresBoundSync` covers the unbound refusal. Registered in `commitPointRegistry` (`ledger.go:PutFacts`), `rawdbHookFailureCases`, and dirty-tracking coverage; the guard and chaos doubles wrap it.

Syncer: `putLedgerReportOptions` writes `first_options` (when absent) and `latest_options` once per attempt in `syncLedger`, after retention resolution and before `parallelSync`, with skip flags computed as `initialActions` does. `TestLedgerReportOptionsPrecedePages` asserts the facts exist before the first page commit and carry request flags; `TestLedgerOptionsSnapshotBeforeAnyPage` asserts an attempt that commits no page is still `latest_options`; `TestLedgerOptionsSnapshotFailureWritesNothing` injects a fact-write failure and asserts no option fact lands and the attempt fails; `TestLedgerDiscardPendingFinishedBindingDoesNotRestartProcessing` asserts the seal-finishing attempt is latest. `ClearIngestInvariantVerification` runs before `parallelSync` when work is pending. Deleted: `ledgerRuntime.prepareMu/beforePage/prepared/optionsRecorded/commitMu/facts/active/closing/mu`, `ledgerPage.reportOptions`, `ledgerAttempts.mu`, `ledgerRunAccounting` and its mutex, `Engine.sealCost` as a production field (now `testSeams.sealCost`). Per-worker counter slots: `ledgerRuntime.workers[i]` is touched only by the goroutine holding worker index i; `errLedgerWorkerIndex` refuses an out-of-range index. `runStats` gained attempt-scoped maps under its existing mutex; `mergeStepDuration` is cumulative-only.

Queue lock: `parallelActionQueue.commitUnlocked` releases `q.mu` around the durable commit on the ledger path and re-takes it for admission and Broadcast. `TestLedgerQueueCommitDoesNotHoldQueueLock` is a held/released pair: with the flag off, `next()` blocks for the commit's duration; with it on, `next()` completes during the commit. `TestLedgerCommitWakesWorkerWaitingForChildren`, `TestLedgerCommitAcrossSiblingAbort` (abort before store acceptance refuses the page; after it publishes), `TestLedgerCommitPublishesBeforeCancellation`. Queue audit C3 is now "no admission after abort" with an `auditCommitBegin` event; the prior C3 ("no commit after abort") was violated by `TestResumeAcrossChangedAnswersTerminates` once commits ran outside the lock, which is the ordering the new contract permits.

Defects the sequence found in itself, each with the test that caught it: routing republished connector waits through `addStepDuration` double-counted them in the run bucket (`TestRateLimitGateWaitsReachSyncStats`, 625 ms for 325 ms; fixed in 8c699a0b with `TestRunStatsMergedDurationsStayOutOfAttemptBucket`); the crash-cut fixture wrote its marker from two workers once commits left `q.mu` (`TestLedgerPublicCrashResume/version--1/wal/workers-4/resource-after` under race; fixed in d7bb92d0, fixture-only). A `getAction(slow.ID)` lookup in the new queue tests was vacuous because `seedLedgerTestRun` reassigns action IDs; replaced by `runActionByIdentity`.

Red-first coverage is partial. The held/released pair and the two injected-failure tests carry their own planted case. The option-snapshot tests were not run against the pre-CO-038 code: they call `putLedgerReportOptions`, which did not exist, so a red run needs a reimplementation of the old per-page ordering rather than a revert. The sibling-abort and wake tests demonstrate an ordering the old code could not reach (commit under `q.mu`), not a defect it had.

Validation: focused crash-resume suite under race 53.959s after d7bb92d0; full race sweep of `pkg/sync` at f31d8749 failed only on the fixture race above. Full suites ran with CO-037 commit 1's phase byte already applied on top of d7bb92d0 (the two full-suite attempts at d7bb92d0 alone hit Go's 10-minute default while another session's fsync-heavy suites shared the disk, with a 2-second test the only one running; that is throughput, not a hang): `pkg/sync` 486.542s, `pkg/dotc1z` 504.874s, `pkg/dotc1z/engine/pebble` 337.655s, `pkg/synccompactor` 534.090s, `pkg/connectorrunner` 2.481s, all under the same contention; the 21 other packages passed before that run was stopped. golangci-lint over `pkg/sync` and `pkg/dotc1z/...` at d7bb92d0: zero issues.

Limits: the two in-memory seal guards (`active` blocks the terminal page, `closing` refuses a late page) were deleted here and are not re-established until CO-037's terminal transition validates them at the engine; between the two, a late page after the seal is caught only by the existing revision check. `ledgerRuntime.workers` slot ownership is a convention asserted by `TestLedgerWaitObservationsStayWithWorker` and the race sweep, not by a type.

## CO-037 — one durable authority for the pass lifecycle

Status: commits 1–4 landed (ba0d8b92, 111af384, f18b11ce, 10cb8c72). Commit 5 (cross-version artifact tooling, test e) deferred to a follow-up by the requester. Suite verdicts at 10cb8c72: full `go test ./...` green in 63 of 64 packages; the one failure, `TestC1ZConcurrentClose` (`pkg/dotc1z`, SQLite file path, "WAL file not empty after close" after 41.8 s under a disk-contended run), passes alone in 8 s, and no SQLite file is in this sequence's diff. The race sweep was dropped by the requester for cost.

The two defects the change order exists for, each with a red-first oracle:

Defect 1, crash between `FinishSync` and the marker-clear left a finished sync with a declaration and the discard fact, so a rebind re-sealed. `endSyncFinalize` now commits one stamp batch (archive, the family's remaining keys, `ended_at`) and writes nothing after `FinishSync`. `TestLedgerSealWritesNothingAfterStamp` counts record batches after the pre-flush hook: one on the prior seal in both modes, zero now; verified by stashing the three production files.

Defect 2, a drained pass on a finished sync classified as "process finished" and recollected under the same ID. The classifier now reads the declaration's phase and reads `ended_at` only when there is no declaration. `TestLedgerDrainedPassSealsWithoutRecollection` rebinds an early-ended drained sync with a connector that refuses every list call: at 111af384 it fails with "expansion replay must not recollect resource types"; at f18b11ce it seals. The earlier exposure was masked because every existing rebind test requested expansion only, whose `Init` enqueues no collection.

Storage: work-state `{version 2, lastID, phase}`; version 1 rejected (`TestLedgerWorkStateRejectsForeignEncodings`). `SetTerminal` validated at `pageUnit.Commit` under `writeMu` before staging: collecting, empty pending range, no work/continuation/children (`TestLedgerTerminalTransitionRefusesUntilDrained`, `TestLedgerTerminalTransitionIsOneBatch`); any page after sealing refused with `ErrLedgerQueueSealing` (`TestLedgerSealingRefusesLatePages`); each guard removed once and its test failed. `endSync` requires phase sealing, or `ended_at` with no declaration. `BeginPass` replaces `ClearLedgerRows` and `RestoreLedgerArchive` in one batch (`ledger_begin_pass_test.go`: history preserved, refusals, staged and commit failure cuts, crash image, purge deferral, empty-family import; a retained seal's buckets are not imported twice). `BeginCollecting` refuses a finished sync. Commit sites registered: `ledger.go:BeginPass` with `ledgerBeginPassHook`; `ledger.go:ClearRows` and `ledger_archive.go:RestoreLedgerArchive` removed with their sites.

Syncer: `ledgerResume.phase`; `preparation` switches on it; `sync.seal_ready` fact removed; `prepareSeal` returns early on sealing; `restoreLedgerState`/`refreshPendingWindow` require a declaration; `prepareLedgerState` reads no facts before classifying. `PageLedgerStore` loses `ClearLedgerRows`, `RestoreLedgerArchive`, `GenerateLedgerReport`, `GetArchivedLedgerOptions`; gains `BeginPass` and `PageWriter.SetTerminal`.

Departures from the frozen text, each recorded in implementation.md: the archive also rides the disposal batch in default mode (a crash between the two batches would otherwise leave no rows to report from); two classifier lines landed in commit 3 because commit 3's storage change exposed them; `prepareSeal` gained `SetTerminal` in commit 2 so the syncer's seal met the new precondition. Test (a) is covered by composition (sealing-image classifier tests plus the engine's mid-seal image tests) rather than a new syncer test; constructing the mid-seal image from `pkg/sync` needs an exported seal hook, not added.

Deleted tests whose subject no longer exists: `TestLedgerDiscardUnfinishedArchiveResumesWithoutCollection` (image unreachable), `ledger_continue_test.go` (`ClearRows`; assertions carried into `ledger_begin_pass_test.go`). Test-only helpers `commitTerminalPage`/`sealWithStats` (engine) and `commitTerminalPage`/`sealLedger` (dotc1z, synccompactor) supply the terminal page production commits through `prepareSeal`.

Limits: no released artifact carries a version-1 work state, so the rejection is a guard, not a migration; the cross-version tooling that would exercise old-host reads of new-host files is the deferred commit 5. Retained mode keeps the frontier (with the legacy token's verbatim state) through the seal, as before this change; not in scope.

## CO-039 — the pass is a state machine; writes are transitions

Status: implemented across c6cbecec (test h, red), 6dc063cc (test b, red), 1993c049 (storage + test a), 794476e6 (syncer), 4db3b17d (interface names). Facts read of the draft by a second reader: pending at the time of writing. Full run at 4db3b17d: `go test ./...` green in all 64 packages. 9c537f7b (no `supports_diff` stamp on the ledger path, requester's call) followed, covered by the state table and the expansion resume tests.

Tests written first, red at the head before their fix, each with the failure the freeze predicted:
- (h) `TestLedgerExpansionOnlyRefusesIncompleteCollection`: `main`'s behavior surfaced as "expansion replay must not recollect grants" — collection continued into the refusing connector. `TestLedgerDontExpandRefusesPassInExpansion`: `nil` — the partial expansion sealed. `TestLedgerExpansionOnlyExpandsFinishedBaselineUpload` green before and after: the C1 path (records, empty-stack legacy token, `ended_at`, only-expand through a connector that refuses every list call).
- (b) `TestLedgerResumeWithoutWorkWritesNoFacts`: five in-process images × seven configurations; red on every `sealing` cell but only-expand (which begins a pass); 34 cells green before. Oracle: a resume that committed no page, completed no action and began no pass leaves the fact family unchanged. The first oracle (phase and row count only) could not see a full pass that ended where it began; completed actions were added.
- (a) `TestLedgerStateTable`: 45 state × event rows from the plan's table. Removing the Expanding page refusal fails exactly `expanding/page`. `TestLedgerBeginExpandingIsOneUnit`: hook and record-commit failures leave Collecting; the guard refuses a queue holding more than the expansion entry.
- (c) `TestLedgerArchiveLinkFollowsThePassNotTheOptions`: red on the options-fact rule (a plain resumer's options dropped the link), green on the follow-on fact.

Storage: `LedgerQueueExpanding` (encoded 3, Sealing stays 2). `BeginExpanding` is one synced write under the write lock with the guard "exactly the expansion entry pending"; not idempotent. Page commits refused in Expanding and Sealing with `ErrLedgerQueuePhase`; the terminal page is the one page Expanding accepts. `BeginPass` stages `c1z.pass.follow_on`; the archive links from it. Registered: `pending_work.go:BeginExpanding` with `ledgerBeginExpandingHook`.

Syncer: `prepareLedgerState` returns the resume phase and refuses conflicts first — `dontExpandGrants` into Expanding; `onlyExpandGrants` with a collection entry queued, with `Init` alone on an unfinished sync, or with nothing under a caller-supplied sync ID. Options are written only when the resume phase is not Sealing. At the expansion pickup, ledger mode reads "resuming" from the phase; the token path keeps the graph heuristic. `supports_diff` is still stamped (`rollback-expansion` reads it on the SQLite path; the sanitizer and `ToPebble` copy it).

Interface: `PageLedgerStore` = `LedgerLifecycle` (`State`, `BeginCollecting`, `BeginCollectingFromToken`, `BeginExpanding`, `Seal`, `BeginPass`; `PageWriter.SetTerminal` for the page-borne transition) + `LedgerQueue` + `LedgerAccounting` + `LedgerArchive`. Rename only.

Corrections the facts read of my own draft made before code: expansion commits no ledger pages in ledger mode (`runPendingLocalStep`), so the transition is a lifecycle write, not a page flag; a finished baseline upload's empty token stack seeds `Init`, so "drained" was the wrong consistency condition and "no collection entry queued" replaced it; a fixture with only the expansion entry pending is a completed collection, not an incomplete one. Each would have been a wrong refusal in production.

Limits: the flag rule reads queued ops, not rows; a pass whose collection entries were all completed but whose terminal page never landed is "drained" and consistent with only-expand by design. `ledgerContinuationSyncer` lost an incidental only-expand flag; tests that needed the flag's behavior are (h). No race sweep (requester's call).


Independent reviews, code only, no plan context, at 2be28f3d's parents:
- Sol: a legacy checkpoint left mid-expansion was taken over as Collecting, so `--dont-expand-grants` on it would have continued as unfinished collection. Fixed in 0ea87a44: `legacyStackPhase` maps a stack of exactly `[expansion]` to Expanding; `legacyTokenFlagConflict` refuses before the takeover write. Red first: `TestLedgerExpansionOnlyRefusesLegacyCheckpointBeforeTakeover`, `TestLedgerLegacyCheckpointInExpansionIsTakenOverAsExpanding`.
- Grok: `BeginPass` keeps the seal's `ended_at`, and `StartOrResumeSync("")` resumes only `ended_at == nil`, so an interrupted follow-on pass followed by an empty-id `Sync()` starts a new sync and `StartNewSync` resets the file. Accurate; not fixed. The fix (resume on a live declaration) failed `TestLedgerEarlyEndPreservesRecovery` and `TestLedgerEarlyEndPublicRecoveryAndReset`: C1's entry is `StartOrResumeSync("")` → plain `EndSync` → `Cleanup` → `StartOrResumeSync("")` expecting a new sync, and CO-031 leaves that early-ended pass with the same finished-plus-declaration state. Reach: follow-on passes exist only through `WithSyncID`, and C1 resumes them by ID. 5f467df3 adds a warning in `startNewSync` when the discarded file held an open pass.

Full run at f2205ce8: `go test ./...` green except `pkg/dotc1z`, `pkg/sync`, `pkg/synccompactor`, which hit the 10m default per-package timeout under parallel load (tests in progress were 0–1s in). Rerun serially with `-p 1 -timeout 40m`: dotc1z 140.6s, sync 497.2s, synccompactor 249.2s, all pass. `TestC1ZConcurrentClose` failed once in the parallel run (SQLite `WAL file not empty after close`); the branch's only change to `c1file.go` is a nolint directive, and it passed serially.

§8, flags belong to phases (three no-context reviews at 6b3b1358; Sol found the first cell, the rest came from tracing it):
- Defect: `BeginPass` restores every archived fact but the two disposal facts, and `initialActions` read `cfg || fact`, so a finished skip pass rebound for a full sync planned no entitlements or grants and sealed as complete. Same on `main`. Reachable only by hand: the compactor and `rollback-expansion` rebind with only-expand.
- Ruling (requester): a phase runs under the flags it started with; collection happens once per sync ID; expansion invocations know nothing about collection and their collection flags are not read.
- Tests first, red at 6b3b1358: `TestLedgerFinishedSyncRefusesCollection` (both rebinds succeeded and skipped), `TestLedgerCollectionFlagsLockedMidCollection` (all four flag changes resumed silently), `TestLedgerExpansionOnlyIgnoresCollectionFlags` (the invocation's skip flags suppressed expansion and its target was collected through the refusing connector). Converted from the old behavior: `TestLedgerFinishedProcessingResumesWithoutReset`, `TestLedgerFinishedLegacyFrontierKeepsPendingWork/empty`, `TestLedgerFinishedRetentionUsesCurrentOptions`, `TestLedgerEmptyStartQuality/finished` rebind with only-expand; `TestRebindingSealedSyncClearsStaleVerification`'s Pebble arm is the compactor's shape, halted after expansion wrote; `initialActionCases` gains `expand-ignores-targets` and `expand-ignores-skip`.
- Syncer: `prepareLedgerState` refuses `ledgerProcessFinished` without only-expand; `flagConflict`'s Collecting case refuses a finished sync at the `Init` seed without only-expand and, with collection work queued, compares the request's collection flags against `first_report_options` (`collectionFlagConflict`, `collectionFlagDifferences`); `legacyTokenFlagConflict` refuses a finished token with nothing but `Init` queued before the takeover. `initialActions` under only-expand reads the skip facts, records no skip fact and drops targets. The fact set read by the check is handed to `restoreLedgerStateWithFacts`, so the resume still reads facts once (`TestLedgerStartupLoadsCountersOnce`).
- Limits: the lock compares only what `first_report_options` recorded; an attempt that finds none continues. `syncResourceTypes` is still read from cfg on every page; the lock refuses a changed selection before any page, which is the only place it could differ.
- Structural-coverage triage at beaf7ffc found three refusal branches that fired during development but had no asserting test after the converted tests were given only-expand: the finished-at-`Init`-seed refusal in `flagConflict`, its legacy-token twin in `legacyTokenFlagConflict`, and the three external-source fields in `collectionFlagDifferences`. Added `TestLedgerFinishedSyncAtInitSeedRefusesCollection` (declaration, legacy empty stack, legacy `Init` alone), three external subcases in `TestLedgerCollectionFlagsLockedMidCollection` (every subcase now also asserts the named field), and `TestLedgerLegacyTakeoverArmsCollectionFlagLock` (the one-resume gap on a legacy takeover, then refusal on the next). Each subcase fails with its branch replaced by `if false` and passes on the real code.
- Review at 75b38cd6 (`ledger_lifecycle.go:216`): an expansion pass with an external source that crashed inside the import could not be resumed. `BeginPass` carries the collection's `first_report_options` into the pass, and `SyncExternalResourcesOp` counted as collection work, so only-expand was refused as mid-collection and a flagless resumer was refused for differing from the collection's flags. The import calls no connector (`SyncExternalResources*` read `externalResourceReader` and the store), so both scans now class it with the expansion step. `TestLedgerExpansionPassWithExternalImportResumes` reproduced both refusals before the change and finishes the pass after it. Reach before the fix: none; no only-expand caller configures an external source.
- Review at 75b38cd6 (`syncer.go:3891`): `setStore` failed `NewSyncer` for `Engine == ""` and for unknown engines, against `connectorstore.StoreMetadata`'s contract that `""` is a store not backed by a c1z and unknown values are not switched on. CO-040: both now take the token path; a Pebble store without `PageLedgerStore`, and a non-Pebble store with one, are still refused. `TestNewSyncerStoreEngineRouting`; the `""` and `other` without-ledger cells of the two attachment tables flip to attach.
- Review at 75b38cd6 (`initial_actions.go:19`): `initialActions` is shared with the token path, so the only-expand planning change applies to SQLite as well. Kept: the rule is about phases, not engines. `TestInitialActionBaseline` already runs every `initialActionCases` entry with `ledgered = false`, including `expand-ignores-targets` and `expand-ignores-skip`.
- Review at b6834d01 (`adapter.go:497`, CO-041): the in-flight stamp was cleared in its own synced write before the seal batch. A crash in that window left a v2 stamp over an unfinished sync with no token: a token-only SDK resumed it from `Init` over the sealed data, and this SDK then refused the file. The stamp now rides the seal batch and the Drop batch, and is set in the first ledger batch rather than before it. `TestLedgerDiscardDurableSealCuts/before-ended` was red before the change. Reach before the fix: a crash between two synced commits plus a downgrade.
