# Syncer on the page ledger: verification evidence

Plan frozen at `01931d8b`; calibration `d9277866`; implementation brief
`644c26cf`. Execution is in progress. Candidate names in the brief are not
passing evidence. The per-criterion index remains conservative: passing mechanism
tests do not close unexecuted coverage-product cells. The [archived execution log](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/evidence.md)
preserves dated results, including superseded implementations. Raw filenames and
historical section references in this index refer to that archive; see
[artifact index](README.md). No criterion is promoted by this condensation.

## Equality normalizations (CO-005)

The canonical cross-run comparison may normalize Attempt, CommittedAt,
TakenOverAt, page/connector/wait durations, retry counts and waits. It may
not remove records, indexes, digest, facts, completion or other committed
accounting. Each normalized measurement is still checked independently by
O5. Record raw artifact digests alongside canonical results without claiming
byte equality. A returned fresh NoSync commit need not survive a crash.

## Instrument coverage and gaps

`tools/cells.py` emits stable cell IDs for P1–P10, CO-002 and C49. CO-021 removes
expansion from the page products: P1 now has 5,400 cells and P2 has 330. It records
required cells, not executed cells. P4's repeated resumes are mandatory
subcases. Additional feature crosses specified by individual criteria still
need fixtures; the generated products are not the entire coverage model.

The strict fixture and companion capability recorder cover page writes, lifecycle
mutators and session writes. Public chaos fixtures install the engine write hook.
Raw snapshots, logical canonical comparisons, process-crash tests and durable-only
VFS crash images have executed; their bounded coverage is recorded in the archived execution log. The
full product-to-executed-test manifest remains incomplete. This is not closure
of C10, C37, C38 or C47 over all required cells.

## Per-criterion record

### C01

- Status: verified to stated coverage.
- Tests run: TestLedgerPublicEngineAttachment; TestLedgerPublicRegisteredPathAttachment; TestLedgerPublicPathAttachment; TestStoreCapsEngineMatrix.
- Coverage: all 16 P3 engine/capability × injected/path cells, including empty metadata refusal. Both entry routes assert unchanged raw keys, zero attempted writes and no connector calls. Refused path attachment closes the returned store once. The two built-in drivers are also exercised directly.
- Planted defects: removing the Pebble capability requirement fails both injected and registered path cases. Adding a connector Validate call during construction fails injected, built-in path and registered path fixtures. All mutations are removed.
- Cell inventory: [attachment cell inventory](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/attachment-coverage.json). Third-party driver selection uses the public registry; no built-in driver is replaced and no fixture store is retained in global registration state.
- Green command/revision: attachment execution entry in the archived execution log.
- Not covered: subsequent Sync entry/resume/stop decisions belong to C02; this closure is attachment only.

### C02

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C02 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C03

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C03 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C04

- Status: evidence incomplete.
- Tests run: TestLedgerRuntimeCrashProcess; TestLedgerPublicCrashResume.
- Coverage: runtime process cuts plus public resource-page/terminal cuts with one/four workers, ordinary WAL recovery and explicitly flushed committed history. Public recovery compares transported raw keys and exact final resource identities/payloads.
- Planted defect: missing-resource-action walk mutation fails the public fixture; this is not a separate torn-record/index mutation.
- Green command/revision: K2b and public process-recovery entries in the archived execution log.
- Not covered: physical WAL-loss cuts, the other handler families, complete logical/index differentials and full P1/P6 products.

### C05

- Status: evidence incomplete.
- Candidate: TestLedgerPageRequiresTransition; TestLedgerPageRejectsDuplicateTransition.
- Required coverage: plan C05 and applicable calibration entries.
- Planted defect: removed the exactly-one-transition guard: Init and list-resources fixtures failed; guard restored.
- Green command/revision: K2a execution entry in the archived execution log; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C06

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C06 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C07

- Status: evidence incomplete.
- Candidate: TestPageWriterAssetCommitFailureAndRetry; TestLedgerAssetPageDiscardKeepsPriorValue; TestLedgerAssetPageSurvivesReopen.
- Bounded evidence: K4a asset overwrite/discard and input-buffer snapshot checks in the archived execution log.
- Planted defect: retaining the caller's asset buffer changed committed bytes; the consumer test failed, then passed after restoration.
- Not covered: the criterion's full resource/entitlement staged-read and secondary-index products; assets have no secondary indexes.

### C08

- Status: evidence incomplete.
- Tests run: TestLedgerExternalDeleteFullIdentity and the page grant-deletion fixtures recorded in the archived execution log.
- Coverage: full-identity external deletion preserves an unrelated grant with the same external ID. The O8 inventory finds only PageWriter.DeleteGrants calls in the ledger external handlers; the CO-002 bare-ID split case is unreachable on these paths.
- Planted defect: replacing full-identity deletion with bare-ID deletion fails the external fixture; restored, as recorded in the handler increment.
- Not covered: complete P6 put/delete-order and failure products; the source-cache storage deletion issue is outside this change.

### C09

- Status: evidence incomplete.
- Candidate: TestLedgerWalkIdentityFields; TestLedgerScheduleWalksNewlyDiscoveredChild.
- Required coverage: plan C09 and applicable calibration entries.
- Planted defect: scheduler dispatched a newly discovered committed child without looking up its row; fixture failed, then passed with incremental walk.
- Green command/revision: K2a execution entry in the archived execution log; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C10

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C10 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C11

- Status: evidence incomplete.
- Candidate: TestLedgerWalkRefusesScrubbedPaginationWithoutWrites; TestLedgerTerminalFailureDoesNotPublishProof.
- Required coverage: plan C11 and applicable calibration entries.
- Planted defect: identity comparison before scrub check treated a scrubbed paginated row as missing; diagnostic assertion failed, then passed with scrub check first.
- Green command/revision: K2a execution entry in the archived execution log; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C12

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C12 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C13

- Status: evidence incomplete.
- No criterion-specific mutant/green execution is recorded. All required
  C13 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

- Baseline audit: reproduced private-runtime contract difference; see [baseline audit](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/baseline-audit.md) and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table in the archived execution log.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C14

- Status: evidence incomplete.
- No criterion-specific mutant/green execution is recorded. All required
  C14 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

- Baseline audit: reproduced private-runtime contract difference; see [baseline audit](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/baseline-audit.md) and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table in the archived execution log.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C15

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C15 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C16

- Status: evidence incomplete.
- Candidate: TestLedgerResumeLogicalDifferential.
- Required coverage: plan C16 and calibration CO-005.
- Planted defect: subtracted one record observation from resumed worker candidates; canonical accounting comparison failed; defect removed.
- Green command/revision: K2b execution entry in the archived execution log.
- Not covered: physical WAL-loss cuts, all handler families, public Sync entry, complete mechanical products.

### C17

- Status: evidence incomplete.
- Candidate: TestLedgerInitialQualitySurvivesResume; TestLedgerInitialQualityCommitFailure; TestLedgerPageFailureDoesNotPublish;
  TestLedgerPageFactValueReadYourWrites; TestLedgerPageFailureDiscardsStagedObservations.
- Required coverage: plan C17 and applicable calibration entries.
- Defect evidence: Fresh quality was absent after Init/reopen; the new test failed before staging the fact. Fact/counter/commit failure cases pass without durable publication.
- Green command/revision: initial-quality execution entry in the archived execution log and earlier restoration entries.
- Not covered: public Sync routing, full mechanical products, all page-failure/crash images and final differential closure.

### C18

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C18 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C19

- Status: evidence incomplete.
- Candidate: TestLedgerInitialQualitySurvivesResume; TestLedgerSealPreservesUnknownIngestQuality; TestLedgerRestoreCheckpointFixtures.
- Required coverage: plan C19 and applicable calibration entries.
- Defect evidence: Missing fresh quality failed after reopen. An unconditional known-quality declaration failed the unknown-prior case; the mutation was removed.
- Green command/revision: initial-quality execution entry in the archived execution log and earlier restoration entries.
- Not covered: public Sync routing, full mechanical products, all page-failure/crash images and final differential closure.

### C20

- Status: evidence incomplete.
- Candidate: TestLedgerPageCumulativeWorkersAndAttempts; TestLedgerScheduleStopsAndJoinsOnError.
- Required coverage: plan C20 and applicable calibration entries.
- Planted defect: not run for this criterion; green mechanism tests only.
- Green command/revision: K2a execution entry in the archived execution log; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

- Baseline audit: reproduced private-runtime contract difference; see [baseline audit](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/baseline-audit.md) and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table in the archived execution log.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C21

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C21 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C22

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C22 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C23

- Status: evidence incomplete.
- No criterion-specific mutant/green execution is recorded. All required
  C23 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

- Baseline audit: reproduced private-runtime contract difference; see [baseline audit](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/baseline-audit.md) and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table in the archived execution log.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C24

- Status: evidence incomplete.
- Candidate: TestLedgerTakeoverLegacyFixtures; TestLedgerTakeoverV0CursorAndParentIdentity.
- Required coverage: plan C24 and applicable calibration entries.
- Planted defect: not run for full C24; fixture decoding and migration are green, crash/reopen matrix remains.
- Green command/revision: K2a execution entry in the archived execution log; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C25

- Status: evidence incomplete.
- Tests run: TestLedgerPublicCrashResume, legacy versions 0/1/2 × one/four workers × before/after takeover and first post-token commit.
- Coverage: before takeover the exact token remains and the frontier is absent; after takeover the token is empty and the exact frontier survives process exit and transport into a new envelope.
- Planted defect: loss of restored page token fails all three versions on a forbidden pre-token connector call; this does not independently qualify the storage batch's atomicity.
- Green command/revision: public legacy takeover execution entry in the archived execution log.
- Not covered: internal stamp/batch I/O failure cuts, every fact/counter family and complete P4 products.

### C26

- Status: evidence incomplete.
- Tests run: TestLedgerTakeoverLegacyFixtures; TestLedgerPublicCrashResume.
- Coverage: saved frontier re-decoding plus public process recovery after takeover with zero page rows, in versions 0/1/2 with one/four workers. Completed legacy pages are never requested again; post-token committed pages also cannot repeat.
- Planted defects: empty frontier restoration in the earlier fixture; erased restored page token in the public fixture. Both fail and are removed.
- Green command/revision: K2a entry and public legacy takeover entry in the archived execution log.
- Not covered: full repeated-resume/process/retention products, physical-loss images and complete differential closure.

### C27

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C27 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C28

- Status: evidence incomplete.
- Tests run: TestLedgerTakeoverCountersImportedOnlyWhenAbsent; TestLedgerPublicCrashResume.
- Coverage: existing-bucket import guard, plus exact public completed-action totals across V0/V1/V2 takeover and post-token process recovery.
- Planted defect: disabled existing-counter guard; migration added historical totals to an existing bucket set; exact-fold assertion failed; restored.
- Green command/revision: K2a execution entry in the archived execution log; disabled candidate is not green evidence.
- Not covered: every counter family across P4 × K × N, physical WAL-loss images and final differential closure.

### C29

- Status: evidence incomplete.
- Candidate: TestLedgerTakeoverRejectsInvalidStateBeforeConsumption; TestLedgerTakeoverRejectsConflictingAndEmptyFrontier.
- Required coverage: plan C29 and applicable calibration entries.
- Planted defect: not run for this criterion; green validation/unchanged-key fixtures only.
- Green command/revision: K2a execution entry in the archived execution log; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C30

- Status: evidence incomplete.
- Candidate: TestLedgerInitialQualitySurvivesResume/unknown-prior; TestLedgerRestoreCheckpointFixtures;
  TestLedgerTakeoverIngestQuality; TestLedgerTakeoverLegacyFixtures.
- Required coverage: plan C30 and applicable calibration entries.
- Defect evidence: An unconditional known-quality declaration changed unknown prior state and was rejected. Earlier restoration defects and their tests are recorded in the archived execution log.
- Green command/revision: initial-quality execution entry in the archived execution log and earlier restoration entries.
- Not covered: public Sync routing, full mechanical products, all page-failure/crash images and final differential closure.

### C31

- Status: evidence incomplete.
- Tests run: TestLedgerSealRequiresTerminalPage; TestLedgerTerminalPageAndSealStats; TestLedgerTerminalFailureDoesNotPublishProof; TestLedgerPublicSyncSealsWithoutToken; TestLedgerPublicSkipSync; TestLedgerPublicStopResume.
- Coverage: terminal proof, public sealing/stats, empty token, default archive/drop, and debug retention.
- Planted defects: no disposal and archive-error fallthrough fail public tests; unsynced archive fails the durable crash-image test. Terminal error cuts also pass.
- Green revision: 6c8e2209; detailed execution entries in the archived execution log.
- Not covered: complete P7 fault/crash product and final differential closure.

### C32

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C32 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C33

- Status: evidence incomplete.
- Tests run: TestLedgerFinishedProcessingResumesWithoutReset; TestLedgerFinishedLegacyFrontierKeepsPendingWork; TestLedgerPublicFinishedContinuationAfterDisposal.
- Coverage: CO-010 lifecycle, retained/restored history, preserved binding timestamps, pending legacy work and public expansion-only entry after disposal/reopen.
- Defect evidence: earlier private-runtime reset assumption was rejected and removed; missing archive restore fails its storage consumer. No new public-entry mutant is claimed.
- Green revision: 6c8e2209 full suite and focused lifecycle race runs.
- Not covered: all baseline caller modes and crash products.

### C34

- Status: evidence incomplete.
- Tests run: TestLedgerFinishedProcessingResumesWithoutReset; TestLedgerSealReadyUnfinishedDoesNotStartAnotherPass; TestLedgerArchiveDurableCrashImages (storage).
- Coverage: stop/reopen after clearing prior rows and after a later committed page; later progress is not reset; archive restore is atomic and idempotent across durable crash images.
- Planted defect: unsynced archive metadata fails its crash-image check. This does not replace the remaining reset-boundary mutants.
- Green revision: 6c8e2209 and the archive durability execution entry.
- Not covered: full before/during/after-reset product across every process identity.

### C35

- Status: evidence incomplete.
- Tests run: TestLedgerPublicDebugRetention; TestLedgerPublicStopResume; TestLedgerPublicTokenRetentionRequiresDebug; TestLedgerDebugReferenceChecksSurviveScrub (storage).
- Coverage: default disposal, debug scrub, explicit no-scrub, inherited durable retain declaration and token-free debug report examples.
- Planted defect: disabled reference validation fails the debug report check; this is not a scrub-erasure mutant.
- Green revision: 6c8e2209 and the effective-retention follow-up tests.
- Not covered: the syncer consumer's full four-location credential-needle crash product; storage-side erasure coverage remains separately recorded.

### C36

- Status: evidence incomplete.
- Tests run: TestLedgerExpansionFinishedReplay; TestLedgerExpansionPublicReplay.
- Coverage: nonempty expansion-only processing over the same finished sync ID,
  after retained/disposed ledger reopen; graph preservation and replay without
  connector collection calls. No CLI change (CO-006).
- Defect evidence: the former page wrapper fails the layer-capability guard;
  skipping replay fails the public resumed-layer fixture. Both are removed.
- Not covered: every option/caller combination and abrupt process-loss cuts.

### C37

- Status: evidence incomplete.
- Candidates run: TestLedgerWriteInstrument, TestLedgerWriteHookInstrument.
- Planted defect: removed the walk prohibition; both walk cases failed with
  a missing expected error. Restored guard passes. Details in the archived execution log.
- Required coverage: every new page/resume fixture and mutation-method inventory.
- Not covered: production walk/handlers, sub-store/session writes and full
  lifecycle recorder coverage; this is instrument validation only.

### C38

- Status: evidence incomplete.
- Candidate: TestLedgerAssetPageSurvivesReopen; TestLedgerCrashProcess with an asset in the page.
- Bounded evidence: K4a uses no asset bypass; the asset and row share the batch.
- Planted defect: direct Engine.PutAsset inside the page failed staged invisibility; the mutation was removed.
- Not covered: the inventory and before/after crash products for every retained auxiliary bypass; production handler integration.

### C39

- Status: evidence incomplete.
- Tests run: TestLedgerScheduleStopsAndJoinsOnError; TestLedgerPageFailureDiscardsStagedObservations; TestLedgerExistingSchedulerPreservesIndependentErrors.
- Coverage: joined workers, writer release, discarded observations and independent sibling errors through the retained scheduler; full sync and focused race suites pass.
- Historical failure: the private executor's independent-error handling differed from main. That executor was deleted; its failed audit remains historical evidence, not an outstanding failure in the current implementation.
- Planted defect: no new per-product mutation claim for the complete C39 matrix.
- Not covered: full error/process/crash products and final differential closure.

### C40

- Status: evidence incomplete.
- Tests run: TestLedgerPublicStopResume; TestLedgerPublicCancelledAfterWalkWritesNothing; TestLedgerRunAccountingDurationStop; page cancellation and scheduler error fixtures.
- Defect evidence: cancelling a public resume immediately after the walk attempted an empty counter-bucket write. The write-recorder assertion failed before the empty-snapshot guard; it passes after the guard.
- Coverage: no page calls or attempted store writes on that cancellation boundary; raw key/value equality; real duration/session accounting still flushes and repeated flushes do not double-count.
- Green command: focused public/run-accounting tests and race three times (4.122s); broad lint zero issues.
- Not covered: all retry/deadline/fatal outcomes across the complete worker/process product.

### C41

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C41 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C42

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C42 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C43

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C43 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C44

- Status: evidence incomplete.
- Audit: source-inventory.md at a3863c3d covers attachment, capability assertions, path predicates, immutable requested config and hook placement.
- Tests run: TestLedgerPublicEngineAttachment, TestLedgerDebugLoggingPreservesRequestedConfig, TestLedgerCanonicalOptionsPreserveFlags and capability/guard tests.
- Planted defect: the attachment guard and requested-config mutation evidence are recorded in their earlier increments; the source inventory itself has no omission mutant.
- Not covered: complete changed-branch inventory and independent final structural review.

### C45

- Status: evidence incomplete under CO-011's revised shared-change boundary.
- Audit: source-inventory.md records the shared Init, scheduler, completion-accounting and filter-observation changes. token.go and run_stats.go have no baseline diff; Checkpoint retains its token body after the ledger fork.
- Tests run: initial-action baseline, existing scheduler, token corpus and full sync suite.
- Planted defects: prior Init ordering, duplicate transition, replay accounting and scheduler warning/commit tests are recorded in the archived execution log; no claim of unchanged SQLite-executed source lines is made.
- Not covered: complete reachable call-graph audit and final independent review.

### C46

- Status: not assessed.
- No criterion-specific mutant/green execution is recorded. All required
  C46 cells and applicable calibration entries remain open; candidates are in
  the archived brief §5.

### C47

- Status: evidence incomplete.
- Candidates run: TestLedgerWriteInstrument and
  TestLedgerRawSnapshotDetectsValueMutation.
- Planted defects: removed walk prohibition; removed snapshot value comparison.
  Both produced assertion failures and were reverted; restored tests pass.
- Not covered: the other oracles, production page faults and per-criterion
  mutation adequacy. Instrument evidence is detailed in the archived execution log.

### C48

- Status: evidence incomplete.
- Instrument run: tools/coverage-summary.py; [statement-coverage output](https://github.com/ConductorOne/baton-sdk/blob/3b35887e02365b3b3f84ed1a54d2be8b622a0d4f/docs/verification/syncer-on-ledger/executed-coverage-d891604d.json) records full sync/Pebble tests and uncovered statement ranges in changed files.
- Instrument qualification: independent covered/uncovered profile fixture and rejection of incomplete result streams; this does not qualify branch or product coverage.
- Additional evidence: public crash and legacy takeover cases in the archived execution log, each with a rejected planted resume defect.
- Not covered: full changed-branch-to-criterion mapping, complete mechanical-cell execution manifest, final independent audit and seeded soak. Statement coverage is not closure of those obligations.

### C49

- Status: evidence incomplete.
- Runs: TestLedgerCostBaseline at eb63f1b5; TestLedgerCostPublic with production collection handlers/default disposal; earlier synthetic scheduler samples remain historical.
- Coverage: 60 public-path interleaved samples across eight configurations; three repetitions per arm in six configurations and one in the two ten-million-record configurations; tables, machine inputs and binary hashes in cost-public-smoke/, cost-current-machine-r1000/ and cost-current-machine-10million/. Separate CPU profiles accompany the first set.
- Defect evidence: the original machine recorder omitted artifact filesystem; its assertion failed before correction. Public samples assert output resource counts, archive presence and ledger disposal.
- Not covered: full matrix, baseline phase timing, encoded-byte decomposition, production-shaped estimate. Collection performance is accepted under CO-020;
  the latency measurements are requester-reported, not independently rerun here. CO-012 settles actual NoSync resume; CO-019 accepts the current shared machine.

### C50

- Status: evidence incomplete.
- Tests run: ledger report projection/scale/observations/options suites; TestLedgerPublicLogsSavedStats; TestLedgerPublicArchiveFailurePreservesRows; archive reopen/crash tests; debug reference tests.
- Coverage: mechanical saved/logged JSON, bounded groups/examples, requested/effective options, observations, default disposal and debug retention. The phase-duration projection is the current increment.
- Planted defects: disabled debug checks, omitted disposal, archive-error fallthrough and unsynced archive all fail their claimed checks; prior report mutations are recorded in the archived execution log.
- Green revision: 6c8e2209 for policy/report suites; subsequent projection results are recorded separately.
- Not covered: full C49 cost matrix and complete failure-product closure. The million-row run including phase projection is recorded in the archived execution log in report-phase-memory.txt; it measures the default report, not debug lookups or the whole sync.

## Latest validation

| Check | Result after CO-021; older checks labeled |
| --- | --- |
| Full sync suite | Pass, 81.728s |
| Attachment race checks, three runs | Pass, 2.502s |
| Public/expansion/lifecycle race checks, three runs | Pass, 33.683s |
| Final expansion race checks, three runs | Pass, 5.324s |
| Broad sync/dotc1z lint | Zero issues |
| Full expansion / Pebble / compactor suites | Pass, 26.225s / 21.345s / 37.392s |

C49's repeated ten-million-record single-worker comparison is 111.5s ledger
versus 81.0s token. The report took about 12ms; the measured storage overhead is
accepted for collection under CO-020. Current results and artifact links are in README.md.

The size cleanup relocates output and removes an obsolete benchmark, not tests
or coverage obligations. The frozen plan and criterion statuses are unchanged.

Cleanup validation: full sync passes (110.150s); broad lint reports zero
issues. Both benchmark binaries build. The revised runner verifies all six
small token/fresh/resumed × one/four-worker cases, including populated public
timing fields; the removed synthetic benchmark is absent from the binary.
These are harness checks, not new performance-acceptance measurements.
The archive download, original document blobs, unchanged frozen plan and all
50 criterion statuses were checked. Production Go has no diff from 0d87cd4c.

## Expansion boundary correction (CO-021)

The original wrapper fails TestLedgerExpansionUsesMainModel because the layer
capability is never called. The corrected dispatch uses main's handler and
adapter; the expander and original grant-storage implementation have no diff
from eb63f1b5. Expansion creates no page rows. Graph-less recovery replays the
phase; no token is written. Removed the PageWriter expanded-grant API and its
private staging/iterator implementation, along with obsolete batch-row fixtures.

TestLedgerExpansionPublicReplay leaves actual derived grants after an injected
layer failure, reopens the file, and compares the full returned grant protos
against a clean run. It also cuts before terminal proof and after terminal proof
but before seal. Counters include one expansion completion, only at terminal
proof. Graph preservation survives the seal restart. Skipped replay and an extra
completion-counter increment each fail the corresponding test; mutants removed.
TestLedgerExpansionFinishedReplay covers retained and disposed ledgers over a
finished sync, preserving binding metadata and making no collection calls.
Skip/read-error fixtures create no expansion page. Strict page hooks remain on.

These are error-injection and orderly close/reopen tests, not a new physical
power-loss matrix. Existing main replay/layer tests and full suites also pass; repository-wide
`go build ./...` passes.
The original expansion-page products are excluded by CO-021; other criteria's
unexecuted products remain incomplete. No new expansion performance percentage
is claimed: capability-use checks establish restoration of the optimized path.
