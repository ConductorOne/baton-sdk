# Syncer on the page ledger: verification evidence

Plan frozen at `01931d8b`; calibration `d9277866`; implementation brief
`644c26cf`. Execution is in progress. Candidate names in the brief are not
passing evidence. The per-criterion index remains conservative: passing mechanism
tests do not close unexecuted coverage-product cells. Dated/revisioned execution
entries below preserve earlier results, including superseded implementations.

## Equality normalizations (CO-005)

The canonical cross-run comparison may normalize Attempt, CommittedAt,
TakenOverAt, page/connector/wait durations, retry counts and waits. It may
not remove records, indexes, digest, facts, completion or other committed
accounting. Each normalized measurement is still checked independently by
O5. Record raw artifact digests alongside canonical results without claiming
byte equality. A returned fresh NoSync commit need not survive a crash.

## Instrument coverage and gaps

`tools/cells.py` emits stable cell IDs for P1–P10, CO-002 and C49. It records
required cells, not executed cells. P4's repeated resumes are mandatory
subcases. Additional feature crosses specified by individual criteria still
need fixtures; the generated products are not the entire coverage model.

The strict fixture and companion capability recorder cover page writes, lifecycle
mutators and session writes. Public chaos fixtures install the engine write hook.
Raw snapshots, logical canonical comparisons, process-crash tests and durable-only
VFS crash images have executed; their bounded coverage is recorded below. The
full product-to-executed-test manifest remains incomplete. This is not closure
of C10, C37, C38 or C47 over all required cells.

## Per-criterion record

### C01

- Status: evidence incomplete.
- Tests run: TestLedgerPublicEngineAttachment; TestStoreCapsEngineMatrix.
- Coverage: eight injected engine/capability combinations, including empty-engine refusal, unchanged key snapshots and zero attempted writes; explicit path attachment for both real engines.
- Planted defect: removing the Pebble capability requirement makes the missing-capability case return success and fail the test; restored.
- Green revision: 6c8e2209 full sync and ledger race runs.
- Not covered: formal P3 reachability accounting for engine/capability combinations that the built-in file factory cannot produce; explicit connector-call instrumentation at attachment.

### C02

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C02 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C03

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C03 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C04

- Status: evidence incomplete.
- Candidate: TestLedgerRuntimeCrashProcess.
- Required coverage: plan C04 and calibration CO-005.
- Planted defect: no separate torn-write mutant; six process-exit boundaries pass.
- Green command/revision: K2b execution entry below.
- Not covered: physical WAL-loss cuts, all handler families, public Sync entry, complete mechanical products.

### C05

- Status: evidence incomplete.
- Candidate: TestLedgerPageRequiresTransition; TestLedgerPageRejectsDuplicateTransition.
- Required coverage: plan C05 and applicable calibration entries.
- Planted defect: removed the exactly-one-transition guard: Init and list-resources fixtures failed; guard restored.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C06

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C06 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C07

- Status: evidence incomplete.
- Candidate: TestPageWriterAssetCommitFailureAndRetry; TestLedgerAssetPageDiscardKeepsPriorValue; TestLedgerAssetPageSurvivesReopen.
- Bounded evidence: K4a asset overwrite/discard and input-buffer snapshot checks below.
- Planted defect: retaining the caller's asset buffer changed committed bytes; the consumer test failed, then passed after restoration.
- Not covered: the criterion's full resource/entitlement staged-read and secondary-index products; assets have no secondary indexes.

### C08

- Status: evidence incomplete.
- Tests run: TestLedgerExternalDeleteFullIdentity and the page grant-deletion fixtures recorded below.
- Coverage: full-identity external deletion preserves an unrelated grant with the same external ID. The O8 inventory finds only PageWriter.DeleteGrants calls in the ledger external handlers; the CO-002 bare-ID split case is unreachable on these paths.
- Planted defect: replacing full-identity deletion with bare-ID deletion fails the external fixture; restored, as recorded in the handler increment.
- Not covered: complete P6 put/delete-order and failure products; the source-cache storage deletion issue is outside this change.

### C09

- Status: evidence incomplete.
- Candidate: TestLedgerWalkIdentityFields; TestLedgerScheduleWalksNewlyDiscoveredChild.
- Required coverage: plan C09 and applicable calibration entries.
- Planted defect: scheduler dispatched a newly discovered committed child without looking up its row; fixture failed, then passed with incremental walk.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C10

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C10 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C11

- Status: evidence incomplete.
- Candidate: TestLedgerWalkRefusesScrubbedPaginationWithoutWrites; TestLedgerTerminalFailureDoesNotPublishProof.
- Required coverage: plan C11 and applicable calibration entries.
- Planted defect: identity comparison before scrub check treated a scrubbed paginated row as missing; diagnostic assertion failed, then passed with scrub check first.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C12

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C12 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C13

- Status: evidence incomplete.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C13 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C14

- Status: evidence incomplete.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C14 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C15

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C15 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C16

- Status: evidence incomplete.
- Candidate: TestLedgerResumeLogicalDifferential.
- Required coverage: plan C16 and calibration CO-005.
- Planted defect: subtracted one record observation from resumed worker candidates; canonical accounting comparison failed; defect removed.
- Green command/revision: K2b execution entry below.
- Not covered: physical WAL-loss cuts, all handler families, public Sync entry, complete mechanical products.

### C17

- Status: evidence incomplete.
- Candidate: TestLedgerInitialQualitySurvivesResume; TestLedgerInitialQualityCommitFailure; TestLedgerPageFailureDoesNotPublish;
  TestLedgerPageFactValueReadYourWrites; TestLedgerPageFailureDiscardsStagedObservations.
- Required coverage: plan C17 and applicable calibration entries.
- Defect evidence: Fresh quality was absent after Init/reopen; the new test failed before staging the fact. Fact/counter/commit failure cases pass without durable publication.
- Green command/revision: initial-quality execution entry below and earlier restoration entries.
- Not covered: public Sync routing, full mechanical products, all page-failure/crash images and final differential closure.

### C18

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C18 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C19

- Status: evidence incomplete.
- Candidate: TestLedgerInitialQualitySurvivesResume; TestLedgerSealPreservesUnknownIngestQuality; TestLedgerRestoreCheckpointFixtures.
- Required coverage: plan C19 and applicable calibration entries.
- Defect evidence: Missing fresh quality failed after reopen. An unconditional known-quality declaration failed the unknown-prior case; the mutation was removed.
- Green command/revision: initial-quality execution entry below and earlier restoration entries.
- Not covered: public Sync routing, full mechanical products, all page-failure/crash images and final differential closure.

### C20

- Status: evidence incomplete.
- Candidate: TestLedgerPageCumulativeWorkersAndAttempts; TestLedgerScheduleStopsAndJoinsOnError.
- Required coverage: plan C20 and applicable calibration entries.
- Planted defect: not run for this criterion; green mechanism tests only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C21

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C21 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C22

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C22 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C23

- Status: evidence incomplete.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C23 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

- Current disposition: the failed private executor was removed. The shared-scheduler regression suite and public sync suite pass; see the public-routing entry. The remaining product cells and physical crash coverage are not closed by that passing suite.

### C24

- Status: evidence incomplete.
- Candidate: TestLedgerTakeoverLegacyFixtures; TestLedgerTakeoverV0CursorAndParentIdentity.
- Required coverage: plan C24 and applicable calibration entries.
- Planted defect: not run for full C24; fixture decoding and migration are green, crash/reopen matrix remains.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C25

- Status: evidence incomplete.
- Tests run: TestLedgerPublicCrashResume, legacy versions 0/1/2 × one/four workers × before/after takeover and first post-token commit.
- Coverage: before takeover the exact token remains and the frontier is absent; after takeover the token is empty and the exact frontier survives process exit and transport into a new envelope.
- Planted defect: loss of restored page token fails all three versions on a forbidden pre-token connector call; this does not independently qualify the storage batch's atomicity.
- Green command/revision: public legacy takeover execution entry below.
- Not covered: internal stamp/batch I/O failure cuts, every fact/counter family and complete P4 products.

### C26

- Status: evidence incomplete.
- Tests run: TestLedgerTakeoverLegacyFixtures; TestLedgerPublicCrashResume.
- Coverage: saved frontier re-decoding plus public process recovery after takeover with zero page rows, in versions 0/1/2 with one/four workers. Completed legacy pages are never requested again; post-token committed pages also cannot repeat.
- Planted defects: empty frontier restoration in the earlier fixture; erased restored page token in the public fixture. Both fail and are removed.
- Green command/revision: K2a entry and public legacy takeover entry below.
- Not covered: full repeated-resume/process/retention products, physical-loss images and complete differential closure.

### C27

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C27 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C28

- Status: evidence incomplete.
- Candidate: TestLedgerTakeoverCountersImportedOnlyWhenAbsent.
- Required coverage: plan C28 and applicable calibration entries.
- Planted defect: disabled existing-counter guard; migration added historical totals to an existing bucket set; exact-fold assertion failed; restored.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C29

- Status: evidence incomplete.
- Candidate: TestLedgerTakeoverRejectsInvalidStateBeforeConsumption; TestLedgerTakeoverRejectsConflictingAndEmptyFrontier.
- Required coverage: plan C29 and applicable calibration entries.
- Planted defect: not run for this criterion; green validation/unchanged-key fixtures only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C30

- Status: evidence incomplete.
- Candidate: TestLedgerInitialQualitySurvivesResume/unknown-prior; TestLedgerRestoreCheckpointFixtures;
  TestLedgerTakeoverIngestQuality; TestLedgerTakeoverLegacyFixtures.
- Required coverage: plan C30 and applicable calibration entries.
- Defect evidence: An unconditional known-quality declaration changed unknown prior state and was rejected. Earlier restoration defects and their tests are recorded below.
- Green command/revision: initial-quality execution entry below and earlier restoration entries.
- Not covered: public Sync routing, full mechanical products, all page-failure/crash images and final differential closure.

### C31

- Status: evidence incomplete.
- Tests run: TestLedgerSealRequiresTerminalPage; TestLedgerTerminalPageAndSealStats; TestLedgerTerminalFailureDoesNotPublishProof; TestLedgerPublicSyncSealsWithoutToken; TestLedgerPublicSkipSync; TestLedgerPublicStopResume.
- Coverage: terminal proof, public sealing/stats, empty token, default archive/drop, and debug retention.
- Planted defects: no disposal and archive-error fallthrough fail public tests; unsynced archive fails the durable crash-image test. Terminal error cuts also pass.
- Green revision: 6c8e2209; detailed execution entries below.
- Not covered: complete P7 fault/crash product and final differential closure.

### C32

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C32 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C36 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C37

- Status: evidence incomplete.
- Candidates run: TestLedgerWriteInstrument, TestLedgerWriteHookInstrument.
- Planted defect: removed the walk prohibition; both walk cases failed with
  a missing expected error. Restored guard passes. Details below.
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
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C41 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C42

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C42 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C43

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C43 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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
- Planted defects: prior Init ordering, duplicate transition, replay accounting and scheduler warning/commit tests are recorded below; no claim of unchanged SQLite-executed source lines is made.
- Not covered: complete reachable call-graph audit and final independent review.

### C46

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C46 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C47

- Status: evidence incomplete.
- Candidates run: TestLedgerWriteInstrument and
  TestLedgerRawSnapshotDetectsValueMutation.
- Planted defects: removed walk prohibition; removed snapshot value comparison.
  Both produced assertion failures and were reverted; restored tests pass.
- Not covered: the other oracles, production page faults and per-criterion
  mutation adequacy. Instrument evidence is detailed below.

### C48

- Status: evidence incomplete.
- Instrument run: tools/coverage-summary.py; executed-coverage-d891604d.json records full sync/Pebble tests and uncovered statement ranges in changed files.
- Instrument qualification: independent covered/uncovered profile fixture and rejection of incomplete result streams; this does not qualify branch or product coverage.
- Additional evidence: public crash and legacy takeover cases below, each with a rejected planted resume defect.
- Not covered: full changed-branch-to-criterion mapping, complete mechanical-cell execution manifest, final independent audit and seeded soak. Statement coverage is not closure of those obligations.

### C49

- Status: evidence incomplete.
- Runs: TestLedgerCostBaseline at eb63f1b5; TestLedgerCostPublic with production handlers/default disposal; earlier synthetic scheduler samples remain historical.
- Coverage: 60 public-path interleaved samples across eight configurations; three repetitions per arm in six configurations and one in the two ten-million-record configurations; tables, machine inputs and binary hashes in cost-public-smoke/, cost-current-machine-r1000/ and cost-current-machine-10million/. Separate CPU profiles accompany the first set.
- Defect evidence: the original machine recorder omitted artifact filesystem; its assertion failed before correction. Public samples assert output resource counts, archive presence and ledger disposal.
- Not covered: full matrix, baseline phase timing, encoded-byte decomposition, production-shaped estimate and acceptance. CO-012 settles actual NoSync resume; CO-019 accepts the current shared machine.

### C50

- Status: evidence incomplete.
- Tests run: ledger report projection/scale/observations/options suites; TestLedgerPublicLogsSavedStats; TestLedgerPublicArchiveFailurePreservesRows; archive reopen/crash tests; debug reference tests.
- Coverage: mechanical saved/logged JSON, bounded groups/examples, requested/effective options, observations, default disposal and debug retention. The phase-duration projection is the current increment.
- Planted defects: disabled debug checks, omitted disposal, archive-error fallthrough and unsynced archive all fail their claimed checks; prior report mutations are recorded below.
- Green revision: 6c8e2209 for policy/report suites; subsequent projection results are recorded separately.
- Not covered: full C49 cost matrix and complete failure-product closure. The million-row run including phase projection is recorded below in report-phase-memory.txt; it measures the default report, not debug lookups or the whole sync.

## K1 instrument execution

Revision: the commit introducing ledger_fixture_test.go and ledger_cost_test.go
(the commit containing this entry). Go 1.26.0, linux/arm64, vendored dependencies.

- `TestLedgerWriteInstrument`: missing page context, empty bypass, bypass
  during walk and context-less walk write are rejected before mutation.
  Each starts with a non-empty seed and compares every key/value afterward.
- `TestLedgerWriteHookInstrument`: bypassing the companion wrapper while
  retaining the page context still reaches the real engine hook and is refused.
- `TestLedgerRawSnapshotDetectsValueMutation`: equal key counts with a changed
  resource-type value differ under the raw oracle. Removing the value
  comparison made this test fail at the expected false assertion; reverted.
- Removing the walk prohibition made `TestLedgerWriteInstrument` fail in
  both walk cases with an expected error missing; reverted. These are oracle
  validation results, not proof of a production resume walk that does not
  exist yet.
- `TestLedgerSnapshotAfterReopen`: a page's resource type, fact, bucket and
  row retain an identical raw snapshot after closing and opening the saved
  c1z read-only. This is graceful save/reopen, not unsynced crash loss.
- `TestStoreCapsEngineMatrix`: the two added capability fields are present
  on Pebble and absent on SQLite. Engine-path validation is still future work.

Commands passed before commit:

```sh
GOTOOLCHAIN=go1.26.0 go test -mod=vendor ./pkg/sync -run '^TestLedger(WriteInstrument|WriteHookInstrument|RawSnapshotDetectsValueMutation|SnapshotAfterReopen)$|^TestStoreCapsEngineMatrix$' -count=1 -timeout 30m
GOTOOLCHAIN=go1.26.0 go build -mod=vendor ./pkg/sync ./pkg/synccompactor
GOTOOLCHAIN=go1.26.0 go vet -mod=vendor ./pkg/sync ./pkg/synccompactor
python3 docs/verification/syncer-on-ledger/tools/cells.py --summary
bash -n docs/verification/syncer-on-ledger/tools/build-baseline.sh
```

The two mutant commands used their respective test names with `-count=1` and
failed assertions, not compilation. No enabled mutant remains in the tree.

### Baseline smoke, not C49 evidence

`tools/build-baseline.sh` built a test executable in a detached eb63f1b5
worktree with only ledger_cost_test.go added. That executable passed
TestLedgerCostBaseline with 10 resource pages, 100 resources per page and
4 independent worker streams: 1,000 resources verified, checkpoint calls
observed, final c1z 38,259 bytes. Timing is deliberately not used as evidence
on this machine. The worktree is removed after build.

The baseline driver records pre-close WAL/flush/compaction bytes and Sync
wall time; final close-related writes, peak RSS, handler/commit and seal
breakdowns, and the two ledger arms still need instrumentation. C49 remains
incomplete. The full 18-configuration matrix is not run. machine.py records
CPU, disk and load inputs without host identity; it does not certify an
unloaded machine.

Final K1a checks also passed:

```sh
GOTOOLCHAIN=go1.26.0 go test -mod=vendor -race ./pkg/sync -run '^TestLedger(WriteInstrument|WriteHookInstrument|RawSnapshotDetectsValueMutation|SnapshotAfterReopen)$' -count=3 -timeout 30m
GOTOOLCHAIN=go1.26.0 golangci-lint run ./pkg/sync/...
```

Lint returned zero issues after adding the package-name suppression already
used elsewhere in pkg/sync to artifact_retention.go. No executable body in
that file changed. No pkg/dotc1z changes are included in K1a.

## K1b instrument execution

Revision: the commit introducing ledger_guard_test.go,
ledger_guard_coverage_test.go, ledger_canonical_test.go and
ledger_crash_process_test.go (the commit containing this entry).

- TestLedgerGuardMutationSurface probes every mutating method of Writer,
  PageLedgerStore, SessionStore, GrantStore, SyncMeta, FileOps and the
  page-reachable optional mutation capabilities. Read methods and the two
  memory-only PageLedger methods are explicitly classified. Probe arguments
  are deliberately minimal: guard rejection must precede input validation.
- TestLedgerGuardPreservesPebbleCapabilities found the wrapper had omitted
  WriteHookStore. Adding its forwarding fixed the test. All resolved Pebble
  capabilities are retained; no test silently loses the engine fast paths.
- TestLedgerSessionWriteGuard uses a real bound session and a seeded value.
  Removing the Set guard allowed the overwrite and failed the expected-error
  assertion. Restoring the guard passes with complete key/value equality.
  An earlier minimal-argument probe also failed on the mutant, but only
  reached session input validation; it is not the mutation premise evidence.
- Tracked page writers must commit successfully or be discarded before
  fixture cleanup; a page Commit during the walk is refused.
- TestLedgerCanonicalRowNormalization normalizes only row attempt/time and
  the permitted page timing fields. A changed written count or next cursor
  still differs. TestLedgerCanonicalRetainsOtherFamilies leaves every other
  record family unchanged and refuses unreadable row/frontier bytes. Full
  cross-attempt bucket folding and final sidecar comparison remain K2/K8 work.
- TestLedgerCrashProcess starts a fresh test process, stages a page, and exits
  with a distinctive status without Close, after staging or after Commit.
  The parent verifies the cut marker and reopens the leftover database with
  a new engine. Records, row, fact and bucket agree; staged-only is absent.
  This tests process death, not machine power loss. It does not claim sampled
  unsynced-WAL-loss coverage or final Sync resume equivalence.

Commands: `GOTOOLCHAIN=go1.26.0 go test -mod=vendor ./pkg/sync -run
'^TestLedger' -count=1 -timeout 30m`, the same expression under `-race
-count=3`, `go vet -mod=vendor ./pkg/sync ./pkg/synccompactor`, and
`golangci-lint run ./pkg/sync/...` pass with the stated toolchain. Lint
reports zero issues. No production code changes or pkg/dotc1z changes in K1b.
All criterion statuses remain as recorded; instrumentation is not product closure.

## K2a runtime execution

Revision: the commit introducing ledger_page.go, ledger_walk.go,
ledger_takeover.go, ledger_schedule.go and ledger_seal.go. The runtime is
private and not selected by any public sync path. SQLite bodies, token
parsing/encoding, old handlers, and pkg/dotc1z are unchanged.

Mechanism fixtures pass for: exactly-one transition, failed observation
isolation, cumulative buckets across workers/attempts, all-seven-field row
identity, scrubbed-row refusal, next/child reconstruction, diamond/cycle
drain, newly discovered committed children, worker stop/join, one-time
V0/V1/V2 takeover, conservative missing ingest quality, malformed-state
refusal, terminal staging failures, retain/scrub, and stats-reader step/call
handover. Every Pebble fixture uses the strict write hook and companion
recorder. Fault wrappers preserve the underlying tracked writer and discard
on non-commit exits. Staging failures before Commit do not simulate the
engine's first-commit stamp or physical failure inside its batch.

Red/green checks executed:

- Missing transition guard: Init and list-resources handlers returned
  success with staged records; both failed the expected-error assertion.
- Frontier replaced by empty input: legacy migration fixtures returned Init
  on later resumes; all affected action/graph expectations rejected it.
- Existing-counter guard disabled: migration inflated existing accounting;
  the exact-fold check rejected it.
- Scrub check after plain identity comparison: a scrubbed paginated row was
  returned as pending instead of refused; the diagnostic assertion failed.
- Newly discovered child dispatched without row lookup: a recorded child
  ran again; the fixture rejected the second handler invocation.

Each defect was removed and the focused ledger suite passed. These results
cover only the listed mechanisms, not every criterion that shares a test.
C47 remains incomplete. No runtime-dependent criterion is fully verified.

Actual failure retained: TestLedgerInterruptedFinishedRebindResumesNewRun
expects the new run's committed next page after a further binding. It gets
Init because ended_at was never cleared and DropLedger runs again. The test
is explicitly skipped pending the storage boundary in implementation.md §9;
its observed failure makes C33/C34 failed, not verified or silently omitted.

The initial race sweep passed three repetitions. Build and vet passed for
pkg/sync and pkg/synccompactor with Go 1.26.0 and vendored dependencies.
The final K2a gate results are recorded below after the last edits. Full
public Sync/race products, storage/compactor suites and final cost matrix
remain K2b–K8 work. The existing package-name lint suppression was added to
pebble_resync_same_file_test.go after revive reported its unchanged package
name; its executable test body did not change.

Final K2a gates passed:

```
GOTOOLCHAIN=go1.26.0 go test -mod=vendor ./pkg/sync -run '^TestLedger' -count=1 -timeout 30m
GOTOOLCHAIN=go1.26.0 go test -mod=vendor -race ./pkg/sync -run '^TestLedger' -count=3 -timeout 30m
GOTOOLCHAIN=go1.26.0 go build -mod=vendor ./pkg/sync ./pkg/synccompactor
GOTOOLCHAIN=go1.26.0 go vet -mod=vendor ./pkg/sync ./pkg/synccompactor
GOTOOLCHAIN=go1.26.0 golangci-lint run ./pkg/sync/...
git diff --check
```

The ledger-suite result includes the explicit interrupted-rebind skip named
above. Lint reports zero issues. No test result here closes that boundary.

## K2b crash and differential instruments

Revision: the commit adding ledger_runtime_crash_test.go and
ledger_differential_test.go. Six abrupt process exits cover an open handler,
a fully staged page before Commit, a returned page commit, a staged terminal
page, a returned terminal commit, and a completed seal. The parent reopens
the orphaned raw Pebble directory without invoking child cleanup. It asserts
whole records/facts/buckets/row transition, atomic terminal proof and
completion. This does not simulate lost unsynced WAL sectors or recover the
.c1z envelope through public Sync. No test claims returned NoSync commits
must survive a power-loss image.

The logical differential uses a common initial file and Go's controlled
clock. It compares every raw key/value family after only the approved
row/frontier normalizations and the semantic fold of counter buckets across
worker/attempt keys. The fold preserves all counter totals, OR flags, step
sums, call totals/errors/timeouts and maximum latency; it does not remove
accounting. discovered_at, started_at and ended_at are retained unchanged,
not normalized away. Raw artifact SHA-256 digests are logged separately.

The fixture has resource types, resources, entitlements and grants, two
concurrent streams, one/four workers, and failure before committing each of
three data-page positions. The interrupted arm closes/reopens its file and
reconstructs the remaining work. It is a controlled interruption fixture,
not a physical crash differential. Assets, external mutation, expansion,
public handlers, all process identities and large products remain untested.

A planted defect decremented the resumed worker's `records` observation
before committing its bucket. The canonical comparison failed at the folded
bucket, with 15 instead of 18 records in the first affected case. The defect
was removed; the differential passed three race repetitions. The canonical
instrument also rejects a same-key counter change with unchanged bucket
count and verifies sum/max/OR behavior independently of the runtime's fold.

The full pkg/sync suite passed at this stage in 75.494 seconds:
`GOTOOLCHAIN=go1.26.0 go test -mod=vendor ./pkg/sync/ -count=1 -timeout 30m`.
The ledger suite passed three race repetitions, including six subprocess
cuts per repetition. Build/vet passed for pkg/sync and pkg/synccompactor;
lint passed after removing a redundant interface declaration in the new
crash fixture. The interrupted-finished-rebind candidate remains skipped
and recorded failed; no boundary was silently declared settled.

The storage durability discrepancy in implementation.md §10 also remains
open: the source commits resumed pages NoSync, whereas storage-plan C11 and
calibration CO-009 describe Sync. No pkg/dotc1z code was changed to disguise
that discrepancy. C49 has no measured acceptance table yet.

## K3a cost-driver smoke

The opt-in TestLedgerCostRuntime used four workers, ten data pages and
100 resources/page. It verified 1,000 resources and twelve ledger commits
(init + ten data pages + terminal). One smoke invocation reported 8,920,670
ns Sync time, 3,057,073 ns inside Commit, 915,589 ns in handlers, 4,163,003 ns
seal time, 37,261 ns counter-fold time, 91,280 WAL bytes, 19,206 flush bytes,
zero compaction bytes before close and a 37,649-byte final artifact. These
are instrument sanity values on an unqualified machine, not a performance
claim, matched comparison, or completed C49 cell. Scrub time is null.

The machine snapshot reports sixteen visible CPUs but a four-CPU cgroup
quota and 32 GiB memory limit. The recorder now includes those limits.
An unloaded run must be demonstrated over the measurement interval; neither
this snapshot nor the smoke invocation establishes that condition.

Command:

```
BATON_LEDGER_COST=1 BATON_LEDGER_COST_WORKERS=4 GOTOOLCHAIN=go1.26.0 go test -mod=vendor ./pkg/sync -run '^TestLedgerCostRuntime$' -count=1 -timeout 5m -v
```

The runtime driver is unselected by default. C49 remains evidence incomplete;
there is no resumed Sync-per-page arm, separate scrub timer, full matrix,
production-shaped estimate or requester acceptance yet.

K3a build/vet and pkg/sync lint pass; lint reports zero issues. The smoke
also passes with one worker. The unchanged Pebble consumer prerequisites
passed with Go 1.26.0:

```
GOTOOLCHAIN=go1.26.0 go test -mod=vendor ./pkg/dotc1z/engine/pebble/ -run '^(TestBoundSyncRecordWritesDoNotSyncTheWAL|TestPageUnitCrashImageStoreEqualsLedger|TestLedgerTakeoverCrashImages)$' -count=1 -timeout 20m
```

This targeted run includes the executable bound-write NoSync premise and
storage takeover crash images. It does not replace the full engine suite
required before landing.

## K3b seal phase observation

C49 remains evidence incomplete. Engine.LastSealCost exposes ledger scrub
and purge durations from the last returned finalize attempt, including an
attempt that fails. This is observational pkg/dotc1z code: it does not
change the store contract, durable keys, write order or page durability.
The retain path reports zero for phases it skips.

TestSealCostIncludesFailedFinalize and TestLedgerSealCostConsumer both
failed when publication of the completed measurement was removed: each
observed zero scrub duration where positive duration was required. Restoring
the publication made both pass. The engine fixture injects the existing
end-stamp failure after scrub and purge; the consumer covers retain on/off
with the strict write recorder. These checks establish observation, not
C49 cost acceptance or new crash consistency coverage.

The full Pebble suite passed in 10.082 seconds with Go 1.26.0 and vendored
dependencies. The broader requested lint command reports six G115 findings:
three duration conversions in adapter_page.go and three integer conversions
in ledger_cost_bench_test.go. All six expressions exist at eb63f1b5; none is
introduced by this change. The final zero-issue lint requirement remains
open; this run is not reported as passing.

## K3c resumed cost smoke and interleaving

[The smoke table](cost-smoke/table.md), raw samples and machine snapshot
record 18 successful processes: token/fresh/resumed, three repetitions,
1,000 pages × 100 records/page, workers one/four. Each process verified
100,000 resources. The runtime verified 1,002 committed pages. The resumed
fixture verified exactly one data page before stop, then reopened and
walked before running remaining pages. I/O totals include both openings.
Scrub and purge are measured separately, and peak RSS comes from wait4.

These are private-runtime, actual-NoSync measurements on a loaded machine.
They cannot estimate production public-Sync overhead or close C49. In
particular they do not supply calibration's Sync-per-page resumed arm,
full matrix, row/bucket/fact byte decomposition or production-shaped
estimate. The phase data identifies purge as most of the measured seal
cost in these two small cells; it does not establish scaling with rows.

After K3b, the ledger race suite passed three repetitions in 8.951 seconds.
Build/vet passed for sync, Pebble and synccompactor. The timing instrument's
planted defect and restored pass are recorded above. The new interleaving
runner is candidate instrumentation; no planted defect is claimed for its
process/RSS collection or ratio aggregation.

The full synccompactor suite also passed in 15.724 seconds with Go 1.26.0.
The concrete Ledger.Drop compactor consumer was inspected while narrowing
the pending reset proposal (implementation.md §13). No reset behavior was
changed, and the interrupted-finished-rebind candidate remains disabled.

## CO-010 lifecycle correction

The requester requires baseline lifecycle behavior, superseding the proposed
completion reset. C33/C34 are evidence incomplete under that requirement;
the earlier execution entries remain as history of the incorrect reset
model. No completed metadata reset was implemented. The private runtime's
finished branch still needs correction before public integration. Storage
LastSealCost remains the only new production storage behavior and is solely
observational. The frozen plan body is unchanged; CO-010 records the changed
requirement in the change-order log.

## Baseline contract audit

Five deliberately failing comparisons reproduce operation-order, spawned
completion-count, duplicate-child, unknown-quality and sibling-error
mismatches. They fail in the ordinary opt-in run and all three race
repetitions. The tests skip unless BATON_LEDGER_BASELINE_AUDIT=1, so normal
suite success cannot be cited as closure. The exact scope, baseline source,
consequences and remaining integration checks are in baseline-audit.md.
No production implementation fixes are included in this audit commit.

## CO-011 scheduling scope correction

The replacement ledger executor will not be integrated or copied from the
baseline scheduler. The existing scheduler remains in use, with small shared
changes permitted for page persistence and restoration. The five audit
failures remain recorded; no replacement-executor fix or production-equivalence
pass is claimed. SQLite behavioral preservation is still required, while an
absolute prohibition on shared-path edits is superseded by CO-011.

## K2c existing scheduler page integration

The production replacement executor has been removed. Its old callers now
compile against a test-only fixture pending migration. The existing
parallelSync/syncParallel queue, batching, retries and joined-error handling
remain in use. The new invocation stages a page, validates its proposed
transition through the existing queue, and commits inside the queue's
transition callback before runState changes. Public attach routing and
production handlers remain off. No pkg/dotc1z changes occur in K2c.

The following tests are normal suite tests with strict page-write auditing.
These are bounded adapter results; the listed criteria remain evidence
incomplete until production handlers, restoration and their required crash
products are covered.

| Criteria / finding | Test | Defect and observed failure before correction |
| --- | --- | --- |
| C13–C16,C41 / A1 | TestLedgerExistingSchedulerOperationBarrier | Historical mixed-operation executor admitted grants while resources were blocked; the replacement test holds two real scheduler workers and asserts no grant start until release. |
| C20,C23,C30 / A2 | TestLedgerExistingSchedulerSpawnedCompletion | Planted exclusion of spawned actions from terminal accounting produced one in runState and zero in the ledger. |
| C05,C13,C14,C39 / A3 | TestLedgerExistingSchedulerRejectsDuplicateBeforeCommit | Planted commit before queue validation changed the raw key snapshot despite duplicate-child rejection. |
| C19,C24,C30 / A4 | TestLedgerSealPreservesUnknownIngestQuality | Historical audit failed on non-nil clean quality with no quality facts. Guard now distinguishes absent, known clean and blocked state. |
| C39,C40 / A5 | TestLedgerExistingSchedulerPreservesIndependentErrors | Planted string-only wrapping lost errors.Is access to both concurrent causes. |
| C05,C17 | TestLedgerExistingSchedulerPublishesFactsAfterCommit | Actual missing publication to runState left the committed needs-expansion fact invisible to subsequent scheduling. |
| C05,C39 | TestLedgerExistingSchedulerCommitNotFoundIsNotWarning | Actual commit NotFound was consumed as a warning and syncParallel returned nil. |
| C05,C39 | TestLedgerExistingSchedulerRootNotFoundDiscardsPage | Actual adapter treated every NotFound as a warning; root listing failure left a durable row. Raw snapshot assertion failed. |
| C05,C13 | TestLedgerExistingSchedulerRejectsAssignedChildBeforeCommit | Actual child-ID rejection occurred after commit; raw snapshot assertion failed. |

All planted edits were restored. WarningCommitsAccounting also compares
existing warning counts with the committed bucket, and CommitFailureKeepsAction
checks the pending action and unchanged durable keys under a commit fault.
These additional assertions are passing candidates, not separate mutation
coverage. Init, finished-artifact processing, duration expiry, resume-walk
publication and production connector writes are not covered by this increment.

K2c validation: full pkg/sync suite passed in 74.329 seconds; all TestLedger
race tests passed three repetitions in 9.437 seconds; full synccompactor
suite passed in 15.736 seconds. Vet passed for sync and synccompactor;
pkg/sync lint reported zero issues. Go 1.26.0 and vendored dependencies were
used. The six previously recorded storage lint findings remain outside this
increment. These checks apply to the final K2c code, including the warning
classification and assigned-child guards.


## Executor fixture removal

ledger_executor_fixture_test.go is deleted. All former callers now seed
runState and invoke parallelSync using real page writers and test handlers.
The fixture helper has no dispatch loop, worker goroutine, queue, retry or
error aggregation implementation. Its only work is the initial ledger walk,
action conversion, handler injection and the call to the existing scheduler.
Synthetic listing roots now identify list-resource-types instead of Init.
The spawned diamond/cycle case uses list-resources and main's admission
rules. Six process-exit cuts and the logical crash differential are retained.

TestLedgerScheduleWalksNewlyDiscoveredChild failed after migration because
the adapter reran a committed child. Page invocation now resolves its row
before opening a writer; the test passes. TestLedgerExistingSchedulerReplaysRowWithoutWrites
checks that this path also passes the write-free instrument and leaves raw
keys unchanged. The latter is an additional assertion, not separate planted
defect evidence. C09/C10/C12 remain incomplete for their full required products.

The two old finished-binding tests were removed because they assert the
reset rejected by CO-010 (one was skipped). Removing those tests does not
close C33/C34. Their historical results above are superseded requirements,
not evidence for preserved lifecycle behavior.

The cost fixture now reports ledger-scheduler arms. Fresh and resumed smoke
runs with one and four workers each verified 1,000 resource records from ten
data pages and 12 committed rows including listing and terminal pages.
Previously committed cost tables remain historical results from the deleted
executor and cannot support acceptance of this implementation. C49 is open.

The now-unused beginLedgerRuntime and ledgerInitialActions were deleted too;
this removes the unconditional finished-binding DropLedger behavior itself,
not just the tests asserting it. loadLedgerResume remains available for
unfinished takeover; finished-artifact integration is still pending. The
cost runner validates the new arm names. One existing test file receives the
same package-name lint directive used elsewhere in pkg/sync; its tests are
unchanged.

Migration validation: full pkg/sync passed in 74.760 seconds; ledger race
suite passed three repetitions in 11.691 seconds; vet passed. After deleting
the unused reset helper and adding the read-only replay assertion, ledger
tests passed again in 0.907 seconds and pkg/sync lint reported zero issues.
Go 1.26.0 with vendored dependencies was used. A source search finds no
ledgerRuntime.execute, ledgerPageResult or ledgerPageHandler remaining.


## K2d atomic Init

Init now uses one store-free action/fact plan for both persistence paths.
The checkpoint path keeps its forced checkpoint; the ledger path commits
its ordered children, facts and completion bucket together before publishing
them into runState. No production connector listing handler or public
attachment route is enabled by this increment. No storage code changes.

TestInitialActionBaseline passed against the original Init body before the
extraction and against the shared planner afterward. Its eight cases assert
ordered operations, facts, parent identity and one Init completion. The
expected operation lists are explicit baseline expectations, not computed by
the planner under test. TestLedgerInitialActionMatchesBaseline checks those
same expectations and the durable row, fact set and cumulative accounting.

TestLedgerInitialActionCommitFailureIsAtomic initially failed because Init
advanced without a page commit and reached the next unimplemented handler.
It now passes for fact staging, counter staging and commit failures: Init
remains pending, no new fact is visible, completion remains zero and raw keys
are unchanged. A planted early publication of the skip-grants fact makes
that test fail. A planted reversal of resources/resource-types in the common
planner makes TestInitialActionBaseline fail even though both persistence
paths share the same faulty planner. Both planted edits were restored.

This supplies bounded adapter evidence for C05/C17 and baseline option
preservation. It does not close their crash products, finished-artifact
processing or full public Sync integration. Restoring the ledger action
history and facts into the existing scheduler remains the next integration
boundary; the removed finished-binding reset must not return.

K2d Init validation: full pkg/sync passed in 74.115 seconds; ledger and Init
baseline race tests passed three repetitions in 10.919 seconds; full
synccompactor passed in 15.050 seconds. Vet passed for sync and synccompactor.
After lint-only formatting and equivalent switch cleanup, Init tests passed
again in 0.158 seconds and pkg/sync lint reported zero issues. Go 1.26.0 and
vendored dependencies were used. No further production behavior changed
after those suite runs.

## K2d read-only state restoration

restoreLedgerState now supplies the existing scheduler's runState, runStats,
inline legacy graph and ingestion-quality state from the ledger. The test
fixture calls this production restoration path rather than constructing an
incomplete substitute state. It does not decide whether to begin another
requested pass and does not modify sync completion metadata.

| Criteria | Test | Failure established before guard / scope |
| --- | --- | --- |
| C17,C19,C20,C24–C30 | TestLedgerRestoreCheckpointFixtures | The old fixture-style restoration lost facts/counts/stats. Nine legacy fixtures failed against independent unmarshalToken expectations, then passed after restoration. No expected values are derived from decodeLedgerCheckpoint. |
| C33,C34 under CO-010 | TestLedgerRestoreFinishedCheckpointPreservesLifecycle | Empty and pending finished tokens lost the prior 17 completions. Both now retain history through takeover and repeated restoration; the entire sync-run record equals its prior value except the consumed token. This covers token-started artifacts, not yet a second pass over a sealed ledger. |
| C09,C10,C20,C41 | TestLedgerRestoreRunsOnlyPendingContinuation | Checks one committed page is skipped, only its remaining continuation runs, prior facts/calls/counts are visible to the handler, and the current-process completion threshold starts at zero. Candidate assertions; no separate planted defect claimed. |
| C20,C23,C39 | TestLedgerRestoreReplayedChildDoesNotCountAgain | Actual integration failure: three in-memory completions versus two committed completions. Replaying a recorded transition now drains the action without counting the old completion again. |
| C10,C40 | TestLedgerRestoreFailureDoesNotPublishState | Injected counter-read failure leaves the original in-memory state and raw keys unchanged. Candidate assertion, not separate mutation evidence. |

Every restoration test uses the write-free instrument around restoration;
the pending-page execution uses strict page-write auditing. The shared
runState transition still records completions by default. Only ledger row
replay disables completion accounting, and replay also preserves the row's
type-scoped planning flag on a continuation. Public attachment and production
connector handlers remain disabled. No storage changes in this increment.

Restoration validation before the separate storage continuation change:
full pkg/sync passed in 73.787 seconds, ledger race tests passed three
repetitions in 11.414 seconds, full synccompactor passed in 14.968 seconds,
vet passed, and pkg/sync lint reported zero issues. These include the shared
completion-accounting flag and replay regression. Go 1.26.0, vendored deps.

## Storage operation for finished-ledger continuation

PageLedgerStore.ClearLedgerRows is a new contract method. On a bound finished
sync it atomically removes page rows, the takeover frontier and named facts,
while retaining counter buckets, other facts, records and sync metadata.
It uses the existing ledger stamp, residue marker and purge path. The wrapper
marks dirty even if a later operation fails. This is the only additional
production storage behavior beyond the previously recorded seal timing.

TestLedgerClearRowsPreservesHistory failed with a planted DropLedger
implementation: the retained history fact disappeared. The rows-only batch
passes that test, including unchanged sync metadata, record preservation,
counter preservation and refusal of subsequent checkpoint writes.
TestLedgerClearRowsRefusesUnfinishedSync covers the guard's refusal arm.
TestLedgerClearRowsFailureCuts checks stamped, staged and committed cuts;
TestLedgerClearRowsCrashImages reopens zero-unsynced-byte crashable-VFS images
at those same cuts and checks coupled row/fact presence plus retained history.
Those crash images cover these three cuts, not arbitrary hardware failures.

TestPebbleStoreClearRowsMarksDirty closes and reopens the artifact after its
only requested mutation is clearing rows. Removing MarkDirty made the old
row survive the reopen; restoring it passes. The write-audit fixture and
storage mutation/commit registries include the new method and its failure
hook. Physical crash coverage of this storage batch does not by itself
establish the complete syncer continuation protocol.

The full Pebble engine suite passed in 6.927 seconds after registering the
new commit/failure hook. Store dirty/contract checks passed in 0.082 seconds.
Vet passed for dotc1z and sync. The broad lint run reports only the same six
pre-existing G115 conversions already recorded above; no new lint findings.


## Syncer consumer of finished-ledger continuation

prepareLedgerState loads the old frontier, clears old page rows only when a
finished pass has seal-ready proof or an empty legacy frontier, and restores
history plus pending actions. The empty-frontier case starts at Init, as
main does. A finished file with a pending frontier is resumed. An unfinished
file with seal-ready proof remains ready to seal and does not start another
pass. No completion timestamp is reset and no checkpoint token is written.

TestLedgerFinishedProcessingResumesWithoutReset failed with the old
finished-means-clear rule: both stop positions attempted ClearLedgerRows
again during the next read-only resume. The corrected rule passes after
artifact close/reopen, both immediately after clearing and after a committed
processing page. History remains at 17 before new work, then reaches 19 for
one Init and one completed processing action. The prior completion timestamp
is unchanged while processing is interrupted. The test uses the actual
scheduler and real page writers, but injects an expansion action handler;
it does not implement or verify production expansion-graph reconstruction.

TestLedgerFinishedLegacyFrontierKeepsPendingWork covers empty and pending
finished legacy frontiers through the same preparation method, preserving
all sync metadata except the consumed token, prior facts and 17 completions.
TestLedgerSealReadyUnfinishedDoesNotStartAnotherPass guards seal recovery.
Those are additional passing assertions, not separate planted-defect closure.
The second preparation in each continuation fixture is under the write-free
instrument, so clearing again is a visible failure rather than silent lost
progress. Page writes remain covered by the installed strict write hook.

C33/C34 now have bounded continuation protocol evidence. They remain evidence
incomplete for the full process/crash products and public WithConnectorStore,
WithSyncID and WithOnlyExpandGrants entry coverage. C36 still needs actual
production expansion behavior. Public attachment, connector handlers and
C49's required performance matrix remain outstanding.

Consumer validation: full pkg/sync passed in 73.911 seconds; ledger race
suites passed three repetitions in sync (12.781 seconds) and Pebble (2.870
seconds); synccompactor passed in 15.230 seconds. Vet passed for sync and
dotc1z. pkg/sync lint reported zero issues. The separately recorded broader
lint findings remain unchanged. Go 1.26.0 with vendored dependencies.

## Existing-scheduler cost rerun (C49, after fafa4f74)

Status remains **evidence incomplete**. `cost-scheduler-smoke/` contains 36
interleaved samples: 1,000/10,000 pages × 100 resources/page × one/four
workers × token/fresh/resumed × three repetitions. The pinned token source
is eb63f1b5; the ledger source is fafa4f74, using the existing scheduler.
Both independently compiled executables passed their fixture assertions in
every sample. Resource counts were 100,000 or 1,000,000; ledger commits were
pages + 2. Samples, complete process logs, binary hashes and machine snapshots
are retained. No record/equality normalization was used for this cost run;
resource counts are not the O4 canonical differential oracle.

Fresh ledger/token write-byte medians range from 1.034 to 1.122; resumed
medians range from 1.172 to 1.187. Synthetic ledger handlers omit production
work, so their smaller wall times do not establish production improvement.
The README and full table state the observation limits. The old
`cost-smoke/` directory is explicitly marked as historical deleted-executor
data. No acceptance is inferred from either set.

Instrument defect: machine.py identified the checkout filesystem but omitted
the temporary directory used by Go test artifacts. The pre-fix output failed
`assert 'artifact_filesystem' in m` with the diagnostic “test artifacts use
the temporary directory, not the checkout.” After adding that observation,
comparison against `stat -f` for Python's process temporary directory passed:
artifacts use ZFS; the checkout reports ext2/ext3. The corrected snapshot has
its own collection timestamp and is retained as machine-after.json, not
substituted for the original pre-run snapshot. This check does not qualify
the machine as unloaded or identify the ZFS backing device.

C49 still lacks the full matrix, production-shaped estimate, encoded
row/bucket/fact decomposition, baseline phase timings, production handlers,
unloaded-machine evidence and the resumed-durability disposition. Its
coverage status and the K5/K6 prerequisite are unchanged. No pkg/dotc1z
behavior changed in this increment.

## Larger-page cost samples and CO-012

C49 remains **evidence incomplete**. CO-012 settles the resumed-arm premise:
measure actual NoSync commits; production durability is unchanged.
`cost-large-pages-smoke/` retains 36 additional interleaved samples at 1,000
pages × 1,000/10,000 resources/page × one/four workers, three repetitions
per arm. The sources remain eb63f1b5 and fafa4f74. Every resource-count and
ledger-commit-count assertion passed. No compilation or other test runs
overlapped these samples. The separate known-quality fix is not included in
the measured binary and requires a future revision rerun.

Fresh write-byte median ratios were 1.021–1.146; resumed ratios were
1.059–1.142. No median wall/byte tripwire fired. Source inspection identifies
forced compaction at ledger seal, but its exact contribution to write bytes
is not isolated. The table's synthetic handlers, machine qualification,
coverage and instrumentation limits remain explicit. Eight of eighteen
workload configurations now have smoke samples, not acceptance evidence.
No canonical normalization was applied; count checks are not O4 equality.

## Initial ingestion quality (C17, C19, C30)

The new `TestLedgerInitialQualitySurvivesResume` failed against the prior
implementation: a fresh run's Init row survived close/reopen, but its
known-quality fact did not. `TestLedgerInitialQualityCommitFailure/fact`
also failed because Init never staged that fact. Fresh restoration now
keeps the known snapshot in runStats, and Init stages its declaration in
the page. Unknown prior input keeps its absent snapshot.

Both tests pass after the fix. The close/reopen cases assert the Init row,
the known/unknown distinction, conservative blocking and unknown-prior
reason, and a write-free restoration. Fact/counter/commit injection cases
assert unchanged durable keys and absence of the fact and Init row after
reopen. The strict write recorder remains installed. A planted unconditional
known-quality declaration failed the unknown-prior case; the guard was
restored before the final tests.

These are bounded initialization checks. They do not close the full fact
products, overlapping page updates, physical crash cuts, cold-process public
Sync behavior, or failures before Init commits. No SQLite persistence path,
engine behavior, source-cache replay orchestration, or scheduler policy was
changed. The ingestion-stat writes in later production handlers remain to
be integrated. C17/C19/C30 stay evidence incomplete; C19/C30's old failed
status referred to defects superseded by the recorded passing regressions.

Final checks for this increment (Go 1.26.0, vendored dependencies):

- Targeted Init/restoration/continuation suite: passed (0.447s).
- Same selection with `-race -count=3`: passed (4.668s).
- `go test ./pkg/sync -count=1 -timeout=30m`: passed (74.165s).
- `go vet ./pkg/sync/...`: passed.
- `golangci-lint run ./pkg/sync/...`: zero issues after removing one redundant
  test-only conversion. No other lint findings in this scope.
- `git diff --check`: passed.

## K4a asset staging (C04, C07, C38; prerequisite for C42)

Storage now accepts PageWriter.PutAsset. The page snapshots the supplied
bytes and stages the asset in its existing RecordBatch, alongside the row,
facts and bucket. Asset identity/validation follows the direct Store method;
repeated IDs use the last staged value. Direct asset writes and SQLite
remain unchanged. This widens PageWriter for external implementers.

| Candidate | Defect or fault exercised | Result and limit |
| --- | --- | --- |
| TestLedgerAssetPageSurvivesReopen | Temporary no-op PutAsset stub | Failed on missing asset after successful page commit and reopen; passes with staging. Starts from a clean reopened artifact to exercise page dirty marking. |
| TestLedgerAssetPageSurvivesReopen | Direct Engine.PutAsset inside the page | Failed because the asset was visible before commit. Mutant removed. |
| TestLedgerAssetPageSurvivesReopen | Retain the caller's byte slice without copying | Failed on changed asset bytes after reopen. Mutant removed. |
| TestLedgerAssetPageDiscardKeepsPriorValue | Discard a staged overwrite | Original value and raw keys unchanged through reopen. No separate mutation for this test. |
| TestPageWriterAssetCommitFailureAndRetry | Existing record-commit error hook | Failed batch preserves old asset and absent row; retry commits last staged value and row. |
| TestPageWriterAssetValidationAndDiscard | Unbound sync, nil/empty references, discarded writer | Refused; discarded data remains absent. |
| TestPageWriterAssetRefusesReplacementSync | Finish original sync, then explicitly start another before old page commits | Old page refused; replacement contains neither asset nor row. This does not change same-ID processing semantics. |
| TestPageAssetStageRejectsOtherKeyFamilies | Supply a ledger key to the typed asset operation | Rejected with the batch still empty. |
| TestLedgerCrashProcess | Exit child process after staging or commit; reopen its DB | Asset, type, fact, bucket and row are present together or absent together. No physical unsynced-WAL loss is simulated. |

The public page fixtures install the strict write hook and companion
recorder. No bypass reason is needed: asset data is part of the batch.
Engine tests use the existing record-commit injection point; no new hook or
commit site was added. Buffers are released on commit/discard. The consumer
and engine tests are storage support, not proof of production SyncAssets:
C42's handler inventory and full crash products remain open. C04/C07/C38
remain evidence incomplete to the plan's full coverage.

K4a final checks (Go 1.26.0, vendored dependencies): full sync suite passed
(74.686s); full Pebble engine suite passed (9.480s); full synccompactor suite
passed (16.306s). Asset/process-crash race selection passed three repetitions
in sync (1.676s) and Pebble (1.168s). Store dirty-marking/capability checks
passed (0.636s); vet passed. Sync lint reports zero issues. Broader lint has
only the same six baseline G115 conversions (three adapter durations, three
benchmark integers); no new findings. The temporary no-op, direct-write and
aliased-buffer mutations are removed. `git diff --check` passes.

## K4b: page-staged resource and entitlement deletion

Brief commit: e09ea022. PageWriter.DeleteResources/DeleteEntitlements take
full identities and defer deletion until after page puts. Direct delete
methods, handlers, scheduling and SQLite bodies are unchanged. The engine
uses its existing typed deletion operations, preserving parent-index and
source-scope cleanup. Page counts include distinct puts even when their
records are deleted in the same page. Entitlement lookup invalidation also
runs after delete-only commits. External PageWriter implementers need the
two new methods.

| Candidate | Planted defect or injected fault | Result and limit |
| --- | --- | --- |
| TestLedgerDeletePageSurvivesReopen | Temporary no-op deletion methods | Failed: committed row existed but target resource remained after reopen. Passes with staging. Strict write hook and raw snapshot assert no pre-commit write; discard/reopen keeps the original image. Same-ID entitlement on another resource survives. |
| TestPageDeleteMatchesDirectIndexCleanup | Delete using only stored values | Failed: staged-only target survived; stored-plus-staged target left a parent-index key. Restored implementation passes. Stored/staged/both/missing cases compare with direct put-then-delete, checking absence of primary, parent and source-scope keys and matching scope-poison state. |
| TestPageDeleteFailureRetryInvalidatesEntitlementLookup | Omit invalidation for delete-only page | Failed: lookup still reported two identities after one was deleted. Restored implementation passes. |
| TestPageDeleteFailureRetryInvalidatesEntitlementLookup | Existing record-commit hook returns an error | Original records and indexes survive, row absent; retry commits deletes and preserves the other identity. No separate error-after-commit fault here. |
| TestPageDeleteValidatesWholeRequest | Invalid final object after a valid target | Request refused without staging the earlier deletion; committing leaves both records present. No separate mutant. |
| TestPageDeleteRefusesReplacementSync | Finish then explicitly start a different sync before commit | Old page refused; replacement records remain. No new lifecycle behavior. |
| TestPageDeleteDoesNotCascade | Delete resource, then entitlement in separate pages | Entitlement survives resource deletion; grant survives both. Discarded writer rejects new deletes. No separate mutant. |

No new rawdb operation, failure hook, commit site or registered bypass was
added. Public consumer checks begin from a clean reopened artifact, so the
page's dirty marking is required to persist the changes. The three temporary
defects are removed. These tests add storage coverage for C04/C07/C08/C38/C42;
all five remain evidence incomplete against the full plan. Deletion-specific
process-crash products, the external-resources handler inventory and CO-002's
grant-delete case are not claimed by this increment. OQ-5's deletion API
portion is implemented; expansion-preserving writes remain outstanding.

K4b validation (Go 1.26.0, vendored dependencies): full sync suite passed
(75.918s), full Pebble suite passed (7.582s), and full synccompactor suite
passed (22.541s). Deletion race tests passed three repetitions in sync
(1.403s) and Pebble (1.456s). Vet passed. Broad lint reports the same six
baseline G115 conversions, with no new findings. `git diff --check` passes.

## K4c: page-staged expansion grants

Brief commit: 7e89d73f. PageWriter.StoreExpandedGrants uses the existing
expansion translation and preserves expansion state, discovery timestamp
and source scope by structured identity at commit. Same-page ordinary puts
supply the preserved state when present; otherwise the stored prior value
is read under the write barrier. That read also supplies deferred-index
cleanup. New derived grants have no expansion state or source scope, and
missing discovery timestamps are filled. Distinct final puts contribute to
the row count. The ordinary-only page stager is unchanged. Direct expansion
methods, the scheduler, production handlers and SQLite are unchanged.

| Candidate | Planted defect or fault | Result and limit |
| --- | --- | --- |
| TestLedgerExpandedPagePreservesStateAfterReopen | Temporarily delegate to ordinary PutGrants | Failed: NeedsExpansion was cleared after commit/reopen. Passes with preservation. Strict page write hook, raw pre-commit snapshot and clean artifact reopen cover invisibility, persistence and discard. PendingExpansion still returns the original grant. |
| TestPageExpandedMatchesDirectPreservation | Omit the prior-state merge | Failed for existing and missing-timestamp records. Restored code matches the direct API's complete grant record; only newly assigned discovery timestamps are normalized in new/missing-time cases. Existing timestamps compare exactly. |
| TestPageExpandedMatchesDirectPreservation | Replace deferred grant staging with inline staging | Failed on the missing deferred-index marker in all three cases. Restored code also verifies needs-expansion/source-scope key counts, no source poisoning, seal clearing the marker, and principal lookup after seal. |
| TestPageExpandedWriteOrder | Ordinary then expanded, expanded then ordinary, repeated expanded payload, delete before expansion | Passes with expected state and distinct row counts; last payload wins and deletes apply after puts. No separate order mutant. |
| TestPageExpandedFailureRetryUsesCurrentPrior | Existing record-commit hook refuses the page; another write changes prior state before retry | Failed batch leaves the original record and no row. Retry preserves the newer timestamp and cleared expansion state, with no needs-expansion index left. No separate retry mutant. |
| TestPageExpandedFullIdentityAndWriterLifetime | Same external ID on a second principal, invalid identity, unbound/completed/discarded/stale writer | Existing grant compares equal; second identity is stored without inheriting its state. Invalid commit and invalid writer lifetimes are refused. No separate identity/lifetime mutant. |

Preservation operates on a copy, so failed attempts do not alter the buffered
input. An expansion page consumes the engine's empty-grant-keyspace proof,
forcing later ordinary puts to check prior records. The same typed deferred
operation used by main handles digest/index obligations; this increment does
not add another rawdb operation, commit site, failure hook or page bypass.
The initial full engine run found a helper-name collision in TestWriteMuHolders;
the helper was renamed and the checker and full engine suite passed.

These candidates add storage evidence for C04/C07/C08/C38/C42; those criteria
remain evidence incomplete against the full plan. No expansion-specific
process-crash matrix, cold-process graph reconstruction or production
SyncGrantExpansion behavior is established here. OQ-5's three proposed API
additions (assets, full-identity deletions and preserving expansion writes)
are implemented, but its complete in-page write inventory still needs the
handler work. PageWriter's new method affects external implementers. All
three temporary defects are removed.

K4c validation (Go 1.26.0, vendored dependencies): full sync suite passed
(91.404s), full Pebble suite passed (10.221s after the helper rename), and
full synccompactor suite passed (35.559s). Expansion race tests passed three
repetitions in sync (1.595s) and Pebble (2.349s). Vet passed. Broad lint has
only the same six baseline G115 findings; `git diff --check` passes.

## K5a: production resource-type pages

Brief commit: 0f27ceb1. SyncResourceTypes now has a ledger handler reached
through the existing parallelSync resource-type dispatch. It stages selected
records, invalid-record observations, connector call/wait accounting and the
next cursor in one page. Progress and diagnostic counters publish only after
commit, outside the scheduler transition lock. Init and resource types are
integrated; the other production handlers remain refused. No scheduler,
public persistence option or fallback is added.

The final engine rule remains mandatory: Pebble always uses the ledger;
SQLite uses checkpoints. The internal ledgered boolean is the attach-time
engine decision, not a user preference. Public attach/routing activation and
its C01/C02/C03 tests remain outstanding while handlers are being completed.

The terminal type-filter check is a deliberate shared correction. Before the
change, TestResourceTypeFilterAcrossConnectorPages failed on main's handler
with `invalid page token: cursor does not belong to this keyspace`: the
connector's continuation cursor was sent to the store reader. The check now
uses exact type IDs, considering the current page's staged selected records
and earlier stored records. Selection itself uses the same ID membership
rule as main. NotFound retains the existing invalid-filter diagnostic; other
read errors propagate. The unused list-validation helper is removed. This
small shared change follows the requester's relaxed token-path rule; no
SQLite write code is added.

| Candidate | Defect/fault and observed result | Limits |
| --- | --- | --- |
| TestResourceTypeFilterAcrossConnectorPages | Pre-fix handler rejects a valid second connector page with its foreign cursor; corrected handler passes. | Controlled token-path baseline fixture, not public attach selection. |
| TestLedgerResourceTypePages | Before integration, production-handler refusal fails the test. With integration, two connector pages commit their rows, call/wait accounting and invalid-record count; selected records survive artifact reopen. | Real handler and existing scheduler; no final sync seal. |
| TestLedgerResourceTypeFailureRetryAndReplay | Direct store write with a deliberately registered test bypass leaves raw data after refused page commit; assertion fails. Mutant removed. | In-process commit refusal, not a storage crash image. |
| TestLedgerResourceTypeFailureRetryAndReplay | Publish invalid-record counter before commit; zero-counter assertion fails. Publish progress before commit; empty-progress assertion fails. Both mutants removed. | Progress means the record-progress callback; main's initial-step notification still occurs before the connector call. |
| TestLedgerResourceTypeFailureRetryAndReplay | Failed page keeps its action, records/counters/progress absent. Retry commits each page once. Reopen and use a new runtime/run state: replay invokes no connector or progress callback and changes no raw keys. | Reconstructs resource-type actions explicitly; full public resume lifecycle remains separate. |
| TestLedgerResourceTypeErrors | Connector failure commits nothing; missing selected type rejects the final page while preserving the earlier committed page. | One non-retryable connector fault; retry policy remains the existing scheduler's. |
| TestLedgerResourceTypeReadFailure | Exact-ID reader error rejects the final page and leaves its row absent. | One read failure, not the full storage error matrix. |
| TestLedgerResourceTypeSelection | Unfiltered, earlier-page-only and terminal-page-only selections preserve records and progress counts; disabled stats stay absent. | Other handler families are not covered. |

Every Pebble page/replay fixture has the strict write hook and companion
write audit. The direct-write mutant deliberately supplied a bypass reason
so the raw-image assertion, rather than only hook rejection, caught it.
No production bypass was added. C04/C07/C15/C17/C20/C42 gain this coverage and
remain evidence incomplete to the full plan. pkg/dotc1z is unchanged in this
increment. Final handler cost measurements and complete crash products remain
outstanding; the earlier table still measures synthetic handlers.

TestLedgerResourceTypeSelectedSync found an omitted sync-selection annotation
in the initial exact-ID helper: the reader saw an empty sync ID when the
caller selected one. The test failed before the correction and passes with
the SyncDetails annotation carried over from main's previous reader request.
This preserves the selected-sync boundary; no binding lifecycle changes.

K5a final checks (Go 1.26.0, vendored dependencies): full sync suite passed
(87.595s). Resource-type, existing-scheduler and Init race tests passed three
repetitions (4.508s). Vet passed; sync lint reports zero issues. Storage and
compactor code are unchanged in this increment; their last full passing runs
are recorded under K4c. `git diff --check` passes. The direct-write,
early-counter and early-progress mutations are removed.

## K5b: production resource and targeted-resource pages

Brief commit: f90461d4. The handlers use main's existing dispatch and worker
queue. Resource writes, next cursors, child identities and page accounting
commit together. Child discovery checks the existing scheduling set again
under its lock immediately before transition, holds it through commit and
queue publication, and records marks only after successful commit. Connector
calls remain concurrent. Recorded children restore the marks during replay.

Main's resource-type reader pagination, full parent identities, trait checks,
latest resource payload, configured child-type selection and raw response
progress counts are retained. Targeted requests retain NotFound and
Unimplemented as empty successful results. Their follow-ups retain the order
that executes children before entitlements before grants; type-scoped types
get no whole-type replacement action. No lifecycle or engine selection change
is included. pkg/dotc1z is unchanged.

| Candidate | Defect/fault and observed result | Limits |
| --- | --- | --- |
| TestLedgerResourcePages | Fails before integration at the production-handler refusal. With integration, two parent pages and one deduplicated child commit the latest payload and accounting with one/four workers. | Controlled connector, no public Sync seal. |
| TestLedgerResourceCommitFailureRetry | Direct PutResources with an explicit test bypass changes the failed-page raw image; snapshot assertion fails. Early child marks fail the unchanged scheduling-set assertion. Both mutants removed. | In-process commit refusal, not process/power failure. |
| TestLedgerResourceCommitFailureRetry | Retry preserves child work, counters and raw response progress; failed page publishes no progress or invalid-resource observations. | Initial-step notification retains main's pre-call behavior. |
| TestLedgerConcurrentChildDiscovery | A barrier forces two connector calls to overlap. Same discovered parent admits one child; distinct parent identities admit both. | Two simultaneous discoveries, not arbitrary concurrent histories. |
| TestLedgerResourceReplay | Reopen, new runtime/action state, strict walk audit: zero additional connector calls, unchanged raw keys, restored child marks. | Resource subtree reconstructed explicitly; public resume remains separate. |
| TestLedgerTargetedResourcePage | Fails before integration. Commit refusal preserves records and child marks; retry records grants/entitlements/children in main's order and carries the full requested parent. | Follow-up handlers are not executed by this fixture. |
| TestLedgerTargetedResourceEmptyAndFailure | Empty/NotFound/Unimplemented results commit empty transitions; Internal failure keeps action and raw image unchanged. | Does not cover all connector error codes. |
| TestLedgerTargetedTypeScoped | Type-scoped grants and entitlements do not create targeted whole-type follow-ups. | Both annotations present in one fixture. |
| TestLedgerResourceControlPage | Failed type-enumeration page does not publish resources-phase evidence; successful page retains both parent fields on children. | Two stored types fit in one reader page. |
| TestLedgerResourceReadFailure | Existing-resource read failure propagates with no page or connector counters committed. | One read-error boundary. |
| TestLedgerConnectorObservationsAccumulate | Overwrite instead of add loses the first call and fails Count/TotalMs assertions. Restored code sums calls, waits and session observations and keeps max latency. | Accounting instrument uses a test handler making two reports. |

All page and replay fixtures install the strict hook and companion audit.
C04/C05/C07/C09/C15/C17/C20/C38/C42 gain the coverage above and remain evidence
incomplete to their full products. The three planted defects are removed.
Remaining handler families, public entry, full crash differentials and C49's
final real-handler measurements are not established by this increment.

K5b checks (Go 1.26.0, vendored dependencies): full sync suite passed
(78.486s); resource, targeted, accounting and existing-scheduler race tests
passed three repetitions (4.498s). Vet passed, sync lint reports zero issues,
and git diff --check passed. Storage and compactor are unchanged; their last
full runs remain K4c's. No additional process-crash coverage is claimed.

## K5c: production entitlement pages

Brief commit: f3f6300d. The existing control planner and leaf dispatch now
stage entitlement pages. TypeScopedPlanned publishes after commit and before
continuation admission. Per-resource/type-scoped requests, skip rules, sibling
cursor parsing, selected sync identity and progress calculations follow main.
Full-sync filtering uses the existing scheduled-type reader (including its
external-resource exception); partial sync retains references to absent types.
Drops set monotone ingest-known/blocked facts and page bucket reasons/counts.
Only committed pages publish in-memory drop and invalid-record deltas.

| Candidate | Defect/fault and observed result | Limits |
| --- | --- | --- |
| TestLedgerEntitlementPages | Before integration, real handlers fail at the refusal. After integration, per-resource pagination and type-scoped pagination/sibling cursors commit accepted records and page accounting. Reopen retains drop counts, reason flags and blocked/known facts. | Controlled connector, no public Sync seal. |
| TestLedgerEntitlementCommitFailureRetry | Planted direct entitlement write with a registered test bypass changes the failed-page image; snapshot assertion fails. Mutant removed. | In-process commit refusal. |
| TestLedgerEntitlementCommitFailureRetry | Early global drop publication fails the zero-count assertion. Retry with corrected code publishes one drop and two successful connector calls. | Not every atomic counter is mutated independently. |
| TestLedgerEntitlementPages | Omitting sync.ingest_blocked leaves counters present but behavioral fact absent after reopen; assertion fails. Mutant removed. | Does not execute source-cache orchestration, which is out of scope. |
| TestLedgerEntitlementPlannerCommitAndReplay | Failed commit leaves planning flag unset; successful control-page continuation carries the flag and emits no second type-scoped action. Read-only replay restores it. | Controlled two-page resource reader. |
| TestLedgerEntitlementReplay | Reopen with a new runtime/run state: no connector calls or raw-key changes while replaying normal and sibling cursors. | Entry actions reconstructed explicitly. |
| TestLedgerEntitlementPartialRetention | Partial sync retains the disabled-type reference that full sync drops. | One absent type. |
| TestLedgerEntitlementDuplicateCursor | Existing scheduler rejects a sibling identical to the continuation; records, facts and global block remain absent. | One duplicate shape; parser's prior tests cover token format limits. |
| TestLedgerEntitlementReadFailures | Resource and scheduled-type read faults propagate without writes or global replay-block changes. | Two read boundaries. |

Nil and missing-resource entitlements are rejected by the existing validator;
observations are counted with the committed page. Every page/walk fixture has
the strict hook and companion audit. C04/C05/C07/C09/C15/C17/C19/C20/C42 gain
these checks and remain evidence incomplete to the full plan. The three
planted defects are removed. No pkg/dotc1z changes or new scheduler are part
of this increment. Public entry, remaining handlers, full crash products and
final real-handler cost evidence remain outstanding.

K5c checks (Go 1.26.0, vendored dependencies): full sync suite passed
(85.256s), entitlement/resource/targeted/existing-scheduler race checks passed
three repetitions (6.271s), vet passed and sync lint reports zero issues.
The strengthened post-reopen bucket equality assertion passed separately
(0.059s). git diff --check passes. Storage and compactor remain unchanged.

## K5d: production grant pages

Brief commit: 4c97f538. Grant control/leaf handlers now stage records,
discovered/fetched resources, expansion/external facts, counters and action
transitions together. The existing planner, skip rules, type-scoped requests,
filter ordering and progress rules remain. The fresh-grant filter now takes
an explicit stats destination internally: existing callers supply their same
global object; pages supply a local object. No filter predicates or store
writes changed on the checkpoint path. Local observations publish as additive
counters and OR'd reasons after commit. Related-resource reads use staged
records and attribute connector calls to the requested resource's type.

| Candidate | Defect/fault and observed result | Limits |
| --- | --- | --- |
| TestLedgerGrantPages | Before integration, handlers fail at the refusal. Corrected handlers retain discovered resources even when their grant is dropped, preserve InsertResourceGrants, filter expansion type IDs and persist facts/counts with per-resource/type-scoped pages. | Controlled two-page connector. Expansion payload checked through PendingExpansionPage; fixture includes a source entitlement ID required by storage. |
| TestLedgerGrantCommitFailureRetry | Planted direct PutGrants with registered test bypass changes the failed-page snapshot; assertion fails. Mutant removed. | In-process commit refusal. |
| TestLedgerGrantCommitFailureRetry | Premature factNeedsExpansion changes run state after failed commit; assertion fails. Passing global filter stats into the page changes its replay block before commit; assertion fails. Both mutants removed. | Not every fact/counter mutated independently. |
| TestLedgerGrantRelatedResourceReadThrough | Replacing PageWriter.GetResource with stored-only lookup makes two connector fetches; one-fetch assertion fails. Corrected code records one call under get-resource:related, not the listing type. | Two grants sharing one related resource in a partial sync. |
| TestLedgerGrantExternalMatchFact | External placeholder principal remains accepted and establishes the external-grants fact without a disabled-type drop. | One ExternalResourceMatchAll annotation. |
| TestLedgerGrantRemovedExpansion | Filtering away every expansion type removes expansion work rather than widening it; no needs-expansion fact, one expansion-drop counter and a blocked fact. | One absent type. |
| TestLedgerGrantReplay | Reopen, restore through the read-only walk and execute remaining state: no new calls or raw writes; expansion fact and ingest quality restore. | Root specified explicitly; public Sync lifecycle remains separate. |
| TestLedgerGrantFullIdentities | Two grants sharing the external ID but naming different entitlement resources both persist and count as two writes. | Two full identities in one page. |
| TestLedgerGrantPlanner | Failed control-page commit leaves planning unset; successful continuation plans the type-scoped action once. | Controlled two-page reader. |

Every fixture installs the strict write hook and companion audit. The four
planted defects are removed. C04/C05/C07/C09/C15/C17/C19/C20/C38/C42 gain this
coverage and remain evidence incomplete to the full products. pkg/dotc1z is
unchanged. Static entitlements, assets, external matching, expansion/graph
reconstruction, public routing and final cost/crash evidence remain pending.

During review, the restoreLedgerState walk was found to omit child scheduling
marks even though invokeActionPage's replay restores them. A committed parent
page with an unfinished child and continuation needs a real-handler stop/reopen
fixture before public routing: a rediscovered parent must not record that
pending child twice. This is not covered by the completed-subtree replay tests.

K5d checks (Go 1.26.0, vendored dependencies): full sync suite passed
(79.848s); grant/entitlement/resource/targeted/existing-scheduler race tests
passed three repetitions (6.451s). Vet passed, sync lint reports zero issues,
and git diff --check passes. Storage and compactor code remain unchanged.

## Pending resource children during restoration

Brief commit: af2e4131. TestLedgerResourcePendingChildRestore failed against
K5d in two independent subcases: restored childSchedule lacked the pending
child, and the resumed continuation durably recorded the child a second time.
This was a defect in state restoration, not a missing storage capability.

restoreLedgerState now uses the walk's existing seen-identity map to rebuild
child scheduling marks, including children whose pages are still absent.
It publishes the map only after all state reads succeed. No extra scan,
store write, lifecycle change or queue implementation was added.

The corrected test commits one real resource page, closes/reopens, restores
from its root and runs its pending child and continuation. Restore changes no
raw keys, exactly one child connector call occurs, and the continuation's
children and written-resource count equal uninterrupted execution.
TestLedgerRestoreFailureDoesNotPublishState now also checks that a failed
counter read preserves prior child scheduling marks. Other restore fixtures
continue to pass. C09/C10/C15/C16/C42 gain this stopped-subtree case; complete
sealed-file crash equality remains evidence incomplete.

Checks (Go 1.26.0, vendored dependencies): restore and stopped-child tests
passed (0.267s); restore, replay and existing-scheduler race tests passed three
repetitions (4.756s). Vet and sync lint passed (zero issues). The last full
sync run remains K5d's 79.848s; storage and compactor are unchanged.

## Static entitlement and asset handlers

Brief commit: c585cf60. Static control actions retain main's complete connector
type enumeration before publishing children, including types outside the
collection filter. Leaf pages synthesize the same entitlement records from
one static response. Asset control actions retain stored-resource enumeration;
leaves stage icon/logo bytes and metadata together. Asset scheduling in Init
remains disabled as on main; restored asset actions are supported. Successful
empty and compatibility paths now record normal page transitions.

| Candidate | Defect/fault and observed result | Limits |
| --- | --- | --- |
| TestLedgerStaticEntitlementPages | Before integration, real handler refusal fails the test. Corrected pages preserve fallback names/descriptions and independently scoped exclusion groups across two resources and connector pagination. | Small fan-out, not a memory bound. |
| TestLedgerStaticEntitlementPlanner | Two connector type pages produce the same unfiltered child order and account for both calls plus an invalid type. | One zero-record control action with ancillary enumeration calls. |
| TestLedgerStaticEntitlementsMatchTokenHandler | Complete returned entitlement protos equal main's checkpoint handler output on the same input. | Controlled fixture; no final seal. |
| TestLedgerStaticEntitlementCommitFailure | Direct PutEntitlements with a registered test bypass changes the refused-page snapshot; assertion fails. Mutant removed. | In-process commit refusal. |
| TestLedgerStaticEntitlementReplay | Close/reopen and read-only state restoration make no new connector calls or raw writes. | Explicit root action. |
| TestLedgerStaticEntitlementLegacyPrefixError | Old lambda prefixError commits an empty terminal transition, preserving main's compatibility behavior. | Exact legacy diagnostic fixture. |
| TestLedgerAssetHandlerReopen | Before integration, handler refusal fails. Corrected page retains byte payload and content type after close/reopen. | One stream/ref. |
| TestLedgerAssetHandlerCommitFailure | Direct PutAsset with registered test bypass changes the refused-page snapshot; assertion fails. Mutant removed. | In-process commit refusal. |
| TestLedgerAssetHandlerErrors | Missing metadata/stream failure discard the action; nil stream retains main's successful-empty behavior. | One stream error cut. |
| TestLedgerAssetHandlerMultipleReferences | Second-stream failure discards the first staged asset. Retry commits both refs and only successful-page call counts. | Icon plus app logo. |
| TestLedgerAssetHandlerReplay | Reopen/restore/replay changes no keys and makes no additional asset requests. | Explicit root action. |
| TestLedgerAssetHandlerNoReferences | Planted success-without-transition fails with the page transition error. Corrected no-connector action records its terminal row. | CO-003 no-connector case; mutant removed. |

Every Pebble page/walk fixture uses the strict hook and companion audit. No
production bypass or pkg/dotc1z change was added. C04/C05/C07/C09/C15/C17/C20/
C38/C42/C47 gain this coverage and remain incomplete to the full products.
Static fan-out buffering and imported-data scale still need cost evidence;
small fixture success is not a production memory claim. External matching,
expansion/graph reconstruction and public routing remain incomplete.

Checks (Go 1.26.0, vendored dependencies): full sync suite passed (81.055s).
Static/asset/stopped-child/existing-scheduler race tests passed three
repetitions (5.480s). Vet passed, sync lint reports zero issues, and git diff
--check passes. Storage and compactor remain unchanged in this increment.

## External import and matching handlers

Brief commits: 385a0239 and 0b373423. Import and matching are sequential pages
of the existing external-resources action. The import commits records, stale
full-identity deletes, ordered principal identities and the matching cursor
as one unit. Matching reads those committed records; a cold continuation does
not reopen the external source. Filtered principal ordering is deterministic.
Main's selection maps, trait defaults, annotations and matching rules remain.

| Candidate | Planted defect/fault and observed result | Limits |
| --- | --- | --- |
| TestLedgerExternalPagesMatchTokenHandler, TestLedgerExternalSelectionMatchesTokenHandler | Final resource types, resources, entitlements and grants equal the existing checkpoint handler for full/filtered import, configured traits, type/resource/grant skips and present/missing profile matches. | Small fixtures; these parity cases have no separate planted defect. |
| TestLedgerExternalImportCommitFailure | Registered direct resource import survives commit refusal and fails raw-image equality. Mutant removed. | In-process commit refusal. |
| TestLedgerExternalImportedGrantIsMatched | Moving matching before import commit leaves an imported placeholder unprocessed; principal assertion fails. Mutant removed. | One imported external-match grant. |
| TestLedgerExternalStaleGrantReimport | Deleting an exact stale identity after its reimport loses the grant; survivor assertion fails. Mutant removed. | One stale resource and reimported grant. |
| TestLedgerExternalMatchingFailureAndResume | Lost principal fact contents produce no match after reopen; count assertion fails. Corrected case refuses matching commit without changing keys, closes the source and destination, restores without writes, and completes without a source reader. | Explicit action root, not public Sync entry. |
| TestLedgerExternalFilteredPrincipalState | Reversing the saved principal identities fails the exact ordered-fact assertion. Mutant removed. | Two filtered principals. |
| TestLedgerExternalPrincipalStateErrors | Absent, malformed, null and incomplete saved identities return errors and leave raw keys unchanged. | Four bad-state fixtures. |
| TestLedgerExternalDeleteFullIdentity | Bare-ID deletion returns an ambiguity error for two stored identities and fails the success assertion. Corrected full-identity delete keeps the unrelated identity. | CO-002 uses the no-bare-ID inventory below, not a claimed split-ambiguity implementation. |
| TestLedgerExternalExpansionRemap | Imported membership identity replaces the external placeholder's expansion source. | MatchID and one expansion source; graph execution is separate. |

O8/CO-002 delete inventory: collectLedgerStaleExternalPrincipals only reads.
stageLedgerStaleExternal uses PageWriter.DeleteResources with resource IDs,
DeleteEntitlements with complete entitlements and DeleteGrants with complete
grants. Matching uses PageWriter.DeleteGrants with its collected original
grants. Its shared matchProfileAndExpand helper reads entitlements and builds
output records; it performs no delete. There is no bare-ID deletion or direct
write bypass inside either external page. This makes the split stored/staged
bare-ID deletion case unreachable on this handler path. The engine's separate
source-cache deletion issue is not changed here.

Every page/walk fixture installs the strict write hook and companion audit.
All six planted mutants failed their intended assertions and were removed.
C04/C07/C08/C09/C15/C16/C17/C38/C42 gain the cases above and remain incomplete
to their full products. Imported and matched grant buffering is not a measured
production memory bound. No pkg/dotc1z changes were made in this increment.

Checks (Go 1.26.0, vendored dependencies): full sync suite passed (77.800s),
external/existing-scheduler race tests passed three repetitions (4.849s),
and the added selection parity cases passed with all external tests (0.557s).
Vet passed. The final external race run including selection parity passed
three repetitions (6.273s). Sync lint reports zero issues after removing an
extra blank line in the parity helper; git diff --check passes.

## Expansion flush qualification

Brief commit: b1eafd09. TestLedgerExpansionFlushQualification compares the
existing projection evaluator's complete output batches against an interrupted
pass plus a fresh-graph resumed pass. All 33 cuts pass across the existing
chain, diamond, directness, filtering, existing-destination and cycle fixtures,
plus a seven-principal chain forced into two-record flushes. Comparison checks
full grant protos in batch order and batch sizes, in addition to final records.
A no-op-write mutant in mergeContributionIntoExistingGrant leaves final grants
unchanged but fails the flush-count assertion after resume. The mutant was
removed. This demonstrates why final-grant equality alone is insufficient.

This is pre-integration qualification using the existing direct expander API,
not a ledger-page or crash-image test. It provides candidate evidence for
C16/C19/C41, not closure: cold graph reconstruction from durable annotations,
actual page commits, iterator cleanup, process death and production-scale
memory/cost are not exercised. The normal run passed (0.849s); race passed
three repetitions (5.785s), and expansion lint reports zero issues.

## Expansion handler and cold reconstruction

Brief commit: 2b7b7e03. The ledger handler consumes bounded output batches from
the existing topological projection evaluator through a pull iterator. The
adapter exposes no direct-write or SST-layer capability. A yielded batch is
staged with StoreExpandedGrants; evaluation resumes after its commit. The
terminal page publishes the completed graph only after committing. Failures,
scheduler exits and Close stop the iterator and release its projection.

| Candidate | Planted defect/fault and observed result | Limits |
| --- | --- | --- |
| TestLedgerExpansionHandlerResume | Initially fails at production handler refusal. Direct output with a registered bypass fails raw-image equality after refused commits. Starting cold graph loading at the saved expansion cursor fails resumed execution. Early graph publication fails the failed-page graph assertion. Omitting failed-stream cleanup fails the nil-iterator assertion. All four mutants removed. | Three page cuts on one chain; in-process refusal followed by artifact reopen, not process death. |
| TestLedgerExpansionMatchesMainProjection | Final grant protos equal the existing adapter's projection output. | One chain, including inherited group principals. |
| TestLedgerExpansionSkipRecordsTransition | Disabled expansion and absent expansion fact each record an empty terminal row. | Direct handler entry. |
| TestLedgerExpansionReadFailure | A source-entitlement read error leaves the raw file unchanged, graph unpublished and no iterator retained. | One read failure. |
| TestLedgerExpansionSchedulerCleanup | Normal scheduler completion and cancellation after one committed batch release the iterator. Main's supports_diff marker stays outside the page at its existing lifecycle point. | Existing scheduler entry; public Sync wiring remains pending. |

The resumed chain rebuilds its graph from stored expansion metadata in a new
graph holder. Its restore walk changes no keys. After completion, grant protos,
worker counter sums and every expansion row equal uninterrupted execution.
The row comparison normalizes only Attempt, CommittedAt and PageDuration;
there are no connector calls or retry waits in this fixture. Every page/walk
fixture installs the strict write hook and companion audit. No store capability
or expansion evaluator implementation changed in this increment.

C04/C05/C09/C15/C16/C19/C36/C38/C41 gain the stated cases and remain incomplete
to full products. The loaded legacy graph is cloned rather than published
while running; partial graph state is rebuilt from the beginning. Full legacy
version products and public finished-file expansion still need integration
checks. Preserving a graph sidecar when resume skips already-complete expansion
also remains a lifecycle obligation. Disabling direct contributions and SST
layers has an unmeasured cost; this is not covered by earlier synthetic C49
samples and must be measured before landing.

Checks (Go 1.26.0, vendored dependencies): full sync suite passed (77.618s);
expansion/external/existing-scheduler race tests passed three repetitions
(8.161s). Vet passed, sync lint reports zero issues, and git diff --check
passes. Storage and compactor production code remain unchanged.

## Run-level accounting

Brief commit: 74fd70ca. Each ledger runtime now owns an empty current-attempt
stats accumulator, separate from restored diagnostic stats and committed page
buckets. Operation time, retry/gate waits, rate-limit wall time and store.*
session calls enter it at their existing observation points. Connector calls,
connector.* session reports and connector-reported waits remain page-owned.
The ledger stop branch writes the whole run snapshot on the existing bounded
detached context. Loop-top Checkpoint still writes nothing.

TestLedgerRunAccountingAcrossAttempts initially failed because no store-session
observation reached the ledger. It now combines a committed connector-call and
connector-session page with run-level session calls and retry waits; stops
twice using an already-canceled context; closes/reopens; restores history into
a new attempt; adds a lower-latency failed session call; and stops again. Durable
counts sum once, session maxima remain correct, the timeout/error survives and
page counts do not duplicate. Initializing the run accumulator from restored
diagnostic stats fails the aggregate assertion. Copying page session reports
into it fails the connector-session count (four instead of two). Both mutants
were removed. Existing runtime tests separately refuse active-page flushes.

C19/C20/C22/C23 gain this coverage and remain incomplete to full crash products.
There is no claim that unflushed process observations survive process death.
Public Sync will put the same snapshot into its terminal page; that handover
is not yet integrated by this commit. No storage or token encoding change.

Review of the run-duration exits found two paths that call Checkpoint directly
instead of checkpointOnStop. TestLedgerRunAccountingDurationStop failed on both:
between actions and after an operation, stored run-session count was zero.
Both ledger exits now flush the run bucket on a bounded detached context and
retain ErrSyncNotComplete. Their checkpoint-path bodies are unchanged. This
is an observed omission found before public routing, not a planted defect.

Final checks (Go 1.26.0, vendored dependencies), including both run-duration
exits: full sync suite passed (77.995s); run-accounting, expansion, external,
existing-scheduler and counter race tests passed three repetitions (8.573s).
Vet passed, sync lint reports zero issues, and git diff --check passes.

## Collection report feasibility (CO-013, CO-014, C50)

Status: evidence incomplete. The report is a test-only prototype, not a public
reader, durable report or complete correctness check. The experiment and sample
output are in report-experiment.md. No production storage methods, metadata,
retention policy or scrub behavior changed.

TestLedgerReportPrototype commits real page units with synthetic data and timing:
complete two-page collection, zero-write terminal, absent continuation and absent
child. It checks counts, timing rank and missing references. A mutant ignoring
missing continuations fails. A mutant describing terminal rows as successfully
empty endpoints fails the required unknown-outcome assertion.

TestLedgerReportPrototypeScope checks both existing saved skip flags and unknown
request scope. A mutant ignoring should_skip_grants fails. This does not claim a
complete request manifest exists. TestLedgerReportPrototypeFullScope verifies
parent and type-scoped identities remain separate. Erasing the parent resource
from grouping fails. All four planted mutants failed assertions and were removed.

The restored tests pass. BenchmarkLedgerReportPrototype measures six cases from
1,000 to 100,000 pages, one or 100 pages per resource, three iterations each. The
final tests and benchmarks completed in 3.030s. At 100,000 pages the report takes
100.35ms without reference reads and 329.05ms with 99,000 continuation checks.
Cumulative allocation is about 92–98MB, not peak resident memory. Retained
aggregation is one current group plus ten ranked groups. This is warm-cache
synthetic smoke evidence; no final C49 cost claim. Output serialization, peak
RSS, cold reads, large fan-out, mixed data, distributions and production-scale
scope/outcome metadata remain unmeasured or unimplemented.

Engine-package lint reports the same six G115 findings in adapter_page.go and
ledger_cost_bench_test.go; none refer to the prototype. git diff --check passes.
The broader public-path migration remains uncommitted and is not validated by
these engine tests. C50 is not closed by this experiment.

## Mechanical timing artifact extension (C50)

Brief commit: 83455685. C50 remains evidence incomplete. No production code or
storage contract changed. The test-only Go generator emits HTML and JSON from
rows/facts; authored analysis is not used as report data.

TestLedgerReportPrototype now asserts total connector time, collection share,
median/p95 histogram intervals, writes per page and rates per 1,000 writes,
including undefined rates at zero writes. TestLedgerReportHistogram covers empty
input, zero, powers-of-two boundaries, nearest-rank rounding and uint64 maximum.
TestLedgerReportRendering checks repeatable bytes, HTML escaping, token omission,
undefined zero-output rates and a denominator larger than the displayed group.
TestLedgerReportRank checks descending timing and full-scope deterministic ties.
TestLedgerReportTopLimit checks eleven equal-time collections retain the first
ten by scope while the denominator still includes all eleven.

Planted defects, removed after assertion failures: floor percentile rank instead
of ceiling (Histogram); collection time as its own percentage denominator
(Rendering); ascending time ranking (Rank). The restored targeted tests pass
(0.046s). The combined tests/benchmark run before the final rendering whitespace
change passed in 3.404s. Cost results and limitations are in report-experiment.md.

The generated HTML was opened in headless Chromium and visually inspected. The
local HTTP response was compared with the generated file. HTML and JSON copies
are published as task artifacts. The original analysis artifact is not the
mechanical report. Engine lint retains six preexisting G115 findings; no new
report findings remain after wrapping the HTML template to the line limit.

## Stats-only serialization correction (C50)

Brief commit: e08fa808. Removed the HTML template and narrative outcome fields.
The prototype emits one JSON stats object with explicit identifier fields, numeric
rates/shares, percentile bounds and tri-state saved skip flags. No token field is
part of the serialized scope. TestLedgerReportRendering now decodes that object
and checks numeric share, null unknown flags/rates/quantiles, identifier round-trip,
deterministic bytes and absence of token fields/values. Existing coverage and
histogram tests remain; the former prose assertions were replaced by typed checks.

Targeted tests plus six JSON-output benchmark cases pass (3.315s). At 100,000 pages,
147.50ms and 340.97ms include aggregation, reference checks and JSON serialization.
Engine lint still has six preexisting G115 findings and no findings in these files.
Production logging integration and full C50 coverage remain incomplete.

## Single-walk stats and memory qualification (CO-016, C50)

Brief commit: 66e03d5c. C50 remains evidence incomplete for production integration.
The test-only aggregator now accepts only a forward iterator. One ledger-family
walk replaces page/child point reads and the all-facts map. The row projection
reads selected scalar/scope fields and skips child bodies and token strings.
Per-type aggregates stream to an optional error-returning sink; bounded log lists
carry omitted-group counts. Exact reference validity is unavailable, not zero.

TestLedgerReportSingleWalk supplies 48 pages across 12 types and 24 collections,
plus a flag fact. It requires one First call, 48 Value calls, 49 scanned keys,
correct collection/type aggregates, bounded top lists and unknown missing-reference
fields. The input has no point-read method. TestLedgerReportStreamErrors checks
iterator errors, sink errors, cancellation and malformed page bytes. Projection
checks normal/scrubbed records against known fields, unknown protobuf fields,
last-value semantics for repeated scalar fields and absent scrubbed hash evidence.
TestLedgerReportWidePageAllocations measures three allocations for both a narrow
row and a 5.4MB encoded row with 100,000 children.

Planted defects removed after failures: a second First call (SingleWalk); reading
the flag value (SingleWalk); treating absent scrubbed hash evidence as known
pagination (Projection); full protobuf decoding including all children
(WidePageAllocations). Existing timing/rank/rate tests continue to pass.

The restored targeted tests passed, including a final lint-only switch cleanup
(0.089s). Race checks passed three repetitions (1.880s). Engine lint has only the
six previously recorded G115 findings, with no new report findings. Diff checks
pass. No production pkg/dotc1z file changed in this step.

Benchmarks at 10,000 / 100,000 / 1,000,000 rows cover many resources, 100-page
collections, one long chain and many resource types. Final three-iteration timing
run passed in 36.113s. Million-row report times are 449–861ms. A separate sampled
memory run passed in 14.637s; million-row sampled heap peaks were 4.37–4.67MB,
post-GC heap 1.94–1.97MB, and total-process sampled RSS 226–280MB. This includes
Pebble's existing cache; during-scan RSS increases were 8.6–41.5MB. Timing runs
had no concurrent test/build workload from this task. The memory sampler was
only enabled in the separate memory run. Full numbers and limits are recorded
in report-experiment.md; these are synthetic smoke results, not C49 closure.

Uncovered: production summary/log publication, complete scope/outcome metadata,
record-family-sized adjacent SSTs, cold-cache qualification, whole-file lifecycle
failure cuts and default disposal. Exact arbitrary graph-reference validation is
excluded by the one-pass implementation rather than asserted from aggregate counts.

### CO-017 / C50: write-family report counters

Status: verified to stated coverage for the family-count slice; C50 overall
remains evidence incomplete. TestLedgerReportWriteFamilies checks two rows with
unequal family counts through projection, collection/type/global aggregation and
JSON, with one iterator walk and one value read per row. It fails on the prior
output (missing breakdown), a resources/entitlements swap, omitted group addition
and omitted global addition. All three planted defects were removed. Report tests
pass normally and with race detection, three runs. This does not verify production
emission, new page observations, disposal, or the remaining CO-017 fields.

### CO-017: retry observation storage

Status: verified to stated coverage for the storage fields, not full retry/report
integration. TestLedgerPageRetryObservations writes through PageWriter, reads the
row before and after token scrubbing, and checks that rows without the fields
remain marked unobserved. It failed before adapter conversion existed and with a
planted omission of ConnectorAttempts. The defect was removed. The full Pebble
suite passes (10.443s). This check does not cover process-crash images or report
serialization of these new fields.

### CO-017: retry observations on successful pages

Status: evidence incomplete for the full criterion. The worker scheduler test
TestLedgerRetryObservationsAcrossPages verifies two failed connector calls then
success, actual retry waits, separately observed rate-limit waits and reset on
pagination. TestLedgerExhaustedRetryHasNoRow verifies exhausted retries leave no
row. The tests reject planted reset-on-each-attempt, omitted-error and omitted-wait
mutants. All ledger sync tests pass (4.538s); race detection passes three runs
(37.338s). Cancellation during a retry wait, coordinator-only retries, restart
loss and concurrent connector wait callbacks still need targeted checks.

Retry follow-up: TestLedgerCoordinatorRetryObservations passes through the
coordinator's retry path. TestLedgerCancelledRetryDoesNotSurviveNewWorker cancels
backoff, verifies no row, then completes using a new worker and verifies that
uncommitted attempts/waits are absent. These are in-process tests, not process
crash evidence. Report projection/collection/type/global/JSON checks now include
attempt observations and explicit measured-page coverage; a planted omitted-call
projection fails. Report race checks pass three runs (1.902s).

### CO-017: collection response and exclusion observations

Status: verified to stated coverage for collection counters; C50 remains evidence
incomplete. PageWriter consumer round-trip checks all fourteen unequal fields
before/after scrub and distinguishes absent observations on older rows. It failed
before read conversion existed. TestLedgerCollectionReceivedAndExcluded covers
resources, entitlements, explicit type selection and invalid omissions.
TestLedgerEmptyResponseDiffersFromFilteredResponse distinguishes empty pagination
from nonempty all-filtered input. TestLedgerCollectionGrantDerivedExclusions
separates received grants from derived-resource exclusions. Projection/group/global
JSON checks preserve all counters and measured-page coverage. Planted omitted
received counts, inverted empty-response classification, omitted selection count
and omitted exclusion projection each fail their owning tests. Defects removed.
Focused sync race checks pass three runs (2.143s). Full Pebble tests pass (11.034s)
and ledger sync tests pass (5.831s). Sync and Pebble lint pass with zero issues;
three preexisting benchmark conversion warnings now state their numeric bounds.

These counters cover the named branches, not all connector-internal filtering or
all SDK annotation transformations. Full-sync overhead, production emission and
safe report/options persistence before ledger disposal are not yet verified.

### Report reader and option snapshots (CO-017, CO-018)

Status: evidence incomplete for C50 as a whole. The production report reader
is verified for read-only generation, commit/reopen/scrub stability, and fixed
scope previews. `TestLedgerReportOptionsBoundedProjection` projects 100,000
selected types to sixteen examples and a total; unknown fields are not emitted.
`TestLedgerReportOptionsCommitWithPage` proves a failed page leaves no snapshot,
and the committed retry saves requested flags separately from effective facts.
Omitting snapshot staging makes that test fail. Returning an empty generated
report makes the store consumer test fail. Both defects were removed.
`TestLedgerCanonicalOptionsPreserveFlags` permits duplicate attempt snapshots
but rejects equivalence when a skip flag changes, and rejects unreadable JSON.
The differential uses an uninterrupted reference with matching worker options.

Focused ledger tests pass in sync, Pebble and dotc1z. Retry timing now includes
failed calls preceding success; concurrent wait callbacks remain worker-local.
Automatic publication, option-history preservation across disposal, phase timing,
debug reference validation and default disposal are not closed by these tests.

### External-page grant overlay (C08, C38, C42)

`TestPageGrantIteratorMatchesCommittedSelection` passes for ordered insertion,
replacement (last staged identity wins), stored and staged deletion, and expansion
annotation round-trip. A stored-only iterator mutant fails on the first expected
principal. The mutant is removed. Full Pebble tests pass (10.757s).
The iterator uses the engine's primary identity encoding and keeps staged rows
plus one stored page in memory. Broader iterator scaling and every expansion-state
combination are not established by this consumer fixture.

### External import/matching atomicity correction

The public `TestChaosConnectorExternalPrincipalResumeUsesCurrentExternalAnswer`
failed against the two-page implementation: matching reused imported principals
from the earlier attempt instead of refreshing the changed external source.
Import and matching now share one page. Matching uses the staged grant iterator
and staged entitlement reads. Existing external parity, imported-carrier, stale
re-import and full-identity-delete tests pass; the public changed-source corpus
passes after migration to staged delete faults. The previous two-page design is
the planted defect. No changed-source expectation was removed.

### Compact archive before disposal (CO-017, C31, C50)

Status: evidence incomplete for the full disposal lifecycle. The archive API's
consumer tests pass: report and complete options survive drop, Close and reopen;
restoration reproduces facts and counters once, preserves timestamps, and leaves
a nonempty later ledger unchanged. Missing restoration makes
`TestLedgerArchivePreservesFinishedState` fail. The defect is removed.
`TestLedgerArchiveFailureCuts` refuses before/after the archive write without
deleting ledger rows, and injects a failed restore batch before successful retry.
Unreadable archives do not write. Compacted base renames retain prior state;
unexplained ID mismatches refuse it. A new sync removes the archive.
`TestLedgerArchiveKeepsCollectionAcrossProcessing` preserves one prior collection
summary through repeated expansion-only passes without recursive report history.
These checks do not yet establish default syncer disposal, debug reference checks,
archive I/O crash images, or the final end-to-end cost table.

### Public Pebble routing and existing-corpus migration

Full `go test ./pkg/sync -count=1 -timeout 30m` passes (80.020s). Pebble tests
pass (11.897s); synccompactor passes (35.622s). Focused ledger, changed-answer
resume and session-stat tests pass under race detection three times (35.595s).
Chaos tests pass with strict page-write hooks (45.641s). These are local runs,
not unloaded-machine cost evidence and not the full physical-crash product.

The public path always selects ledger for Pebble and refuses engine/capability
mismatches. SQLite keeps its token path. Existing Pebble tests now read sealed
stats rather than decoding an empty token. The cut harness cuts actual page
commits, counts pending spawned work from the ledger walk, and rejects any token
write; it no longer silently sweeps zero checkpoint cuts. Test-only stores state
the engine and page capability they represent.

The warn-retain lifecycle corpus failed before terminal ingest facts and flags
were saved with seal readiness; it passes with that correction. Duplicate page
arrivals in the changed-answer sweep failed with a busy-page error before the
identity claim. `TestLedgerDuplicatePageWaitsForCommittedRow` fails when the claim
is removed and passes with one handler execution; cancelled waits release cleanly.
No alternative scheduler was introduced. Final-data comparison for conflicting
sibling writes uses an uninterrupted run with the same commit order.

Default report publication/deletion is not enabled by this increment. Public
retention tests still exercise retained ledger rows until the next lifecycle step.

### Default report disposal and debug checks (CO-017, C31, C50)

Default public Sync saves a compact archive, logs the same structured JSON and
then drops the ledger. Skip-full-sync saves its options as well. Debug retains
rows and performs explicit child/continuation reference checks; tokens are
scrubbed unless retention is explicitly requested in debug mode. Reference
examples are limited to sixteen and contain no cursor or cursor hash. No
inference about absent upstream data is made from an empty collection.

`TestLedgerDebugReferenceChecksSurviveScrub` fails with debug validation disabled
and passes after restoration. It checks missing children and a missing next page,
a valid reference, bounded examples and token nondisclosure before/after scrub.
`TestLedgerDefaultReportDoesNotCheckReferences` verifies the default omits indexed
lookups. `TestLedgerReferenceTargetSkipsChildPayload` checks a thousand references
to a row containing a thousand children; target decoding projects only identity
and scrub state. This is a structural complexity check, not a scale benchmark.

`TestLedgerPublicSyncSealsWithoutToken` fails when disposal is disabled.
`TestLedgerPublicArchiveFailurePreservesRows` fails when archive-error handling
falls through to disposal. Both mutations are removed. Public debug/no-scrub and
rejected-no-scrub tests pass. Finished continuation after disposal/reopen restores
prior skip facts without collecting again; binding preserves the original start
and end stamps, while the later successful seal updates ended_at as on main.
The preceding collection summary remains in the saved artifact.

Full sync tests pass (81.795s); focused public/finished/seal-ready tests pass
(0.743s), and race detection passes those tests three times (4.985s). The full
Pebble suite passes (17.237s). Lint reports zero issues. These local results do not
close physical crash images or the C49 unloaded-machine cost matrix. The default
currently scrubs at seal before disposal; the additional scrub/purge work must
be included in the final cost report. No claim of a scrub-free disposal path is
made.

### Archive crash images and effective retention options

`TestLedgerArchiveDurableCrashImages` opens five durable-only crash images:
sealed before archive, archived before disposal, disposed, restore stamp before
batch, and restored. The report survives once archived; archived facts/counters
restore once after disposal; a failed restore before its batch can retry. Sync
completion and empty token remain intact. Replacing the archive's synced metadata
write with NoSync makes the archived crash image lose the report and fail. The
mutation is removed. This adds physical durability evidence for these five cuts;
it does not cover every archive I/O error or all P7 cells.

Saved requested debug/retention values remain the caller's options. Separate
effective fields record debug logging and a prior durable retain-token declaration.
The public resume test verifies an inherited declaration remains effective even
when the resumed invocation does not request it again. Resolved debug policy does
not mutate syncConfig. Focused policy tests pass (0.311s) and lint reports zero
issues. `TestLedgerPublicLogsSavedStats` checks the log's structured JSON equals
the saved artifact.

### Public-Sync cost smoke (C49)

Status remains evidence incomplete. `cost-public-smoke/` contains 36 interleaved
samples against eb63f1b5 with actual production handlers, default report/disposal,
verified output counts, binary hashes and machine inputs. Fresh wall ratios range
0.941–1.199 and resumed ratios 1.400–1.673 in these four small-page configurations.
Report generation/archive is 1.7–10.6 ms; the resume walk is below 0.1 ms. Separate
CPU samples locate most of the resumed increase in storage reads. Tripwires and
limitations are recorded in implementation.md §49.1 and the table README. No
unloaded-machine, full-matrix or production-size acceptance claim is made.

### Final disposal increment checks

At 6c8e2209, the full sync suite passes (88.146s), full Pebble passes (9.641s),
and synccompactor passes (19.790s). All TestLedger tests pass under race detection
three times (35.158s); broad sync/dotc1z lint reports zero issues. A public-Sync
fixture exports the saved JSON artifact after verifying 1,000 resources and an
empty ledger. The optional export is test-only. These checks do not change the
remaining coverage/product and cost qualifications stated above.

### Recorded phase elapsed projection (C50)

`TestLedgerReportRecordedPhaseElapsed` checks persisted coordinator durations
across two attempts, repeated bucket overwrite, a five-second page duration that
must not replace 150 ms of coordinator time, and 2,000 excluded retry labels.
Disabling the counter-bucket projection makes the expected phase map absent and
fails the test; the mutation is removed. Malformed, negative and overflowing
values are rejected. The report retains only eight supported collection phase
names. No scheduler timing, SQLite path or additional dataset walk is introduced.
Unrecorded abrupt-crash time and untimed stages remain absent, not invented.

The one-million-row, three-iteration component check in report-phase-memory.txt
measures 0.706s for many resources, 0.622s for one chain and 0.865s for many types.
Sampled Go heap peaks are 4.40–4.81 MB; process RSS peaks are 288–385 MB including
Pebble and its cache. Cumulative allocations are 33–74 MB per report, not retained
heap. These are warm-cache local component results, not C49 end-to-end evidence.
The full Pebble suite passes (8.785s) and broad lint reports zero issues for the
phase projection increment.

### Debug reference lookup complexity correction (C50)

The first target-identity projection was insufficient: its field walker revisited
all repeated child fields for every incoming reference. This allowed O(references
× target children) work. The small fan-in fixture's counts did not detect it.
The corrected scanner validates each stored row's identity once and gives reference
lookups an interface with SeekGE, Key and Error only. Target values are unavailable
through that interface. A separate counted source iterator verifies one Value read
per stored row. Pebble's indexed key/block lookup cost remains in the debug path.

`TestLedgerReferenceFanInReadsEachRowOnce` observes 1,001 source-value reads and
2,000 indexed seeks. Planting a per-reference value reread produces 3,001 reads
and fails the test. The mutation is removed. Identity mismatches count stored
rows once, independently of incoming edge count; a ten-reference malformed-target
fixture verifies that rule. Missing-reference, scrub and token-free-example checks
still pass. Focused tests pass (0.058s), and broad lint reports zero issues.

### Attachment and empty-stop consumer checks (C01, C40)

TestLedgerPublicEngineAttachment compares every key/value and write-recorder
length before/after all eight engine/capability combinations. Removing the
Pebble capability guard fails the refusal case; the mutation is removed.
TestLedgerPublicPathAttachment loads both real engines through WithC1ZPath and
checks the selected path. The public cancellation-after-walk test failed on an
attempted empty run-bucket write before the IsZero guard. Nonempty observations
remain durable under the existing repeated-flush tests. Focused checks pass;
public/run-accounting race checks pass three times (4.122s), lint has zero issues.
The repository-wide build and Baton command tests also pass (0.923s for commands).

After the empty-stop guard, the full sync suite passes (81.140s). The latest
storage projection/reference revision passed full Pebble (9.086s), targeted
storage race checks three times (1.618s), and the preceding full sync run
(82.627s). No mutation remains in the working tree.

### Executed-test and source inventory at d891604d (C48)

`executed-coverage-d891604d.json` is generated by tools/coverage-summary.py from
full `go test -json -covermode=atomic` runs of pkg/sync and the Pebble engine.
The runs pass in 81.539s and 8.976s respectively. The manifest records 1,847
passing test results (including subtests), 33 skips and no failures. It lists
uncovered statement ranges in changed production files and separately lists
changed files outside those profiles. Package statement coverage is 82.2% and
74.9%; neither percentage proves branch, crash-cut or Cartesian-cell coverage.
This is not closure of C48. The manifest predates the session cases below.

The summarizer rejects incomplete event streams and overlapping file profiles.
An independent profile fixture with three uncovered and two covered statements
produces exactly those totals; an empty event stream is rejected. No test-log
messages, local paths, machine identifiers or credentials enter the artifact.

### Session writes through a failed page (C38)

TestLedgerSessionMutationsSurviveFailedPage exercises Set, SetMany, Delete and
Clear through the session wrapper with the strict write recorder. Each case
stages a resource type, performs its session mutation, then fails the page.
After close/reopen, the requested session mutation remains, a different prefix
is unchanged, and neither the staged resource type nor its ledger row exists.
Clearing the bypass reason makes all four cases fail at the direct write; the
mutation is removed. The restored tests pass, including race detection three
runs (1.804s); broad sync/dotc1z lint reports zero issues.

This covers handler failure plus orderly close/reopen. It does not claim abrupt
process-death or physical-loss coverage for sessions, or prove the public
loadStore wiring. Those C38 products remain incomplete. Production code is
unchanged by this verification increment.

### Current environment accepted for C49 (CO-019)

The requester accepts this machine; unloaded-machine qualification is no longer
a blocker. At 0618510e, 18 interleaved public-Sync samples cover 1,000 pages ×
1,000 records with one/four workers and three repetitions. All verify one million
resources and, for ledger arms, saved report plus default disposal. Results,
executable hashes, machine snapshot and metric ratios are committed under
cost-current-machine-r1000. Fresh wall ratios are 1.093/1.179; resumed/fresh-token
ratios are 1.422/1.472. The latter include reopening/existing-store effects.
Report generation remains below 3 ms at these sizes. The full C49 matrix, byte
attribution and production-shaped estimate remain incomplete; no acceptance is
claimed. The host concession does not waive those obligations.

### Public Sync process-crash recovery (C04, C09, C10, C31, C33, C50)

TestLedgerPublicCrashResume executes 22 cases: seven process-exit cuts with one
and four workers, plus four pre-seal cuts with explicitly flushed history for
both worker counts. Cuts are before/after an actual resource page, before/after
the terminal page, after sealing, after report archival and after disposal.
Each child proves its cut with an exit code and marker. Recovery transports the
surviving Pebble directory into a new envelope and asserts all keys unchanged.
Public Sync resumes the same ID with strict write hooks and a key-equal read-only
walk. Every missing resource page runs once; surviving pages must not reach the
connector. Final resource identities/payloads, completion, empty token, saved
report and empty ledger facts are checked. Finished cases request expansion-only
processing; they do not change baseline lifecycle semantics.

The first fixture hit the generic resource-planning page, not a connector page.
A missing-resource-work mutant survived that weaker fixture. The corrected cut
requires a resource-type identity, and the flushed arm requires committed history
in the recovered image. Suppressing absent resource actions now fails with zero
connector calls where six are required. The mutant is removed. No production code
was changed. Full sync passes (87.621s); the final expansion-only fixture passes
focused tests (1.052s), race three times (12.796s) and broad lint (zero issues).

These are process-exit and explicit-flush images, not physical WAL-loss injection.
The connector fixture covers resource types and resources, not all record families.
It checks deterministic records and transport equality, not complete uninterrupted
versus resumed logical/index/report equality. Legacy takeover and the rest of the
mechanical products remain incomplete. These results do not close C04/C16/C48.

### Public legacy takeover process recovery (C24–C26, C28)

The public crash fixture now has 40 cases, including 18 takeover cases:
V0/V1/V2 × one/four workers × before takeover, after takeover and after the
first post-token resource commit. Explicit fixture JSON describes two pending
resource actions at token 1. Four pre-token resource records are seeded through
token-path writes. Before takeover the token is intact with no frontier; after
takeover it is empty and the frontier contains the exact original JSON after
process exit and envelope transport. Seeded pages have no ledger row and cannot
be fetched again. Post-token committed pages also cannot repeat; missing pages
run once. Final accounting is four completed actions: two imported and two newly
completed actions, including when a post-token page survived the crash.

Erasing PageToken in decodeLedgerCheckpoint fails each of V0/V1/V2 after-takeover
cases on an attempted pre-token resource call. The mutation is removed. Focused
tests pass (1.762s), race detection passes three runs (21.012s), and broad lint
reports zero issues. No production code changes were needed.

This adds public cursor migration and accounting evidence; it does not cover
storage's internal stamp-only cut (C27), every parent/type-scoped/graph/fact
combination, every counter family or physical-loss products. C25/C26 remain
incomplete at their full stated coverage.

Full sync suite after the takeover increment passes (82.714s).

### Ten-million-record cost extension (C49)

Six samples at a3863c3d verify ten million records each. Single-worker fresh/token
wall is 1.541 and resumed/token 1.490; four-worker ratios are 1.055 and 1.098.
Report generation is below 11 ms. The single-worker increase is a tripwire,
not accepted overhead. The README and metric table preserve the per-arm
decomposition and one-sample limitation. The full matrix remains incomplete.
