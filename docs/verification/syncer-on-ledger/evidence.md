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
- Tests run: TestLedgerRuntimeCrashProcess; TestLedgerPublicCrashResume; TestLedgerPublicFamilyCrashDifferential.
- Coverage: runtime process cuts plus public resource-page/terminal cuts with one/four workers, ordinary WAL recovery and explicitly flushed committed history. Public recovery compares transported raw keys and exact final resource identities/payloads.
- Planted defect: missing-resource-action walk mutation fails the public fixture; this is not a separate torn-record/index mutation.
- Green command/revision: K2b and public process-recovery entries in the archived execution log.
- New coverage: 40 public process cuts across five populated collection families, WAL/flushed images and changed worker counts; exact record/index/digest comparisons after save/reopen.
- Added final verification: TestPublicLedgerDurableCrashImages covers 80 durable-only images across full, targeted and asset work plus terminal proof; TestPublicLedgerTargetAssetProcessCrashes adds 16 real process exits.
- Not covered: every P1/P6 feature cross and every in-batch I/O failure through the public entry. MemFS power-loss simulations run on Unix; real-filesystem process tests also run on Windows.

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
- Coverage: full-identity external deletion preserves an unrelated grant with the same external ID. CO-023 restores main's full-identity batch deletion outside pages; the CO-002 staged/stored split is no longer an external-import page case.
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

- Status: evidence incomplete.
- Tests run: TestLedgerPublicFamilyCrashDifferential; TestLedgerPublicCrashResume; TestLedgerPublicCancelledAfterWalkWritesNothing; TestLedgerWriteInstrument.
- Coverage: public process resumes bracket the walk with a rejecting write recorder and exact key/value snapshots, including 40 multi-family cases and changed worker counts. Lifecycle writes outside the walk remain separate under CO-004.
- Planted defect: the existing write-instrument mutation removes the walk prohibition and is rejected; the stop-before-page counter-write defect is recorded under C40. No new walk mutant was added in this audit.
- Not covered: early stop after every visited row in P8 and every session/bypass combination.

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
- Tests run: TestLedgerResumeLogicalDifferential; TestLedgerPublicFamilyCrashDifferential.
- The public fixture compares exact primary-record, index, counter and digest key/value bytes across process death and resumed Sync, including save/reopen. It paginates resource types, resources, static entitlements, entitlements and grants; crosses before/after a populated continuation commit, WAL/flushed recovery, and one/four workers (40 cells). Resume switches worker count. Committed action and connector-call accounting also agree.
- The original public test exposed an ingest-quality mismatch after all initial pages were lost. CO-024 fixes it with read-only detection of an unfinished sync without surviving collection history. The test now requires exact ingest-quality equality in all 40 cells; its temporary unknown-quality exception is removed. Existing records, legacy token/frontier, archived history and finished runs cannot use the empty-start exception.
- Only duration fields are normalized in the public accounting comparison. Primary record timestamps are held equal by deterministic test time, not stripped. Raw artifact hashes are logged without claiming byte equality. Runtime-generated metadata, report options and report rankings are not part of this raw-family comparison.
- Planted defect: suppressing PutGrants in the production collection handler causes the independent expected-grant assertion to fail (expected 4, actual 0); restored. The earlier resumed-worker accounting mutation remains recorded in the archive.
- Added final verification: targeted/asset process exits and durable-only images now compare complete primary/index/digest bytes and structured stats through the real adapter. The older 40-case fixture still supplies exact ingest-quality comparison.
- Not covered: all feature combinations and the full mechanical product; the different crash instruments are not interchangeable.

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

- Status: evidence incomplete.
- Tests run: TestLedgerPublicFamilyCrashDifferential; TestLedgerTakeoverCountersImportedOnlyWhenAbsent; TestLedgerCanonicalFoldsCommittedBuckets.
- Coverage: exact completed-action and connector-call totals across one-to-four and four-to-one worker process recovery; takeover import suppression and bucket folding have separate fixtures.
- Planted defect: the existing disabled takeover-counter guard duplicates historical totals and is rejected. The new process matrix adds no separate bucket-index mutant.
- Not covered: all P5 fields and combinations, particularly nonzero errors/timeouts and latency maxima during these public process cuts.

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

- Status: evidence incomplete.
- Tests run: TestLedgerTakeoverCrashImages; TestLedgerTakeoverIsOneUnit; TestLedgerPublicCrashResume; TestLedgerTakeoverLegacyFixtures.
- Coverage: durable-only pre-stamp, post-stamp/pre-batch and post-batch images preserve the original token or the complete frontier/facts/counters. A stamped token-only image retries takeover; an old SDK refuses its layout. Public V0/V1/V2 cases kill the process before/after takeover and after a subsequent resource commit, with one/four workers. Repeated reads of the migrated frontier leave keys unchanged.
- Failure evidence: injected takeover-batch failure leaves the token intact. No new independent token-clear mutant was run in this audit.
- Not covered: the full version × process identity × retention product at the intermediate stamped boundary in the public syncer fixture. Storage crash images and public version tests cover complementary parts of that product.

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

- Status: evidence incomplete.
- Tests run: TestLedgerDiscardFailureRetriesSeal; TestLedgerDiscardBatchFailure; TestLedgerDiscardDurableSealCuts; TestLedgerDiscardUnfinishedArchiveResumesWithoutCollection; TestLedgerDiscardPendingFinishedBindingDoesNotRestartProcessing; TestFailedSealDropsItsStatsOverlay.
- Coverage: archive/discard/purge/stamp/final-marker errors and recovery, including old finished timestamps; a later engine seal consumes its own stats rather than the failed attempt's overlay. Consumer recovery forbids recollection/restarted processing.
- Planted defects: earlier pending-seal restart and empty-report replacement mutations are rejected, as recorded in the disposal entry. The new fallback assertion requires successful archive-failure sealing to clear its pending declaration.
- Not covered: the complete invariant/cleanup/seal P7 cross-product and a public consumer injection into the engine stats-overlay failure itself.

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

- Status: evidence incomplete.
- Tests run: TestPublicLedgerDurableCrashImages; TestPublicLedgerTargetAssetProcessCrashes; handler failure/read-through tests.
- Coverage: targeted resource and asset actions through public Sync with the production store adapter; before/after commits, lost unsynced state or flushed prefixes, and one/four workers exchanged at recovery. Assets are reached through a legacy pending action because normal Init does not enable asset collection.
- Outcome: exact record/index/digest bytes and structured stats match uninterrupted runs. Report presence and default ledger disposal are checked. The hook rejects unregistered direct page writes.
- Not covered: every parent/scope/feature combination. External import remains outside pages under CO-023; its known baseline replay limitation remains separately documented.

### C43

- Status: evidence incomplete.
- Tests run: TestPublicLedgerDisposedFilesCompact; TestPublicLedgerArchiveFailureKeepsDataAndStats; TestPublicLedgerDurableCrashImages.
- Coverage: real saved full/partial SDK artifacts after default disposal feed both overlay and fold compaction. Output record counts, primary/index/digest bytes agree. Structured stats readers also agree after public crash recovery and after archival failure retains scrubbed history.
- Not covered: the complete scrubbed/retained/debug × compactor strategy product and injected stats-sidecar I/O degradation in this public fixture. Storage-side sidecar degradation tests remain separate.

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
- Not covered: full changed-branch-to-criterion mapping, complete mechanical-cell execution manifest and final independent audit. The six-seed scheduler soak and full cut enumeration have now run; see the final recovery audit entry. Statement coverage is not closure of those obligations.

### C49

- Status: evidence incomplete.
- Runs: TestLedgerCostBaseline at eb63f1b5; TestLedgerCostPublic with production collection handlers/default disposal; earlier synthetic scheduler samples remain historical.
- Coverage: 60 public-path interleaved samples across eight configurations; three repetitions per arm in six configurations and one in the two ten-million-record configurations; tables, machine inputs and binary hashes in cost-public-smoke/, cost-current-machine-r1000/ and cost-current-machine-10million/. Separate CPU profiles accompany the first set.
- Defect evidence: the original machine recorder omitted artifact filesystem; its assertion failed before correction. Public samples assert output resource counts, archive presence and ledger disposal.
- Not covered: full matrix, baseline phase timing, encoded-byte decomposition, production-shaped estimate. Collection performance is accepted under CO-020;
  the latency measurements are requester-reported, not independently rerun here. CO-012 settles actual NoSync resume; CO-019 accepts the current shared machine.

### C50

- Status: evidence incomplete.
- Tests run: ledger report projection/scale/observations/options suites; TestLedgerPublicLogsSavedStats; TestLedgerPublicReportAccessFailureKeepsSavedReport; archive reopen/crash tests; debug reference tests.
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

Debug-report follow-up: the public archived-report check initially reported one
missing child for expansion. It now excludes expansion's phase-frontier entry
from page-row lookup. The consumer asserts reference validation actually ran,
while existing missing-child/continuation and fan-in checks stay enabled.
Focused sync/report race checks pass three times (6.715s / 2.193s); broad
lint reports zero issues after this correction.

## External transaction removal (CO-023)

TestLedgerExternalImportsPagesDirectly fails on the old transaction because the
destination is still empty when the next source grant page is requested. With
main's handler restored, each import page is stored first and no external action
row is created. Matching/deletion one-pass comparisons remain; transaction-only
fixtures and the unused staged iterator/deletion APIs are removed. Chaos faults
again intercept direct store writes. Local-phase completion joins expansion in
the terminal run bucket. The independently reproduced upstream replay defect is
not fixed or hidden by this change; its investigation fixture is outside this PR.
Validation after external transaction removal: full sync 96.346s, Pebble
23.082s and compactor 42.091s pass; focused external/chaos race checks pass
three times in 10.442s.

## Single-purge disposal (CO-022)

The old default sequence fails TestLedgerDiscardSealPurgesOnce with two purge
invocations. The new sequence archives before deletion and purges once; zero
scrub time, one archive write, empty final ledger and physical token erasure are
asserted. Later report access reuses the saved report. Archive-write failure
retains scrubbed rows and does not add a second purge.

TestLedgerDiscardDurableSealCuts captures eight durable-only VFS cuts for both fresh
and finished bindings: before/after archive write, deletion, purge, before
ended_at, before/after declaration removal and final completion.
Records/report/counters survive and retry preserves the nonempty report. Purge
cancellation, finished-stamp failure and declaration-removal failure are separately
retried. An unfinished
foreign archive cannot donate completion facts. Before-ended recovery initially
allowed a checkpoint token because both rows and the in-flight flag were gone.
Keeping the token-free discard declaration until finalization completes closes
that gap and prevents finished-binding retries from starting another pass.

The public consumer constructs a matching post-disposal/pre-ended file, reopens
it and seals without connector collection calls. This consumer fixture is not a
new physical process cut; the storage VFS cases exercise those durable images.
Skipping archive restoration, restarting a pending finished-binding pass, and
replacing the cached report with an empty projection each fail their tests; the
mutants are removed. Both finalization batches also have direct commit-failure
tests registered in the mechanical commit-point inventory. The public process
suite now has 38 cases: the separate post-Drop cut was removed because disposal
occurs inside seal. Its existing post-seal cut now sees an archived, disposed file.

Validation: full sync 101.792s, Pebble 15.778s and compactor 44.028s pass.
Broad recovery race checks pass three times (47.707s / 6.248s); final disposal
race checks pass three times (2.651s / 3.650s). Broad lint has zero issues and
repository-wide build passes. Four small public benchmark cases verify records, zero scrub and
nonzero report/disposal timing. These are harness checks, not new performance
acceptance measurements. Other unexecuted criteria remain incomplete.


## Review cleanup and comment audit

The comment audit covers hand-written comment additions against `eb63f1b5`,
including changed blocks in existing files, Go tests, proto sources and harness
scripts. Generated protobuf comments are generator output, not edited separately.
The starting inventory has 98 comment-bearing added lines: 68 package lint
directives, five interpreter directives, five numeric lint suppressions and
20 lines of prose. After cleanup, 13 added prose lines remain.

| Category | Disposition |
| --- | --- |
| Test narration and call-site explanations | Delete the checkpoint-cut overview, baseline narration and graph-resume reminder; test names/assertions and the token restore code carry these facts. |
| Repeated contracts and implementation descriptions | Delete the file-level ledger introduction and disposal-constant description; the commit and seal method contracts own these facts. Remove the finalization step list. |
| Public behavior not expressed by signatures | Keep asset snapshot/overwrite semantics, archived-report reuse, archive restoration conditions, ClearLedgerRows atomicity/preservation, and seal archival-failure behavior. Shorten DropLedger to what it preserves/removes. |
| Non-obvious internal constraints | Keep the record-batch membership constraint, local-phase reference exclusion and failed-attempt scope of LastSealCost. Reduce finalization's comment to its required sealed state and error recovery. |
| Tooling directives | Keep interpreter and package-name directives. Delete four unused numeric suppressions. Replace the fifth's signed-count assumption with a helper that takes the response list and derives its length. |

No new scheduler, storage abstraction or lifecycle rewrite is justified by these
comments. The small list-helper change makes a previously documented assumption
structural. Historical baseline comments outside changed blocks remain untouched.

`LedgerDiscard` now measures deletion batches only, including the final declaration
removal; `LedgerPurge` measures compaction separately. `disposal_ns` and
`seal_purge_ns` follow those meanings. They can be added without double-counting,
although both remain components of the enclosing `seal_ns` measurement. Debug
warnings state the report/reference failure without implying it controls retention.

CI used golangci-lint 2.13.2, while the earlier local zero-issue results used 2.9.0.
The newer findings were test-path taint checks, dummy cursor strings classified as
credentials, and unused suppressions. Test file writes now use directory-scoped
`os.Root` operations; the report exporter has one targeted G703 exemption for
creating the directory explicitly selected by the test runner. No production
security checks or repository-wide lint rules were disabled.

Validation for this cleanup: full sync suite passes (210.017s), full Pebble suite
passes (17.088s), and affected collection/resume race tests pass three repetitions
(16.855s). Public cost-driver and report-export smoke tests pass. The baseline
harness builds against `eb63f1b5`; baseline and ledger smoke arms each verify 100
resources and write their output files. These are functional harness checks, not
new performance measurements.

Repository-wide lint passes with zero issues using golangci-lint 2.13.2 and Go
1.27.1 on GitHub's merged PR tree (`6dd330e7`) plus this cleanup. Running that
linter against the unmerged historical branch instead reports six findings in
unchanged baseline files; those files are not modified to accommodate the newer
linter. The merged-tree check matches CI's source and toolchain configuration.


## Final recovery audit execution

The public family fixture executes 40 process-crash cases. It uses populated
continuation pages, both sides of commit, WAL/flushed images, and resumes with the
opposite worker count (one/four). Exact primary record bytes include discovered
at timestamps; deterministic time holds those equal. The comparison also includes
all secondary-index, record-counter and digest bytes present in this fixture,
checks them after artifact save/reopen, and compares the stats passed to
EndSyncWithStats with duration fields normalized. Every fixture installs the
write hook and rejects writes during the resume walk.

The fixture independently requires two resource types, four resources, twelve
entitlements, four grants, four principal-index entries, four grant-content-hash
index entries, and a nonempty digest. Suppressing production PutGrants fails that
check (expected four, observed zero). Deleting an index entry from the recovered
comparison image fails the byte comparison; that is qualification of the
comparison, not a separately induced on-disk index corruption. Both temporary
mutations are removed.

Strict ingest-quality equality initially failed in all 20 unflushed cases. The
quality fact disappeared with the initial pages; the normal fallback marked the
prior quality unknown and blocked source-cache replay. The token path on baseline
has the same fallback. The test now explicitly asserts that current behavior when
the recovered known-quality fact is absent, and exact quality equality when it is
present. This exposed a contract difference, not an allowed normalization. CO-024 below
resolves it through read-only empty-state detection; it adds no durable provenance
marker. The temporary test exception described here has been removed.

The full opt-in cut sweep (`BATON_TEST_NIGHTLY=1 BATON_CUT_SWEEP=full`) runs
52 commit cuts, 45 response cuts and 45 expiry cuts, including repeated cuts and
worker-count changes. It passes in 15.60s. The six fixed-seed scheduler soaks pass
in 0.87s. These fixtures supplement process death; their cancellation/expiry cuts
are not relabeled as power-loss tests.

The audit also found an immediate purge in ClearRows followed by another purge
at seal. ClearRows now leaves its durable pending-cleanup marker for seal rather
than compacting immediately. TestLedgerClearRowsDefersPurgeUntilSeal fails before
this change (one premature purge), then passes with zero purges at clear, one at
seal, and neither old nor new token bytes in the saved checkpoint. The three
ClearRows durable crash images now assert that the marker survives and is consumed
by a later seal. Archive-failure fallback separately asserts that successful
finalization removes its pending disposal declaration.

Before the purge deferral, lifecycle/scheduler/takeover/disposal race checks pass
three repetitions (sync 16.607s; storage 5.568s). The new family crash matrix and
archive/clear/overlay checks also pass three race repetitions (sync 52.648s;
storage 3.974s). Post-deferral validation is recorded below.


Post-deferral validation: full sync (124.875s), Pebble (30.865s) and compactor
(73.078s) suites pass. Finished-continuation/archive/disposal race checks pass
three repetitions (sync 5.974s; storage 10.032s). The final public family test
passes with verbose artifact-digest output (3.301s); its subprocess output is also
checked for race-detector warnings. CI-equivalent repository lint with Go 1.27.1
and golangci-lint 2.13.2 reports zero issues. The then-open C16 quality mismatch is
resolved by CO-024 below.


## Empty-start quality recovery (CO-024)

The strict public quality comparison fails before the fix: the resumed artifact
has SourceCacheReplayBlocked=true and UnknownPriorCheckpoint while the
uninterrupted artifact does not. The temporary equality exception is removed.
All 40 process-crash cells now compare quality exactly, in addition to records,
indexes, digests and committed accounting.

BoundSyncUnstarted holds the binding/write locks while reading the sync record,
checking archive absence and seeking three collection-state key ranges. It does
not iterate collection rows or write anything. Normal fresh starts and resumes with existing
facts/accounting do not need the query. An empty resumed run establishes clean
quality in memory; Init saves that fact atomically as before. Session state and
the compatibility stamp alone do not imply prior collection.

TestBoundSyncUnstarted covers every primary family, indexes, counters, sessions,
digests, source-cache and ledger state; legacy token and migrated frontier;
archive presence; finished/no binding; stamp-only state; cancellation, corrupt
sync metadata and a closed engine. TestLedgerEmptyStartQuality exercises the
public store capability through restoration, including existing resource/grant
records, token/frontier, known clean/blocked quality, finished state and a query
error. Read-only cases compare raw snapshots under the rejecting write recorder.
The clean fact is absent before Init and survives save/reopen after Init.

An omission mutant removed the primary-record range from the query. Five storage
family cases and the consumer's surviving-resource case reject it. The mutation
was run in an isolated checkout and removed. The read capability and lock-holder
inventories include the new method; the structural lock test caught its initially
missing registry entry. No schema, durable marker, token parser, or SQLite path
change is made.

Validation: full sync 118.552s, Pebble 19.336s and compactor 36.538s pass. Focused
empty-start/legacy/finished/public-crash race checks pass three repetitions
(sync 84.638s, storage 2.635s). Final query/consumer/structural checks pass
(0.474s / 0.578s). CI-equivalent Go 1.27.1 / golangci-lint 2.13.2 lint has zero
issues. C16's broader unexecuted products remain evidence incomplete; this closes
the specific quality mismatch found by the public family fixture.

The storage-query commit `5be2f4b8` builds independently in a detached checkout;
its storage-query and public capability-inventory checks pass (0.194s / 0.091s).
The final verbose public differential passes in 7.298s and records raw artifact
hashes; these hashes are not used as an equality claim.

CI follow-up: TestPebbleStoreDirtyCoverage in the parent pkg/dotc1z package
rejected the unclassified BoundSyncUnstarted capability. The earlier engine-only
storage command did not execute that adapter audit. The method is now classified
as read-only. The complete `go test ./pkg/dotc1z/... -count=1 -timeout 20m` tree
passes (parent 40.017s; Pebble 17.358s), and the adapter checks pass with CI's
Go 1.27.1 and baton_lambda_support tag (0.178s). Windows was canceled by the
matrix's fail-fast policy, not a separately diagnosed Windows failure.


## Verification before independent model review

The reported missing-record issue is fixed in BoundSyncUnstarted: absence or a
different stored sync ID falls through to history inspection, while other read
errors still propagate. The public invalid-binding check was run on both
`eb63f1b5` and this branch. Both reject Sync before collecting anything and leave
all keys unchanged; allowing a read-only binding is not a promise that Sync can
finish without a run record. The new query tests distinguish empty/populated files
for both missing-record cases.

A test-only adapter constructor wraps the actual pebbleStore and decorates its
PageWriter, preserving its optional interfaces. It does not introduce a production
option or a replacement scheduler/store. The public syncer runs against
CrashableMem and captures 80 durable-only images, covering five collection families,
targeted resources, assets and terminal proof, both sides of commit, with/without
flushed prefixes and one/four workers. The image probe observes 60 absent target
rows and 20 durable target rows; flushed after-commit rows must be present and
before-commit rows absent. Recovered records, indexes, digests and structured
stats equal uninterrupted output. No live ledger family survives default seal.

Sixteen additional real-filesystem process exits cover targeted/assets. The
subprocess must exit at the requested cut and its output must contain no race
warning. MemFS engine staging uses Unix path separators, so only the simulated
power-loss fixture skips Windows; real process, archive fallback and compactor
checks remain enabled there.

The archive-failure test makes report metadata unreadable at terminal commit.
Public Sync succeeds under the documented fallback: records and stats match the
normal run, history remains scrubbed, no pending disposal declaration remains,
and no report is falsely claimed saved. This is a report-generation failure,
not an injected disk-write failure. Existing storage failure cuts cover the latter.
Two real public SDK artifacts, full and targeted partial, are then consumed by
both compactor strategies. Primary records, indexes and digests agree, and stats
readers report the expected records.

Full pre-review suites pass: sync 84.616s; dotc1z 29.226s; Pebble 12.054s;
compactor 21.164s; other dotc1z subpackages pass. Focused public-adapter race tests
pass three repetitions (27.726s). CI-equivalent Go 1.27.1 / golangci-lint 2.13.2
lint has zero issues. Final fixture refinements are checked again before freezing
the reviewer revision. These tests do not claim the entire original mechanical
product has executed; that remaining scope is provided to the independent reviewers.

The final focused race rerun passes three times (dotc1z 25.093s; sync 1.179s),
and the final merged-tree lint rerun has zero issues. Independent reviewers will
start from the committed revision containing these checks, without seeing each
other's findings.

## CO-025: bounded static entitlement materialization

The independent review at `91017ff0` confirmed that one static template staged
all resources of its type before commit. A counting-writer probe grew from 102
to 21,002 staged entitlements; the baseline wrote the latter in three batches
with a maximum of 10,000. The correction caps each materialization page at
10,000 resources. The same probe now peaks at 10,000 staged entitlements.
This measures staged records, not peak RSS or an absolute byte bound.

`TestLedgerStaticMaterializationBounded` fails before the correction (two staged
records against a forced one-record resource page) and passes afterward.
`TestLedgerStaticMaterializationOrderAndResume` compares final records with
the token handler, exercises identical templates within/across remote pages,
and resumes after a failed second local commit without refetching committed
definitions. Reversing the child ordering incorrectly makes this test fail
(expected middle, got first); adding a connector refetch to materialization
also fails its exact call assertion. Both planted defects were removed.

Cursor/version and invalid resource-page tests reject malformed state, oversized
responses and nonadvancing cursors without durable changes. The retained-ledger
seal/save/reopen test verifies internal template tokens are scrubbed. Public
durable crash images and process-exit cases now include materialization, with
records, indexes, digests and accounting compared to uninterrupted output.

The parent page records connector calls and received templates. Local children
record generated writes and local duration. Completed-action accounting includes
these children; it does not claim to equal the checkpoint path's logical action
count. Memory still includes an arbitrary connector response and one resource
page plus generated entitlements. Template bytes repeat in continuation tokens;
this correction does not claim a fixed byte budget for oversized templates.

Full suites pass: sync 83.236s, dotc1z 26.491s, Pebble 9.463s, compactor 18.691s.
Focused race checks pass three repetitions (sync 2.558s, dotc1z 44.675s).
CI-equivalent lint has zero issues. The independent design review is complete;
review of the implementation correction is still pending. These checks close
the identified staging regression, not every original coverage product.

## CO-026: independent review corrections

The implementation follow-up at `7b1e85b7` found missing materialization time
in sync summary totals and encoded template payloads in debug action logs.
A separate isolated review at `91017ff0` reproduced default-resumer deletion
of durably retained history; it independently reproduced the static staging
regression too. Its requested model/provider could not be confirmed from the
worker's supplied runtime context, so these are independent review sessions,
not an attested cross-provider review.

`TestStaticMaterializationSummaryTiming` fails before correction with zero
operation time rather than 2,000ms. `TestActionLogsOmitPagePayload` fails before
correction with connector tokens and encoded template data in admission,
completion and pointer-action error logs. The corrected log projection omits
PageToken while the same test verifies ordinary JSON round-trips it unchanged.
This is coverage of action-object logging, not every possible diagnostic path.

The public stop/reopen/resume reproduction fails before correction because the
retained row is gone. `TestLedgerPublicStopResume` now covers default and debug
resumers, retained tokens, exact connector continuation, a write-free walk,
and requested versus effective archived flags. The restored retention fact
enables effective debug retention before execution and seal, with a warning.
No storage schema, engine write or checkpoint serialization changes are needed.

All three regressions pass after their fixes. Focused race checks pass three
repetitions (2.666s); merged-tree lint reports zero issues. The full sync suite passes (84.120s). Independent follow-up review of these
corrections remains pending.

### Finished-binding retention follow-up

Review of `ff3d713a` identified that the restored retention policy also survived
clearing completed page history. `TestLedgerFinishedRetentionUsesCurrentOptions`
reproduced both failures: the default invocation retained rows, and debug without
explicit token retention kept unsanitized tokens. The explicit-retain control
passed. An initial fixture incorrectly pre-started a sync and failed before the
case under test; it was corrected before recording the regression result.

Clearing completed history now clears its retain-token declaration in the same
existing atomic batch. All three modes pass after correction, including exact
remote continuation, preserved start metadata and archived effective options.
Unfinished public resume, finished-processing crash recovery and pending-seal
guards pass alongside it (0.305s); three race repetitions pass (3.979s), and
merged-tree lint reports zero issues. No engine method, schema or lifecycle-reset
behavior changed.

The second review harness stream identifies GPT-6 Astra despite its requested
Claude task label. Its independent findings remain useful, but it is not a
Claude review. Later build attempts during overlapping checkout changes in that
review worktree are excluded from evidence; the affected reviewer is paused.
