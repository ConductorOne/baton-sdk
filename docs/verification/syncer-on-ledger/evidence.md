# Syncer on the page ledger: verification evidence

Plan frozen at `01931d8b`; calibration `d9277866`; implementation brief
`644c26cf`. Execution is in progress. Candidate names in the brief are not
passing evidence. No criterion is closed by this initial record.

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

The initial strict fixture observes direct puts, grant deletion, assets,
checkpoint and EndSync. Sub-store/session writes and other lifecycle methods
are not yet covered by its companion recorder. Raw snapshots include every
key/value and have a close/reopen test. Process-crash images and canonical
normalization instruments remain to be built. Do not treat their absence as
closure of C10, C37, C38 or C47.

## Per-criterion record

### C01

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C01 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C08 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: failed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C13 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

### C14

- Status: failed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C14 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

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

- Status: failed.
- Candidate: TestLedgerPageCumulativeWorkersAndAttempts; TestLedgerScheduleStopsAndJoinsOnError.
- Required coverage: plan C20 and applicable calibration entries.
- Planted defect: not run for this criterion; green mechanism tests only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

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

- Status: failed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C23 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

### C24

- Status: evidence incomplete.
- Candidate: TestLedgerTakeoverLegacyFixtures; TestLedgerTakeoverV0CursorAndParentIdentity.
- Required coverage: plan C24 and applicable calibration entries.
- Planted defect: not run for full C24; fixture decoding and migration are green, crash/reopen matrix remains.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C25

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C25 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C26

- Status: evidence incomplete.
- Candidate: TestLedgerTakeoverLegacyFixtures.
- Required coverage: plan C26 and applicable calibration entries.
- Planted defect: replaced the saved frontier state with empty input on resume; legacy fixture family failed with Init replacing saved actions; restored.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

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
- Candidate: TestLedgerSealRequiresTerminalPage; TestLedgerTerminalPageAndSealStats; TestLedgerTerminalFailureDoesNotPublishProof.
- Required coverage: plan C31 and applicable calibration entries.
- Planted defect: not run for this criterion; terminal staging-failure fixtures and public stats-reader checks are green.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C32

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C32 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C33

- Status: evidence incomplete.
- Candidate: TestLedgerFinishedRebindDropsCompletionOnly; TestLedgerInterruptedFinishedRebindResumesNewRun. Both require reassessment under CO-010.
- Required coverage: baseline caller lifecycle equivalence under CO-010, including same-ID deferred expansion and interruption.
- Planted defect: the synthetic interrupted-rebind candidate exposed the private runtime's reset assumption; it does not justify changing the storage lifecycle.
- Green command/revision: none for CO-010; the disabled candidate is not green evidence.
- Not covered: baseline caller comparison, public routing, persisted lifecycle/fact/accounting equivalence and crash products.

### C34

- Status: evidence incomplete.
- Candidate: TestLedgerFinishedRebindDropsCompletionOnly; TestLedgerInterruptedFinishedRebindResumesNewRun. Both require reassessment under CO-010.
- Required coverage: baseline caller lifecycle equivalence under CO-010, including same-ID deferred expansion and interruption.
- Planted defect: the synthetic interrupted-rebind candidate exposed the private runtime's reset assumption; it does not justify changing the storage lifecycle.
- Green command/revision: none for CO-010; the disabled candidate is not green evidence.
- Not covered: baseline caller comparison, public routing, persisted lifecycle/fact/accounting equivalence and crash products.

### C35

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C35 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: failed.
- Candidate: TestLedgerScheduleStopsAndJoinsOnError; TestLedgerPageFailureDiscardsStagedObservations.
- Required coverage: plan C39 and applicable calibration entries.
- Planted defect: not run for this criterion; green joined-worker and writer-release checks only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. K2c added restored-pass adapter guards; see the K2c table below.

### C40

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C40 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C44 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C45

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C45 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C48 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C49

- Status: evidence incomplete.
- Candidate runs: TestLedgerCostBaseline at eb63f1b5; TestLedgerCostRuntime at fafa4f74, through the existing scheduler.
- Coverage: 36 interleaved samples in cost-scheduler-smoke; see the execution entry below for dimensions and limitations.
- Planted defect: the machine recorder omitted the artifact filesystem; the missing-field assertion failed before the fix and the filesystem comparison passed after it.
- Not covered: full matrix, production handlers, unloaded-machine qualification, baseline phase timing, encoded-byte decomposition and acceptance. CO-012 settles the durability premise.

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
