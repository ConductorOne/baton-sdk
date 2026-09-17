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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C07 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. No restored-pass guard exists yet.

### C14

- Status: failed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C14 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. No restored-pass guard exists yet.

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
- Candidate: TestLedgerPageFailureDoesNotPublish; TestLedgerPageFactValueReadYourWrites; TestLedgerPageFailureDiscardsStagedObservations.
- Required coverage: plan C17 and applicable calibration entries.
- Planted defect: not run for this criterion; green mechanism tests only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

### C18

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C18 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C19

- Status: failed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C19 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. No restored-pass guard exists yet.

### C20

- Status: failed.
- Candidate: TestLedgerPageCumulativeWorkersAndAttempts; TestLedgerScheduleStopsAndJoinsOnError.
- Required coverage: plan C20 and applicable calibration entries.
- Planted defect: not run for this criterion; green mechanism tests only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. No restored-pass guard exists yet.

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

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. No restored-pass guard exists yet.

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

- Status: failed.
- Candidate: TestLedgerTakeoverIngestQuality; TestLedgerTakeoverLegacyFixtures.
- Required coverage: plan C30 and applicable calibration entries.
- Planted defect: not run for this criterion; green mechanism tests only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. No restored-pass guard exists yet.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C38 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C39

- Status: failed.
- Candidate: TestLedgerScheduleStopsAndJoinsOnError; TestLedgerPageFailureDiscardsStagedObservations.
- Required coverage: plan C39 and applicable calibration entries.
- Planted defect: not run for this criterion; green joined-worker and writer-release checks only.
- Green command/revision: K2a execution entry below; disabled candidate is not green evidence.
- Not covered: public Sync routing, full mechanical products, physical WAL-loss images and final differential closure.

- Baseline audit: reproduced private-runtime contract difference; see baseline-audit.md and TestLedgerBaselineContractAudit. No restored-pass guard exists yet.

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
- Candidate run: TestLedgerCostBaseline in a detached eb63f1b5 test executable.
- Premise: 10 pages × 100 resources, 4 streams; all 1,000 resources verified
  and checkpoint calls observed. This is a smoke check only.
- Planted defect: no C49 metric/arm mutant run yet.
- Not covered: full matrix, unloaded-machine qualification, ledger arms,
  before/after-close byte accounting, timing decomposition and acceptance.

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
