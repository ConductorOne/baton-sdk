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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C04 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C05

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C05 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C09 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C10

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C10 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C11

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C11 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C12

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C12 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C13

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C13 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C14

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C14 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C15

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C15 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C16

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C16 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C17

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C17 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C18

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C18 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C19

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C19 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C20

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C20 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C23 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C24

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C24 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C25

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C25 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C26

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C26 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C27

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C27 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C28

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C28 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C29

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C29 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C30

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C30 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C31

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C31 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C32

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C32 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C33

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C33 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

### C34

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C34 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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

- Status: not assessed.
- Candidate: implementation.md §5; not yet executed for this criterion.
- Required coverage: plan C39 and applicable calibration entries.
- Planted defect: not run for this criterion.
- Green command/revision: none.
- Not covered: all required cells until an explicit execution entry is added.

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
