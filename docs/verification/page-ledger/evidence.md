# Page ledger: verification evidence

Plan: `docs/verification/page-ledger/plan.md`. Commit under verification:
`04644cf6` (`kans/ledger-storage`, `origin/main..HEAD`).

## Status of this file

No instrument in plan §7 has been run under the plan. The authoring
machine was under load; the plan's §0.4 records what was and was not
executed. Every criterion below is therefore in one of: **not
assessed**, **evidence incomplete** (a candidate test exists whose
assertions were read but not run under this plan), **explicitly
excluded**, or **deferred**. No criterion is **verified** and no
criterion is **failed** by measurement; two are marked **expected
failed** from reading, which is a prediction and not a result.

What was executed at `04644cf6`:

```
go build ./pkg/dotc1z/... ./pkg/synccompactor/...   # ok
go vet ./pkg/dotc1z/engine/pebble/ ./pkg/dotc1z/c1zstore/ ./pkg/dotc1z/   # ok
```

Status vocabulary (plan §6): not assessed · evidence incomplete ·
verified to stated coverage · failed · explicitly excluded · deferred.

## Per-criterion status

Each entry: status; candidate artifact (exists at `04644cf6`, read, not
run under this plan); the cells the candidate does not cover; the
instrument that closes it.

### C01 Failed commit lands nothing; success lands everything together

- Status: evidence incomplete.
- Candidate: `TestPageUnitCommitIsOneFact`,
  `TestPageUnitFailedCommitLandsNothing`,
  `TestLedgerFactsAndBucketsRideThePageUnit`
  (`pkg/dotc1z/engine/pebble/ledger_test.go`, `ledger_state_test.go`).
- Not covered by the candidate: the retain-fact side-state cell under a
  failed commit; a failed commit followed by a successful re-commit of
  the same unit with O1 equality on the first and full presence on the
  second. The "unit stays usable" half is asserted; the "byte-identical"
  half uses a record count, not a keyspace digest (O1 as written).
- Closes with: I3 driver plus a keyspace digest helper.

### C02 No torn page in any crash image

- Status: evidence incomplete (sampled criterion).
- Candidate: `TestPageUnitCrashImageStoreEqualsLedger` — 120 pages,
  unsynced fractions {0, 20, 50, 80, 100}%, oracle "row present ⇔ all
  page records present", plus a WAL-sync-point arm.
- Not covered: F3 (Sync page commit, resumed sync) as a separate arm;
  facts and bucket presence inside the per-page oracle (the candidate
  checks records only); the token-only reopen identity on each image.
- Closes with: I1.

### C03 Discard and spent-unit refusal

- Status: evidence incomplete.
- Candidate: `TestPageWriterGetResourceAndDiscard`,
  `TestPageUnitReadsAfterCommitAreRefusedNotPanics`.
- Not covered: refusal of every `Stage*` and `DropStagedRows` after
  `Commit` (the candidate covers the two reads); buffer release under O12
  after `Discard`.
- Closes with: extend the candidate; I9 ride-along.

### C04 Read-your-writes on an open unit

- Status: evidence incomplete.
- Candidate: `TestPageUnitReadSeesOwnWrites` (buffered and store-only).
- Not covered: the dropped cell (`DropStagedRows` then `Get*` returns the
  store's row) and the absent cell, for both kinds.
- Closes with: extend the candidate.

### C05 Path equivalence over record shapes

- Status: evidence incomplete.
- Candidate: `TestPageWriterMatchesSingleCallAdapters`
  (`adapter_page_test.go`) — new records of all four kinds.
- Not covered: 11 of 15 record-shape cells (overwrite same value,
  overwrite index-affecting, duplicate-in-page for each kind) and all
  three grant-delete cells. The candidate compares typed reads, not a
  full keyspace snapshot, so secondary index, digest, and source scope
  rows are not compared.
- Closes with: I3 with the O2 snapshot.

### C06 In-page delete ordering and `DropStagedRows` selector agreement

- Status: not assessed.
- Candidate: none. `page_unit.go:Commit` drops doomed buffered puts and
  stages `stageGrantDeleteIfPresentLocked`; `DropStagedRows` is
  documented but no test compares its selector to
  `DeleteSourceCacheRowsInScope`.
- Closes with: I3's 3 + 12 cells.

### C07 `*_written` counts equal distinct committed records

- Status: not assessed; **expected failed** from reading.
- Reason: `page_unit.go:Commit` sets `ResourceTypesWritten` etc. from
  `len(u.resourceTypes)` and friends, while `stageResourceRecords` and
  the other stagers dedup by identity (last wins). A page with a
  duplicate key reports one more than it committed. Silent, well-formed,
  durable in the row.
- Closes with: the I3 duplicate cells; fix is to count staged keys.

### C08 Identity compare, every field, scrubbed and unscrubbed

- Status: evidence incomplete.
- Candidate: `TestLedgerIdentityMismatchReadsAsAbsent` (page-token
  difference only, unscrubbed).
- Not covered: 17 of 20 P5 cells; `ledgerMismatches` delta is not
  asserted (no test accessor; plan §9).
- Closes with: I4.

### C09 Key encoding injective and prefix-bounded

- Status: evidence incomplete.
- Candidate: `TestLedgerKeyEncoding`.
- Not covered: empty-string fields, shared-prefix ids, `TypeScoped`
  flip, and the two prefix iterators against an independent filter.
- Closes with: extend the candidate.

### C10 Fresh-sync proofs through the page path

- Status: not assessed.
- Reading: the stagers consume `takeFresh*Empty` inside
  `stage*Records`, so a page whose batch commit then fails has consumed
  the proof; the direction is conservative (slow path armed). Not
  asserted anywhere for the page path.
- Closes with: I3 with proof-flag inspection through `testSeams`.

### C11 Durability class per write

- Status: not assessed.
- Reading: `page_unit.go:Commit` uses `writeOpts(durability)` and
  `pebble.NoSync` when `e.freshSync`; `takeoverToken` and
  `PutLedgerCounterBucket` use `pebble.Sync`; `markLedgerInFlight` uses
  `MetaSet` with Sync. No test asserts the option chosen; the crash-image
  test shows the fresh half indirectly.
- Closes with: the write-options observer in plan §9, or I1's "a Sync
  write is never unsynced in an image" assertion.

### C12 Stamp durable before first row; token-only acceptance by state

- Status: evidence incomplete.
- Candidate: `TestLedgerInFlightStampGatesTokenOnlyReaders` (L0 accepted,
  L2 refused, L4 accepted after seal).
- Not covered: L1 (F4 image), L3, L5, L6 under a token-only reopen;
  "every crash image with a row has the stamp" as an assertion inside
  I1.
- Closes with: I1 with `withTokenOnlySDK` reopen per image.

### C13 `CheckpointSync` / plain `EndSync` gating by state

- Status: evidence incomplete.
- Candidate: `TestLedgeredSyncSealsOnlyWithStats` (L2: plain `EndSync`
  refused, `EndSyncWithStats` accepted),
  `TestCheckpointRefusedWhileLedgerRowsExistWithoutTheStamp` (L6).
- Not covered: L1 and L3 for both operations; L0 acceptance after
  `DropLedger` and after `ResetLedger` (only `ResetForNewSync` is
  covered, in `TestResetForNewSyncClearsTheInFlightStamp`).
- Closes with: a table test over P1's two rows × 7 states.

### C14 Seal-cut dichotomy

- Status: evidence incomplete.
- Candidate: `TestFailedSealDropsItsStatsOverlay` — a same-process F7
  class failure (finalize error) drops the overlay.
- Not covered: F8, F9, F10, F11 entirely; every cut under the three
  reopen identities; the idempotent second `EndSyncWithStats`.
- Blocked on: the `testSeams` hooks named in plan §9 (after purge, after
  `clearLedgerInFlight`, after `ended_at`; `PersistSyncStats` failure).
- Closes with: I2.

### C15 Scrub result and byte-level absence

- Status: evidence incomplete.
- Candidate: `TestLedgerScrubAtSealForSensitiveTokens`,
  `TestLedgerScrubLeavesNoSSTResidue` (retain control, default zero
  hits, `skipLedgerResiduePurge` mutant shows non-zero),
  `TestLedgerScrubReachesTheTakeoverFrontier`.
- Not covered: identity compare by hash on every scrubbed row after
  seal (the candidate checks the token fields, not a `GetLedgerRow`
  round-trip); the frontier under the byte oracle (the needle test plants
  tokens in rows only).
- Closes with: extend the candidates with an O6 pass and a
  frontier-planted needle.

### C16 Retain declaration durable; absent or unreadable scrubs

- Status: evidence incomplete.
- Candidate: `TestRetainDeclarationSurvivesCrashAndItsAbsenceScrubs`.
- Not covered: the read-error cell (`sealScrubsTokens` returns scrub +
  error); the takeover-writes-the-fact cell; "flag only, fact only" in a
  fresh process.
- Closes with: extend the candidate; the read-error cell needs a fact
  read fault injection.

### C17 Scrub batch bound and idempotence

- Status: not assessed.
- Reading: `ScrubLedgerTokens` re-mints at `ledgerScrubBatchBytes =
  16<<20` and skips rows already scrubbed. Not asserted.
- Closes with: a 10^4-row scrub with a batch-count observer and a second
  call asserting zero writes.

### C18 Facts encoding and last-writer-wins

- Status: evidence incomplete.
- Candidate: `TestLedgerFactsAndBucketsRideThePageUnit`,
  `TestLedgerKeyEncoding` (fact value decode).
- Not covered: empty-valued fact; `SetFact` overwriting `SetFactValue`
  on the same key; takeover-written facts overwriting page-written ones.
- Closes with: extend the candidate over the 3 × 2 × 2 cells.

### C19 Counter buckets keyed and folded

- Status: evidence incomplete.
- Candidate: `TestTakeoverBucketSurvivesWorkerZerosPage`,
  `TestTakeoverPersistsStatsOnlyCounters`,
  `TestLedgerFactsAndBucketsRideThePageUnit`.
- Not covered: `RunBucketWorker` collision cell; two-run fold; O7 as an
  independent fold across all five fields (the candidates check specific
  fields).
- Closes with: extend with an O7 table.

### C20 Takeover is one Sync batch

- Status: evidence incomplete.
- Candidate: `TestLedgerTakeoverIsOneUnit`,
  `TestLedgerTakeoverRequiresOpenSync`.
- Not covered: no-token returns `""` and writes nothing (read as the
  code's behaviour, not asserted); retain-on cell (takeover stages the
  retain fact); F5 crash image under I1.
- Closes with: extend; I1 F5 arm.

### C21 Drop / reset remove every sub-family and the stamp

- Status: evidence incomplete.
- Candidate: `TestResetLedgerWipesEveryLedgerSubFamily`,
  `TestDropLedgerClearsTheInFlightStamp`, `TestLedgerWipedWithItsSync`,
  `TestResetForNewSyncClearsTheInFlightStamp`.
- Not covered: F12 image (L6) and its recovery by each of the three
  operations; `ResetForNewSync` refused while `IsFreshSync`;
  `BoundSyncFinished` ⇔ `ended_at`.
- Closes with: I5 rows for the three operations; I1 F12 arm.

### C22 Store dirty marking

- Status: evidence incomplete.
- Candidate: `pkg/dotc1z/pebble_store_dirty_test.go`
  (`TestPebbleStorePageCommitMarksDirty`,
  `TestPebbleStoreResetLedgerMarksDirty`, and siblings).
- Not covered: error cells (a failed call must not mark dirty) for each
  of the six methods; `Close` persists a page commit (reopen and read).
- Closes with: extend the candidate with an injected failure per method.

### C23 Write-hook coverage of record-mutating store methods

- Status: not assessed.
- Reading: `pebble_store.go` has 16 `s.seam(ctx, …)` calls and
  `source_cache.go` has 5. `FinishExpandedGrantLayer` and
  `AddExpandedGrantLayerContributions` mutate records and have no hook
  call. `StrictWriteSeam`'s three-way behaviour is asserted nowhere.
- Closes with: I6.

### C24 Capability presence and absence

- Status: evidence incomplete.
- Candidate: `var _ c1zstore.PageLedgerStore = (*Engine)(nil)` in
  `adapter_page.go` (engine only).
- Not covered: `pebbleStore` through the `dotc1z` open path for all three
  interfaces; the SQLite negative assertion.
- Closes with: two one-line tests in `pkg/dotc1z`.

### C25 Downstream readers family-bounded

- Status: not assessed; fold cell **expected failed** from reading.
- Reading: `compactPebbleFold` calls `copyFileForFold` on the base and
  never `DropLedger`/`ResetLedger`; the output carries the base's rows
  under a new sync id and `ledgerActive` is true on it. Rebuild modes
  (`compactPebble`) materialize records fresh and write no ledger key.
  `cloneSync` excises only the counter/session span and keeps the ledger.
  Stats, CLI readers, explorer, and sanitizer were not read for this
  criterion beyond confirming none references `TypeLedger`.
- Closes with: I7.

### C26 Compaction provenance in the sidecar

- Status: evidence incomplete.
- Candidate: `pkg/synccompactor/compactor_provenance_test.go` (fold,
  chained fold, rebuild provenance).
- Not covered: base without a sidecar (`readSourceSyncStats` returns
  nil) in fold; timing fold correctness against an independent
  `FoldCallStats`; "no output writes a token" as an explicit assertion.
- Closes with: extend the candidate.

### C27 Stats overlay lifecycle

- Status: evidence incomplete.
- Candidate: `TestFailedSealDropsItsStatsOverlay`,
  `TestLedgeredSyncSealsOnlyWithStats`.
- Not covered: F11 (`PersistSyncStats` fails, seal finishes without a
  sidecar) and `SourceCacheReplayEligible` on that file; a stale overlay
  never reaching a later token-only `EndSync`.
- Closes with: I2 F11 arm.

### C28 Commit-point registry

- Status: evidence incomplete.
- Candidate: `commit_point_enumeration_test.go` — entries for
  `page_unit.go:Commit`, `adapter_page.go:Commit`,
  `ledger.go:ScrubLedgerTokens`, `ledger.go:takeoverToken`, and the
  `ledger.go:PutLedgerCounterBucket` exclusion. Read, not run.
- Closes with: running the meta-test.

### C29 Concurrency

- Status: not assessed (sampled criterion).
- Candidate: none.
- Closes with: an N-unit `-race` test and the two race pairs (page vs
  `Close`, page vs `EndSyncWithStats`).

### C30 Cost

- Status: not assessed (sampled criterion). Benchmarks exist and were
  not run: `BenchmarkLedgerPageCommit`, `BenchmarkLedgerPageCommitSync`,
  `BenchmarkLedgerResumeWalk`, `BenchmarkLedgerSealCost`,
  `BenchmarkLedgerSealCostNoGrantIndex`. A `Put*Records` baseline at
  matched page sizes does not exist.
- Closes with: I8 on an unloaded machine.

### C31 Mutant adequacy per oracle

- Status: evidence incomplete.
- Candidate: the `skipLedgerResiduePurge` arm of
  `TestLedgerScrubLeavesNoSSTResidue` (O5 only).
- Not covered: mutants for O1, O2, O3, O4, O6, O7, O8, O9, O10.
- Closes with: I10.

### C32 Syncer-owned obligations

- Status: deferred to the syncer integration change. Owner: that change's
  plan. The bundle: read-only walk; absent-or-mismatch → re-run; L3
  handling (OQ-1); `BoundSyncFinished` then `ResetLedger` on rebind;
  `WithPageWriteBypass` registrations; one commit per page; counters and
  facts producers; takeover trigger; stats fold across attempts.

## Explicit exclusions (plan §3.4)

- P5 hash collision at 128 bits: not constructible.
- Concurrent `Commit` on a single `PageUnit`: single-goroutine by
  contract; the `done` guard is asserted once under C03.
- SQLite store ledger behaviour: none exists; one negative assertion
  under C24.
- Token-only SDK writing into an L4/L5 file: refused by the pre-existing
  sealed-sync check; not re-derived here.

## Structural coverage triage: P3 (sub-family × surface), from reading

Legend: W writes · C clears · R reads · — must not touch · ? not
confirmed by a test. Every cell is a reading result until I5 runs.

| Surface | rows | facts | buckets | frontier | retain fact | stamp | token key |
| --- | --- | --- | --- | --- | --- | --- | --- |
| page commit | W | W | W | — | W (flag) | W (Sync) | — |
| takeover | — | W | W | W | W (flag) | W (Sync) | C |
| `PutCounterBucket` | — | — | W | — | — | W (Sync) | — |
| scrub | W (token fields) | — | — | W (token) | R | — | — |
| purge | bytes | bytes | bytes | bytes | bytes | — | — |
| `DropLedger` | C | C | C | C | C | C | — |
| `ResetLedger` | C | C | C | C | C | C | — |
| `ResetForNewSync` | C | C | C | C | C | C | C (whole span) |
| `ledgerActive` | R | R | R | R | R | R | — |
| `CloneSync` | copy | copy | copy | copy | copy | copy | copy |
| compactor fold | copy? | copy? | copy? | copy? | copy? | copy? | rewritten |

Cells marked `?` (compactor fold): the fold path byte-copies the base, so
every ledger key is expected to be present in the output; no test
confirms it. The stamp column for takeover and `PutCounterBucket` is from
reading `ledger.go:takeoverToken` and `ledger.go:PutLedgerCounterBucket`,
which both call `markLedgerInFlight` before their batch; the
`TakeoverToken × L0` cell in P1 is still an I5 assertion, not a result.

## Evidence commands (not run under this plan)

Listed so the next pass can run them unchanged. Package path
`pkg/dotc1z/engine/pebble` unless stated.

```
go test -run 'TestPageUnit|TestLedger|TestLedgered|TestCheckpointRefused|TestResetForNewSync|TestTakeover|TestRetainDeclaration|TestDropLedger|TestFailedSeal|TestResetLedger' ./pkg/dotc1z/engine/pebble/
go test -run 'TestPageWriter' ./pkg/dotc1z/engine/pebble/
go test -run 'TestCommitPoint' ./pkg/dotc1z/engine/pebble/
go test -run 'TestPebbleStore.*Dirty' ./pkg/dotc1z/
go test -run 'Provenance' ./pkg/synccompactor/
go test -race -run 'TestPageUnit' ./pkg/dotc1z/engine/pebble/
go test -run '^$' -bench 'BenchmarkLedger' -benchtime 20x ./pkg/dotc1z/engine/pebble/   # unloaded machine only
```

## Performance evidence

None. C30 and OQ-7 are open until I8 runs on an unloaded machine.

## Process corrections

- The plan was authored after implementation (plan §0.1). The model's
  axes were fixed before criteria were written; the fix commits were
  placed into cells afterwards (plan §3.6) and generated no new axis.
- Three contract disagreements between the brief and the code were
  resolved as change orders before modeling (CO-001..003), not by
  silently testing the code's behaviour.
- No test was run under this plan because of the authoring machine's
  load. Candidate artifacts are named from reading their assertions;
  their pass state at `04644cf6` is not claimed here.
