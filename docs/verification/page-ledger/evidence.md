# Page ledger: verification evidence

Plan: `docs/verification/page-ledger/plan.md`. Commit under verification:
`8feb0b06` (`kans/ledger-storage`, `origin/main..HEAD`). The plan was frozen
at `04644cf6`; §0.5 records the change orders since.

## Status of this file

Candidates have been run. Most criteria are still **evidence incomplete** —
a candidate exists and passes, without covering every cell the criterion
names — and that is a statement about coverage, not about the candidate.
Two things changed from the first version of this file: C07 was **failed**
by measurement and is now fixed and pinned, and C22's interface-drift
direction is closed by a meta-test.

A passing candidate is not closure. Where a criterion asserts an absence,
the entry says whether the assertion was validated against a planted
defect; where it was not, the entry says so.

What was executed at `8feb0b06` unless noted:

```
go test ./pkg/dotc1z/                    # ok 130.1s
go test ./pkg/dotc1z/engine/pebble/      # ok 158.1s
go test ./pkg/sync/                      # ok 282.8s
go test ./pkg/synccompactor/             # ok 283.8s
go test ./pkg/sync/expand/               # ok  91.4s   (at 2c321bea)
golangci-lint run pkg/dotc1z/...         # 0 issues
```

Machine under load for parts of the run; every number above is a wall
clock on a shared machine and none of it is a cost measurement. C30 has
no evidence.

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

- Status: **failed** by measurement at `2bbfa1b3`; fixed at `11f8c8c3` and
  `21d4349e`; now evidence incomplete.
- The failure. `page_unit.go:Commit` set all four counts from
  `len(u.resourceTypes)` and friends while the stagers dedup by identity,
  so a page that staged one identity twice reported a key it did not
  write. One page staging 2 resource types (1 distinct), 3 resources, 3
  entitlements and 3 grants (2 distinct each) committed a row reading
  `2, 3, 3, 3` against a keyspace holding `1, 2, 2, 2`. Silent,
  well-formed, durable in the row.
- The resource-type kind failed for a second reason: `stageResourceTypeRecords`
  had no dedup pre-pass at all, the only one of the four without one.
- The fix. All four stagers return the number of keys they staged and
  `Commit` writes those into the row; `stageResourceTypeRecords` got the
  pre-pass, last occurrence winning like the other three.
- Candidate: `TestLedgerRowCountsDistinctKeysNotBufferedRecords`
  (`adapter_page_test.go`) — 4 kinds × duplicate, each count against an
  independent iteration of the keyspace; and
  `TestFreshSyncWithinCallDuplicateResourceTypeDedup` (`mutation_test.go`),
  which pins *which* occurrence survives the new pre-pass.
- Mutation adequacy. Restoring the buffer-length counts fails the first
  test (2 vs 1). Inverting the new pre-pass to keep the first occurrence
  passes it — one key either way — and fails the second on the survivor's
  display name. Both were run.
- Not covered: the 4 no-duplicate cells are exercised by every other
  ledger test but never asserted against a key count; the doomed-put half
  of the criterion is correct by reading and has no test.
- Closes with: the remaining I3 duplicate cells.

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
- Candidate: `TestRetainDeclarationSurvivesCrashAndItsAbsenceScrubs`,
  `TestLedgerTakeoverCrashImages` (the takeover-writes-the-fact cell: the
  post image is reopened by an engine that never set the flag, and
  `sealScrubsTokens` returns false from the fact alone).
- Not covered: the read-error cell (`sealScrubsTokens` returns scrub +
  error); "flag only, fact only" in a fresh process.
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

- Status: verified to stated coverage.
- Evidence: `TestLedgerTakeoverIsOneUnit` (token × open × retain off;
  injected commit failure; second takeover is a no-op with the bucket
  count unchanged), `TestLedgerTakeoverRequiresOpenSync` (no sync →
  refused; sync with no token → `""`, no frontier),
  `TestLedgerTakeoverCrashImages` (token × open × retain on, on
  `CrashableMem`: the pre image, the image between the in-flight stamp and
  the batch, and the post image with every unsynced byte dropped; the mid
  image is refused by a token-only SDK and taken over again by this one).
- Mutation adequacy: `batch.Commit(pebble.Sync)` → `pebble.NoSync` at
  `ledger.go:393` fails `TestLedgerTakeoverCrashImages` at "post: token
  cleared". The injected-error arm cannot see that mutant; the crash arm is
  what pins durability.
- Unexercised: {no token, retain on}. The `state == ""` return at
  `ledger.go:340` precedes the flag read, so it is the retain-off path.

### C21 Drop / reset remove every sub-family and the stamp

- Status: evidence incomplete.
- Candidate: `TestResetLedgerWipesEveryLedgerSubFamily`,
  `TestDropLedgerClearsTheInFlightStamp`, `TestLedgerWipedWithItsSync`,
  `TestResetForNewSyncClearsTheInFlightStamp`. These check the keyspace,
  not the bytes.
- The bytes are covered separately, and were a defect. A ledger dropped
  or reset mid-sync leaves its verbatim page tokens in the SSTs a
  checkpoint hard-links, and `endSyncFinalize`'s `ledgerActive` gate finds
  no ledger on the later seal and skips the purge. Fixed with a durable
  marker that outlives the rows; `TestLedgerResidueOutlivesTheLedger`
  covers both deletion shapes (`DropLedger` mid-sync then seal; an
  interrupted ledgered sync followed by a ledger-free one) with a needle
  planted in page tokens and a byte scan of the checkpoint.
- The two arms differ in kind: `DropLedger` tombstones with `DropKeyRange`
  and owes a compaction, so it arms the marker; `ResetForNewSync` excises
  the whole v3 keyspace, so no SST survives to hold residue, and the arm
  asserts zero needle hits right after the reset with no marker armed and
  `ledgerResiduePurges` still zero at the replacement's seal. (An earlier
  revision excised sub-ranges around the engine-global metadata, which left
  narrowed SSTs and needed a second marker kind plus an O(file) compaction
  at the next seal; a whole-keyspace excise followed by the Open-time
  initialization removed both.)
- Not covered: F12 image (L6) and its recovery by each of the three
  operations; `ResetForNewSync` refused while `IsFreshSync`;
  `BoundSyncFinished` ⇔ `ended_at`.
- Closes with: I5 rows for the three operations; I1 F12 arm.

### C22 Store dirty marking

- Status: evidence incomplete; the interface-drift direction is closed.
- Candidate: `pkg/dotc1z/pebble_store_dirty_test.go`
  (`TestPebbleStorePageCommitMarksDirty`,
  `TestPebbleStoreResetLedgerMarksDirty`, and siblings), plus
  `TestPebbleStoreDirtyCoverage`, a meta-test that walks three capability
  interfaces by reflection and fails on any method not classified as
  marking dirty or justified as not needing to.
- The meta-test found a defect in a method set it did not yet scan.
  `AddExpandedGrantLayerContributions` reached `markDirty` on no path: the
  first Add arms the deferred `by_principal` rebuild and a segment that
  fills mid-layer is ingested into the live keyspace, both before `Finish`,
  which was the only method marking the store. `Begin → Add → Abort →
  Close` therefore left a clean store over a mutated file. Latent, because
  every path that persists goes through `Finish`. Fixed at `2c321bea`,
  which also extended the walker to `pebbleStoreGrantLayerStorer` and to
  the `pebbleStoreGrants` receiver.
- Mutation adequacy. Dropping the new `markDirty` fails the extended
  meta-test naming that method; leaving a method unclassified fails it
  listing the method. Both were run.
- Not covered: error cells (a failed call must not mark dirty) for each
  of the six methods; `Close` persists a page commit (reopen and read);
  the `Begin → Add → Abort → Close` save path is argued, not tested — the
  fix is that the store is dirty, and no test reopens the file to confirm
  the ingested rows survive.
- Closes with: extend the candidate with an injected failure per method.

### C23 Write-hook coverage of record-mutating store methods

- Status: evidence incomplete.
- Candidate: `TestWriteSeamOutcomes`, `TestWriteSeamContextHelpers` —
  these close the second clause, `StrictWriteSeam`'s outcomes inside and
  outside `WithOpenPage`, `WithPageWriteBypass`, empty-reason rejection
  and hook removal.
- Not covered: the first clause, set equality. No test compares the set of
  record-mutating store methods against the set that calls `seam` first.
  `TestPebbleStoreDirtyCoverage` is not a stand-in: different property
  (`markDirty`, not `seam`) over a different method set.
- Correction to the earlier reading: `FinishExpandedGrantLayer` and
  `AddExpandedGrantLayerContributions` are methods on `pebbleStoreGrants`
  (`pebble_store.go:793,801`), not on `*pebbleStore`. They still have no
  `seam` call. `BeginExpandedGrantLayer` and `AbortExpandedGrantLayer` are
  exclusion candidates rather than gaps.
- No caller marks a page context outside tests, so the hook is a no-op
  today for every method. That makes the gap cheap either way; it is not
  a reason to call the set closed.
- Closes with: I6.

### C24 Capability presence and absence

- Status: verified to stated coverage (single per store, both stores).
- Candidate: the three assertions at `pebble_store.go:33,40,41` cover
  `pebbleStore` through the `dotc1z` open path, plus the runtime `ok`
  checks in `pebble_store_dirty_test.go` and
  `pebble_store_write_seam_test.go`. A comment on the assertions records
  what they do not mean: `pebbleStore` embeds `*pebble.Engine`, so a
  promoted mutating method satisfies an interface while skipping
  `markDirty`. `TestPebbleStoreDirtyCoverage` covers that.
- Absence: `TestSQLiteStoreOffersNoLedgerCapabilities` probes a store
  opened with `WithEngine(EngineSQLite)` for all three and requires false
  on each. `*C1File` has none of the five methods, so the result is a
  property of the type, not of the file's state.
- Mutation adequacy. Giving `*C1File` a `SetWriteSeam` method fails the
  `WriteSeamStore` assertion by name. Run and reverted. The test also
  carries a premise assertion — the same probe finds
  `connectorstore.DBSizeProvider`, which the store does offer — so three
  falses cannot come from probing a store that implements nothing.
- Not covered: nothing in the criterion as stated. The criterion does not
  ask whether a caller that finds no capability behaves correctly; that is
  C32, deferred to the syncer change.

### C25 Downstream readers family-bounded

- Status: evidence incomplete; the fold cell is covered and passes.
- The fold cell was predicted failed from reading and is not: the fold
  calls `DropLedger` at `compactor_pebble.go:508` (`1f6cd380`), clearing
  rows, facts, buckets, frontier and the retain fact, clearing the stamp
  and rewriting the token key. Candidate:
  `TestCompactPebbleFoldDropsInheritedBaseLedger`. That closes CO-003 in
  the code's favour.
- Not covered: the rest of the P7 table. Rebuild modes
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
  never reaching a later token-only `EndSync`; the warn branch's
  `takeSyncStatsOverlay` call, which closed an overlay leak on the
  sidecar-failure path and has no test.
- Closes with: I2 F11 arm.

### C28 Commit-point registry

- Status: verified to stated coverage.
- Candidate: `commit_point_enumeration_test.go` — entries for
  `page_unit.go:Commit`, `adapter_page.go:Commit`,
  `ledger.go:ScrubLedgerTokens`, `ledger.go:takeoverToken`, and the
  `ledger.go:PutLedgerCounterBucket` exclusion. Runs green in the package
  run above.
- What that means: the registry matches the code as the meta-test reads
  it. A commit point the meta-test's scan does not reach is outside what
  the pass covers.

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
  `TestLedgerScrubLeavesNoSSTResidue` (O5 only). Five more mutants were
  planted and run by hand this round, each against the assertion written
  to catch it: buffer-length counts restored (O3), a first-wins dedup
  pre-pass (O3 survivor), `markDirty` dropped from grant-layer ingest, a
  method left unclassified (O9's meta-test), and a `SetWriteSeam` method
  on `*C1File` (C24's absence half). Only the first is in the tree as a
  switchable arm; the other five were reverted after the run.
- The first-wins mutant is the one worth keeping: it passes the count
  assertion, because either pre-pass leaves one key, and fails only the
  assertion on which record survived. A count oracle cannot see it.
- Not covered: mutants for O1, O2, O4, O6, O7, O8, O10, and no switchable
  arm for the four run by hand.
- Closes with: I10.

### C32 Syncer-owned obligations

- Status: deferred to the syncer integration change. Owner: that change's
  plan. The bundle: read-only walk; absent-or-mismatch → re-run; L3
  handling (OQ-1); `BoundSyncFinished` then `ResetLedger` on rebind;
  `WithPageWriteBypass` registrations; one commit per page; counters and
  facts producers; takeover trigger and resume from the frontier (plan
  §5.1 itemizes these: the engine moves the token unparsed, and the
  token is gone once it has, so the reading side is where a wrong resume
  skips pages with no fallback); stats fold across attempts.

## Explicit exclusions (plan §3.4)

- P5 hash collision at 128 bits: not constructible.
- Concurrent `Commit` on a single `PageUnit`: single-goroutine by
  contract; the `done` guard is asserted once under C03.
- SQLite store ledger behaviour: none exists; one negative assertion
  under C24.
- Token-only SDK writing into an L4/L5 file: refused by the pre-existing
  sealed-sync check; not re-derived here.

## Structural coverage triage: P3 (sub-family × surface), from reading

Legend: W writes · C clears · R reads · — must not touch. Cells are reading
results except where the note below names the test that asserts them.

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
| compactor fold | C | C | C | C | C | C | rewritten |

The compactor fold row was read as `copy?` and is not: the fold calls
`DropLedger` after copying the base, so the output carries no ledger key,
asserted by `TestCompactPebbleFoldDropsInheritedBaseLedger`. The `purge`
row's `bytes` cells are asserted by the needle scans in
`TestLedgerScrubLeavesNoSSTResidue` and
`TestLedgerResidueOutlivesTheLedger`. The takeover row is asserted whole by
`TestLedgerTakeoverCrashImages`: every W and the C land together in the
post image and none of them in the pre image, and the stamp's W (Sync)
lands ahead of the batch, which is the mid image. That also settles the
`TakeoverToken × L0` cell in P1. The `PutCounterBucket` stamp cell is still
from reading `ledger.go:PutLedgerCounterBucket`; the rest of the table is a
reading result.

## Evidence commands

The package runs recorded under "Status of this file" include all of
these. Listed so a next pass can run a criterion's candidates alone.
Package path `pkg/dotc1z/engine/pebble` unless stated.

```
go test -run 'TestPageUnit|TestLedger|TestLedgered|TestCheckpointRefused|TestResetForNewSync|TestTakeover|TestRetainDeclaration|TestDropLedger|TestFailedSeal|TestResetLedger' ./pkg/dotc1z/engine/pebble/
go test -run 'TestPageWriter' ./pkg/dotc1z/engine/pebble/
go test -run 'TestCommitPoint' ./pkg/dotc1z/engine/pebble/
go test -run 'TestPebbleStore.*Dirty|TestWriteSeam' ./pkg/dotc1z/
go test -run 'Provenance|TestCompactPebbleFold' ./pkg/synccompactor/
go test -run 'TestClearCompactionSection|TestBuildCompactedToken' ./pkg/sync/
go test -race -run 'TestPageUnit' ./pkg/dotc1z/engine/pebble/
go test -run '^$' -bench 'BenchmarkLedger' -benchtime 20x ./pkg/dotc1z/engine/pebble/   # unloaded machine only
```

## Performance evidence

None. C30 is open until I8 runs on an unloaded machine.

OQ-7 is narrowed, not answered. The ledger-free half is settled without a
benchmark: `endSyncFinalize` gates the purge on `ledgerActive`, so a file
that never had a ledger pays nothing, pinned by
`TestLedgerFreeSealSkipsResiduePurge` against a counter. What remains is
the ledgered half — whether the purge's compaction on a 10^5-row seal with
the deferred grant index off is within a stated fraction of the
`BuildGrantDigests` seal time. Nothing measures it.

Page-commit cost is also unmeasured. The counts fix added a dedup pre-pass
over the resource-type buffer, allocating a `map[string]int` sized to the
buffer on any page staging more than one record. The argument that this is
immaterial next to the marshal and batch-write it sits beside is an
argument, not a measurement.

## Process corrections

- The plan was authored after implementation (plan §0.1). The model's
  axes were fixed before criteria were written; the fix commits were
  placed into cells afterwards (plan §3.6) and generated no new axis.
- Three contract disagreements between the brief and the code were
  resolved as change orders before modeling (CO-001..003), not by
  silently testing the code's behaviour.
- The first version of this file claimed nothing had been run, which was
  true at `04644cf6` and is no longer. Candidates have since been run at
  the commits recorded above.
- The plan froze at `04644cf6` and the branch did not stop. Per plan §6,
  a post-fix change restarts the clock; these are the change orders since,
  each re-routed through the risk model before landing:
  - `1f6cd380` fold calls `DropLedger`, resolving CO-003 and C25's fold
    cell.
  - the residue marker: `DropLedger` leaves a durable marker so a later
    seal purges bytes whose rows are already gone (C21). `ResetForNewSync`
    excises the whole keyspace instead and owes nothing.
  - `11f8c8c3` + `21d4349e` the counts fix, which is C07's own defect
    found by this plan's reading and closed by measurement.
  - `2c321bea` the `markDirty` fix on grant-layer ingest (C22), with the
    meta-test extended to the method set that hid it.
- Two verification gaps are stated rather than closed, and are recorded
  above under C22 and Performance evidence: the `Begin → Add → Abort →
  Close` save path is argued, not tested, and page-commit cost is
  unmeasured.
