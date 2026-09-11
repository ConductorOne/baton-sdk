# Page ledger: coverage-driven verification plan

Change under verification: branch `kans/ledger-storage`, `origin/main..HEAD`
at `04644cf6` (eleven commits, `c7edb4c7..04644cf6`). A page's records and
the ledger row that marks the page complete now commit in one pebble batch,
so a crash leaves exactly the set of completed pages. This replaces the
single opaque checkpoint token rewritten per checkpoint.

Contract sources, in precedence order: `docs/tasks/sound-syncs-solutions-brief.md`
§3 (the page ledger), the proto doc comments on `LedgerRow`,
`LedgerActionIdentity`, `LedgerCounterBucket`, `LedgerFrontier`, and
`CompactionProvenance` in `proto/c1/storage/v3/records.proto`, and the
interface docs on `c1zstore.PageWriter`, `c1zstore.PageLedgerStore`,
`c1zstore.SyncStatsStore`, and `c1zstore.WriteSeamStore`. Where the brief and
the implementation disagree, the plan names the disagreement in §0.3 and
tests the implemented contract; the disagreement is a change order, not a
silent re-read.

Scope boundary: the engine (`pkg/dotc1z/engine/pebble`) and store
(`pkg/dotc1z`) layers are implemented. Nothing outside tests calls
`BeginPage`, `TakeoverToken`, `PutCounterBucket`, or `EndSyncWithStats`.
The syncer's resume walk, its bypass registrations, and its counters and
facts producers are a separate future change. §5 splits every criterion
into closable now or deferred to that change by name.

## 0. Guardrails, risk verdict, and authorship

### 0.1 Authorship

This plan was written after the eleven commits landed, by an author who
read the commits, the tests, and the two review-fix commits (`5eda7c18`,
`d9dbdb62`) before freezing the model. `docs/BUG_CATCHING.md` §2 asks for
the plan to be frozen before implementation, and for the coverage model
to be built from the contract rather than from bugs already found.

What this weakens:

- The known-bug bias. Nine of the fixes in `5eda7c18` and `d9dbdb62` sit
  in cells this author already knew were live. A model derived after
  reading them can look complete because it covers the bugs it was shown.
- The reading-order bias. Reading `page_unit.go:Commit` before writing the
  atomicity criterion makes it easy to state the criterion in the code's
  own terms (one `RecordBatch`) rather than in the observable's terms
  (no image with a torn page).

How this plan mitigates it:

- The axes in §3 are mechanical products (operation × file state, cut
  point × process identity, sub-family × surface, record shape). Every
  cell exists whether or not a known bug lives in it. §3.6 places each
  fix commit's bug in its cell as a check on the model, not as a source
  of cells.
- Criteria in §5 are stated as observables on the file (keys present
  after reopen, bytes present in SSTs, errors returned to a caller),
  never as "the code calls X".
- §5 marks, per criterion, whether an existing test asserts it. The
  evidence file starts every criterion at "not assessed" or "evidence
  incomplete" until an instrument is run under this plan; existing tests
  are candidates, not closure.

### 0.2 Risk verdict and routing

Escape axes (`docs/REVIEW_CHECKLIST.md`):

- Silence: a torn page, a misattributed row, a wrong `*_written` count,
  or a skipped-not-rerun page produces a well-formed `.c1z`. No reader
  errors. Silent.
- Durability: the ledger is a new keyspace family (`TypeLedger = 0x0C`)
  in the durable artifact; the in-flight stamp
  (`keyspaceVersionLedgerInFlight = 3`) is a cross-version acceptance
  contract; scrub and purge decide what bytes ship in the artifact.
  Durable, and the format decision cannot be retracted by a rollback.
- Uncontrolled dimensions: crash timing (per byte of a NoSync WAL), the
  SDK version pair that writes and later reads a file, page size and
  row count, and the order in which the syncer (not yet written) will
  issue pages.
- Consumer distance: readers of the file are the compactor, the clone
  path, the sanitizer, the CLI tools, and every older SDK that opens a
  file this SDK wrote. None of them are in this diff.

Verdict: **HIGH**. Silent and durable with version-pair dependence.
Consequence rung: a bad file requires migrate-or-coordinate (an in-flight
file older SDKs refuse cannot be resumed by them; a scrub that misses a
row ships a token). Cost contracts on two hot paths: the per-page commit
and `EndSync` (artifact seal), which now runs scrub and a range
compaction.

Exposure of the reachable delta, today, on the default path: the shared
staging refactor of `Put*Records` (`fceb24c4`) is on every write; the
write-hook check `pebbleStore.seam` is on every store write; `ledgerActive`
runs a bounded iterator on every `CheckpointSync` and `EndSync`; the
compactor now writes provenance to the stats sidecar instead of the token
(`91bf689e`); the accepted `supportedKeyspaceVersions` set changed at
`Open`. The page path itself is unreachable outside tests.

Routing: the full pass set. Pass 2 (failure modes and crash cuts), Pass 3
(durability and version pairs), Pass 5 (differential against the
`Put*Records` path), and Pass 6 (cost) are the heavy passes. Pass 1
(contract read), Pass 4 (consumers), and Pass 7 (process) are selected.
`docs/BUG_CATCHING.md` §2 step-up applies: coverage model, closure
criteria, instruments with planted violations, implementation-obligation
addendum.

### 0.3 Contract disagreements resolved before modeling

- Brief §3.8 says an older SDK resumes a ledgered sync through the token
  and says not to bump `keyspaceVersion`. The implementation writes no
  token during a ledgered run (§3.6 of the brief: "the token is never
  written") and stamps `keyspaceVersionLedgerInFlight` so a token-only
  SDK refuses the in-flight file. §3.6 wins; the §3.8 downgrade path is
  superseded. This plan tests the stamp contract (S6) and records the
  supersession as CO-001.
- Brief §3.10 claims no fsync per page. `bindCurrentSync` sets
  `freshSync = false`, so a resumed sync commits every page with
  `pebble.Sync`. The implemented contract is: fresh syncs are NoSync per
  page, resumed syncs are Sync per page, and the fresh case is the cost
  claim under test (S9). Recorded as CO-002.
- `ledger.go:DropLedger` documents compaction outputs as a caller. The
  compactor's fold path (`compactPebbleFold`) byte-copies the base and
  does not call it. The plan tests the implemented behaviour (fold
  output inherits the base's ledger) as a criterion with an expected
  status of open (C25), and records it as OQ-6.

### 0.4 Machine constraints on this authoring pass

No benchmark, no full suite, and no crash-image sweep was run while
authoring. `go build ./pkg/dotc1z/... ./pkg/synccompactor/...` and
`go vet` on the three changed packages pass at `04644cf6`. Every
"candidate artifact" named in the evidence file is a test that exists and
whose assertions this author read; none was executed under this plan.

## 1. Frozen core: stage claims (S#)

Each claim is stated on the file, not on the code.

- **S1 Atomic page.** For every page unit, either every record staged in
  it, its ledger row, its facts, and its bucket are present after any
  crash or reopen, or none of them is. There is no third image. A commit
  that returns an error leaves the file byte-identical to before the
  call and leaves the unit usable.
- **S2 Path equivalence.** Records committed through a page unit produce
  the same primary rows, secondary index rows, grant digest and source
  scope rows, and fresh-sync proof state as the same records committed
  through `Put*Records` and `DeleteGrants` in the same order. The ledger
  sub-families are the only difference.
- **S3 Read-side identity.** `GetLedgerRow` returns a row only when every
  field of `LedgerActionIdentity` matches the caller's tuple: `Op`,
  `ResourceTypeId`, `ResourceId`, parent type and id, `TypeScoped`,
  `Spawned`, and the page token (by value, or by hash when the row is
  scrubbed). Any mismatch reads as absent, is counted in
  `ledgerMismatches`, and never returns a different action's row.
- **S4 Ordering.** Within a page: puts land before grant deletes, so a
  buffered put doomed by a delete in the same page never lands. Across
  pages: because every page rides one WAL batch and the WAL is a prefix,
  a child page's row is present only if every earlier-committed page's
  row is present; any Sync commit (takeover, `PutCounterBucket`, the
  in-flight stamp, seal) hardens every earlier NoSync page.
- **S5 Sub-family isolation.** Rows (`0x00`), facts (`0x01`), counter
  buckets (`0x02`), and the frontier (`0x03`) are written only through
  `rawdb.RecordBatch` staging ops; every write surface touches exactly
  the sub-families its contract names (§3.3); `ResetLedger`, `DropLedger`,
  and `ResetForNewSync` remove all four; the token key is written by
  `takeoverToken` (cleared) and by nothing else on a ledgered run.
- **S6 Version stamp.** The in-flight stamp is durable (Sync) before the
  first ledger row lands and is cleared before `ended_at` is written. A
  token-only SDK opens a sealed file, opens a no-row file, and refuses an
  in-flight file. `ledgerActive` is true when the stamp is set or any
  ledger key exists; `CheckpointSync` and plain `EndSync` refuse while it
  is true.
- **S7 Seal.** `EndSyncWithStats` runs scrub, purge, stamp clear,
  `ended_at`, stats persist, flush, in that order; each step is
  idempotent; a crash at any cut leaves a file that is either unfinished
  and sealable by re-running `EndSyncWithStats`, or finished with all of:
  scrubbed rows (unless retained), no pre-scrub token bytes in any SST or
  WAL, stamp at `keyspaceVersion`, stats sidecar present.
- **S8 Scrub and retain.** Scrub is the default. Every row's
  `next_page_token` and `page_token`, and the frontier's token, are
  empty after seal with their hashes intact and identity compare still
  working. The retain declaration is a durable fact
  (`c1z.retain_tokens`) that survives a crash; an absent or unreadable
  fact scrubs.
- **S9 Cost.** Fresh-sync page commit costs one NoSync batch, no fsync,
  no extra iterator; `ledgerActive` costs one bounded seek; scrub costs
  O(rows) with bounded batch memory (`ledgerScrubBatchBytes`); purge
  compacts only `LedgerBounds`; the resume walk over 10^4–10^5 rows is
  read-only and bounded.
- **S10 Stats and provenance.** A ledgered sync seals only through
  `EndSyncWithStats`; the stats it carries land in the sidecar with
  ingest quality; a failed seal drops its overlay; compaction provenance
  lands in `SyncStatsRecord.compaction` for fold and rebuild, chains
  across folds, and never writes a token.
- **S11 Store layer.** `pebbleStore` satisfies `PageLedgerStore`,
  `SyncStatsStore`, and `WriteSeamStore`; every mutating ledger method
  marks the store dirty on success and not on failure; every direct
  record-mutating store method consults the `WriteSeamHook` first; the
  SQLite store has none of these capabilities and fails the type
  assertion cleanly.
- **S12 Downstream readers are family-bounded.** Stats, diff, export,
  explorer, sanitizer, clone, and the compactor's rebuild modes read no
  ledger key and emit no ledger key; the ledger family's presence in a
  sealed file changes none of their outputs.

## 2. Failure properties and oracles (O#)

Where no single-run oracle exists the entry says so and names the
instrument in §7.

- **O1 Batch-atomicity oracle.** Digest of every key/value under
  `[versionV3, versionV3+1)` before and after a failed commit is equal.
  For a crash image: for each committed page, `row present ⇔ every
  record of that page present` over all four record kinds, facts, and
  bucket. No single-run oracle for the crash case; instrument I1 sweeps
  cut points.
- **O2 Path-equivalence differential.** Two fresh files, same sync id,
  same inputs; one via page units, one via `Put*Records`/`DeleteGrants`.
  Full keyspace snapshot excluding `LedgerBounds` and the engine-meta
  stamp key must be byte-equal, including secondary indexes, grant
  digest rows, source scope rows, and `SourceScopeMayExist` answers.
- **O3 Durable-state oracle.** Close (or `CrashClone`) and reopen; then
  inspect raw keys per sub-family with `IterateLedgerRows`,
  `LedgerFacts`, `SumLedgerCounters`, `GetLedgerFrontier`, and a raw
  iterator over `LedgerBounds`. Presence, not process state, is the
  oracle.
- **O4 Token-only reader oracle.** Open the file with
  `supportedKeyspaceVersions` shrunk to `{keyspaceVersion}` via
  `withTokenOnlySDK`; accept/refuse is the observable.
- **O5 Byte-absence oracle.** `checkpointNeedleHits` over every SST and
  WAL in the checkpoint: count of a planted needle in any page token.
  Zero after seal without retain; non-zero with retain (control).
- **O6 Identity-compare table.** For each of the eight identity fields,
  a row written with tuple T and read with T' differing in exactly that
  field returns `found = false` and increments `ledgerMismatches` by one;
  T read with T returns the row.
- **O7 Fold oracle.** `SumLedgerCounters` equals an independent fold:
  sums for the four counts and `Retries`, computed by the test from the
  buckets it wrote. Buckets under `TakeoverBucketWorker` and
  `RunBucketWorker` are included in the sum and never collide with
  worker `0`..`N`.
- **O8 Dirty oracle.** `pebbleStore.dirty` after each mutating call, and
  `Close` followed by reopen showing the mutation.
- **O9 Hook coverage oracle.** Meta-test: the set of `pebbleStore`
  methods that stage a `RecordBatch` (reflect over the store's method set
  intersected with the writer interfaces) equals the set that calls
  `seam` first, plus an explicit exclusion list with reasons.
  `StrictWriteSeam` returns `ErrUnregisteredPageWrite` for a call
  outside `WithOpenPage` and nil inside; `WithPageWriteBypass` clears it.
- **O10 Seal-cut dichotomy.** After a crash at cut F7..F11 (§3.3) and
  reopen: either `GetSyncRunRecord.ended_at` is unset and a second
  `EndSyncWithStats` succeeds and produces the finished image, or
  `ended_at` is set and O3 shows scrubbed rows, O5 shows zero hits, O4
  accepts, and `ReadSyncStatsRecord` returns a record with ingest
  quality.
- **O11 Cost curves.** ns/page and bytes/page for page commit vs
  `Put*Records` at page sizes 100, 1k, 10k; seal time vs row count at
  10^3, 10^4, 10^5 with and without the deferred grant index; walk time
  vs rows. Threshold is a ratio to the `Put*Records` baseline, not an
  absolute. No single-run oracle; instrument I8 is a benchmark sweep run
  on an unloaded machine.
- **O12 Resource ride-along.** Every test in the package closes with zero
  open pebble iterators, batches, and snapshots on the engine; every
  page unit path (commit success, commit failure, discard, refused
  read) releases its buffers.

## 3. Coverage dimensions and cross-product (D#, P#)

### 3.1 Axes

- **D1 Lifecycle operation (17):** `BeginPage`+`Stage*`; `Commit`;
  `Discard`; `GetLedgerRow`; `TakeoverToken`; `PutCounterBucket`;
  `SetRetainLedgerTokens`; the three reads (`LedgerFacts`,
  `LedgerCounters`, `LedgerFrontier`) as one; `BoundSyncFinished`;
  `ResetLedger`; `DropLedger`; `EndSyncWithStats`; plain `EndSync`;
  `CheckpointSync`; `StartNewSync`/`ResetForNewSync`; bind
  (`ResumeSync`/`SetCurrentSync`); `Open`.
- **D2 Ledger file state (7):** L0 no rows, no stamp; L1 stamp only;
  L2 rows + stamp, in flight, unscrubbed; L3 rows, stamp cleared,
  unfinished (crash inside the seal window); L4 sealed, scrubbed, stamp
  at `keyspaceVersion`; L5 sealed, unscrubbed (retain), stamp at
  `keyspaceVersion`; L6 rows without stamp, unfinished (an image from a
  build before the stamp, or from a `DropLedger` that failed between
  `DropKeyRange` and `clearLedgerInFlight`).
- **D3 Cut point (12):** F1 page batch commit returns an error; F2 crash
  after a NoSync page commit with k% of pages unsynced; F3 crash after a
  Sync page commit; F4 crash after `markLedgerInFlight` before the first
  batch; F5 takeover batch fails or crashes; F6 `PutCounterBucket` fails;
  F7 seal: after scrub before purge; F8 after purge before
  `clearLedgerInFlight`; F9 after clear before `ended_at`; F10 after
  `ended_at` before `PersistSyncStats`; F11 `PersistSyncStats` fails
  (non-fatal by design); F12 `DropLedger` after `DropKeyRange` before
  `clearLedgerInFlight`.
- **D4 Process identity at reopen (4):** same process, no reopen; new
  process, this SDK, writable; new process, token-only SDK
  (`withTokenOnlySDK`), writable; read-only `Open`.
- **D5 Sub-family (7):** rows `0x00`; facts `0x01` (bare and valued);
  buckets `0x02`; frontier `0x03`; the retain fact (a fact with its own
  consumer, `sealScrubsTokens`); the in-flight stamp (engine meta); the
  sync-run token key.
- **D6 Write surface (11):** page commit; takeover; `PutCounterBucket`;
  scrub; purge; `DropLedger`; `ResetLedger`; `ResetForNewSync`;
  `ledgerActive` (a read that decides a write); `CloneSync`; compactor
  fold.
- **D7 Record shape:** kind (4) × mutation {new; overwrite same value;
  overwrite with an index-affecting change (parent, principal, source
  scope); duplicate within the page, last wins} = 16, minus resource
  types' index-affecting cell (no secondary index) = 15; grant delete
  target {buffered put in the same page; store row; absent} = 3;
  `DropStagedRows` kind {resources, entitlements, grants} × selector
  {canonical id; principal in scope; principal out of scope; both} = 12;
  side state {bare fact; valued fact; empty-value fact; bucket; retain
  on; retain off} = 6; empty page = 1.
- **D8 Identity compare:** eight fields × {unscrubbed row, scrubbed row}
  = 16; exact match × 2 = 2; the two non-identity fields
  (`next_page_token`, `children`) do not participate × 1 each = 2.
- **D9 Durability class × write:** {fresh sync (NoSync), resumed sync
  (Sync)} × {page commit, takeover, bucket} = 6.
- **D10 Downstream reader (8) × sealed state {L4, L5} (2):** Stats;
  CLI readers (diff, export, access); explorer; sanitizer; compactor fold
  output; compactor rebuild output (k-way and overlay as one); clone;
  token-only `Open`.
- **Sampled axes:** k in F2 at {0, 20, 50, 80, 100}% over 120 pages;
  concurrent page units N ∈ {2, 8, 32} under `-race`; rows at
  {10^3, 10^4, 10^5}; page size at {100, 1k, 10k}.

### 3.2 Sub-products and cell counts

| Product | Axes | Cells | Reduction | Distinct |
| --- | --- | --- | --- | --- |
| P1 lifecycle × state | D1 × D2 | 17 × 7 = 119 | the four reads leave state unchanged in every L (collapse to one obligation per read per L, still 28 cells but one oracle) | 119 |
| P2 cut × identity | D3 × D4 | 12 × 4 = 48 | F1 and F6 land nothing, so the three reopen identities equal same-process: 6 cells collapse | 42 |
| P3 sub-family × surface | D5 × D6 | 7 × 11 = 77 | none; every cell states writes / clears / reads / must-not-touch | 77 |
| P4 record shape | D7 | 15 + 3 + 12 + 6 + 1 | none | 37 |
| P5 identity compare | D8 | 16 + 2 + 2 | none | 20 |
| P6 durability × write | D9 | 6 | none | 6 |
| P7 reader × sealed state | D10 | 8 × 2 | none | 16 |
| **Bounded total** | | | | **317** |

Closure by exhaustion is claimed only for P1–P7. F2's k% sweep,
concurrency, scale, and cost are measured sampling and are reported as
such in the evidence file.

### 3.3 What must hold per cell

**P1 (op × state).** Each cell states (a) whether the operation is
accepted or refused and with which error, and (b) the resulting L state.
The rows that are not obvious from the interface docs:

- `Commit` in L0 → L2 (the stamp is Synced first, so an image of L1 is
  possible: F4). `Commit` in L3, L4, L5 → refused by `requireCurrentSync`
  when the sync is sealed; in L3 (unfinished) accepted and the stamp is
  re-set. `Commit` in L6 → accepted, stamp set, state becomes L2.
- `CheckpointSync` in L0 → accepted; in L1, L2, L3, L6 → refused with
  `ErrLedgeredSyncWritesNoToken`; in L4/L5 → refused by the sealed check.
  L6 is the cell fixed in `d9dbdb62`
  (`TestCheckpointRefusedWhileLedgerRowsExistWithoutTheStamp`).
- Plain `EndSync` in L1, L2, L3, L6 → refused with
  `ErrLedgeredSyncNeedsStats`; in L0 → accepted.
- `EndSyncWithStats` in L0 → accepted, stats overlay applied, no scrub
  work (no rows), stamp untouched; in L1 → accepted, stamp cleared; L2,
  L3, L6 → accepted → L4 (or L5 with retain).
- `ResetForNewSync` in L2..L6 → all four sub-families gone and stamp
  cleared (`TestResetForNewSyncClearsTheInFlightStamp`); refused while
  `IsFreshSync`.
- `TakeoverToken` in L0 with a token → L2 (frontier + facts + bucket,
  token cleared) in one Sync batch; without a token → returns `""` and
  writes nothing; without an open sync → refused.
- `Open` in L1, L2, L3, L6 with this SDK → accepted, `ledgerInFlight`
  mirror set from the stamp (L6 sets it from rows through
  `ledgerActive`, not from the stamp: this is a cell to assert, not
  assume); with a token-only SDK → L1, L2 refused; L3, L6 accepted
  (stamp is `keyspaceVersion`) — L3 and L6 are the cells where an older
  SDK can open an unfinished ledgered file and must not be able to
  resume it by token (the token key is absent; the older SDK sees "no
  token" and starts fresh, which is safe). Assert, do not assume.
- `bind` in L4/L5 → accepted; the syncer's obligation to call
  `BoundSyncFinished` and then `ResetLedger` is deferred (C32).

**P2 (cut × identity).** Each cell states the reopened image and who can
resume. F2 and F3: O1 holds and the stamp is present in every image with
a row. F4: image is L1; this SDK opens and either commits (→ L2) or seals
(→ L4 with no rows); a token-only SDK refuses (rowless in-flight file:
OQ-3). F5: the token is intact and the frontier absent, or the token is
cleared and frontier + facts + bucket present (`TestLedgerTakeoverIsOneUnit`).
F7–F11: O10. F12: image is L6; O3 and P1's L6 row.

**P3 (sub-family × surface).** Stated as a matrix in the evidence file.
The cells that carry the most weight: scrub touches rows and frontier and
nothing else (`TestLedgerScrubReachesTheTakeoverFrontier` covers the
frontier half); purge touches bytes, not keys; `DropLedger` and
`ResetLedger` clear all seven except the token key; `ledgerActive` reads
rows-or-any-key and the stamp; `CloneSync` copies all seven as they
stand; compactor fold copies all seven (OQ-6); page commit writes rows,
facts, bucket, retain fact, stamp and never the frontier or the token;
takeover writes frontier, facts, retain fact, bucket, and clears the
token, never a row.

**P4 (record shape).** Each cell is an O2 differential run plus, for the
delete and `DropStagedRows` cells, an O3 check that the doomed row is
absent and the surviving rows present. `DropStagedRows` cells also assert
the returned count equals the buffered rows removed and that the store
half's `DeleteSourceCacheRowsInScope` semantics agree with the buffer
half's selector on the same input (differential over the twelve
selector cells).

**P5 (identity).** O6 for every field. Scrubbed cells use the hash path
and additionally assert that a different token with the same 16-byte
truncated hash is unrepresentable in the test (explicitly excluded: no
constructive collision at 128 bits).

**P6 (durability).** Assert the `pebble.WriteOptions` chosen per write:
fresh page → NoSync; resumed page → Sync; takeover → Sync always;
`PutCounterBucket` → Sync always; stamp → Sync always. Observable through
`testSeams` on the batch commit or through the crash image (a Sync write
never appears unsynced in a `CrashableMem` image).

**P7 (readers).** For each reader, output on a sealed file with the
ledger family present equals output on the same file with
`LedgerBounds` excised. For compactor fold output the plan expects a
difference (the ledger is inherited) and records it as open.

### 3.4 Cells this plan excludes, with reason

- Hash collision on the 16-byte truncated SHA-256 (P5): not
  constructible; the contract is "collision → re-run", and the re-run
  side is the syncer's (deferred).
- Concurrent `Commit` on one `PageUnit`: the unit is documented
  single-goroutine; the `done` guard converts misuse into
  `ErrPageUnitCommitted`, asserted once (C03), not swept.
- SQLite store: has no ledger; one negative type assertion (C24).
- Token-only SDK writing to an L4/L5 file: it would `CheckpointSync`
  into a sealed sync, which the sealed check refuses regardless of the
  ledger. Covered by pre-existing sealed-sync tests, not re-derived.

### 3.5 Non-single-run oracles and their instruments

O1 crash half → I1. O10 → I2. O11 → I8. O12 → I9 (ride-along). Every
other oracle is single-run.

### 3.6 Fix commits placed in the model (check, not source)

`5eda7c18`: scrub missing the frontier (P3: scrub × frontier);
`CheckpointSync` accepted in L6 (P1); `ResetForNewSync` leaving the stamp
(P3: reset × stamp); takeover bucket lost to a worker-zero page (P3:
bucket key collision, now `TakeoverBucketWorker`); page-unit read after
commit panicking (P1: `GetResource` after `Commit`); retain declaration
not durable (P3: retain fact × page commit; P2: F2 × new process).
`d9dbdb62`: `DropLedger` leaving the stamp (P2: F12); failed seal keeping
its overlay (P2: F7–F9 same process; S10); `ResetLedger` missing a
sub-family (P3: reset × each family). `802da981`: retain as a durable
fact (P3). `04644cf6`: scrub default (P1: `EndSyncWithStats` × L2 without
`SetRetainLedgerTokens`).

Every one of these lands in a cell the products generate without it.
None required an axis the products did not already have. That is the
check the model passes; it is not evidence the model is complete.

## 4. Criteria ids and coverage levels

Coverage levels: **exhaustive** (every cell of the named product),
**sampled** (named sample), **single** (one representative cell, used
only where the contract has no dimension).

## 5. Criteria, oracle, coverage level, and stage (C#)

"Now" means closable at the engine/store layer at `04644cf6`. "Deferred"
names the change that closes it. "Candidate" names an existing test that
asserts the criterion as written; absence of a candidate means a new
instrument is required (§7).

| ID | Claim | Criterion | Oracle | Coverage | Stage | Candidate |
| --- | --- | --- | --- | --- | --- | --- |
| C01 | S1 | A failed page commit leaves the keyspace byte-identical and the unit usable; a successful commit lands records, row, facts, bucket, retain fact together. | O1 | exhaustive over P4 side-state cells × {fail, succeed} | now | `TestPageUnitCommitIsOneFact`, `TestPageUnitFailedCommitLandsNothing`, `TestLedgerFactsAndBucketsRideThePageUnit` |
| C02 | S1, S4 | No crash image has a torn page; row present ⇔ all page records present; a WAL sync point hardens every earlier page. | O1 (I1) | sampled: k ∈ {0,20,50,80,100}% × 120 pages, plus F3 | now | `TestPageUnitCrashImageStoreEqualsLedger` |
| C03 | S1 | `Discard` writes nothing and releases buffers; a spent unit refuses every `Stage*`, `Get*`, `DropStagedRows`, and `Commit` with `ErrPageUnitCommitted`, never panics. | O3, O12 | exhaustive over the unit's methods | now | `TestPageWriterGetResourceAndDiscard`, `TestPageUnitReadsAfterCommitAreRefusedNotPanics` |
| C04 | S2 | `GetResource`/`GetEntitlement` on an open unit return the buffered value when present, else the store's, and after `DropStagedRows` return the store's. | O3 | exhaustive over {buffered, store-only, dropped, absent} × 2 kinds | now | `TestPageUnitReadSeesOwnWrites` (partial: buffered and store-only) |
| C05 | S2 | For every P4 record-shape cell, the page-unit file equals the `Put*`/`DeleteGrants` file outside `LedgerBounds` and the stamp key, including indexes, digests, and source scope rows. | O2 | exhaustive over P4's 15 + 3 cells | now | `TestPageWriterMatchesSingleCallAdapters` (partial: new records only; no overwrite, index-affecting, duplicate, or delete cells) |
| C06 | S2, S4 | A grant delete in the same page removes the buffered put (never lands) and deletes a store row with its index rows; `DropStagedRows` selector semantics equal the store's `DeleteSourceCacheRows*` on the same input for all twelve cells. | O2, O3 | exhaustive over P4's 3 + 12 cells | now | none |
| C07 | S1 | `LedgerRow.*_written` equals the count of distinct records the page committed, after in-page dedup and doomed-put removal. | O3 vs an independent count | exhaustive over 4 kinds × {no dups, dups} | now | none. Expected **failed**: `page_unit.go:Commit` records raw buffer length while the stagers dedup by identity. |
| C08 | S3 | Every P5 cell: exact match returns the row; each single-field difference reads absent and increments `ledgerMismatches` once; `next_page_token` and `children` differences do not affect the match; scrubbed rows match by hash. | O6 | exhaustive over P5 (20) | now | `TestLedgerIdentityMismatchReadsAsAbsent` (partial: token only, unscrubbed) |
| C09 | S3 | `encodeLedgerKey` is injective over the tuple and prefix-ordered by (op, rt, rid); `IterateLedgerRowsForOp` and `IterateLedgerRowsForResource` return exactly the rows under the prefix and no neighbour's. | key round-trip + prefix scan vs. an independent filter | exhaustive over field-boundary cases (empty strings, shared prefixes, `TypeScoped` flip) | now | `TestLedgerKeyEncoding` (partial) |
| C10 | S2 | The page path consumes and honours the fresh-sync proofs (`takeFresh*Empty`) identically to `Put*`: a proof consumed by a page whose commit fails leaves the slow path armed; a later colliding overwrite after a page uses read-before-write. | O2 plus `SourceScopeMayExist` and proof-flag inspection | exhaustive over 4 kinds × {commit ok, commit fail} | now | none |
| C11 | S4, S6 | Fresh pages are NoSync; resumed pages Sync; takeover, bucket, stamp always Sync (all P6 cells). | P6 write-option inspection + crash image | exhaustive over P6 (6) | now | none (`TestPageUnitCrashImageStoreEqualsLedger` shows the fresh half indirectly) |
| C12 | S6 | The stamp is durable before the first row: every crash image with a row has the stamp; F4 yields L1; token-only SDK refuses L1 and L2, accepts L0, L4, L5, and (assert) L3, L6. | O3, O4 | exhaustive over D2 × {this SDK, token-only} | now | `TestLedgerInFlightStampGatesTokenOnlyReaders` (partial: L0, L2, L4) |
| C13 | S6 | `CheckpointSync` and plain `EndSync` are refused in L1, L2, L3, L6 with the named errors and accepted in L0; accepted again after `ResetForNewSync`, `DropLedger`, or `ResetLedger`. | error identity + O3 | exhaustive over P1's two rows × 7 states | now | `TestLedgeredSyncSealsOnlyWithStats`, `TestCheckpointRefusedWhileLedgerRowsExistWithoutTheStamp` (partial) |
| C14 | S7 | Seal-cut dichotomy at F7, F8, F9, F10, F11 for each of the four D4 identities; second `EndSyncWithStats` after an unfinished cut is idempotent and reaches the finished image. | O10 (I2) | exhaustive over P2's seal cells (5 × 4) | now | `TestFailedSealDropsItsStatsOverlay` (F7-class, same process only) |
| C15 | S8 | After seal without retain: every row's tokens and the frontier token are empty, hashes intact, identity compare (by hash) still finds every row; zero needle hits in any SST or WAL; purge is what makes the byte count zero (mutant `skipLedgerResiduePurge` shows non-zero). | O3, O5, O6 | exhaustive over {rows, frontier} × {retain off, retain on, mutant} | now | `TestLedgerScrubAtSealForSensitiveTokens`, `TestLedgerScrubLeavesNoSSTResidue`, `TestLedgerScrubReachesTheTakeoverFrontier` |
| C16 | S8 | The retain declaration is a fact written by page commit and by takeover when the flag is set; it survives a crash and a reopen in a process that never called `SetRetainLedgerTokens`; absent fact scrubs; an unreadable fact scrubs and returns the error. | O3, O5 | exhaustive over {flag only, fact only, both, neither, read error} × {same process, new process} | now | `TestRetainDeclarationSurvivesCrashAndItsAbsenceScrubs` (partial: no read-error cell) |
| C17 | S9 | Scrub batches re-mint at `ledgerScrubBatchBytes`; peak batch memory bounded at 10^5 rows; scrub is idempotent (second call is a no-op with zero writes). | batch count vs rows × row size; O3 | sampled: 10^3, 10^4, 10^5 rows | now | none |
| C18 | S5 | Facts: bare (`0x01`), valued (`0x02`+bytes), and empty-valued (`0x02` alone) round-trip through `LedgerFacts`; last writer wins across pages; facts survive reopen; `SetFact` after `SetFactValue` on the same key overwrites. | O3 | exhaustive over {bare, valued, empty} × {page, takeover} × {first write, overwrite} | now | `TestLedgerFactsAndBucketsRideThePageUnit` (partial) |
| C19 | S5 | Buckets keyed (runID, worker) are blind-written whole; `SumLedgerCounters` folds all five fields across workers and runs; `TakeoverBucketWorker` and `RunBucketWorker` never collide with a page worker including `0`; `PutCounterBucket` is Sync and outside any page. | O7, O3 | exhaustive over {worker 0, worker N, takeover, run} × {one run, two runs} | now | `TestTakeoverBucketSurvivesWorkerZerosPage`, `TestTakeoverPersistsStatsOnlyCounters` (partial) |
| C20 | S5, S6 | Takeover writes frontier + facts + retain fact (if set) + bucket and clears the token in one Sync batch; no token → `""` and no write; no open sync → refused; F5 yields one of exactly two images. | O3, I1 | exhaustive over {token, no token} × {sync open, none} × {retain on, off}; F5 sampled | now | `TestLedgerTakeoverIsOneUnit`, `TestLedgerTakeoverRequiresOpenSync` |
| C21 | S5 | `DropLedger`, `ResetLedger`, `ResetForNewSync` each remove all of rows, facts, buckets, frontier, retain fact, and clear the stamp; `ResetForNewSync` is refused while `IsFreshSync`; F12 yields L6 and L6 is recoverable by any of the three; `BoundSyncFinished` is true exactly when `ended_at` is set. | O3 | exhaustive over 3 ops × 6 sub-families + F12 | now | `TestResetLedgerWipesEveryLedgerSubFamily`, `TestDropLedgerClearsTheInFlightStamp`, `TestLedgerWipedWithItsSync`, `TestResetForNewSyncClearsTheInFlightStamp` |
| C22 | S11 | Every mutating `PageLedgerStore`/`SyncStatsStore` method on `pebbleStore` marks dirty on success and not on error; `Close` after a page commit persists it. | O8 | exhaustive over {Commit, TakeoverToken, PutCounterBucket, ResetLedger, DropLedger, EndSyncWithStats} × {ok, err} | now | `pebble_store_dirty_test.go` (partial: success cells) |
| C23 | S11 | The set of `pebbleStore` methods that mutate records equals the set that calls `seam` first, modulo a stated exclusion list; `StrictWriteSeam` refuses outside `WithOpenPage`, allows inside, and `WithPageWriteBypass` allows outside. | O9 | exhaustive by meta-test | now | none. Known open cells: `FinishExpandedGrantLayer`, `AddExpandedGrantLayerContributions` (record-affecting, unguarded). |
| C24 | S11 | `pebbleStore` satisfies `PageLedgerStore`, `SyncStatsStore`, `WriteSeamStore` through the `dotc1z` open path; the SQLite store satisfies none (type assertion false, no panic). | compile-time + runtime assertion | single per store | now | `var _ c1zstore.PageLedgerStore = (*Engine)(nil)` (engine only) |
| C25 | S12 | For each D10 reader × {L4, L5}: output on the file equals output with `LedgerBounds` excised. Fold output is expected to differ (inherits the base's ledger) and is recorded as open, not verified. | P7 differential | exhaustive over P7 (16) | now for Stats, CLI readers, sanitizer, clone, token-only Open, rebuild; fold expected **failed** (OQ-6) | none |
| C26 | S10 | Fold and rebuild write `CompactionProvenance` with mode, base, partials, counts; chained folds accumulate; timings fold via `FoldCallStats`/`FoldDurations`; a source with no sidecar contributes nothing; no output writes a token. | sidecar read + token absence | exhaustive over {fold, k-way, overlay} × {base has sidecar, has none} × {first, chained} | now | `compactor_provenance_test.go` (partial) |
| C27 | S10 | `EndSyncWithStats` lays stats over the counted record and persists ingest quality; a failed finalize drops the overlay; a later plain `EndSync` on a token-only sync does not see a stale overlay; F11 leaves a finished file without a sidecar and `SourceCacheReplayEligible` fails closed on it. | `ReadSyncStatsRecord`, overlay inspection | exhaustive over {ok, finalize fail, stats-persist fail} × {ledgered, token-only} | now | `TestFailedSealDropsItsStatsOverlay`, `TestLedgeredSyncSealsOnlyWithStats` (partial) |
| C28 | S1 | Every `RecordBatch` commit site introduced by the change is in `commitPointRegistry` with a hook or a stated exclusion. | registry meta-test | exhaustive | now | `commit_point_enumeration_test.go` |
| C29 | S1, S9 | N page units on distinct pages commit concurrently under `-race` with O1 holding per page; a page commit racing `Close` is either fully in the saved file or absent; a page commit racing `EndSyncWithStats` is refused or lands before scrub (never after). | O1, O3 under `-race` | sampled: N ∈ {2, 8, 32} | now | none |
| C30 | S9 | Page commit ns/page and bytes/page within a stated ratio of `Put*Records` at 100/1k/10k; seal time vs rows at 10^3–10^5 with and without the deferred grant index; walk time vs rows; `ledgerActive` cost per `CheckpointSync` on an L0 file is one seek. | O11 (I8) | sampled | now, but not runnable on the authoring machine | `BenchmarkLedgerPageCommit`, `BenchmarkLedgerPageCommitSync`, `BenchmarkLedgerResumeWalk`, `BenchmarkLedgerSealCost`, `BenchmarkLedgerSealCostNoGrantIndex` (exist, unrun) |
| C31 | all | Each oracle O1–O10 catches a planted violation: torn page (write records without the row), wrong-field row, unscrubbed row, missing stamp, skipped purge, missing dirty mark, unguarded writer. | mutant per oracle | exhaustive over oracles | now | `TestLedgerScrubLeavesNoSSTResidue` mutant arm only |
| C32 | S3, S4 | Deferred: the resume walk writes nothing; absent or mismatched row → re-run, never skip; a scrubbed row in an unfinished file (L3) is read as "done" only because every action is done at seal (OQ-1); the syncer calls `BoundSyncFinished` then `ResetLedger` on rebind of a finished sync; every non-page write on the sync path is registered through `WithPageWriteBypass`; one commit per page; counters and facts producers; takeover trigger; stats fold across attempts. | deferred | deferred | deferred to the syncer integration change | none |

Count: 32 criteria. 31 closable now (C30 requires an unloaded machine; C07
and the fold half of C25 are expected to fail as written), 1 deferred
(C32, which bundles the syncer-owned obligations so the split is visible).

## 6. Closure criteria

A criterion is **verified to stated coverage** when every cell of its
named coverage level has run under an instrument in §7, the instrument's
premise and mutant checks (C31) pass for the oracle it uses, and the
evidence file records the command and the commit hash.

A criterion is **failed** when any cell fails; the evidence entry names
the cell and the observed value. C07 and C25's fold cell are expected to
enter this state on first run.

The stage claim S# is **closed** when every criterion citing it is
verified, failed-and-change-ordered, explicitly excluded, or deferred
with a named owner.

Bounded closure (P1–P7, 317 cells) is claimed only when a cell log
exists per product. Sampled criteria (C02's k%, C17, C29, C30) report the
sample and never claim exhaustion.

## 7. Required instruments (I#)

- **I1 Crash-cut sweep.** `CrashableMem` harness; for each F2 k% level
  and F3, F4, F5, F12: run, cut, reopen through each D4 identity, apply
  O1/O3/O4. Extends `TestPageUnitCrashImageStoreEqualsLedger` with the
  stamp and takeover cuts and the token-only reopen. Premise check: a
  planted torn page (records without row via `Put*Records` on the same
  batch boundary) is caught.
- **I2 Seal-cut harness.** `testSeams` hook after each of scrub, purge,
  `clearLedgerInFlight`, `PutSyncRunRecord(ended_at)`, and a
  `PersistSyncStats` failure injection; crash or return at the hook;
  reopen under each D4 identity; apply O10. Requires hooks that do not
  exist yet at F8, F9, F10 (obligation, §9).
- **I3 Path-equivalence differential.** Two-engine driver over P4's 18
  record cells and the 12 `DropStagedRows` cells; O2 snapshot excluding
  `LedgerBounds` and the stamp key. Premise check: a deliberately
  different record set produces a diff.
- **I4 Identity table.** Table-driven over P5's 20 cells with
  `ledgerMismatches` deltas (O6).
- **I5 Sub-family matrix.** For each D6 surface, execute once on an L2
  file with every sub-family populated, then O3 per D5 family: state
  whether each is present, cleared, or unchanged. Produces the P3 cell
  log.
- **I6 Hook coverage meta-test.** Reflect over `*pebbleStore` methods in
  the writer interfaces; parse `pebble_store.go` and `source_cache.go`
  for `s.seam(ctx,` as the first statement; diff against an exclusion
  list. Plus `StrictWriteSeam` three-way test (O9).
- **I7 Reader differential.** For each D10 reader, run on a sealed
  ledgered file and on a copy with `LedgerBounds` excised; compare
  outputs (P7).
- **I8 Cost sweep.** The five existing benchmarks plus a `Put*Records`
  baseline at matched page sizes; run on an unloaded machine with
  `-benchtime` fixed; report ratios (O11).
- **I9 Resource ride-along.** The package's existing leak ledger applied
  to every new test (O12).
- **I10 Mutants.** One planted violation per oracle (C31), each a
  test-only hook in `testSeams` or a deliberately wrong input.

## 8. Placement map

| Instrument | Location |
| --- | --- |
| I1, I2, I4, I5, I10 | `pkg/dotc1z/engine/pebble/ledger_*_test.go` (extend `ledger_test.go`, `ledger_state_test.go`, `ledger_lifecycle_regression_test.go`) |
| I3 | `pkg/dotc1z/engine/pebble/adapter_page_test.go` |
| I6 | `pkg/dotc1z/pebble_store_write_seam_test.go` (new; cites `write_seam.go`) |
| I7 | `pkg/dotc1z/` for Stats/clone/sanitizer; `pkg/synccompactor/` for fold and rebuild outputs; `cmd/baton` readers through their existing golden tests |
| I8 | existing `*_bench_test.go` in `pkg/dotc1z/engine/pebble/` |
| I9 | existing ride-along in the package's `TestMain` |

## 9. Implementation-obligation addendum

Hooks and helpers the instruments need that do not exist at `04644cf6`:

- `testSeams` injection points after purge, after `clearLedgerInFlight`,
  and after `PutSyncRunRecord(ended_at)` inside `endSyncFinalize`
  (I2). Today `skipLedgerResiduePurge` is the only seal-window hook.
- A `PersistSyncStats` failure injection (I2, C27 F11 cell).
- A write-options observer on the page-unit batch commit, or an
  equivalent `CrashableMem` assertion that a Sync write is never
  unsynced in an image (C11).
- A `ledgerMismatches` reader for tests (C08). It is an unexported
  atomic today; a `test_seams.go` accessor suffices.
- A `DropStagedRows` store-half comparator: the buffer-half selector must
  be checked against `DeleteSourceCacheRowsInScope` on identical input
  (C06). No shared helper exists; the test constructs both.

None of these change production behaviour.

## 10. Open questions (OQ-#) with settling checks

- **OQ-1 Scrubbed rows in an unfinished file (L3).** A crash between
  `ScrubLedgerTokens` and `ended_at` leaves rows with
  `next_page_token = ""` and a non-empty hash. The future walk must not
  read `""` as "no next page" for an action that was mid-pagination.
  Today this is safe only because scrub runs after every action is done.
  Settling check: I2 at F7/F8/F9 with this SDK reopen, then assert that
  re-running `EndSyncWithStats` (not a walk) is the only accepted path,
  and record in C32 that the walk must refuse L3 or treat it as sealed.
- **OQ-2 Page begun under one sync, committed after rebind.** `BeginPage`
  captures `syncID`; `Commit` checks only `requireCurrentSync`. A unit
  begun under sync A and committed after `SetCurrentSync(B)` writes A's
  `sync_id` into record values under B. No caller today. Settling check:
  one test in I5 that binds B between `BeginPage` and `Commit` and
  asserts either refusal or the observed `sync_id`; the answer decides
  whether `Commit` needs a sync-id equality check.
- **OQ-3 Rowless in-flight file (L1).** F4 leaves a file a token-only SDK
  refuses although it holds no ledger data. Is refusal the intended
  contract (conservative) or should `Open` clear a rowless stamp?
  Settling check: I1 F4 × token-only reopen; the plan takes refusal as
  correct until the brief says otherwise.
- **OQ-4 Seal without a sidecar.** F11 (`PersistSyncStats` fails) is
  non-fatal, so a ledgered sync can finish with no ingest quality while
  `ErrLedgeredSyncNeedsStats` exists to guarantee stats. Settling check:
  C27's F11 cell; then decide whether F11 should fail the seal.
- **OQ-5 `DropLedger` on an unfinished ledgered sync.** It removes keys
  with `DropKeyRange` and clears the stamp without scrub or purge, so
  pre-scrub token bytes stay in SSTs until a later compaction, and the
  file is now accepted by a token-only SDK. Documented caller is
  rebind-of-finished only. Settling check: I5 `DropLedger` row on an L2
  file with O5; then either purge in `DropLedger` or document the
  contract as "finished syncs only" and assert it.
- **OQ-6 Compactor fold inherits the base's ledger.** `compactPebbleFold`
  byte-copies the base and never calls `DropLedger`; the fold output
  (a new sync id) carries the base's rows attributed to a different
  sync, and `ledgerActive` on the output is true, so any token-writing
  rebind of the output would be refused with
  `ErrLedgeredSyncWritesNoToken`. Not reachable until a ledgered base
  exists. Settling check: C25's fold cell with I7; expected to fail;
  change order to call `DropLedger` (or `ResetLedger`) on the fold
  output before `PutSyncRunRecord`.
- **OQ-7 Purge cost inside `EndSync`.** `PurgeLedgerResidue` un-pauses
  the compaction scheduler and compacts `LedgerBounds`. With the
  deferred grant index off, seal has no other large step to hide behind.
  Settling check: I8 `BenchmarkLedgerSealCostNoGrantIndex` at 10^5 rows
  on an unloaded machine; threshold is a stated fraction of the
  `BuildGrantDigests` seal time at the same scale.
- **OQ-8 `*_written` counts.** C07 is expected to fail. Settling check:
  the I3 cell with in-page duplicates; the fix is to count staged keys,
  not buffer length.

## 11. Change-order log (CO-###)

- **CO-001 (clarification).** Brief §3.8's token-based downgrade path is
  superseded by the in-flight stamp; §3.6's "token is never written"
  governs. S6 states the implemented contract.
- **CO-002 (clarification).** Brief §3.10's "no fsync per page" applies
  to fresh syncs only; resumed syncs commit Sync per page by
  `bindCurrentSync`. S9 and C11 state both halves.
- **CO-003 (extension).** `ledger.go:DropLedger` names compaction outputs
  as a caller; the compactor does not call it. C25's fold cell and OQ-6
  carry this until a fix or a documentation change lands.
