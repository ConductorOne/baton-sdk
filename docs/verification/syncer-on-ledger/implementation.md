# Syncer on the page ledger: implementation brief

Current design correction: CO-010 and CO-011 supersede the finished-run
reset, replacement executor, scheduler-copy and absolute shared-path-freeze
proposals below. Reuse the existing scheduler and preserve lifecycle
behavior. Sections 16–21 record the revised integration, restoration and
history-preserving continuation constraints; earlier sections retain the
history of the committed brief. The alternate executor is deleted entirely.

CXE-1358. Behavioral baseline: plan.md at `01931d8b`, with CO-001–CO-009
appended after calibration. This document is the step-3 deliverable. It is
committed alone before code. It describes planned work, not verified behavior.
There were 49 criteria and no implementation evidence when the initial brief
was committed. Current evidence and remaining gaps are in evidence.md.

## 1. Result and constraints

Pebble runs pages through a ledger runtime. A successful page publishes its
records, transition, facts and cumulative worker accounting together. Resume
reconstructs pending actions from the frontier and trustworthy rows without
writing during the walk. SQLite retains the baseline bodies and token path.

The ledger runtime gets its own type, files and receivers, owned by one
syncer field. The explicitly required `ledgered` boolean is the separate
attachment predicate; it is not inferred from nil capabilities at use sites.
Capabilities are resolved only in store_caps.go. Options live on syncConfig;
all syncer test hooks live on syncTestHooks. Existing runState, runStats,
checkpoint serialization and token scheduler bodies remain unchanged.

The final production integration inserts leading ledger forks. Copies of
store-touching handlers and helpers serve the ledger path; only store-free
logic may be shared. Attachment error propagation is a necessary additional
change: setStore validates and returns an error to NewSyncer/loadStore. An
empty engine is an unknown third-party engine, not SQLite (CO-008).

## 2. Runtime, page and scheduling design

### Attachment and lifecycle

`setStore` resolves capabilities, switches on Metadata().Engine and validates
Pebble+PageLedgerStore or SQLite-without-PageLedgerStore. Only then does it
publish store/caps/ledgered together. Failed attachment cannot leave a new
store paired with old capabilities. New SyncOpts include
`WithRetainLedgerTokens(bool)`; omitted configuration never erases an
already durable retain fact. C01–C03,C35,C44,C45 own these decisions.

The ledger Sync function owns validation, binding, migration, walk, worker
execution, invariants, cleanup, terminal page and seal. For a finished
explicit binding it checks BoundSyncFinished before DropLedger and begins
new requested work over existing records. It does not import finished token
state or old accounting. A reset failure aborts before scheduling. An
unfinished binding never drops its ledger. C15,C33,C34,C36 own this order.

Keep the public Sync setup needed to attach a path-open store before the
ledger fork. Do not invoke the old loop and try to intercept Checkpoint:
that would execute old metadata writes and token restore logic. The final
source audit must show the fork precedes all such writes. SkipSync and
explicit Checkpoint/stop entry points get equivalent leading forks where
reachable; a ledger checkpoint never marshals or writes a token (C06,C31).

### Page transaction

A `ledgerPage` owns one PageWriter, the full input identity, the pending
transition, observations, worker-bucket candidate and any staged-read overlay.
The worker owns it exclusively; no writer is shared between goroutines.
`nextPageOrFinishAction`'s ledger fork records a proposed transition without
mutating global scheduling state. Ledger-only handlers must also use that
transition operation for warning completion and no-connector actions.

The runner installs WithOpenPage, invokes the handler, validates exactly one
transition, stages facts and a candidate cumulative bucket, then commits.
Success without a transition is an error, including Init/control handlers
(CO-003). A second transition is also an error. Any handler/staging/commit
failure discards the writer and observation candidate. It does not publish
facts, counters, graph progress or children. A failed retry gets a fresh
writer. Commit success publishes observations and scheduling state before
that worker accepts its next page. No old checkpoint call is used here.
C04–C08,C13,C17,C20,C39,C47 own this protocol.

The runtime coordinates admission and completion under a scheduler mutex;
connector work and page staging run outside it. Identity admission prevents
two workers executing the same identity simultaneously. Commit publication
and admission have an explicit ordered handoff: children become dispatchable
only after their parent's commit returned successfully. Worker bucket state
is private; fact publication and queue transition share the coordinator's
publication boundary. No runtime lock is held during connector calls. The
implementation-obligation inventory will pin the exact lock order before
these tests are classified. C13,C14,C18,C39,C40 cover the handoff.

A page's durable row includes all seven identity fields, next token,
children with full identities and Spawned flags, TypeScopedPlanned,
completion metadata, Attempt, written counts and durations. Attempt records
provenance only. There is no attempt-based trust check or test (CO-001).
Do not duplicate engine-owned distinct-written counts with slice lengths.

Internal no-connector work uses explicit, deterministic phase/cursor tokens
within the existing identity tuple. Init, store-listing fan-out, graph work
and terminal completion must advance a phase, enqueue children or finish.
No phase may repeatedly return the same identity as its own next page.
Internal token encoding is versioned and unambiguous with connector tokens;
the ledger-only decoder distinguishes the two. The brief does not change
ActionOp's existing token wire values. C05,C12,C13,C41 cover this encoding.

### Read-only walk

Seed pending state from the takeover frontier when present, otherwise from
new-run Init. Restore facts and aggregate counters first. Inspect seal-ready
before attempting ordinary reconstruction. For each pending identity, a
readable, matching, non-scrubbed row replaces that action with its recorded
next page and children. Trust has no other inputs (CO-001). Missing or
mismatched rows remain pending. A read error is an error, not a found row or
an empty frontier. A scrubbed row without completion proof returns the
accepted diagnostic without mutation.

Rebuild admission history for spawned identities while walking, including
completed ones, so diamond and mutual-mention graphs drain. Preserve normal
versus spawned completion accounting and TypeScopedPlanned. Do not derive
identity from generated stack IDs or empty ResourceID. The walk uses a
worklist and identity set, O(rows + children), rather than restarting at Init
for each missing row. It cannot increment connector statistics or write
counter buckets. C09–C16,C23,C26,C37,C46 own this behavior.

The no-write interval begins at the first row lookup and ends when the first
pending handler runs. For a fully reconstructed run it ends when the walk
returns and the lifecycle resumes. Takeover, finished-run reset, run-level
stop flush and seal occur outside that interval and have separate trace
assertions (CO-004). The test recorder logs these phase boundaries; a bypass
reason never exempts a write inside the walk.

### Facts and accounting

A page accumulates facts and ingest quality locally. Other workers see only
committed facts; a page may read its own local observations. The existing
five durable fact names remain unchanged. Ingest-known and ingest-blocked
are facts; numerical ingest counts are bucket counters and reason bits are
OR-folded. A page that drops data commits the gate, reasons and counts in
one unit. Missing legacy quality establishes conservative unknown-prior
state during takeover. Existing ingest helpers that mutate syncer-global
atomics are copied for the ledger path, not reused as writers (C17–C19).

Each execution attempt has a new accounting runID; workers begin with zero
current-attempt totals. Prior buckets are aggregated only for reporting and
gates, never copied into every worker's starting total. Every committed page
replaces its own worker's cumulative bucket. Page observations from a failed
attempt at that page do not contaminate the next candidate. Run-level
step/session observations use RunBucketWorker and are flushed on stop and
before seal. Migrated statistics use TakeoverBucketWorker, never worker 0.

Retry and wait instrumentation distinguishes page-committed observations
from run-level failed-attempt observations; the field ownership table in
evidence names one owner per metric. Replacing a whole bucket on repeated
stop/final flush must not increment totals. C20–C23,C28,C30 own the fold;
O5 independently verifies real measurements even when CO-005 permits their
normalization in the cross-run data comparison.

### Takeover

Read the open token and frontier before migration. Parse token state with
unmarshalToken before clearing anything; collect supported facts/accounting.
If ledger counters already exist, do not import legacy counters again,
including when only duration/session fields are present. A nonempty token
with no frontier is moved by TakeoverToken. A stamp-only mid-image follows
that same path. On an empty return, read the frontier; never interpret that
return alone as a fresh sync. Repeated resumes keep the original frontier
and import no second takeover bucket. C24–C30 own these branches.

Normalize decoded legacy compaction statistics out of the in-memory takeover
input. Do not rewrite the opaque frontier: its raw token is the engine's
migration record, not active compaction provenance. No SyncStats compaction
field is added, and no compactor code changes (CO-007). Accepted old inline
graphs retain the decoder's semantics for migration, but ongoing ledger
recovery rebuilds required working graphs from stored data.

### Terminal page, seal and rebind

After workers drain, invariants pass, and cleanup succeeds, commit a
no-connector terminal page containing the seal-ready fact and final durable
run accounting. The marker and terminal row form an atomic unit under all
applicable F cuts. Its identity is reserved in the ledger-only internal
phase encoding. The marker means no collection or expansion work remains;
it is never set merely because one worker's queue is empty.

Seal reads durable counters/facts, constructs SyncStats and invokes
EndSyncWithStats. A restart finding seal-ready skips collection and completes
sealing even with a partly scrubbed frontier or child tokens. Missing proof
plus scrubbed state is refused without writes, as CO-004 permits. The
engine's stats-persist fault remains successful degraded sealing; consumer
checks verify handover and fallback Stats(), not a guaranteed sidecar under
that fault (CO-007). Retention follows its durable declaration. C11,C22,
C31–C35 own this boundary. No seal-phase store method is needed.

## 3. Handler boundaries and proposed resolution of OQ-5

The following is a concrete proposal, not a claim that the storage contract
already supplies these methods. OQ-5 remains open in the plan until its
boundary proposal is dispositioned. No changes to pkg/dotc1z exist yet.

| Handler family | Ledger implementation | Criteria |
| --- | --- | --- |
| Resource types/resources/targeted | Copy connector/filter flow; stage puts; use page reads where records may be staged; preserve parent identity | C04,C07,C15,C42 |
| Entitlements/static entitlements | Copy per-resource/type-scoped planning; page-local filters/facts; stage entitlements and child identities | C05,C13,C14,C17,C19 |
| Grants | Stage grants, needs-expansion/external-grants facts and page counts together | C04,C17–C23 |
| External resources | Keep imports, remapping and replacement grants in pages; delete grants by complete refs using DeleteGrants | C08,C38,C42,CO-002 |
| Assets | Enumerate resource assets as deterministic child pages, one GetAsset stream per page; stage asset bytes rather than direct PutAsset | C04,C05,C42 |
| Expansion | Rebuild graph read-only; record deterministic expansion phase progress; stage output through an expansion-aware page adapter | C36,C38,C41 |

**Proposed PageWriter additions:**

- `PutAsset(ctx, assetRef, contentType, data)` with the existing Store argument
  types. Stages the asset record in the same page batch; no direct MetaSet.
- `DeleteResources(ctx, resources ...*v2.Resource)` and
  `DeleteEntitlements(ctx, entitlements ...*v2.Entitlement)`. Resolve by full
  structural identity and stage primary/index removal after puts, including
  same-page puts; never accept a bare external entitlement ID as identity.
- `StoreExpandedGrants(ctx, grants ...*v2.Grant)`. Stage expansion output with
  the existing GrantStore preservation semantics for original expansion
  metadata. Plain PutGrants is not assumed equivalent. Existing direct
  StoreExpandedGrants and SQLite bodies remain unchanged.

Each method must participate in discard, error, stale-sync refusal, dirty
marking and atomic commit exactly as existing page stages do. Extract or add
storage-only staging helpers as needed; do not alter direct operation
semantics. Test staged/stored/split identities and secondary indexes in the
same batch. These additions widen PageWriter for third-party implementers;
record that source-compatibility cost in the boundary disposition. They add
no alternate engine path and no separate capability fallback.

CO-002 is resolved by making bare-ID deletion unreachable in ledger handlers:
the external handler retains each grant and passes it to DeleteGrants. It
also compares replacement survivors by full identity, not only GetId(). The
O8 inventory must cover every page-reachable delete, including helpers. A
split stored/staged same-ID fixture checks exact-reference deletion preserves
the other identity. No change to DropStagedSourceCacheRows is included; its
separate storage fix belongs to the requester.

The new expansion adapter declines the existing streaming SST layer API,
whose intermediate publishes cannot share the page row's batch. It implements
expansion-preserving staged writes and provides read-through listings from
committed data plus current-page output where the expander reads its writes.
It retains read-side principal ordering and full-ref lookup requirements.
Graph construction is repeatable read-only work; walking graph-load rows
cannot be used as proof that a cold process already has that graph.

Expansion phase boundaries must align with deterministic graph work, with
progress published only after page commit. If RunSingleStep writes an
unbounded layer, the ledger-only coordinator must divide work at bounded
destination/output chunks and give those chunks identities; it cannot
silently buffer an entire large expansion or bypass the row. The final
obligation inventory records the actual chunk state and overlay listing
semantics before C41/C46 evidence is classified. If this cannot be done
without changing the shared expander's executed SQLite path, it requires a
ledger-only copy or a boundary amendment; no shared refactor is preapproved.

Session-store writes are the initial candidate for registered bypass: they
carry connector-session state, not committed sync records. Each actual method
gets a reason and crash proof under C38. Graph sidecar publication and ingest
verification lifecycle writes stay outside pages. Records, assets, deletes
and expansion output are not allowed bypasses. An unproven auxiliary write
remains a failure, not a reason string waiting to be filled in.

## 4. Commit sequence and independent gates

The sequence is dependency-ordered. Each code commit builds and vets on its
own; passing compilation does not make an intermediate runtime deployable.
Public Pebble routing is activated only after all handlers and lifecycle
paths exist. Before that, tests invoke the new runtime directly. No temporary
public option allows Pebble token fallback in the final implementation.

| Step | Commit contents | Independent validation |
| --- | --- | --- |
| D1 | Calibration appended to plan, OQ disposition references, C49 assignment | Frozen-text comparison, nine CO entries, diff check |
| D2 | This implementation brief alone | Criterion/CO coverage and sequence audit; diff check |
| K1 | Mandatory strict fixture, cell generator, raw/canonical oracle, legacy artifact inputs, baseline cost executable and machine recorder; evidence.md initialized per criterion | Fixture premise/mutant tests; baseline executable build/vet; no product behavior change |
| K2 | Private ledger runtime, page runner, walk, counters/facts, terminal page and seal; controlled test handlers for all lifecycle cuts | C04–C30,C31–C35 mechanism tests with red/green mutants; build/vet pkg/sync and engine |
| K3 | End-to-end benchmark driver over baseline and new runtime, timing/byte observers, first measured table and tripwire explanations | CO-009 matrix on recorded unloaded machine; O1/O5 fixture sanity; build/vet benchmark and affected packages |
| K4 | Dispositioned OQ-5 PageWriter additions and page-preserving expansion mutation, each with engine and public-consumer atomicity tests | pkg/dotc1z and Pebble tests, dirty/commit registry, build/vet/lint; no SQLite changes |
| K5 | New ledger resource/type/targeted/entitlement/static/grant handlers, with tests; existing production bodies unchanged | Relevant P1/P2/P6/P9 cells, type-scoped/spawned tests; pkg/sync build/vet/test |
| K6 | New external, asset and expansion handlers, staged-read overlays and graph recovery; no bare-ID page deletes | C08,C36,C38,C41,C42, CO-002 inventory, staged/stored split fixtures; build/vet and affected tests |
| K7 | Atomic attachment validation, public leading forks and ledger-only stop/checkpoint/skip routing; retention SyncOpt | C01–C03,C31,C37,C40,C44,C45; full pkg/sync test and token golden audit |
| K8 | Remaining public-entry product sweep, race/soak, consumer/compactor checks, final cost table and evidence closure | All repository gates, final-revision audit/read and CO-009 acceptance |

K2's test handlers produce real connector responses and real page mutations
through the runtime; they are fixtures, not production handler copies. K3
runs the full bind→collection→terminal→seal lifecycle with the deterministic
connector before K5/K6 add the production copies. The first table is clearly
labeled a pre-handler runtime measurement; it is not claimed to measure
final production dispatch. K8 repeats through public Sync with the final
handlers and reports the difference. If the requester requires final public
handler execution in the first table, that conflicts with its placement
before handler commits; do not relabel a component benchmark to hide it.

Every K commit runs `go build` and `go vet` on its changed package set and
consumers. For pkg/sync work that includes pkg/synccompactor; for PageWriter
changes it includes pkg/dotc1z/... and pkg/sync. Use Go 1.26.x and vendored
deps. Record exact revisions/commands in evidence, including failures.
No red mutant stays enabled in a green commit. Red evidence comes from a
reverted defect patch against the candidate test, before adding its guard,
or a controlled faulty driver where the oracle is the subject under test.

## 5. Candidate tests and planted defects

Names below are planned, not existing tests. Each criterion gets its own
entry in evidence even when a table-driven family covers several. Tests run
through the owning runtime or public Sync; storage-only tests cannot prove
syncer order. P1–P10 expand to explicit cell IDs with premise and skip/refusal
outcomes; unsupported combinations are not quietly omitted.

| Candidate family | Criteria | Defect the candidate must reject |
| --- | --- | --- |
| TestLedgerAttachMatrix | C01,C02 | Unknown/empty accepted or missing Pebble capability falls back |
| TestLedgerFrozenTokenPath | C03,C44,C45 | Existing SQLite body altered; capability assertion outside store_caps; second path predicate |
| TestLedgerPageCutMatrix | C04,C05,C06 | Torn records/row; transition published before commit; successful no-transition handler including Init |
| TestLedgerStagedRecords | C07,C08 | Reads miss latest staged value; delete happens before put; identity collapsed to external ID |
| TestLedgerWalkRows | C09,C10,C12 | Missing row skipped, exact row rerun, parent/token/type-scope lost; no attempt-trust mutant or test |
| TestLedgerSealRecovery | C11,C31,C32,C35 | Scrubbed next token treated as done; early seal-ready; plain EndSync; retain declaration lost |
| TestLedgerActionGraph | C13,C14 | Children lost when next token present; child admitted before parent commit; duplicate/cycle never drains |
| TestLedgerScopeResume | C15,C16 | Targeted parent identity collapsed; resumed data differs in one index/value |
| TestLedgerPageFacts | C17,C18,C19 | Failed fact becomes visible; concurrent publication loses fact; missing prior quality becomes clean |
| TestLedgerCounterFold | C20,C21,C22,C23 | Delta replaces total; reserved bucket collision; old totals seeded per worker; warning gate loses history |
| TestLedgerLegacyTakeover | C24,C25,C26,C27 | Frontier ignored when token empty; stamp prevents retry; V0 action/page reset to Init |
| TestLedgerTakeoverStats | C28,C29,C30 | Reimport after resume; malformed state becomes fresh; stats-only payload discarded; stale compaction adopted |
| TestLedgerFinishedRebind | C33,C34,C36 | Old rows suppress new work; ledger dropped on unfinished binding; expansion depends on rewritten token |
| TestLedgerWriteDiscipline | C37,C38 | Missing page context or unregistered write escapes hook; reason permits write during walk |
| TestLedgerStopAndResources | C39,C40 | Writer leak, commit after worker join/stop, checkpoint token on cancellation |
| TestLedgerExpansionRecovery | C41 | Cold graph skips already-ledgered load data; expansion output loses prior metadata; staged reads omit earlier chunk |
| TestLedgerExternalAndAssets | C42 | Direct asset put survives failed page; stale deletes outside batch; split-identity grant deleted by bare ID |
| TestLedgerConsumerOutput | C43 | Compactor/stats reads token as authority; injected sidecar degradation misclassified as failed seal |
| BenchmarkLedgerWalkScale | C46 | Full-history scan per row; writes during large walk |
| TestLedgerOracleMutations | C47 | Broken oracle accepts representative planted violations from each family |
| TestLedgerCoverageManifest | C48 | Reachable changed branch has no criterion/cell disposition; absent strict fixture accepted |
| LedgerSyncCost driver | C49 | Mislabeled Sync/NoSync arm, hidden sleep, omitted page, wrong baseline, byte counter reset or zero denominator |

C47 includes the CO-003 handler-success/no-transition mutant explicitly.
CO-002 uses the unreachable bare-ID alternative and an executable source
inventory plus full-identity survival check. CO-001 forbids an extra attempt
trust test; provenance is inspected only as accounting output where needed.

## 6. C49 measurement protocol

The cell space is 3 page counts × 3 record counts × 2 worker counts = 18
configurations, three arms = 54 runs per repetition. Records have stable
identities and fixed payload sizes; the deterministic connector has no
sleep or network dependency. Count distinct generated records and committed
pages so a fast run that lost work cannot look like an optimization. Declare
record-family mix and payload bytes in the report; use the same generator
for every arm. Large cells stream generation and verification instead of
holding their expected record set in memory.

Build two executables with the same Go toolchain and dependency revision:
(a) baseline eb63f1b5 with test-only observers; (b/c) the ledger runtime.
Do not mutate baseline production behavior to emulate ledger code. Run A/B/C
in interleaved rotated order per cell on the same machine, each in a separate
process with a new file and identical cache policy. Arm C commits page 1,
closes/reopens and completes through a new syncer; assert actual Sync write
options for subsequent pages. Also record a cold resume from the page-1
crash image; distinguish durable page count from returned commits.

At least three measured repetitions per completed cell; retain all samples,
median and spread. Record CPU model, core count, RAM, disk model/filesystem,
mount/storage type, Go version, Pebble options, compression, cache policy,
source revisions and load/IO observations. Stop evidence measurement if the
machine is loaded. Such results are labeled smoke checks only. A capacity
failure is a reported incomplete cell, never an extrapolated measured value.
The 10^5 × 1,000 estimate must be shown with its measured inputs and method;
extrapolation, if necessary, cannot close an unmeasured required cell.

Collect wall time, handler and Commit spans (non-overlapping definitions),
WAL/flush/compaction bytes from engine metrics, final c1z bytes, process peak
RSS, scrub time, counter-fold time, remaining seal time, and arm-C walk time.
Use engine-level test observers for scrub/fold and write metrics if existing
interfaces do not expose them. They are measurement instrumentation, not new
store behavior. Time no-op baseline phases as absent, not zero-duration
operations secretly compared by division.

Report B/A and C/A for each metric; a zero/absent baseline denominator is
`N/A` with raw values. Report extra bytes/page for row, bucket and facts from
encoded payload measurement, separately from total WAL/flush/compaction write
amplification. Decompose seal time versus rows. Validate observers against a
known-size write and a deliberately mislabeled durability arm.

Required artifacts: `cost-machine.md`, `cost-results.json`, `cost-table.md`,
raw run logs, invocation script and the CO-005 normalization list in
evidence.md. The first measured table precedes K5/K6. After measurement,
append every wall ratio >1.10, bytes ratio >1.25, or superlinear seal result
here with the measured cause, evidence and proposed response. These are
tripwires, not automatic rejection or acceptance. Requester acceptance of
the numbers is required before landing, as CO-009 states. No numbers,
tripwire explanations, or acceptance are claimed in this step-3 document.

## 7. Evidence, remaining boundaries and current status

C01–C49: not assessed; the candidate tests above do not yet exist. No code
commit exists, no mutation has been run, and no build/test result is claimed.
The documentation commits are independently reviewable and do not change
compiled code. The final full test/race/lint commands remain those in plan §8.

CO-004 settles the lifecycle-write and unproven-L3 questions; the design
uses an atomic terminal page, not a new seal method. CO-005 supplies the
explicit equality normalizations; O5 still checks accounting. CO-006 leaves
the SQLite rollback CLI frozen. CO-007 drops stale compaction provenance and
accepts documented missing-sidecar degradation. Retention, ingest split,
page-local facts and one-time takeover keep plan §5.2's decisions with these
clarifications.

OQ-5 has a proposed answer in §3: four page-writer additions, full-identity
deletes, and staged expansion output. It is not marked dispositioned by a
calibration entry that did not settle it. The expansion chunk/read-overlay
contract and the widened public PageWriter are the material design risks;
they need explicit boundary disposition before their code commit.

Calibration supplied the requested lifecycle, equality, rollback and stats
answers. It did not settle OQ-5's mutation surface. Its cost order also leaves
a distinction between the pre-handler runtime measurement and final public
Sync measurement; §4 makes that distinction explicit for review. Neither
uncertainty prevents committing this implementation brief, and neither may
be hidden by claiming a passing test or benchmark that has not run.


## 8. Execution note: K1 split

K1 is being delivered in two independently buildable commits. K1a contains
the strict fixture's initial direct-write coverage, raw snapshot/reopen
oracle, cell enumerator, per-criterion evidence record, machine recorder and
pinned baseline smoke executable. K1b completes sub-store/lifecycle write
coverage, canonical comparison and crash-image/legacy integration before
K2. This changes commit granularity, not required coverage or the order of
production handler work. The two capability fields are resolved in
store_caps.go in K1a so test fixtures obey the capability-resolution rule;
no execution path selects the ledger yet.

K1b supplies the mutation-surface recorder, page-writer ownership checks,
row/frontier canonicalization and process-crash database recovery fixtures.
Existing versioned token/artifact fixtures remain the migration inputs; they
are not regenerated. Instrument expansion that requires the runtime's
accounting and scheduling semantics happens with K2, and remains incomplete
in the evidence until exercised. The physical WAL-loss sweep and full
canonical accounting comparison are explicitly still required.

## 9. K2 execution and interrupted finished rebind

K2 is split into K2a (private page/walk/takeover/scheduling/terminal runtime)
and K2b (remaining lifecycle failure cuts and crash/differential instruments).
Neither enables public routing. The pre-handler cost table still follows K2.

The interrupted finished-rebind fixture found a storage contract gap:
SetCurrentSync unseals the engine, but preserves ended_at; DropLedger removes
the ledger and preserves ended_at too. A new page can commit over the old
records. On the next process's binding, BoundSyncFinished is still true,
so the required finished-rebind branch drops that new page and restarts Init.
A process without an explicit sync ID can instead overlook this run entirely
because its old completion timestamp remains present.

Proposed boundary disposition: extend DropLedger's finished-bound-run case
to atomically remove the old ledger and reopen the same sync-run record by
clearing ended_at and the old token. Invalidate stale stats and completion
provenance in that same lifecycle operation; retain the record data and sync
identity. On a non-finished binding, retain the existing DropLedger contract.
The syncer still refuses to drop an unfinished run. The storage commit must
cover failure cuts before/after reset, preserve data/indexes, and prove that
an interrupted new run is discoverable as unfinished. This is additional to
§3's PageWriter proposal and must be resolved before public integration.

Candidate TestLedgerInterruptedFinishedRebindResumesNewRun fails against
the current contract: the expected next token is `remaining`, but the actual
pending action is Init with an empty token after a second ledger reset. It
is retained as an explicitly disabled boundary candidate until that storage
change exists; it is not passing evidence for C33/C34. Ordinary finished
rebind and uninterrupted completion tests do not cover this failure.

## 10. C49 resumed-arm durability discrepancy

The executable baseline at eb63f1b5 disagrees with the storage plan's C11
and CO-002 description carried into calibration CO-009. In options.go,
recordWriteOpts is pebble.NoSync for fresh and bound syncs, independent of
Options.durability. page_unit.go commits every page with recordWriteOpts.
Changing WithDurability therefore cannot produce CO-009's asserted resumed
Sync-per-page arm. The same code is present at this branch's baseline;
no implementation change here caused the discrepancy.

The first C49 table must identify the actual resumed NoSync path. A
hypothetical Sync-per-page arm can be measured only with an explicitly
identified test-only engine modification, or after a requester-directed
storage contract change. It must not be labeled the existing production
resume path. CO-005 says no engine durability change, so this PR does not
silently change recordWriteOpts to satisfy the table label. Requester
boundary disposition is pending; C49 remains evidence incomplete.

## 11. K3 harness increment

K3a adds an opt-in fresh private-runtime cost driver beside the pinned
baseline executable. It uses the same deterministic resource generator and
verifies all generated resources plus init/data/terminal commit counts.
Timers separate handler execution, page commits, counter fold and total
seal. WAL/flush/compaction bytes and final artifact size are reported.
Scrub timing is explicitly unavailable, rather than inferred by subtracting
unrelated seal phases. The machine recorder includes CPU and memory cgroup
limits; host CPU count alone overstates this environment's available budget.

This increment is smoke instrumentation, not the first C49 acceptance table.
The actual resumed arm, any hypothetical Sync arm, phase observation inside
the engine, process RSS, interleaving driver and unloaded-machine evidence
are still required. The private handler combines init and resource-type
collection and does not model all production control phases; its output
cannot establish the final public Sync overhead. K5/K6 remain behind the
required first table and its boundary dispositions.

## 12. K3 phase measurements and interleaved runs

The resumed smoke arm stops after one committed data page, closes/reopens,
walks its recorded frontier, and runs the remaining pages. It is explicitly
named NoSync to match the source. I/O totals include both engine openings;
the first is flushed before its metrics snapshot so close-time
materialization is counted. This measurement flush occurs after workers
join, outside any page or walk. It does not introduce a checkpoint token.

The interleaving runner rotates token/fresh/resumed order, launches a fresh
process per sample, verifies input/output dimensions, captures each child's
peak RSS and binary digest, and keeps raw logs plus JSON and Markdown
median/ratio tables. Missing metrics are N/A. Its outputs remain smoke
results while C49's instrumentation, machine qualification and contract
disposition are incomplete.

The first storage instrumentation change adds an immutable snapshot of the
last entered EndSync finalize attempt's ledger scrub and purge durations.
The getter reports the two phases separately, including a finalize attempt
that fails after entering either phase. The observation changes no write
ordering, error handling, durability or stored data. A consumer test checks
scrub versus retain; an engine test checks failed-finalize reporting. This
serves C49's decomposition and does not settle the resumed durability gap.

## 13. Scope of the proposed finished-run reset

The concrete Engine.Ledger().Drop operation also serves compactor output
cleanup (synccompactor/compactor_pebble.go). Its semantics must remain ledger
removal with the completion record retained. The proposed reset belongs at
the PageLedgerStore finished-rebind boundary, implemented by a distinct
engine operation called from pebbleStore.DropLedger. It must not make the
compactor reopen a sealed output.

The proposed atomic batch removes ledger state and stale completion/stats
state while reopening the bound finished SyncRunRecord; it retains record
families and secondary indexes. Token purging and the durable pending-purge
marker still have to survive failure cuts. The required tests compare both
consumers: interrupted syncer rebind remains unfinished and resumable, while
compactor ledger removal preserves completion. This narrows §9's proposal;
it is not an implemented contract change or a resolved C33/C34 claim.


## 14. CO-010 supersedes the lifecycle reset proposal

Sections 9 and 13's atomic completion reset proposal is withdrawn. Their
claim that the storage contract must change is not an accepted conclusion.
The requester requires the baseline lifecycle behavior to remain intact.

The SDK service-mode task honors SkipExpandGrants with DontExpandGrants,
seals its collection and uploads it. An existing finished artifact can then
be selected with WithSyncID for expansion over its existing records. The
baseline startOrResumeSync returns newSync=false for an explicit ID. It
loads the recorded state and stats; selecting the ID does not clear ended_at.
The expansion-only option controls the requested work. The compactor is
another existing consumer of same-ID expansion over stored data.

The private beginLedgerRuntime's unconditional finished-ledger drop and
fresh Init are not approved production integration. Before replacing that
behavior, tests must compare the actual baseline caller sequence and its
persisted metadata, facts and counters through interruption. The ledger
must distinguish completed collection from the progress of subsequently
requested processing without redefining the sync lifecycle. The earlier
synthetic test exposes the private runtime's assumption; it does not prove
that changing storage completion semantics is the correct fix.

This correction leaves CO-009's durability wording unresolved. No code or
storage lifecycle behavior changes in this documentation commit.


## 15. Baseline contract audit changes the integration design

The [baseline audit](baseline-audit.md) records five reproduced private-runtime
differences and the unchanged main behavior each must preserve. These are
implementation defects against existing criteria, not requests to change
main's behavior. No ledger scheduling or lifecycle integration is approved
by the earlier generic graph description alone.

Preserve the baseline operation stack and batch eligibility rules. The
ledger executor must not dispatch all restored operations together. Preserve
completion counts for spawned actions separately from per-resource progress.
Validate same-response duplicate cursors before committing their page;
across-response admission history remains a separate check. Preserve unknown
quality at seal and conservative legacy restoration. Retain independent
worker errors while draining. Public stop, warning, retry and callback
behavior must match the baseline caller contracts listed in the audit.

K2 is not complete, and K3's private smoke measurements do not establish the
cost of the corrected scheduler. Add baseline comparisons to K2 before
claiming runtime equivalence, then repeat the cost measurements after those
corrections. Tests comparing two ledger runs remain useful for crashes but
are not an oracle for unchanged caller behavior. No production guards are
changed in the audit commit; the five diagnostic comparisons remain red.


## 16. Existing scheduler, engine-specific persistence

CO-011 permits small shared integration changes. The existing parallelSync,
syncParallel, parallelActionQueue and syncOneAction remain the scheduling
implementation. Do not copy them or repair the generic ledger executor into
a second implementation. Existing operation order, batch admission, retries,
warnings, error aggregation and caller callbacks remain authoritative.

The ledger integration belongs at page execution, transition commit and
resume restoration. A page stages its records, facts and accounting; its
validated transition becomes visible only after the page commits. The
existing scheduler then publishes and executes that transition using its
existing eligibility rules. Restoration must provide the action state that
scheduler expects, rather than passing a flattened stack to another queue.

Use small shared changes where those boundaries require them. Preserve
SQLite's existing checkpoint and write behavior; do not extend it, refactor
it for the ledger, or create abstractions solely to avoid an inert branch.
The requester has authorized these ordinary shared integration changes;
they do not require another exception request merely because SQLite reaches
the same function.

The private ledgerRuntime.execute implementation is rejected. Commit a42c8a32
deletes it from tests as well as production. Recovery fixtures and the cost
driver now call the existing scheduler; no alternate executor is retained.
K2 and subsequent sequence details must be revised around these boundaries
before claiming completion. This clarification changes no production code.

## 17. Shared scheduler integration commits

K2c adds a page invocation function used by the existing parallelSync and
syncOneAction call sites. The SQLite invocation delegates directly to the
same handler. The ledger invocation stages a page and its proposed
transition, then submits that transition to the existing transitioner.
parallelActionQueue keeps its validation and dispatch behavior. Inside its
existing transitionActionState callback, the page commits before runState
publishes the transition. No new queue or batch-selection loop is added.
Worker indexes travel in the ledger invocation context; sequential phases
use worker zero after the prior batch has drained.

Warning pages commit terminal page/accounting state, then return the original
warning for main's existing finishActionWithWarning path. Other handler
errors discard the page and flow to main's retry and error handling. Both
normal and spawned actions contribute completion counts. The queue rejects
same-response duplicates before its callback reaches the page commit.

K2c tests invoke the real scheduler with test handlers declared in
syncTestHooks. The first increment does not enable store attachment routing,
implement production record handlers, or claim Init/resume/finished-artifact
integration complete. Its acceptance is the existing scheduler's ordering,
warning/accounting behavior, transition atomicity and independent errors
with real page writers. The rejected generic executor was briefly retained as a test fixture, then
deleted in a42c8a32. Its historical tests and cost driver now call the existing
scheduler. Do not reintroduce a ledger-specific dispatch or worker loop in
production or test code.

K2d restores the ledger frontier into the existing action stack, handles
Init's page boundary using shared planning, and preserves the baseline
same-ID post-processing lifecycle. The fixture-removal portion of K2e is complete in a42c8a32; its cost and
recovery callers must continue onto the completed lifecycle integration. Run the full
sync suite, ledger race repetitions, build/vet and applicable lint at each
completed increment. Record any diagnostic still aimed at the historical
fixture as such; it is not a regression guard for the integrated scheduler.

K2c keeps warning eligibility at the existing call site. A NotFound from a
root resource-type listing remains an error and discards its page; scoped
worker warnings may commit terminal accounting. Persistence errors retain
their cause for errors.Is but cannot enter the connector-warning branch.
New child IDs are checked before commit because runState otherwise rejects
them after the durable write. Page facts become visible in runState only
after the page commits, before the existing transition is published.

## 18. Remove the historical executor before further integration

The requester requires immediate migration of all remaining test and cost
callers and deletion of ledger_executor_fixture_test.go. Fixtures will seed
the existing runState and invoke parallelSync; they will not implement a
queue or worker loop. Synthetic listing fixtures use listing operations,
not Init as a substitute for a connector call. Their transitions use
nextPageOrFinishAction. Diamond/cycle fixtures use main's spawned-action
admission rules, including registration of the root as spawned.

A committed child discovered by an uncommitted parent is resolved at page
invocation through the ledger row before any writer is opened. Identity and
scrub checks apply there as in the initial walk. This serves C09/C10/C12;
a regression guard must reject running that child's handler. The removed
finished-binding reset tests asserted superseded behavior and do not count
as lifecycle evidence. CO-010 lifecycle verification remains outstanding.

After migration, K2d proceeds with atomic Init planning and restoration
through the existing scheduler. No cost result from the deleted executor
will be presented as evidence for that integration.

## 19. Atomic Init using the existing operation plan

K2d first extracts Init's store-free decision into one plan of child actions
and facts. Both persistence paths consume that plan. The checkpoint path
still finishes Init, establishes its facts, pushes children in the same
order and forces its checkpoint. The ledger path stages those same facts
and children in its Init page and publishes them through the existing
transition callback only after commit. The scheduler retains its Init case;
no alternate loop is introduced. Listing handlers remain unavailable until
their planned integration and cost prerequisite.

TestInitialActionBaseline records eight observable cases against the old
Init body before extraction: full collection, both skip modes, expansion
only, external processing, inherited skip state, targeted resources and
deferred expansion. It checks ordered actions, facts, parent identity and
completion counts. The ledger failure test must first expose the current
non-atomic Init behavior, then prove failed commit leaves Init pending with
no new facts or durable rows. The passing ledger cases compare the committed
row's children and facts against the same baseline expectations. This is
bounded C05/C17 coverage; physical crash cuts and lifecycle restoration stay
open.

## 20. Restore ledger state into the existing scheduler

K2d restores the pending identities from the read-only walk into runState,
then loads the ledger's folded completion counts, per-operation warning
counts, facts and runStats. Restored history must not count as list-resource
completions in this process. Unknown ingest provenance receives the same
conservative in-memory treatment as a checkpoint resume. Inline legacy
graph state is restored; compaction provenance is not imported. Publication
of the restored state happens only after all reads and validation succeed.
The restoration method itself writes nothing and does not initialize new
work merely because the stack is empty or the sync has ended.

Tests compare restoration against independent unmarshalToken results for
all accepted fixture versions. Finished token artifacts with pending work
must preserve both that work and ended_at through takeover and repeated
resume. Empty finished frontiers retain their accounting and facts, leaving
main's existing empty-stack decision to request Init. An interrupted ledger
case checks that the actual scheduler runs only the uncommitted continuation
and finishes with prior counts plus new completions. Faulted reads must not
publish partial state. Fixtures will use this production restoration method
instead of rebuilding a reduced runState for tests.

A finished ledger with scrubbed rows also needs the old completion rows
replaced before another requested pass can record the same page identities.
DropLedger cannot implement this safely: it deletes facts and accounting as
well as rows. That lifecycle operation is separate from restoration and
must preserve the sync-run record and atomically retain history. It will be
specified and tested as its own storage change before public integration;
no ended_at reset or fallback checkpoint write is allowed.

Restored counts exposed a replay accounting defect: a committed child found
later by a newly executed parent was counted a second time when its recorded
transition removed it from runState. The shared transition implementation
will take an explicit completion-accounting flag, with its existing entry
always passing true. Only the ledger row-replay entry passes false. It still
removes the action and drains spawned work, but neither increments historical
counts nor the current-process list-resource threshold. The regression test
first observes three in-memory completions against two committed completions.

## 21. Finished ledger continuation without a lifecycle reset

Add PageLedgerStore.ClearLedgerRows(ctx, clearFacts). It requires a bound
finished sync and atomically deletes only page rows, the old takeover
frontier and the explicitly named facts. All counter buckets, other facts,
record families and the complete sync-run record remain unchanged. The
syncer clears only its seal-ready fact. This provides the storage operation
missing from DropLedger under CO-010; it does not write a checkpoint token
or create another sync-run record. The operation stamps the file ledgered
and commits with Sync. Deleted token residue uses the existing durable purge
marker and purge path. The store wrapper marks dirty even on a post-commit
purge failure so Close cannot discard a committed deletion.

The syncer uses it only for a finished ledger with seal-ready proof, or a
finished legacy frontier with an empty action stack after takeover. It then
starts at Init while preserving all history. After the atomic clear, the
absence of seal-ready proof and the fresh Init row chain describe pending
processing despite the unchanged ended_at. A later resume walks that chain;
it does not clear it again. A legacy frontier with pending actions is never
cleared. Restoration stays read-only; this operation is lifecycle work
before the first row lookup. Sealed-token scrubbing remains the default.

Commit the storage contract and implementation independently, with engine
consumer tests for preserved facts/counters/records/sync metadata, refusal
on an unfinished sync, and staged/committed failure cuts. Then commit the
syncer consumer with same-ID stop/reopen/resume tests, including a stop just
after clearing and another after committing a page. Old completion markers
must not suppress requested work; new progress must not be discarded.
Physical crash images and byte-residue checks remain explicit evidence
requirements; successful in-process fault checks alone do not close them.

## 22. Cost measurements through the retained scheduler

Repeat the interleaved token/fresh/resumed samples after deleting the alternate
executor. Record the token source (`eb63f1b5`) and ledger source (`fafa4f74`),
binary hashes, all samples and spread. Start with 1,000 and 10,000 pages,
100 resources/page, one/four workers, three repetitions. These synthetic
page handlers exercise the existing scheduler but omit production filtering
and control phases; wall ratios cannot establish production improvement.
Both ledger arms use the engine's actual NoSync writes. The required larger
cells, byte decomposition and final public Sync comparison remain open.

The machine recorder must identify the test artifact filesystem as well as
the checkout filesystem: Go tests use the process temporary directory.
C49's instrument check rejects output missing that separate observation;
record the failure before adding it. This is measurement code only. A
snapshot taken after samples is labeled with its collection time and cannot
retroactively establish unloaded-machine qualification. The new table keeps
that limitation and does not authorize K5/K6.

## 23. Actual resume cost and larger pages

CO-012 settles §10's durability discrepancy. Continue with the existing
fresh/resumed NoSync arms; no engine behavior changes or hypothetical arm
are needed. CO-009's other measurement obligations remain. Earlier pending
durability statements record the state before this disposition.

Extend the existing interleaved smoke to 1,000 pages × 1,000/10,000
resources/page × one/four workers, three repetitions per arm. Keep samples
separate from §22's data, with their own machine snapshot and binary hashes.
The larger pages test whether row and bucket overhead diminishes relative
to record work. Synthetic handlers and incomplete machine qualification
still prevent acceptance claims. Preserve all samples, including tripwire
results; explain observed regressions without inventing a cause from ratios.

## 24. Preserve initial ingestion-quality knowledge

Restoration currently initializes a fresh run's in-memory ingestion quality
as known clean, but Init does not commit that knowledge. A restart after
Init therefore restores unknown prior quality. C17/C19 require the initial
fact to survive with the page that establishes it; C24/C30 require the
restored state to distinguish fresh known quality from unknown legacy data.

Store fresh known quality in the restored runStats snapshot as well as the
existing ingestFilterStats state. Init stages the known-quality fact when
that snapshot exists. Unknown resumed input leaves the snapshot absent and
must not acquire a known-clean declaration. This does not add source-cache
replay behavior or change the checkpoint path.

Before the fix, add a fresh-versus-unknown Init/close/reopen test: the fresh
case must fail when its durable known-quality fact is absent. Add page-stage
failure cases asserting no fact or Init row becomes durable. After the fix,
run restoration/Init/continuation tests and their race cases, then the sync
suite and lint. Do not overlap compilation or test execution with the cost
samples. The measured executable stays pinned to fafa4f74; these changes
require a later cost executable revision.

## 25. Continue storage work while completing cost evidence

K4 may proceed while K3's measurement coverage remains incomplete. CO-009
places the first measured table before handler commits; it does not require
the entire cost criterion to close before independent storage work. K4 adds
the page-staged asset, deletion and expansion-preservation operations proposed
in §3, with their own public-consumer tests and failure cuts. It activates no
handler or public routing. Do not treat an incomplete performance matrix as
a reason to stop this independent work. Full C49 evidence and its final
revision rerun remain requirements before landing.

## 26. K4a: assets in a page

Add PageWriter.PutAsset with Store.PutAsset's argument types and validation
(nil/empty reference is an error). The page copies asset bytes when staged,
keeps the bound sync ID and discovery timestamp, and writes the asset through
a typed RecordBatch.StageAssetPut in the same batch as its row. Repeated
asset IDs use the last staged value. Assets have no secondary indexes. Direct
PutAsset/PutAssetRecord and SQLite bodies stay unchanged. Discard releases
the buffer; an old-sync page cannot write into a replacement sync.

C04/C07/C38/C42 candidates cover: staged invisibility, committed asset plus
row after reopen, overwrite/discard, same-ID last write, input-buffer reuse,
invalid references, failed record-batch commit followed by retry, discarded
writer refusal and stale-sync refusal. The public-store consumer uses the
strict page write hook and begins from a clean reopened artifact, so a
missing dirty mark cannot be masked by StartNewSync. Engine tests inject
the existing record-commit failure hook; no new failure hook is needed.

Before implementation, a temporary no-op PutAsset stub makes the new
consumer test compile and fail on the missing asset. A direct-write mutant
must fail the staged-invisibility assertion. Neither stub nor mutant is
committed. The interface addition is a source-compatibility change for
external PageWriter implementers; engine selection still has no fallback.
This commit adds storage support only, not SyncAssets integration. Other
record families, process-crash products and the complete handler inventory
remain open.

## 27. K4b: resource and entitlement deletion in a page

Add PageWriter.DeleteResources and DeleteEntitlements, taking full v2
objects. Validate the complete request before appending identities; reject
missing resource identities. Entitlement identity uses the same conversion
as the exact-reference store delete. Missing rows are no-ops; duplicate
deletes are harmless. Deletions apply after all page puts, irrespective of
call order, and do not cascade. Reads inside the page continue to expose
puts until commit, as the existing deferred DeleteGrants contract does.

Keep the resource and entitlement puts in the batch. For each deletion,
use the latest staged value if present, otherwise the stored value, and
invoke the existing typed RecordBatch deletion operation. This removes
both the old and replacement parent indexes and retains source-scope
cleanup/poison behavior for put-then-delete. Dropping buffered puts would
skip those obligations. Counts record distinct puts, including records
subsequently deleted in the page; deletion is not a negative put. Invalidate
the entitlement lookup cache after a successful delete-only commit too.
Do not change direct deletion methods, handlers, or SQLite.

C04/C07/C08/C38/C42 candidates compare committed records and raw index keys
with direct put-then-delete behavior. Include stored-only, staged-only and
stored-plus-staged targets; a changed parent and source scope; two
entitlements sharing an external ID on different resources; duplicate and
missing targets; delete-before-put; and invalid requests. Public consumer
checks use the strict write hook, start from a clean reopened store, and
verify invisibility before commit, discard, and durable deletion plus row
after reopen. Engine checks inject record-batch failure then retry, verify
delete-only lookup cache invalidation, and check completed/stale writers.

First run the consumer test with temporary no-op methods and record its
failure. Mutants that delete using only stored values or omit lookup cache
invalidation must fail their targeted tests. The existing page batch remains
the only commit; no new rawdb operation or failure hook is needed. These
interface additions affect external PageWriter implementers. Process-crash
coverage and handler integration remain separate obligations.

## 28. K4c: preserve grant state during page expansion writes

Add PageWriter.StoreExpandedGrants. Use the existing store-free expansion
translation, which strips consumed expansion annotations and gives new
derived grants no source scope. At commit, under the engine write barrier,
preserve Expansion, NeedsExpansion, DiscoveredAt and SourceScopeKey from
the prior full identity. Backfill a missing discovery timestamp. The payload
comes from the last staged write. Ordinary PutGrants followed by expansion
in the same page preserves that staged ordinary write's state; a later
ordinary PutGrants replaces it. Repeated expansion writes preserve the same
state. Deletes retain their existing after-put semantics.

Track expanded records in the existing grant buffer rather than introducing
another executor or changing callers. Pages without expansion writes keep
the existing grant stager. For expansion pages, resolve the last occurrence
and preceding ordinary put for each identity. Read the stored prior value
once per final expanded identity, using it for both preservation (unless a
staged ordinary put supplies that state) and typed deferred-index cleanup.
Use StageGrantPutDeferred, as main does, including the rebuild marker,
needs-expansion index, digest invalidation and source-scope obligations.
Do not mutate buffers during preservation: a failed commit can be retried
against the then-current stored state. Count distinct surviving puts in the
row and retain the engine's diagnostic expansion-write counters.

C04/C07/C08/C38/C42 candidates cover existing/new grants, same external ID
on distinct identities, nil discovery timestamps, staged ordinary/expanded
write order, last payload, deletes, discard, failed commit/retry, a prior
row changed between staging and commit, completed/stale writers and invalid
identities. Public consumer fixtures use the strict write hook and clean
reopen to check invisibility and artifact persistence. Compare preservation
with the direct expansion API and inspect pending-expansion results and
index-rebuild state. First use an ordinary PutGrants delegation as a planted
defect: the preservation test must fail before the implementation exists.
After implementation, omit the prior-state merge and separately omit the
deferred index operation; targeted tests must fail. No production expansion
handler, graph replay, lifecycle change or SQLite change is included here.

## 29. K5a: resource-type pages and final engine routing

Pebble's final public path is always the ledger, selected once from engine
metadata at attach. There is no SyncOpt, feature flag, checkpoint fallback or
runtime choice for Pebble. The existing internal ledgered boolean records
that engine decision; SQLite retains checkpoint persistence. Public routing
is activated with the complete handler/lifecycle path, not offered as an
optional mode. C01/C02/C03 public-entry tests must prove these refusals and
selection before the PR lands.

Integrate SyncResourceTypes through the existing page invocation and
scheduler. Its ledger handler calls the connector once, uses the same record
validator and configured type selection, and stages records, next token,
connector timing/wait accounting and invalid-record counters together.
Publish progress and in-memory counters only after successful commit, outside
the scheduler's transition lock. Failed pages keep their action and publish
no counters or progress; committed rows skip connector invocation on replay.
The checkpoint handler keeps its connector/write flow.

Replace its terminal filter validation with exact resource-type ID probes,
shared with the ledger handler through a store-read helper taking this page's
selected records. The current handler feeds its connector cursor into the
store's ListResourceTypes and checks only one reader page; neither establishes
whether the requested types exist. First reproduce rejection of a valid
multi-page connector result. The helper checks staged selected IDs first,
then GetResourceType for earlier committed IDs; NotFound returns the existing
invalid-filter diagnostic and other read errors propagate. This is an
intentional correction to cursor coupling, not a change to filter selection.
It is a small shared change under the requester's relaxed token-path rule.

C04/C07/C15/C17/C20/C42 candidates cover two connector pages, missing and
selected types across the page boundary, nil/invalid records, connector and
commit failures, retry without double counting, committed-row replay and
close/reopen. Use real production handlers with the strict write hook. Plant
a direct resource-type write to fail pre-commit invisibility, and premature
counter/progress publication to fail a commit-failure check. These fixtures
use the existing scheduler; no executor or scheduler is added. Other handlers
remain refused until individually integrated. C49's pre-handler smoke table
exists; final public-handler cost and full coverage remain required.

## 30. K5b: resources and targeted resource pages

Keep main's resource-type enumeration, parent identities, resource validation,
latest-observation writes, raw response progress counts and targeted follow-up
order. Resource pages stage their records and discovered child actions in one
transition. Targeted requests keep NotFound/Unimplemented as successful empty
results and keep whole-type entitlement/grant enumeration out of targeted
follow-ups. Reuse read-only trait and annotation helpers.

Child scheduling is part of page publication. Collect candidates without
changing childSchedule. Immediately before passing the transition to the
existing scheduler, hold childSchedule's lock, remove previously committed
candidates and duplicate candidates within this page, and rebuild the row's
children to match. Hold that lock through commit and queue publication, then
record admitted child identities only on success. Connector calls remain
concurrent. Replay restores these marks from recorded resource children before
publishing their transition. This preserves main's per-process child dedupe
without failed pages consuming its marks; it adds no scheduler.

Invalid-resource observations, connector/session/wait accounting and progress
publish after commit; the durable counter bucket carries those observations
with the records. Connector accounting must add multiple responses rather
than overwrite prior observations, for subsequent handlers that make related
resource requests. Resource-type behavior remains unchanged.

C04/C05/C07/C09/C15/C17/C20/C38/C42 candidates exercise real resource handlers
through the existing scheduler: pagination, duplicate resource observations,
child annotation duplicates, parent propagation, targeted follow-up order,
missing targets, invalid records, connector/read/commit failures and replay.
Before implementation, the production-handler refusal must fail these tests.
Plant early writes and early child marks: commit-failure snapshots and retry
must detect them. Concurrent pages discovering the same child must admit it
once; different parent identities must remain distinct. Record the tested
subset, not the entire crash product, in evidence. Commit this brief alone,
then implement and validate the resource increment before other handlers.

## 31. K5c: entitlement pages

Preserve the existing entitlement planner, per-resource versus type-scoped
requests, skip annotations, selected sync identity, connector sibling cursors
and progress semantics. The control page records TypeScopedPlanned with its
children; successful commit publishes that state before the existing
scheduler exposes its continuation. Replay already restores it from rows.
No planner or queue is replaced.

A leaf page reads its resource (or constructs the existing type-scoped stub),
calls ListEntitlements once and stages accepted records, its next cursor and
spawned actions. Reuse the store-free validator and sibling-token parser.
Apply the same full-sync disabled-type filter, including the existing future
external-resource exception, using read-only scheduledResourceTypeExists.
Collect invalid observations and drops in the page, not global atomics.
A drop stages sync.ingest_known and sync.ingest_blocked facts, a drop counter
and the corresponding reason flag in the worker bucket. These are monotone
facts; concurrent pages cannot clear another page's block. Publish global
counter deltas and OR reason flags only after commit, never replace a shared
snapshot. This serves C17/C19/C20 without adding source-cache orchestration.

C04/C05/C07/C09/C15/C17/C19/C20/C42 candidates cover per-resource pagination,
type-scoped request markers and sibling cursors, duplicate-cursor rejection,
disabled-type drops, partial-sync retention, malformed records, resource/type
read errors, failed commit/retry and reopened replay. A control-page fixture
checks that failure does not advance TypeScopedPlanned, while commit/replay
preserve it across reader pages. Strict page/walk hooks remain installed.
First run real-handler tests against the production-handler refusal. Plant a
direct entitlement write and early drop publication to prove atomicity tests;
omit the blocked fact to prove durable behavioral state is checked. Record
remaining product gaps. Commit this appendix alone after K5b, then the tested
implementation. No pkg/dotc1z change is planned for this increment.

## 32. K5d: grant collection pages

Keep main's grant control planner, type-scoped request markers, skip rules,
continuation/sibling cursors and progress rules. Grant records, discovered or
fetched resources, expansion/external-match facts and accounting commit as
one page. Read related resources through the page writer, so an earlier fetch
in the same page prevents duplicate fetches just as main's direct put does.
Preserve the existing ordering: collect InsertResourceGrants resources before
filtering grants, resolve related resources, then put discovered resources
and grants. Staged reads do not change full resource identities.

Reuse fresh-grant filtering by adding an explicit ingestFilterStats parameter
to its internal helpers. Existing entry methods pass the same global stats
object they use today; the ledger handler passes a page-local object. This
small shared change leaves all filter predicates, read failures, annotation
rewrites and SQLite writes unchanged. It avoids copying the expansion-type
and external-match rules. Stage the local drop counts/reasons and monotone
known/blocked facts with the page, then add deltas/OR flags to global stats
after commit. Invalid discovered resources follow main's existing validator.
Related GetResource accounting uses the requested resource identity, even
when it differs from the listing action's type, and accumulates across calls.
No scheduling or lifecycle policy changes are planned.

C04/C05/C07/C09/C15/C17/C19/C20/C38/C42 candidates cover ordinary/type-scoped
pages, filtering and expansion-annotation rewriting, InsertResourceGrants
payload preservation, a valid discovered resource from a dropped grant,
related-resource staged reads, commit failure/retry/replay, full-identity
collisions, and expansion/external facts. First fail real-handler fixtures at
the production refusal. Plant early grant writes, premature facts and global
filter deltas; snapshot/fact/retry tests must detect each. A same-page repeated
related resource must trigger one GetResource request, and per-type call stats
must name that related type. Keep strict page/walk instruments. Commit the
brief alone, then the tested grant increment. pkg/dotc1z needs no new method.

## 33. Restore child scheduling from the read-only walk

A resource page may commit its child identity and continuation, then stop
before either runs. On resume, restoreLedgerState currently rebuilds pending
actions without restoring childSchedule. If the continuation rediscovers the
same parent resource, it can record the pending child again. This changes the
recorded transitions from the uninterrupted run and can admit duplicate
in-flight work. The explicit invokeActionPage replay test does not cover the
restoration path, which consumes committed rows before scheduler dispatch.

Use the walk's existing seen-identity map when restoring state. After all
reads succeed, rebuild childSchedule from visited SyncResources identities
with complete parent identities, including pending children. These are already
scheduled actions, whether their page is recorded yet or not. Publish the
map with the restored run state; no additional row scans, store writes or
scheduler changes. Failed restoration must leave prior in-memory state intact.
The seen set continues to use complete page identities; only the existing
childSchedule key intentionally omits pagination, matching main's dedupe rule.

C09/C10/C15/C16/C42 candidate: commit one real resource page, stop before its
child and next page, reopen, restore from the root and run both remaining
actions. Assert a write-free restore, one child call, and no duplicate child
in the continuation's durable row. Before the fix, require the missing
restored scheduling mark and duplicate recorded-child assertions to fail.
Compare the resulting transition with uninterrupted execution. Add a failing
read check that retains prior scheduling state. Commit this appendix alone,
then the tested correction. This is a resource restoration correction, not
a new work queue or a change to lifecycle selection.

## 34. Static entitlement and asset pages

Preserve main's static-entitlement planning: its zero-record control action
re-enumerates connector resource types completely before publishing children,
including types outside the collection filter. Do not substitute the stored
type set or interleave leaf execution with planner enumeration. Record those
ancillary connector calls and invalid-type observations in the control page.
Each leaf stages the concrete entitlements produced from one static connector
response across all stored resources of that type. Preserve fallback display
name/description, exclusion-group scoping, slug, purpose and grantable types.
The legacy lambda prefixError remains a successful empty transition. A failed
reader page or commit discards all synthesized entitlements in that action.
This buffering can grow with static fan-out; small fixtures do not establish
its production memory cost, which remains a reported limitation.

Asset control pages retain main's stored-resource enumeration. Resource leaves
read the same icon/app-logo references and stream their bytes and metadata
before staging PutAsset. All assets of the resource action share its commit;
missing metadata or a later stream failure discards earlier staged assets.
Retain main's nil-stream compatibility behavior. Successful leaves use the
normal transition instead of finishing runState directly, so the page wrapper
can commit and replay them. Init's disabled asset scheduling is unchanged;
these handlers still need to support restored actions. Stream call accounting
publishes only after commit. No SQLite writes or storage methods are changed.

C04/C05/C07/C09/C15/C17/C20/C38/C42 candidates: static planning over two type
pages, two resources with scoped exclusion groups, connector pagination,
prefixError, commit refusal/retry and replay; asset byte/content-type reopen,
multiple refs, missing metadata, stream failure, nil stream and failed commit.
Use strict hooks in each page/walk fixture. First fail real handlers at the
production refusal, then plant direct writes and a missing transition to
prove atomicity and completion checks. Compare resulting records with main's
handlers where both are executable. Commit this appendix alone, then tested
handler code. No scheduler or lifecycle policy changes are planned.

## 35. External import and matching pages

Preserve main's ordering: choose/copy external types, reconcile stale imported
principals, copy current principals/entitlements/grants, then match connector
grants against the resulting store. Matching scans imported grants too and
can read newly imported entitlements. PageWriter offers point reads, not a
merged grant iterator. Running both phases against one uncommitted writer
would silently hide imported rows from matching.

Use two sequential pages of the existing SyncExternalResources action. The
empty cursor performs import; an internal matching cursor performs matching.
No new queue, worker policy or action operation is introduced. The import
page stages an ordered list of current principal identities as a fact value,
with its records and continuation. Matching rebuilds principals by full
identity from the committed store. This retains the import's ordering and
selection across a stop without re-reading a potentially changed source or
inventing a second selection rule. An absent/unreadable matching fact fails
before writes. Main's trait selection, entitlement filter, skip annotations,
BatonID marking and matching rules remain. Legacy external actions start at
the empty cursor. The fact is external import state, not source-cache replay
or a checkpoint token.

Import collects main's same record sets using its read-only external readers.
Stale resources/entitlements/grants use full-identity page deletes. Since the
writer applies deletes after puts, remove any stale-grant deletion whose full
identity is being re-imported: main deletes first and then its put survives.
Keep same-ID/different-identity deletes distinct. Matching uses the existing
read-only profile/expansion helper and the same principal indexes; stage its
new grants and delete unmatched originals by full identity. Do not retain any
bare-ID deletion or a direct-write bypass. The import and matching phases are
individually atomic; a stop between them resumes matching from the stored
principal list. Bulk buffering, already present for principals and matched
grants in main, now includes imported grant batches; production-size memory
cost is not established by small correctness fixtures.

C04/C07/C08/C09/C15/C16/C17/C38/C42 and CO-002 candidates compare final records
with main for both import modes, profile/ID/all matches, trait/skip rules,
stale cleanup and expansion-ID remapping. Include an imported external-match
grant to prove matching sees newly committed input, and a stale grant also
re-imported to detect after-put deletion loss. Exercise failure/retry of both
pages, close/reopen between phases, corrupt/missing principal fact, full-ID
collisions and strict write hooks. First fail real handlers at the production
refusal. Plant direct writes, a bare-ID delete, stored-only matching before
import commit, and lost principal order/state; record which candidates detect
each. Inventory every in-page delete for CO-002's unreachable-bare-ID reading.
Commit this brief alone before handler code. No storage method is proposed.

The entitlement-filter import uses main's bare-ID selection map, whose
iteration order is unspecified. Visit that map's keys in sorted order on the
ledger path before recording principal identities. The selected members stay
the same (one per map key), while the new durable fact becomes deterministic
across processes. The full trait-selected import retains the external reader's
order, including its existing handling of equal IDs across resource types.
This is needed for C16's fact equality; it does not change the selection map
or matching predicates. Test multiple filtered principals and plant reversed
recorded order to check the deterministic-fact assertion.

## 36. Expansion write-boundary qualification

The production Pebble RunSingleStep invokes the complete topological
projection evaluator. It already limits output flushes, uses a disk-backed
source projection and has partial-interruption data-parity tests. One ledger
page around that call would instead retain the entire expansion output until
commit. Do not introduce that memory behavior or replace the evaluator.

Before integrating expansion, qualify the existing output flush boundary
against C16/C19/C41: does restart from partially written data produce only the
remaining output, with the same total write accounting and batch identities?
Final-grant idempotence alone does not establish either. Add a read/write
recording wrapper in the existing expansion test package, using its chain,
diamond and cycle fixtures, and compare uninterrupted output with output from
an interrupted pass plus a fresh-graph resumed pass at each batch cut. Record
both full output identities and write totals. Keep this qualification separate
from production integration; a failed accounting or boundary comparison is
an observed design constraint, not a reason to relax C16 or to label an
idempotent direct write as atomic.

If boundaries change on resume, the implementation must expose a stable unit
of evaluated graph work rather than number the surviving dirty flushes.
Retain the existing projection, merge rules and scheduler. Any added boundary
must bound staged output, let descendants read committed predecessor output,
and advance only with its ledger row. The next brief amendment will specify
the boundary using these observations before production code changes.

Commit this qualification brief alone, then its tests/evidence. No production
expansion, SQLite behavior, engine durability or store capability changes are
part of the qualification commit. Its scope is deliberately narrower than
closing C41: public-entry cold reconstruction and ledger failure cuts remain
required for integration.

## 37. Expansion pages through the existing projection evaluator

Use the existing projection evaluator through a pull iterator over its bounded
StoreExpandedGrants batches. The adapter embeds only the read/store interface,
not the optional direct-write or SST-layer capabilities. Its write method
yields one batch and returns only when the next handler resumes evaluation,
after the prior page has committed. The evaluator's temporary projection thus
observes that output after commit and descendants read committed predecessors.
No alternate work queue, topological implementation or SQLite adapter changes
are needed. The existing scheduler executes each continuation normally.

Each handler stages one yielded batch with PageWriter.StoreExpandedGrants and
records the next numbered expansion cursor. A final empty page records the
terminal transition; only its successful commit publishes the completed graph
and drop report. On any page failure, stop the suspended iterator and discard
its graph/projection. Stop it also when the enclosing scheduler exits or the
syncer closes. This bounds retained output to an existing evaluator batch,
plus its existing read/projection buffers; it does not buffer an entire graph's
output. Runtime pull state is process-local and is never a checkpoint token.

Cold execution rebuilds the graph read-only from PendingExpansionPage starting
at the beginning, validates source/resource relationships as main does, fixes
cycles, then runs the same projection over committed grants. A remembered
load cursor cannot suppress this reconstruction. Preserve an accepted loaded
legacy graph by cloning it; incomplete legacy graphs are rebuilt. Existing
StoreExpandedGrants preserves the annotation side-state needed for rebuilding.
The 33-cut qualification supports numbering the remaining flushes after the
last committed cursor: already applied contributions generate no dirty writes.
Additional real-store tests must compare rows/accounting as well as records;
any divergence blocks integration rather than being normalized away.

Main's supports_diff marker remains in the existing scheduler at its current
lifecycle boundary and keeps its fresh/resume predicate. Skip-expansion paths
must record an empty page instead of directly finishing the action. The marker
is outside an open page and means collection completed, not expansion completed.
Do not move it into the handler or change finished-sync binding semantics.

C04/C05/C09/C15/C16/C19/C36/C38/C41 candidates: chain output through multiple
pages; strict write hooks; commit refusal after a prior output page; reopen
with empty graph and replay walk; raw equality on refusal; full expanded grant
and row/count equality with uninterrupted execution; empty/skip paths; source
lookup errors; failed terminal commit; iterator cleanup on stop/error. Plant
direct expansion writes, rebuilding from the saved cursor and premature graph
publication. Compare actual grants with main's projection output. These tests
are candidates until the defects fail. Commit this amendment alone, then
implementation with its evidence. Cost from disabling the direct contribution
and SST layer paths must be reported before landing; no speed claim follows
from the small qualification fixtures.

## 38. Run-level accounting before public routing

Keep committed-page statistics separate from current-process observations.
The ledger runtime owns a fresh runStats accumulator for elapsed operation
time, retry/gate waits, rate-limit wall time and store.* session calls. It
starts empty on every attempt. Existing global stats remain the diagnostic
view restored from durable history plus current observations. Do not derive
run buckets by subtracting cumulative maxima or serialize the whole diagnostic
view: either approach can misattribute history or duplicate page accounting.

At the existing timedStep, retry wait, rate-limit wall and store-session
observation points, route a ledgered sync's observation into both its diagnostic
view and its current-attempt accumulator. Connector-reported wait/session
annotations stay page-owned and are mirrored diagnostically only after commit.
No connector.* or connector-call counts enter the run bucket. SQLite keeps its
existing recording behavior. Share only a store-free duration-recording helper
where needed; the approved small shared changes do not introduce another
scheduler or change retry policy.

checkpointOnStop gets one ledger fork: after workers have stopped, use the
existing bounded detached context to blind-write the entire attempt's run
bucket. Loop-top Checkpoint remains a ledger no-op, so neither a resume walk
nor an active worker flush writes run accounting. The public seal integration
will place the same cumulative snapshot in its terminal page. Repeated flushes
supersede; another attempt adds a new bucket. This serves C19/C20/C22/C23/C31.

Candidates: mix committed page call/session/wait observations with run-level
observations; flush twice; reopen under a new attempt; record lower-latency
session calls and flush again. Assert sums, maxima and absence of duplicate
page counts in durable counters. Exercise canceled stop contexts and reject
flushes with active pages. Plant a bucket initialized from restored stats and
page session usage copied into it; assertions must fail before evidence is
closed. Commit this brief alone before implementation. Public routing and
seal wiring follow separately, using this tested accumulator.

## 39. Mandatory engine attachment and public lifecycle

At setStore, resolve capabilities once and validate Metadata().Engine. Pebble
requires PageLedgerStore; SQLite rejects it; empty/unknown engines fail.
Store the decision only in ledgered. SyncOpt cannot return an error, so retain
an attach error for NewSyncer to return after options; loadStore returns it
for path-based attachment. No option or environment variable selects a Pebble
checkpoint path. WithRetainLedgerTokens is a SyncOpt for token retention only,
default false, forwarded to the store before page commits. A prior durable
retention declaration continues to govern the same sync after reopening.

Keep Sync's existing validation, targeted-resource selection and
startOrResumeSync binding decisions. Fork before token-state restoration and
prior-verification clearing into a new ledger lifecycle function. It creates
a fresh attempt identity, calls prepareLedgerState, then runs the existing
scheduler. Neither SyncID nor ended_at is reset for a finished binding.
prepareLedgerState's tested ClearLedgerRows handling allows another requested
pass and keeps same-ID data/history/accounting intact. SkipSync gets a ledger
fork after its existing store load and connector validation; it retains main's
new-empty-full-sync behavior and seals through EndSyncWithStats.

A resume walk must not clear an ingestion verification marker. Invalidate it
once at the first executing page before records can change, under a registered
bypass whose reason is that removing verification cannot make an unfinished
page appear committed. Serialize that one-time preparation across workers and
retry a failed invalidation. A refused unproven scrubbed file reaches no
invalidation or other write. This preserves main's rule that old verification
cannot survive into a record-changing window, while keeping replay read-only.
The marker remains absent after a refused first page; existing data is safe
but conservatively unverified. Tests must assert this file state.

After work drains, retain main's ingestion-invariant checks and halt hook,
cleanup/error behavior, graph preservation, verification publication and
connector cleanup. Only after all fallible pre-seal work succeeds does the
terminal page commit seal-ready and the current attempt's run bucket. Seal
through EndSyncWithStats. A resumed seal-ready file skips collection and can
finish the seal even if rows were scrubbed. A missing stats sidecar under the
engine's documented persistence failure remains a successful seal. Preserve
main's post-seal log-only handling of graph/verification/connector cleanup
failures; they cannot turn a completed artifact into a failed sync.

When graph preservation is requested and replay skipped completed expansion,
rebuild the graph read-only from stored annotations and mark its edges complete
only with proof that expansion finished. Strip transient state as main does,
then bind the sidecar to the sealed grant digest. Do not rerun expansion output
to reconstruct an optional sidecar. Missing/incomplete proof must not invent a
completed graph. No source-cache replay orchestration or new source-cache
facts are included.

Session-store writes during a connector page are auxiliary session state,
not sync records or resume progress. Where loadStore already installs the
session store, wrap its four mutation methods with a registered reason before
instrumentation; reads stay delegated. Do not add a new session-store setup
path for callers that main leaves responsible for configuring their store.

C01–C03/C10/C11/C19–C36/C38/C41/C43 candidates use the public constructor and
Sync: attach mismatch/empty engine errors; fresh paginated Pebble output and
sealed empty token; stop/reopen without committed connector calls repeating;
legacy takeover through accepted token fixtures; finished same-ID expansion;
new-empty SkipSync; retention across reopening; stats handover; seal failure
and scrubbed-proof recovery; unproven scrubbed refusal with raw-image equality;
verification invalidation before first mutation; graph sidecar after skipped
expansion; session writes with strict hooks. Migrate tests that specifically
assert Pebble checkpoint tokens to ledger outcomes; retain SQLite/token golden
coverage. Plant a missing engine refusal, plain EndSync/token write, repeated
page and missing stats handover. Record any incomplete products honestly.
Commit this amendment alone; public routing and lifecycle must build and pass
together before the code commit. No temporary public engine switch is added.

## 40. Collection report priority (CO-013, C50)

After public lifecycle integration, design and implement one report for
customers and connector authors: expensive collections ranked by recorded
connector/handler time, page counts, written records per page, latency
summaries and reported rate-limit waits, with individual-resource drill-down.
This precedes final performance qualification and PR completion. It is not
implemented or verified yet.

Design for one sequential row pass, no connector calls or record-family scans,
and bounded memory for ranking and latency summaries. Resolve how full
resource drill-down is served without an unbounded in-memory group map.
Prefer sharing an existing seal scan if it can preserve failure semantics;
do not require another full scan merely for logging. Measure actual cost
rather than calling the report cheap based only on algorithmic complexity.

Before code, specify the report access surface, durable format, grouping and
ranking, exact versus approximate latency statistics, and behavior after
scrubbing. Do not require page tokens or assume scrubbed rows preserve page
order. Separate cumulative worker time from elapsed time, and written-record
counts from source response counts. Decide whether retained rows provide
additional drill-down value after the report exists or can be removed after
completion is durable. No ledger deletion timing changes are authorized by
this docket entry alone. Add the candidate defects and failure cuts once
that design is concrete.

## 41. Report viability experiment before remaining integration

Requester priority: investigate report value before lifecycle completion or
retention mechanics. Use a test-only prototype over actual Pebble ledger rows;
no production contract or retention change in this experiment. Report requested
scope separately from observed work. Current rows cannot distinguish a tolerated
warning from successful zero-output collection; never label those rows as proof
of an empty endpoint. Current config is not a complete durable request manifest.

Exercise completed pagination, zero-write terminal pages, missing continuations,
missing child work, grants explicitly disabled and unknown request options.
Group by full action scope excluding the page token. Stream groups in ledger key
order, retaining only a bounded top list for timing; emit full resource summaries
incrementally. Check referenced identities with point reads rather than retaining
all pages in memory. Measure the scan plus reference checks, including allocations,
at increasing page counts. Synthetic timing values demonstrate presentation, not
production latency. Prototype checks cover incorrect empty-endpoint attribution,
missing-edge detection and scope collisions. The full report design, metadata
additions and retention decision follow the experiment.

## 42. Mechanical timing report experiment

Add the requested timing sections to the existing test-only report generator.
The artifact must be emitted by Go from rows and saved facts, not assembled by
an agent. Include page connector-time total, share of all recorded connector
time, maximum, approximate median/p95, reported rate-limit waits, writes per
page, and connector milliseconds plus pages per 1,000 writes. Call counts are
not recorded per row, so never describe pages as API calls. Zero-output rates
are undefined and rendered unavailable. Wait time is inside connector time;
worker totals do not represent elapsed time or the critical path.

Use a fixed 65-bucket integer histogram for connector milliseconds per page.
Return quantile intervals, not exact percentiles. Retain one group and top ten
ranked groups, with deterministic full-scope tie ordering. No unbounded list of
page latencies. Add sum totals while scanning so collection shares use all
rows, not only the top ten. The prototype remains a diagnostic experiment;
zero-duration rows do not establish whether connector timing was enabled.

A standalone HTML report and JSON are generated from the same result. HTML
uses escaped templates; no page tokens, next tokens, raw error text or arbitrary
arguments are emitted. An explicit test export directory requests the fixture
artifacts. Repeated generation must produce identical bytes. Add checks for
histogram boundaries, empty distributions, zero-output rates, ranking/shares,
HTML escaping and token omission. Plant percentile, denominator and ranking
defects before claiming those checks. Rerun cost measurements after the additions.

## 43. Stats-only log payload

Requester correction: output is structured log stats, not an HTML report or
narrative findings. Remove the HTML renderer and interpretation strings. Emit a
single deterministic JSON object with numeric counters/timings, top-ten collection
identifiers, histogram percentile bounds and numeric rates. Undefined rates and
unrecorded skip flags are null, not an inferred verdict. Scope uses an explicit
allowlist that excludes page tokens. Keep definitions in development documentation,
not the payload. This remains the report feasibility prototype; production log
integration follows the still-open scope/outcome design and public-path work.

## 44. One-pass, bounded-memory stats

Requester constraints supersede the prototype's reference lookups: one forward
iterator over the ledger family, no per-page point reads, no record-family scans,
no page-count-sized maps or latency arrays. Read the two saved skip flags by key
while visiting the same ledger iterator; do not load all fact values (an external
principal fact can be large). Ignore counter/frontier values for this experiment.

Rows are ordered by operation, resource type, resource, parent scope, type-scoped
flag and token hash. Keep current full-scope and current operation/type aggregates
with fixed histograms, plus bounded top-ten lists. Emit complete operation/type
summaries through a caller sink as groups finish. The sink must stream; collecting
its output into memory is outside the aggregator's bound. Log top-type summaries
with counts indicating truncation.

Count recorded continuations and children but do not claim missing references.
Exact graph validation needs state or access that this pass does not have.
Serialize its availability as false and missing-reference counts as null. No
subtraction of advertised actions from completed rows establishes completeness.

Decode only identity scope, counts, next-token presence/hash and timing fields.
Skip child bodies and token bytes without building child objects or token strings.
Protobuf field definitions remain the source for the projection; test against
normal protobuf decoding, including unknown fields and malformed input. A
scrubbed next-token hash can identify terminal pages without retaining tokens.
Working memory is bounded in row count, not in arbitrary input byte length: the
Pebble iterator still exposes a whole encoded row, and selected identifiers have
variable lengths. Measure large fan-out separately and report this bound honestly.

Verify no point reads through an iterator-only aggregation input. Test one walk,
group totals and truncation, scope separation, unknown reference validity,
cancellation, writer/iterator errors and large child lists. Run increasing row
counts through one million with long pagination and many distinct scopes. Report
wall time, cumulative allocations and peak live heap/RSS separately; do not call
B/op a peak-memory measurement. Commit the design before the implementation.

## 45. Default report observations (CO-017)

Implement in independently tested increments: existing family counts in the
report; additive page observation storage; observations in collection/retry
execution; effective options and phase intervals; production report persistence
and safe disposal; debug validation/retention. Resume-reuse counters and general
failed-call history are excluded. Do not change scheduling or retry decisions.

First retain all four existing write counters through the projection, group,
summary and JSON. A mixed-family fixture must distinguish counters even when
totals match: plant a resources/entitlements swap, and omission of group or global
accumulation. No new store data or scan is needed. These remain write counts,
not distinct final records.

For retry accounting, one accumulator belongs to the executing page across
attempts; discard staged records on an error without discarding this accumulator.
Reset after commit before the next page. Observe calls at the connector boundary
rather than counting storage errors as connector errors. Actual retry waits join
that accumulator, distinguishing rate-limit waits. Commit it on success only.
Tests must exercise two errors then success, pagination reset, exhaustion,
cancellation and crash loss. Plant reset-on-retry and leak-into-next-page defects.

Capture returned counts from typed connector results before SDK filtering, and
reuse exclusion observations at existing branches. Explicit successful-response
classification distinguishes an empty response from planning or warning actions.
No upstream transport/request/completeness claims and no raw error messages.
Test mixed valid/invalid/out-of-scope responses and derived grants. Plant a
zero-writes-is-empty defect and missing exclusion-reason attribution.

Options use an explicit non-secret allowlist, not serialized argv/config. Phase
intervals use existing lifecycle boundaries and distinguish active time from
worker sums and offline time. Full seal elapsed time is logged after seal; it
must not be invented in an artifact finalized beforehand. Final persistence and
disposal ordering will be detailed before their lifecycle implementation, with
failure cuts proving report/options retention and resumability.

### 45.1 Retry observation storage and ownership

Add optional numeric row fields for connector attempts/errors, actual SDK retry
backoff and actual SDK rate-limit waits, plus an observation-presence bit. Keep
connector-reported wait time in its existing field. Old rows have unknown attempt
coverage, not asserted zero attempts. No token/identity or durability changes.
Round-trip through PageWriter and GetLedgerRow must retain values, including after
seal scrubbing. Plant adapter field omission before closing that consumer check.

The coordinator and each scheduler worker own a separate retry accumulator on
context. Select the page by its full identity; changing identity clears the
accumulator. Connector observations increment it, and the existing wait observer
adds actual sleeps. A successful page copies a snapshot before commit and clears
the accumulator only after commit. Failed staging is discarded normally. Keep
access synchronized because connector wait callbacks can execute concurrently.
No per-resource map. A missing accumulator in direct handler tests uses a local
single-attempt accumulator. Do not classify an action/store error as a connector
error. Streaming asset errors must be observed at their connector boundary.

### 45.2 Received and excluded records

Store optional collection observations on a page: response counts (successful
list, empty list, empty list with continuation), received records by four families,
selected-type exclusions by affected family, and invalid records actually omitted
by the existing resource/type/entitlement validators. Use fixed numeric fields,
not per-record maps or raw validation strings. Nil observations mean unmeasured.
Planning actions have a measured zero-response observation, not an empty response.

Observe typed list results only after a successful connector return, before SDK
filtering. Internal store lists never count as connector responses. Counts describe
the successful execution that committed; discarded attempts contribute only the
separate attempt/error/wait observations. Grant-derived resource exclusions are
separate from received resource counts. Removing an expansion annotation or an
entry within it is not dropping a record; keep those existing diagnostics distinct.

Storage consumer tests must preserve unequal counters through commit and scrub.
Handler tests cover empty with continuation, all-filtered nonempty input, selected
resource types and mixed grant-derived content. Plant zero-writes-as-empty and
omitted-selection-counter defects. Aggregation reads the new fixed fields in the
existing projection and does not add record lookups.

### 45.3 Production report reader

Move the qualified iterator, fixed aggregates and JSON renderer out of test-only
files. Add GenerateLedgerReport(ctx) to PageLedgerStore, implemented by the Pebble
store through its engine. It reads one ledger iterator and returns the bounded
summary; it writes nothing and does not dispose of the ledger. Existing exhaustive
store-method coverage must classify it as read-only. The consumer test compares
parsed counters after page commit, verifies repeatability and checks unchanged
store contents. Plant an empty-report return before closing the consumer check.

This increment does not enable automatic publication or change seal ordering.
Options, phase timing and durable report publication precede default disposal.
Keep the existing iterator/projection and mutation tests on the production code;
retain fixture exporters and benchmarks in test files. Do not add a second report
implementation or materialize all rows to cross the package boundary.
