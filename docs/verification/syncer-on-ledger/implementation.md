# Syncer on the page ledger: implementation brief

Current design correction: CO-010 and CO-011 supersede the finished-run
reset, replacement executor, scheduler-copy and absolute shared-path-freeze
proposals below. Reuse the existing scheduler and preserve lifecycle
behavior. Section 16 records the revised integration constraints; earlier
sections retain the history of the committed brief.

CXE-1358. Behavioral baseline: plan.md at `01931d8b`, with CO-001–CO-009
appended after calibration. This document is the step-3 deliverable. It is
committed alone before code. It describes planned work, not verified behavior.
There are 49 criteria; none has new executable evidence yet.

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

The private ledgerRuntime.execute implementation is rejected for production
integration. Its tests remain useful as recorded failures and fixtures, but
the integration tests and cost driver must move to the existing scheduler.
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
with real page writers. The rejected generic executor is retained only as
a test fixture until its historical tests and cost driver are migrated; it
must no longer be compiled into production.

K2d restores the ledger frontier into the existing action stack, handles
Init's page boundary using shared planning, and preserves the baseline
same-ID post-processing lifecycle. K2e moves cost and recovery callers to
that integration and removes the historical executor fixture. Run the full
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
