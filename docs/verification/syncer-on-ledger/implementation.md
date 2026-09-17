# Syncer on the page ledger: implementation brief

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
