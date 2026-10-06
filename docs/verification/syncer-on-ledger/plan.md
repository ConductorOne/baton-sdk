# Syncer on the page ledger: behavioral verification plan

CXE-1358, stage 2b. Written against `origin/main` at `eb63f1b5` on
2026-09-17. The commit introducing this file alone is the frozen baseline.
Calibration precedes the implementation brief and all code. Later claims
append in §11; frozen wording is not replaced.

Contract sources: the task brief §§1–8; `pkg/dotc1z/c1zstore/ledger.go`
and `write_hook.go`; `docs/verification/page-ledger/plan.md`, particularly
C32 and §5.1; that plan's `evidence.md`; `docs/BUG_CATCHING.md` §2 and
`docs/REVIEW_CHECKLIST.md`. The task brief controls disagreements with
older prose. In particular, engine identity selects the path; `Spawned`
is accounting metadata, not an eighth identity field.

## 0. Risk verdict, authorship, and routing

**HIGH.** Missing pages can leave valid-looking durable artifacts. Crash
cuts, worker schedules, old-token versions, and cold-process resumes affect
meaning. The default Pebble write path changes. Repair may require re-sync,
artifact migration, or coordination with older readers. There is no
single-run oracle for equivalence to uninterrupted execution.

This plan is derived from the contracts and the existing token-only syncer.
The first ledger-syncer implementation and its tests have not been obtained
or inspected. Storage-side evidence was read as required; its passing
candidates do not establish syncer behavior. No product code or tests were
written before this plan. No test execution is claimed at freeze.

Route all seven review passes: edges, resume, products, errors and budgets,
invariant gating, cost, concurrency. Primary instruments are the executable
page/action model, crash/reopen differential, old-artifact takeover corpus,
and write-hook ride-along. Review cannot substitute for these instruments.
Before classifying implementation evidence, append the inventory of new
resource owners, writes, caches, lock ordering, and partial-commit cuts in
§9. Final independent evidence audit and decorrelated review must report
zero new findings; fixes reset that requirement.

## 1. Stage claims stated on the file

- **S1 Page atomicity.** A durable page has its records, indexes, action
  transition, facts, and cumulative worker bucket together. Failed or lost
  pages leave none of those effects. The independently durable in-flight
  stamp is the sole storage-contract exception. Records that predate a
  page remain unless that page commits their replacement or deletion.
- **S2 Resume.** A walk changes no stored key or value. Trustworthy rows
  suppress exactly their pages; absent or identity-mismatched rows cause
  execution. Reconstruction preserves next pages and every child. A cold
  resumed run reaches the same complete data as its uninterrupted model.
- **S3 Facts and accounting.** Only committed observations steer subsequent
  work. Every durable bucket contributes once, across workers and attempts.
  Sealed stats and ingest quality derive from durable observations, with
  no checkpoint token as a second authority.
- **S4 Migration.** Every accepted old token resumes at its decoded frontier.
  Moving it is atomic and happens once; empty token plus frontier remains
  resumable. No ledger run writes a new checkpoint token.
- **S5 Seal and rebind.** Successful sealing carries stats through
  `EndSyncWithStats`. Interrupted sealing never makes a scrubbed next-token
  field into pagination evidence. Rebinding a finished sync starts new work
  over its data; no old row, fact, bucket, or frontier suppresses that work.
- **S6 Engine boundary.** Pebble always selects ledger execution; SQLite
  always selects its existing checkpoint execution. Invalid engine/capability
  combinations fail at attachment before sync work. SQLite production bodies
  remain unchanged below the inserted forks.
- **S7 Write discipline.** Every page test and resume test detects a direct
  write lacking a registered reason. A registered reason requires crash
  evidence that its effect is safe ahead of the row; a string is not proof.
- **S8 Bounded work.** Walk work grows with visited rows and children, not
  the product of visited rows and all previously visited rows. Page memory
  grows with the current page and active work; retry does not accumulate
  abandoned writers, workers, or snapshots.

“Same sealed file” includes complete records, secondary indexes, digest,
facts, completion and accounting, not just counts. Literal equality of
attempt IDs and real timings is unresolved in OQ-2; that uncertainty must
not be used to exclude a record or accounting mismatch.

## 2. Oracles

- **O1 Durable page model.** Independently apply a deterministic connector
  response and transition to a pre-page key/value map. Reopen each image and
  compare full primary and secondary data, row, facts, and buckets. For
  overwrite/delete fixtures compare values, not presence. Every image is a
  union of whole durable page effects in commit order. Do not infer durability
  from a successful return on a fresh NoSync page.
- **O2 Execution and transition trace.** A fixture specifies identities,
  next tokens, children and expected terminal actions without invoking the
  production transition helper. Compare actual connector calls and admitted,
  completed, pending actions against that graph and the durable row set.
  A found row must suppress a call; a missing row must cause it.
- **O3 No-write oracle.** Install `WriteHookStore` in every Pebble page and
  resume fixture. Mark open-page contexts and reject empty bypass reasons.
  During the walk reject every write, including writes with reasons. Pair
  the hook with a store-call recorder and complete before/after snapshots:
  the contract only promises hook coverage under `WithOpenPage`, and a
  hook alone cannot prove that contexts or mutation methods were covered.
- **O4 Sealed differential.** Run the same deterministic input uninterrupted
  and from every selected crash image through actual `Sync`, close and reopen
  the resulting c1z. Compare canonical key/value contents and consumer output.
  Compare every stats field separately with O5. Fix clocks and IDs where
  possible; list every normalization explicitly. Also record a raw artifact
  digest so canonical equality is never reported as byte equality (OQ-2).
- **O5 Independent fold.** The test owns a table of committed observations
  keyed by `(attempt, worker)`, replacing cumulative buckets, summing totals,
  taking latency maxima, OR-ing flags. Compare all counters, step durations,
  connector and session calls, errors/timeouts, ingest fields, and final
  sidecar. Resume walking itself adds no historical call or completion.
- **O6 Compatibility.** Open genuine token-only c1z fixtures and V0/V1/V2
  token fixtures with this SDK. Compare the next action, calls, facts, and
  eventual file against the pre-migration decoded state and known data.
  An in-process synthetic token is not a substitute for the artifact arm.
- **O7 Boundary and errors.** Assert error identity/class, phase, absence of
  connector work, and full unchanged target state on invalid attachment or
  invalid state. An unrelated setup error never satisfies a refusal test.
- **O8 Structure.** Compare production source with `eb63f1b5`: old handler,
  checkpoint, token, scheduler and state bodies are untouched except leading
  ledger forks and required attachment validation. Inspect the call graph
  for newly shared store-touching helpers; use AST checks for capability
  assertions, path predicates, options and hooks. This verifies the explicit
  structural contract, separately from behavioral closure.
- **O9 Resources and cost.** Track acquired/released page writers and worker
  exits on every test; count ledger reads, writes and allocations at scale.
  Use named seeds and repeatable schedules for concurrency; races and timing
  samples are reported as samples, not exhaustive proofs.
- **O10 Token absence.** Inspect sync token, ledger token fields and exported
  bytes after reopen/seal. Plant distinct synthetic secret needles in a page
  identity, next token, child token and old-token frontier. Default seal has
  zero exported needle hits; explicit retention is the positive control.

## 3. Mechanical coverage products

### 3.1 Axes

- **A (11 actions):** Init, resource types, resources, list resources for
  entitlements, entitlements, grants, external resources, assets, grant
  expansion, targeted resource, static entitlements. These are the eleven
  non-unknown `ActionOp`s at the baseline. No-call actions still own durable
  transitions. Unknown operations are error fixtures.
- **T (5 transitions):** finish; next page only; children only; next page
  plus children; terminal warning. Children contain both normal and spawned
  actions. An unsupported action/transition combination requires an explicit
  rejection/no-mutation test, not silent removal from the product.
- **F (9 page cuts):** before connector; after response before staging;
  after a proper subset of stages; all staged before commit; after the first
  durable stamp before batch; batch failure; batch durable before in-memory
  transition; after transition before child dispatch; after child dispatch
  before stop. Inject a process crash and, where an operation returns, an
  error/cancellation. A cut must assert it fired.
- **R (3 reopen identities):** same process with a newly constructed syncer
  and store; new process from saved c1z; fresh process from a crash image
  dropping unsynced state. All use cold run/graph/filter caches.
- **W (4 worker pairs):** 1→1, 1→4, 4→1, 4→4. The arrow separates the
  crashed attempt from its resumer. Attempt counts N = 1, 2, 3.
- **L (9 input states):** empty fresh file; token-only unfinished; stamp-only
  unfinished; frontier-only unfinished; rows unfinished; rows without stamp
  unfinished; partially scrubbed unfinished; wholly scrubbed unfinished;
  sealed. The sealed state is split by retain on/off in P7.
- **E (8 attachment states):** {Pebble, SQLite, empty engine, unknown engine}
  × {ledger capability present, absent}. Repeat through injected store and
  path-open attachment. Engine and capability disagreement is not fallback.
- **J (11 row cases):** exact; absent; seven single-field mismatches;
  unreadable row; scrubbed row. The seven identity fields are Op, type ID,
  resource ID, parent type ID, parent resource ID, page token, TypeScoped.
- **V (3 formats):** V0, V1, V2 as accepted by `unmarshalToken`; include
  graph-bearing and graph-less legacy state as separate fixture features.
- **M (6 migration images):** token intact/no stamp; stamp/token/no frontier;
  token empty/frontier/no rows; token empty/frontier/rows; neither token nor
  frontier; conflicting token and frontier. Last state requires explicit
  refusal or a contract clarification, never guessed authority.
- **K (4 buckets):** worker 0, another worker, RunBucketWorker,
  TakeoverBucketWorker. Payload forms: zero, counters only, flags only,
  connector calls only, step/session stats only, all fields.
- **Q (record forms):** new, same-value overwrite, changed-value overwrite,
  duplicate identity with differing values, empty result. Cross all four
  record kinds. Grant delete targets: staged, existing, absent.
- **H (8 stop/seal cuts):** workers drained; invariants complete; cleanup
  complete; seal-ready publication; partial scrub; scrub complete before
  ended_at; ended_at before stats persist; stats persist failure.

### 3.2 Products and coverage level

| Product | Required space | Coverage |
| --- | --- | --- |
| P1 | A × T × F × R × W = 5,940 cells | bounded exhaustive for scripted fixtures; log unsupported cells with refusal evidence |
| P2 | A × J × R = 363 cells | bounded exhaustive; scrubbed cases governed by C11, not normal row skipping |
| P3 | E × {injected, path-open} = 16 cells | bounded exhaustive, including public constructor errors |
| P4 | V × M × R × {no counters, existing counters} = 108 cells | bounded exhaustive; each applicable cell N=1,2,3 resumes |
| P5 | K × 6 payload forms × W × N = 288 cells | bounded exhaustive; each compares all payload fields |
| P6 | 4 record kinds × 5 Q forms × {commit, fail, crash} × R = 180; grant deletes 3 × 3 × R = 27 | bounded exhaustive at owning syncer handler boundary |
| P7 | H × R × {retain off,on} = 48 cells | bounded exhaustive; includes partially scrubbed frontier/children |
| P8 | L × {automatic resume, explicit WithSyncID, new targeted run} × R = 81 | bounded exhaustive; invalid combinations assert refusal |
| P9 | {each of 5 existing sync facts, ingest known, ingest blocked} × {commit,discard,fail} × {single,overlapping workers} = 42 | bounded exhaustive; reason/count fields separately in P5 |
| P10 | {chain,tree,diamond,duplicate spawn,mutual spawn cycle} × W × {before parent commit,after parent commit,after child commit} = 60 | bounded exhaustive small action graphs |

Product counts are obligations, not executed coverage. Overlapping products
are not added into a misleading unique-scenario total. No product is closed
by testing its factors separately. The implementation brief will map each
cell to a fixture or a documented reduction; a reduction needs a reason and
cannot retain an exhaustive label when it only samples interactions.

Additional sampled space: 100 fixed recorded topology/fault seeds; workers
2, 8, 32 under race; rows 10³, 10⁴, 10⁵; page sizes 100, 1,000, 10,000;
WAL-loss fractions 0, 20, 50, 80, 100 percent across 120 pages. Cost target:
O(visited rows + children) walk calls, zero walk writes, no whole-history
serialization per page; report allocation slopes and compare unchanged
baseline collection at matched sizes. Timing thresholds require measured
baseline data before any performance claim.

### 3.3 Scope and exclusions

Source-cache replay orchestration and new source-cache replay facts are
excluded to the later source-cache change. Existing ingest quality and its
behavioral gate remain in scope. SQLite removal is CXE-1311. No work here
refactors away capabilities on the assumption that SQLite is gone.

Unbounded schedules, arbitrary connector nondeterminism, cryptographic hash
collisions and external service durability cannot be exhausted. Construct
identity mismatch directly instead of finding a hash collision. The
connector model is stable by page identity; changing upstream responses
must still preserve whole committed pages but cannot promise the same data
as a different upstream history. Physical WAL loss is sampled, not proved
exhaustively by process-kill tests. Bypasses are not scope exclusions.

## 4. Criterion levels and initial status

There are **48 criteria**, C01–C48. All are due in stage 2b. Each starts
**not assessed** with no new syncer-ledger test and no planted-defect run.
Existing tests named in §7 are candidates only. An open contract question
blocks closure of its affected criteria, not inclusion in this plan.

## 5. Criteria

Every row is an observable; O8 rows additionally enforce the task's required
source shape. Bounded means the named finite product, not all possible inputs.

| ID | Claim | Required observable | Oracle / coverage |
| --- | --- | --- | --- |
| C01 | S6 | Attachment accepts exactly Pebble+ledger and SQLite-without-ledger; other engine/capability pairs return an attachment error before calls or writes. | O7; P3 |
| C02 | S6 | Path selection remains the attached engine's decision for every page, resume and stop; a missing capability never causes a Pebble checkpoint. | O3,O7,O8; P3 × entry/stop |
| C03 | S6 | SQLite executed bodies and token encoding are unchanged below added forks; token golden bytes and existing SQLite outcomes remain unchanged. | O8,O6; all changed production bodies and token corpus |
| C04 | S1 | After reopen each page's four record families, indexes, row, facts and bucket are all present or all absent relative to the pre-image. | O1; P1,P6 |
| C05 | S1 | Success publishes one transition for one page, including empty results and no-connector actions; failure publishes none and leaves the action runnable. | O1,O2; A × T × F |
| C06 | S1 | A failed first page may leave the stamp, but its next attempt still runs the page and never writes a checkpoint token. | O1,O10; stamp cut × R × W |
| C07 | S1 | Duplicate/overwrite values and their secondary indexes equal the model; staged reads see this page's latest resource/entitlement, never another worker's uncommitted value. | O1; P6 × staged/store-only/absent reads |
| C08 | S1 | A grant deleted in a page is absent after that page commits even if also put; failed page deletion preserves pre-existing grants. | O1; P6 delete cells |
| C09 | S2 | A trustworthy exact row suppresses its connector call and restores its next token and children; absent or identity-mismatched rows run, never skip. | O2; P2 |
| C10 | S2,S7 | Before the first pending page executes, a ledger resume walk leaves every key/value unchanged and produces zero store writes, including counter flushes, metadata, sessions and bypassed writes. | O3; P8, with early stop after each visited row; OQ-1 |
| C11 | S2,S5 | Partial or complete scrubbing in an unfinished file never terminates pagination from an empty token; proven seal-ready input finishes sealing without connector reruns; unproven input returns a diagnostic error without mutation. | O2,O3,O7; P2 scrubbed cells,P7; OQ-3 |
| C12 | S2 | Parent and child identities round-trip all seven fields. Changing Spawned, TypeScopedPlanned, Attempt or timing alone does not create another page identity. | O2; seven mismatches plus four metadata changes × R |
| C13 | S2 | Next-page plus child transitions retain both; interrupted dispatch neither loses children nor runs children before their parent page is durable. | O1,O2; P1,P10 |
| C14 | S2 | Type-scoped planning happens once; spawned cursors drain, duplicate mentions do not duplicate committed work, and finite cyclic mention graphs terminate. | O2; P10 × per-resource/type-scoped |
| C15 | S2 | Full, resources-only, targeted and expansion-only runs keep their requested scope after a cold resume; same resource under different parents is not conflated. | O2,O4; 4 modes × W × R, distinct-parent fixtures |
| C16 | S2,S3 | Repeating crash/resume yields the uninterrupted sealed data and correct cumulative accounting; no completed durable page is re-fetched. | O4,O5; P1 plus sampled WAL loss and N=1,2,3; OQ-2 |
| C17 | S3 | Each existing sync fact survives exactly with the page that establishes it; failed/discarded observations cannot alter another worker's committed decision or row. | O1,O2; P9 |
| C18 | S3 | Overlapping fact updates preserve all committed facts without lost updates; resume reads the same gate values as before the crash. | O1,O2; P9 × reverse completion order |
| C19 | S3 | Missing prior ingest quality remains conservatively blocked; known clean stays known; committed drops/invalid observations preserve blocked state and reasons across resume and seal. | O5,O6; known/unknown/blocked × P9 outcomes × R |
| C20 | S3 | Each worker bucket replaces its own cumulative total, never a delta or another worker's contribution; failed pages cannot leak increments into a later bucket. | O1,O5; P5 × committed/failed preceding page |
| C21 | S3 | Worker 0, other workers, run and takeover buckets remain distinct across attempts and changed worker counts; aggregate totals include each once. | O5; P5 |
| C22 | S3 | All map fields, flags, latency maxima, errors and timeouts survive the fold, including stats-only payloads and zero counts; repeated flushes do not double-count. | O5; P5 × repeated flush |
| C23 | S3 | Completed/warning action accounting, retry waits and warning-threshold decisions remain consistent across resumes; walking old rows contributes no new call, retry or completion. | O2,O5; warning/retry/clean × W × N |
| C24 | S4 | V0/V1/V2 migration preserves decoded current action, remaining stack, page tokens, all five facts, accounting and accepted graph semantics after reopen. | O6; P4, old graph/no-graph fixtures |
| C25 | S4 | Takeover's durable image contains either intact old token with no moved state, or cleared token with frontier/facts/counters; the stamp may stand alone between them. | O1,O6; P4 migration cuts |
| C26 | S4 | Empty token plus frontier and zero rows resumes at the frontier's first pending action, not Init; repeated resumes do not move or erase the frontier again. | O2,O3,O6; V × R × N |
| C27 | S4 | Stamp plus intact token and no frontier retries takeover and reaches the same frontier as an uninterrupted move. | O1,O6; V × R × retain on/off |
| C28 | S4,S3 | Migrated counters fold once across every later resume, even after worker-zero pages and run-bucket flushes; existing ledger counters are not imported again. | O5; P4 × K × N |
| C29 | S4 | Malformed token/frontier, read errors and conflicting authorities do not silently start fresh, clear usable state or seal missing work; errors identify the invalid state. | O3,O7; malformed/read failure/conflict × token/frontier × R |
| C30 | S4,S3 | Missing legacy fields do not acquire invented historical counts or graphs; stats-only legacy tokens preserve their stats; absent ingest quality retains its unknown-prior reason. | O5,O6; V × absent/zero/nonzero supported fields × R |
| C31 | S5 | Every successful ledger seal leaves an empty sync token and stats supplied through EndSyncWithStats; no path reaches either ledger token-write or plain-EndSync refusal. | O3,O5,O10; P7 plus fresh/resume/skip/empty/expansion-only |
| C32 | S5 | Invariants, cleanup and seal failures return the specified error or resumable incomplete result; resume does not falsely report success, lose pages or inherit a failed seal's stats overlay. | O4,O5,O7; P7 |
| C33 | S5 | A finished WithSyncID rebind retains input data and accepts one kind of new work, expansion; a collection rebind is refused before any write. The expansion pass reads the collection's facts and none of its own collection flags; old rows, counters, frontier and retain declaration cannot suppress or pollute it. | O1,O2,O5; P8 sealed × retain on/off |
| C34 | S5 | Failure during finished-run ledger reset cannot cause an unfinished-run resume to discard committed pages; repeated rebind/reset converges to the new requested run. | O1,O2,O7; before/during/after reset × R |
| C35 | S5 | Default seal scrubs identity, next, child and frontier token bytes; explicit SyncOpt retention survives a fresh finishing process without repeating the option. | O10; P7 and all four needle locations |
| C36 | S5 | Expansion-only rebind derives expansion from stored grants without trusting old completion rows or a token rewrite; the resulting grants and sources match a fresh expansion. | O2,O4; sealed scrubbed/retained/token-only × R; OQ-4 |
| C37 | S7 | Every Pebble page/resume test rejects an unregistered direct store write; missing open-page context or an unobserved write method is detected by the companion recorder. | O3,O8; every new Pebble fixture and mutation-method inventory |
| C38 | S7,S1 | Each registered bypass preserves correct reopen/resume output if execution dies immediately after it; none writes this page's atomic records independently of its row. | O1,O3,O4; each registered method × before/after × R; OQ-5 |
| C39 | S1,S8 | Commit error, connector error, cancellation, warning and success release every page writer and worker; stopped workers cannot publish later pages or race final seal. | O1,O9; F × W, race samples |
| C40 | S1,S2 | Stop/retry preserves committed pages and reruns only pending pages; cancellation before any new page does not flush a bucket or checkpoint. | O2,O3,O7; retry/deadline/cancel/fatal × W × R |
| C41 | S1,S2 | Expansion resume reconstructs required graph state from durable records, including crashes during graph loading; no remembered cursor skips graph input missing in the new process. | O2,O4; graph absent/partial/loaded × load/expand cuts × R |
| C42 | S1 | External-resource import, targeted/static entitlement and asset work retain their data dependencies after each cut; unsupported atomic mutations cannot be hidden behind a bypass reason. | O1,O4; relevant A × F × R; OQ-5 |
| C43 | S5 | Sealed output remains consumable by stats readers and compactor fold/rebuild; downstream record output agrees with the uninterrupted file and no token becomes stats authority. | O4,O5; fold/rebuild/stats × sealed scrubbed/retained |
| C44 | S6 | Capability assertions occur only in store_caps; the ledger boolean is the sole path predicate; config remains immutable and test hooks remain on syncTestHooks. | O8; full changed-source inventory |
| C45 | S6 | Existing runState/token/checkpoint bodies remain unchanged; no new shared store-touching helper reroutes SQLite execution. | O8; baseline diff plus reachable call-graph audit |
| C46 | S8 | Walk reads and allocations scale with visited rows/children; repeated page execution does not serialize the prior ledger; reported ratios include large-page and long-chain cases. | O9; declared scale samples, not closure over all sizes |
| C47 | S1–S8 | Every oracle rejects a planted representative violation and its setup proves the intended cut/state; each criterion names a test that fails on its own claimed defect class. | O1–O10; all criteria and distinct mechanisms |
| C48 | S1–S8 | The production diff and executed cell manifest agree: no reachable changed branch lacks a criterion or explicit gap, and final review/audit plus seeded soak find no new violation. | O8,O9; full branch inventory, recorded final revision |

### 5.1 Inherited storage C32 mapping

The storage plan's C32 is a bundle; it is not this plan's C32 numbering.
Every inherited cell is due here:

| Storage obligation | This plan |
| --- | --- |
| Read-only walk | C10,C37,C40 |
| Absent/mismatch means run | C09,C12 |
| L3, no pagination inference from scrub | C11,C32; OQ-3 |
| Finished binding checked before old ledger is dropped | C33,C34 |
| Every direct page write has a bypass reason and evidence | C37,C38,C42 |
| One commit per page | C04,C05,C13 |
| Counter and fact producers | C17–C23 |
| Takeover trigger and frontier resume | C24–C30 |
| Stats folded across attempts | C16,C20–C23,C28,C31 |
| §5.1 every accepted token version, same action/page after reopen | C24,C30 |
| §5.1 frontier with zero rows and already-empty token | C26 |
| §5.1 mid-image stamp plus token retries takeover | C27 |
| §5.1 migrated counters exactly once over repeated resumes | C28 |

### 5.2 Decisions and reasons

**L3.** Choose a durable seal-ready fact, established only after every action
and required pre-seal work completes, before any token scrubbing begins.
Recovery with that proof goes directly to idempotent sealing; it never
walks scrubbed rows. Partially scrubbed rows get the same treatment. Without
proof, return an explicit recovery error without writes. The existing
SetFact/LedgerFacts surface can carry proof; no new store method is proposed
at freeze. Whether a terminal no-connector page can publish this fact, and
whether legacy unproven L3 must be recoverable, remain OQ-1/OQ-3. An empty
NextPageToken alone is never proof. This decision does not claim universal
recovery until those questions are settled.

**Retention.** Expose `WithRetainLedgerTokens(bool)` as a SyncOpt stored in
syncConfig, default false. A committed durable retain declaration remains
authoritative on resume when the finishing process omits the option. Do not
introduce per-process-only retention or silently revoke a prior declaration.
Finished rebind starts a new declaration because it is a new run.

**Rollback expansion.** For ledger data, force expansion means new
expansion-only work over the finished sync's stored grants, with old ledger
state dropped and the graph reconstructed. `WithOnlyExpandGrants` must make
that work happen without relying on a token. The existing CLI rollback
implementation explicitly uses SQLite's C1File and is frozen here; no Pebble
rollback storage implementation is added. Its future Pebble integration must
use this new-run behavior. C36 verifies the syncer entry point now; OQ-4
records the CLI boundary.

**Ingest.** Known/blocked quality is behavioral state; store it as durable
facts, including conservative unknown-prior migration. Numerical drops and
invalid-observation counts belong in cumulative buckets; reason bits fold by
OR. Both originate in the same page commit. Reconstruct IngestQuality for the
consumer gate and final stats without importing the whole old snapshot into
each worker. This preserves the existing gate without adding source-cache
replay orchestration or its new facts.

**Facts under a page.** Keep runState and its token path unchanged. The new
ledger runtime owns page-local observations and publishes them to committed
in-memory state only after commit; it restores that state from LedgerFacts.
Workers do not mutate the shared fact set ahead of their rows. A reader may
see its own staged observations; another page may rely only on committed
ones. The implementation brief must specify locks and publication ordering
that satisfy C17/C18; wrapping the old setFact with a callback is not an
acceptable rewrite of SQLite's state machinery.

**Takeover.** Decode through unmarshalToken before destructive migration;
carry its accepted stack, facts, completed/action counts and representable
stats. Use the reserved takeover bucket only when there are no existing
ledger counters, including stats-only payloads. Preserve the moved raw state
as the frontier, so a crash after migration cannot require the deleted token.
On empty token always inspect the frontier. Existing ledger counters win over
re-import; malformed or contradictory durable authorities return an error.
Carry only supported legacy fields: no invention of measurements absent in
V0, no transient caches; honor existing graph-bearing/graph-less decode
semantics and reconstruct store-derived expansion state. Migration counters
are not a worker's starting cumulative total. Unmapped compaction provenance
must be resolved explicitly under OQ-6 rather than silently dropped.

## 6. Closure and evidence rules

At freeze C01–C48 are not assessed. `evidence.md` will give every criterion
one of: verified to stated coverage, evidence incomplete, failed, not
assessed. For each: required cells, covered cells, reductions, test path and
assertion, tested boundary, premise assertion, oracle, planted defect,
red command/revision, green command/revision, and uncovered cases. A
candidate without a demonstrated defect failure is evidence incomplete.
One instrument may serve multiple criteria but each gets an entry.

Bounded closure requires an enumerated cell log with no unexplained gaps.
Sampled criteria report exact seeds, sizes, schedules, counts and limits.
Missing instruments leave every dependent criterion incomplete. No open
question is resolved merely by matching whatever code was easiest to write.
No “done” claim until all current criteria are verified or a requester
change order explicitly changes scope, all OQs are dispositioned, repository
gates pass, and the final independent audit/read plus clean soak hold.

The freeze is a documentation milestone only. After it is pushed, wait for
calibration. Append calibration CO-### entries, then commit implementation.md
alone. It maps structural choices and buildable commits to these criteria,
and names candidate tests and planted defects before writing product code.

## 7. Instruments and candidate sources

- **I1 Model and crash driver (O1,O2,O4).** New syncer-owned driver invokes
  real Sync over Pebble with deterministic connector responses. Sweep P1,
  retain images before cleanup/Close, reopen in a subprocess, and compare
  data and execution. Extend storage CrashableMem support as necessary;
  ordinary graceful Close is not a simulated crash. Seed overwrite and
  delete targets non-empty. Mutants: records without row, row without
  records, publish transition before commit, lose a child.
- **I2 Walk/write ride-along (O2,O3).** Mandatory fixture constructor installs
  strict hook, recorder and snapshots. Include tests that deliberately strip
  WithOpenPage, write without a reason, and write with a reason during walk.
  Fail if the helper was not installed. Audit mutation methods through public
  wrappers and sub-stores; register each kept bypass in the evidence file.
- **I3 Migration corpus (O5,O6,O7).** Existing token golden fixtures and
  TestSyncTokenV0FromC1Z are candidate inputs. Storage candidates
  TestLedgerTakeoverIsOneUnit and TestLedgerTakeoverCrashImages establish
  useful images, not consumer correctness. Extend through real syncer resume
  for every P4 cell. Mutants: decode empty token instead of frontier, skip
  migration on stamp-only input, double-import counters.
- **I4 Accounting/facts driver (O1,O5).** Controlled worker barriers and
  distinct per-worker values exercise P5/P9. Mutants: worker delta replaces
  total, run bucket collides with worker 0, failed-page fact leaks, max is
  summed, stats-only migration treated as empty.
- **I5 Seal/rebind driver (O4,O7,O10).** Public Sync entry and real file
  reopen around P7/P8; inspect sidecar and token bytes. Candidate storage
  tests: TestFailedSealDropsItsStatsOverlay,
  TestRetainDeclarationSurvivesCrashAndItsAbsenceScrubs,
  TestLedgerScrubReachesTheTakeoverFrontier. Mutants: scrubbed empty token
  finishes a nonterminal page, stale rows skip rebound expansion, plain
  EndSync, retention lost in the finishing process.
- **I6 Source and error audit (O7,O8).** Attach matrix, AST/diff checks,
  existing token corpus, method inventory and branch-to-criterion manifest.
  Mutants: capability fallback, changed SQLite handler body, use-site type
  assertion. Audit error/close ordering at the public caller boundary.
- **I7 Race, resource and cost driver (O9).** All new fixtures track writer
  and worker release; -race repetitions and fixed-seed soak accompany the
  bounded schedules. Count lookups/allocations across stated scales.
  Mutants: unreleased writer, worker publishes after stop, repeated full
  history scan. Benchmark harness and reference baseline must exist before
  a cost claim is marked verified.

These instruments are required deliverables, not existing evidence. Test
hooks added to pkg/sync live only in syncTestHooks and are reached as
s.testHooks.x. Store-side hooks use that layer's existing convention and
must be recorded as a pkg/dotc1z change if needed.

## 8. Placement and repository gates

Plan, implementation brief, evidence and cell manifest live in
`docs/verification/syncer-on-ledger/`. New ledger functions and tests live
in pkg/sync. No token golden rewrite is permitted to make a new test pass.
Engine extensions, if required by a settled OQ, need their own contract,
consumer test, failure cuts and rationale; they are not assumed authorized
by an instrument's convenience.

For code commits, build and vet each commit independently with Go 1.26.x and
vendored dependencies; record actual versions and commands. Final gates:

```sh
go test ./pkg/sync/ -count=1 -timeout 30m
go test -race ./pkg/sync/ -run '<ledger test expression>' -count=3
go test ./pkg/dotc1z/engine/pebble/ -count=1 -timeout 20m
go test ./pkg/synccompactor/ -count=1 -timeout 20m
golangci-lint run ./pkg/sync/... ./pkg/dotc1z/...
```

Lint must report zero issues. Tests touching public store wrappers also run
pkg/dotc1z; any CLI change adds the relevant cmd/baton tests. Documentation
freeze validation is source/diff inspection, criterion enumeration, link
checks and git diff --check; it is not a product build claim.

## 9. Implementation-obligation addendum and freeze report

Empty at freeze: implementation has not been inspected or authored. Append
before evidence classification: every new batch/writer, lock, worker,
iterator, cache and ownership transfer; each direct mutation and failure
cut; derived-state validation; all bypass reasons and their recovery proof.
Each entry names a criterion or requests a new CO, an oracle and owning
boundary. Do not retrofit behavioral claims into the frozen sections.

Freeze report: commit SHA, pushed branch, 48 criteria, unsettled OQs, and
confirmation that implementation.md, code and new tests do not yet exist.
No pkg/dotc1z changes at freeze. No first-implementation calibration has
arrived, so no statement about what calibration omitted is yet possible.

## 10. Open questions and settling checks

- **OQ-1 Literal write-free resume versus migration/finalization.** The brief
  requires zero store writes before the first executed page, but takeover
  must durably move a token first. A fully committed resumed run may have
  zero remaining connector pages yet must seal and persist stats. Proposed
  boundary: the ledger walk is strictly read-only; legacy migration and
  proven seal-only recovery are explicit lifecycle writes outside it, and a
  terminal control page may establish seal-ready. This is a requested
  clarification, not an adopted exception. Settling check: record the full
  trace of token-only, frontier-only, all-pages-complete and L3 recovery;
  calibration must identify allowed lifecycle writes. C10/C11/C25/C31 open.
- **OQ-2 Equality and durability strength.** Attempt and CommittedAt differ
  across resumes; real call durations and retry counts cannot be byte-equal
  to an uninterrupted process. The engine contract permits fresh NoSync page
  loss after Commit returns. Proposed equality is complete logical data plus
  independently correct committed accounting/provenance, not physical bytes;
  “committed” means present in the durable crash image. Settling check:
  compare raw and canonical artifacts and each stats field with fixed clocks,
  then a retrying run. Calibration must settle permitted normalizations and
  the success-return durability interpretation. C04/C16/C23 remain open at
  the stronger wording until then; no engine durability change is proposed.
- **OQ-3 L3 without seal-ready proof.** The existing interface can store a
  fact but cannot enumerate all ledger rows or report seal phase, and
  scrubbed frontier.State cannot recover its original stack. Proposed
  behavior for files made by this SDK uses the durable fact; unproven L3 is
  refused unchanged. Does the required recovery domain include old/manual
  L3 without proof? If yes, a read-only durable seal-phase/completion query
  (with an engine-owned durable marker before scrub) is needed; row emptiness
  is insufficient. Settling check: partial/full scrub with frontier-only and
  multi-page child graphs, with/without proof, C11/P7. No method added yet.
- **OQ-4 Rollback boundary.** Baseline rollback-expansion explicitly operates
  on SQLite C1File; its token rewrite must remain unchanged under the frozen
  SQLite rule. Ledger expansion-only rebind can be tested without changing
  that CLI. Confirm this scope at calibration. Settling check: C36 through
  WithConnectorStore/WithSyncID/WithOnlyExpandGrants; inspect actual CLI engine
  reachability before claiming a Pebble CLI rollback path exists.
- **OQ-5 Mutations outside PageWriter.** Assets, resource/entitlement deletes,
  expansion layer output and auxiliary metadata have no corresponding
  methods on PageWriter. A bypass cannot justify torn page records. Determine
  which operations are reproducible auxiliary work and which belong to the
  page atomic unit. If any required record mutation cannot be expressed with
  existing puts/DeleteGrants, propose the specific PageWriter extension and
  consumer-level crash test in a CO before code. Settling check: C38/C42's
  inventory and crash-after-each-write images. This is not permission to
  replace atomicity with idempotence.
- **OQ-6 Legacy compaction and missing stats.** tokenParts can carry compaction
  stats, while SyncStats has no compaction field; the storage plan also
  permits a stats-persist failure to leave a sealed file without a sidecar.
  The brief requires stats in the sealed file. Decide preservation ownership
  and whether a missing sidecar is a failed seal or an explicit contract
  limitation. Settling check: genuine legacy compaction token through
  takeover/seal and stats-persist fault through Sync, C24/C30/C31/C43.

All six need boundary settlement before affected implementation choices or
closure. The plan can freeze while naming them; none is settled by assuming
that the unavailable implementation chose correctly.


### Calibration dispositions at 01931d8b

OQ-1 and OQ-3 are dispositioned by CO-004; OQ-2 by CO-005;
OQ-4 by CO-006; OQ-6 by CO-007. OQ-5 remains open and gains
CO-002's split stored/staged delete cell. These references record the
calibration return without replacing the frozen questions or decisions.

## 11. Change-order log

No change orders at freeze. Calibration entries will be appended as CO-001
onward, without revising §§0–10. Each records source, classification
(correction/clarification/extension), observable missing claim, motivation,
contract delta, owning boundary, affected criteria, verification delta, risk
routing and PR placement. Candidate tests supplied for an existing cell are
listed as candidates, not reported as measured evidence or missing claims.

## CO-001 — trust in a row is identity, readability, and not-scrubbed

- **Classification:** clarification
- **Source:** calibration
- **Claim:** Trust in a ledger row is exactly: identity matches on all
  seven fields, the row is readable, and it is not scrubbed. `Attempt` is
  not a trust input. The engine's single-sync contract
  (`pkg/dotc1z/engine/pebble/adapter.go:101–117`: every `startNewSync`,
  full or partial, wipes the keyspace when a prior sync-run exists) plus
  `DropLedger` on finished rebind (your C33) means no row under the open
  sync's identity can belong to another sync. `CloneSync` carries the
  sync ID into the clone.
- **Motivation:** S2 says "trustworthy rows" without defining trust;
  C12 rules `Attempt` out of identity without saying whether it bears on
  trust. Left undefined, an implementer may add an attempt check that
  guards against an unreachable case.
- **Contract delta:** none.
- **Owning boundary:** `pkg/sync` resume walk.
- **Affected criteria:** C09, C12. No new J case.
- **Verification delta:** none. Do not add an attempt check or a test for
  one.
- **Risk routing:** unchanged.
- **PR placement:** this PR.

## CO-002 — a bare-ID delete split across stored and staged rows

- **Classification:** extension
- **Source:** calibration
- **Claim:** A delete by bare external ID whose candidates are split — one
  identity already stored, one staged in the open page — is rejected as
  ambiguous and deletes neither. Or: the plan shows the case unreachable
  because every in-page delete carries full identity.
- **Motivation:** Each half's ambiguity check sees one match; together
  they can delete two identities where the checkpoint path, with
  everything stored, rejects the ID. In scope: `syncer.go:3727` deletes a
  grant by `GetId()` inside the external-resources phase, which becomes a
  page. The store-side `DeleteGrantRecord` (`grants.go:922`) checks stored
  rows only; a same-ID grant staged in the same page is invisible to it.
  `PageWriter.DeleteGrants` takes `*v2.Grant` and resolves by identity, and
  the handler already holds the grant. Keeping a bare-ID delete as a
  registered bypass does not satisfy this CO.
- **Contract delta:** none in `pkg/sync`. The same defect exists
  storage-side in `PageWriter.DropStagedSourceCacheRows`
  (`page_unit.go:236`, `:310`, staged half only); that is out of your
  scope and is being fixed separately by the requester.
- **Owning boundary:** external-resources page handler; Q axis.
- **Affected criteria:** C08 (add the split cell to Q's grant-delete
  targets: staged + existing under one ID), C38, C42, OQ-5.
- **Verification delta:** one fixture: one stored grant and one staged
  grant sharing an external ID; a delete of that ID inside the page; assert
  rejection and both present after commit — or, under the unreachable
  reading, an O8 inventory showing no bare-ID delete inside any page.
- **Risk routing:** HIGH, unchanged.
- **PR placement:** this PR.

## CO-003 — a handler that succeeds without transitioning

- **Classification:** extension to C47
- **Source:** calibration
- **Claim:** A page handler that returns success without having
  transitioned its action fails the page; nothing of it is durable. It
  does not return success leaving staged records no commit will take.
- **Motivation:** C05 states the positive ("success publishes one
  transition") but no mutant plants the defect. Silent loss of a page's
  writes has no single-run signal.
- **Contract delta:** none.
- **Owning boundary:** page wrapper in `pkg/sync`.
- **Affected criteria:** C05, C47.
- **Verification delta:** add to the I1 mutant list; include a
  no-connector action among the fixtures.
- **Risk routing:** unchanged.
- **PR placement:** this PR.

## CO-004 — scope of "the walk writes nothing"; L3 recovery domain

- **Classification:** clarification. Settles OQ-1 and OQ-3.
- **Source:** requester
- **Claim:** "The walk writes nothing" is scoped to the walk: from the
  first row lookup to the first handler that runs. Lifecycle writes outside
  it are permitted and are their own cells: the takeover (C25–C27),
  `DropLedger` on a finished rebind (C33–C34), the run-level bucket on stop
  (C22–C23), and the seal (C31). A durable seal-ready marker is such a
  lifecycle write. If it needs a store method the contract lacks, propose
  it as a `pkg/dotc1z` change with its own consumer test and failure cuts.
  The existing fact surface through a terminal page is acceptable if you
  show the terminal page is itself atomic under the F cuts.
  OQ-3's recovery domain: no ledgered syncer has shipped, so no unproven L3
  file exists anywhere. Refusing unproven L3 with a diagnostic and no
  writes is the accepted answer.
- **Motivation:** OQ-1 and OQ-3 asked.
- **Contract delta:** none unless you propose the store method.
- **Owning boundary:** `pkg/sync` lifecycle; `pkg/dotc1z` only if
  proposed.
- **Affected criteria:** C10, C11, C25, C31; P7.
- **Verification delta:** C10's write-free assertion is bounded to the walk
  as defined; the lifecycle writes are asserted under their own criteria.
- **Risk routing:** unchanged.
- **PR placement:** this PR; a store method, if proposed, in its own
  commit with its own test.

## CO-005 — equality and durability strength

- **Classification:** clarification. Settles OQ-2.
- **Source:** requester
- **Claim:** Equality in O4/C16 is complete logical data — every record
  family, secondary indexes, digest, facts, completion, and committed
  accounting — not bytes. Permitted normalizations, to be listed
  explicitly in the evidence: `Attempt`, `CommittedAt`, `TakenOverAt`,
  page/connector/wait durations, retry counts and waits. The raw artifact
  digest is recorded beside the canonical comparison and never reported as
  equality. "Committed" means present in the durable crash image. A fresh
  page's NoSync loss after `Commit` returned is the storage contract
  (page-ledger plan CO-002, C11); the differential must hold at every image
  without requiring that page to be present.
- **Motivation:** OQ-2 asked.
- **Contract delta:** none. No engine durability change.
- **Owning boundary:** O4, O5 in `pkg/sync` tests.
- **Affected criteria:** C04, C16, C23 close at this wording.
- **Verification delta:** the normalization list is a required artifact
  in `evidence.md`.
- **Risk routing:** unchanged.
- **PR placement:** this PR.

## CO-006 — rollback boundary

- **Classification:** clarification. Settles OQ-4.
- **Source:** requester
- **Claim:** `cmd/baton/rollback_expansion.go` operates on the SQLite
  `C1File` today and is frozen with the rest of the SQLite path. C36 is
  verified through the syncer entry (`WithConnectorStore` + `WithSyncID` +
  `WithOnlyExpandGrants`) only. No Pebble CLI rollback is added or claimed.
- **Motivation:** OQ-4 asked.
- **Contract delta:** none.
- **Owning boundary:** `pkg/sync`.
- **Affected criteria:** C36.
- **Verification delta:** none beyond C36 as stated.
- **Risk routing:** unchanged.
- **PR placement:** this PR.

## CO-007 — compaction provenance and the stats sidecar

- **Classification:** clarification. Settles OQ-6.
- **Source:** requester
- **Claim:** Compaction provenance is owned by the compactor's sidecar
  write (`pkg/synccompactor/provenance.go`). The token's compaction section
  is stale provenance of a prior artifact and is already stripped on the
  checkpoint path (`compactor_pebble.go:786`, `:1302` via
  `ClearCompactionSection`); takeover drops it the same way. `SyncStats`
  gets no compaction field. A `PersistSyncStats` failure at seal is the
  engine's documented degradation (`engine/pebble/adapter.go:438–447`:
  warn, drop the overlay, seal succeeds, `Stats()` falls back to
  iteration), not a failed seal.
- **Motivation:** OQ-6 asked.
- **Contract delta:** none.
- **Owning boundary:** `pkg/sync` seal path; `pkg/synccompactor` unchanged.
- **Affected criteria:** C24, C30, C31, C43, C32.
- **Verification delta:** C31/C43 assert the handover through
  `EndSyncWithStats`; a missing sidecar under an injected
  `PersistSyncStats` fault is not a C31 failure. C32's "inherit a failed
  seal's stats overlay" is covered storage-side (page-ledger C27) and needs
  only a consumer-level check here.
- **Risk routing:** unchanged.
- **PR placement:** this PR.

## CO-008 — the empty-engine cell

- **Classification:** clarification. E axis.
- **Source:** requester
- **Claim:** `Metadata().Engine` is never empty for either supported store:
  `C1File.Metadata` normalizes unset to SQLite (`c1file.go:1512–1519`) and
  `pebbleStore.Metadata` reports from the engine. An empty engine
  therefore identifies a third-party store and takes the same refusal arm
  as an unknown engine. It is not a SQLite alias. Superseded by CO-040:
  both take the token path.
- **Motivation:** the E axis lists "empty engine" without an expected
  outcome.
- **Contract delta:** none.
- **Owning boundary:** `setStore`.
- **Affected criteria:** C01; P3 keeps the cell with this expected outcome.
- **Verification delta:** none beyond the P3 cell.
- **Risk routing:** unchanged.
- **PR placement:** this PR.

## CO-009 — end-to-end sync overhead against the token path

- **Classification:** extension (new criterion, S8)
- **Source:** requester
- **Claim:** The cost of a full Pebble sync on the ledger, relative to the
  same sync on the token path at `eb63f1b5`, is measured and reported
  before this lands — not bounded by an a-priori threshold, but known. The
  deliverable is a table and an estimate, and a decomposition of where the
  overhead goes.
- **Motivation:** S8/O9/C46 cover the walk's scaling shape. Nothing covers
  the hot path: a fresh sync now commits a row, a bucket and facts with
  every page, and the seal scrubs and folds O(rows). The storage side's
  cost criterion (page-ledger C30) is unassessed — its five benchmarks were
  never run and there is no `Put*Records` baseline at matched page sizes.
  This change is the one that puts that cost on every production sync. We
  need to know it is not a disaster, or have a decent estimate of what it
  is.
- **Contract delta:** none.
- **Owning boundary:** `pkg/sync`; the harness is yours. Baseline is
  `eb63f1b5`'s Pebble token path, measured in the same run on the same
  machine, interleaved with the ledger arm.
- **Affected criteria:** new criterion under S8 (C49 or your numbering);
  C46 stays as the walk's half.
- **Verification delta:**
  - Fixture: a deterministic zero-latency connector so store cost is not
    hidden behind connector time. Pages 10³, 10⁴, 10⁵; records per page
    100, 1,000, 10,000; workers 1 and 4. The 10⁵ × 1,000 cell is the
    production-shaped estimate.
  - Arms: (a) token path at `eb63f1b5`; (b) ledger, fresh sync (NoSync
    pages per the storage contract); (c) ledger, sync resumed at page 1
    and run to completion (Sync per page — the worst case, and a real one
    after any crash).
  - Metrics per cell: wall time; time inside page `Commit` vs handler;
    Pebble bytes written (WAL, flush, compaction) from the engine's
    metrics; final c1z size; peak RSS; seal time split into scrub and
    fold; resume-walk time vs rows for (c).
  - Report: the ratio (b)/(a) and (c)/(a) per metric per cell, and the
    decomposition — bytes per page attributable to row + bucket + facts
    versus record bytes; scrub and fold as a function of rows.
  - Tripwire, not acceptance: any cell where (b)/(a) exceeds 1.10 on wall
    or 1.25 on bytes written, or where seal time grows faster than linearly
    in rows, is explained in the implementation brief before code lands.
    The requester decides acceptance from the numbers.
  - Candidates: `BenchmarkLedgerPageCommit`, `BenchmarkLedgerPageCommitSync`,
    `BenchmarkLedgerResumeWalk`, `BenchmarkLedgerSealCost`,
    `BenchmarkLedgerSealCostNoGrantIndex` in `pkg/dotc1z/engine/pebble`
    exist and are unrun; they are component-level and do not substitute for
    the end-to-end arms.
  - Machine: unloaded, recorded (model, cores, disk). A loaded-machine run
    is a smoke check, not evidence.
- **Risk routing:** cost pass, per §0's seven passes.
- **PR placement:** the harness and the first measured table land before
  the handler commits, so the numbers exist while the design can still
  move. The final table is re-run at the PR's last revision.

### CO-009 criterion assignment

**C49 (S8, O9; sampled cost evidence).** Measure and report every CO-009
fixture/arm/metric cell, with its specified baseline, ratios, decomposition,
production-shaped estimate and machine record. Explain each tripwire in
implementation.md and obtain requester acceptance of the numbers before
landing the change. The harness and first measured table precede handler
commits; the final revision gets a new table. Initial status: not assessed.
There are now 49 criteria; the frozen §4 count describes the baseline.


## CO-010 — preserve baseline lifecycle behavior

- **Classification:** correction
- **Source:** requester; lifecycle preservation instruction after caller review.
- **Claim:** This change preserves the baseline's sync lifecycle behavior.
  Selecting an existing finished sync with WithSyncID does not itself start
  a new collection run, reset the sync record, or erase completion state.
  Requested processing of existing data, including deferred expansion, keeps
  the existing sync identity and baseline lifecycle semantics.
- **Motivation:** The finished-run reset proposal inferred lifecycle meaning
  from ended_at alone. Baseline callers also use finished artifacts as inputs
  to further processing under the same sync ID.
- **Contract delta:** withdraw the proposed completion-record reset. Any
  ledger-specific processing state must preserve existing caller behavior.
- **Owning boundary:** pkg/sync lifecycle and ledger reconstruction.
- **Affected criteria:** C15, C24, C30–C36; S5's new-run interpretation is
  superseded where it conflicts with this preservation requirement.
- **Verification delta:** compare baseline and ledger behavior for a finished
  collection with deferred expansion, same-ID processing, interruption and
  subsequent invocation. Assert identity, completion metadata, carried facts
  and accounting, requested work and preserved input data. The synthetic
  reset candidate alone cannot establish the required behavior.
- **Risk routing:** HIGH, unchanged.
- **PR placement:** this PR.


## CO-011 — reuse existing scheduling and allow small shared changes

- **Classification:** clarification and correction
- **Source:** requester; clarification of the SQLite constraint.
- **Claim:** Existing scheduling and lifecycle behavior remain in use. The
  SQLite constraint prevents wasted SQLite write work and new behavioral
  risk; it does not prohibit small, non-invasive shared changes needed for
  ledger integration. SQLite checkpoint/write behavior remains unchanged.
  Neither a replacement scheduler nor a copied scheduler is required.
- **Motivation:** Literal avoidance of every SQLite-executed line led to an
  unnecessary executor with different scheduling behavior.
- **Contract delta:** no store contract change. Supersedes structural claims
  that forbid all edits to shared executed paths or require scheduler copies.
- **Owning boundary:** pkg/sync page execution, transition publication,
  restoration and engine-specific persistence.
- **Affected criteria:** C03,C13–C16,C39,C40,C44,C45. Source-diff checks must
  accept justified shared integration edits; behavioral preservation remains
  required. No criterion is closed by this clarification.
- **Verification delta:** exercise ledger pages through the existing
  scheduler; retain its batch, duplicate-cursor, retry, warning, stop and
  error behavior. Check SQLite behavior at shared integration points. Do not
  spend effort extending SQLite writes or duplicating scheduling to avoid
  those points.
- **Risk routing:** HIGH, unchanged.
- **PR placement:** this PR.

## CO-012 — measure actual resume durability

- **Classification:** correction
- **Source:** requester; accepted explanation of the baseline's actual writes.
- **Claim:** C49's resumed arm measures the existing NoSync page commits,
  matching main. A hypothetical forced-disk-write comparison, if included,
  is labeled separately and is not the production resume arm. Production
  durability remains unchanged.
- **Motivation:** CO-009 described resumed pages as Sync, but eb63f1b5 uses
  recordWriteOpts (NoSync) for fresh and resumed pages alike.
- **Contract delta:** none; corrects the measurement premise in CO-009.
- **Owning boundary:** C49 cost harness and report.
- **Affected criteria:** C49. Other coverage and acceptance requirements remain.
- **Verification delta:** report the actual fresh and resumed write options;
  no hypothetical arm is required to close this discrepancy.
- **Risk routing:** cost pass, unchanged.
- **PR placement:** this PR.

## CO-013 — inexpensive collection report

- **Classification:** extension
- **Source:** requester
- **Claim:** One customer- and connector-author-facing report identifies the
  most expensive collections, page counts, written records per page, latency
  distribution and reported rate-limit waits, with drill-down to individual
  resources. Producing it must be relatively cheap; measured time and memory
  cost accompany the implementation. Raw ledger retention after recovery is
  no longer needed must have a demonstrated diagnostic use.
- **Motivation:** Aggregate sync totals do not explain which collections or
  resources consumed collection time. Retaining all rows without a usable
  report does not justify their artifact cost.
- **Contract delta:** not yet selected. Do not drop recovery state before
  completion is durable. Report storage, access and post-seal ledger removal
  require a design decision; this entry does not authorize early deletion.
- **Owning boundary:** ledger diagnostic reader/report and artifact lifecycle.
- **Affected criteria:** new C50; C49 includes report production cost.
- **Verification delta:** compare report aggregates and ranked collections
  against known page fixtures, including token-scrubbed rows and parallel
  work; label overlapping durations and written-record counts accurately.
  Measure report time, peak memory and artifact size as row counts grow.
- **Risk routing:** correctness, cost and operability passes.
- **PR placement:** high-priority deliverable in this PR; design before code.

## CO-014 — report value includes correctness and requested scope

- **Classification:** clarification and priority change
- **Source:** requester
- **Claim:** Assess useful information from the ledger before continuing the
  remaining implementation. The report is not limited to slowness: explore
  collection coverage and correctness evidence, requested arguments/flags,
  intentional exclusions, and timing. Missing work must not be confused with
  an empty endpoint or collection disabled by flags. Establish report value
  before deciding retention or token-scrubbing mechanics.
- **Motivation:** Retained page history needs a useful consumer; its value
  determines subsequent work.
- **Contract delta:** none in the experiment. Any additional scope/outcome
  metadata requires a subsequent design and verification amendment.
- **Owning boundary:** report experiment and effective-request provenance.
- **Affected criteria:** C50; C49 still requires measured cost.
- **Verification delta:** examples and tests distinguish missing references,
  zero writes with unknown outcome, explicitly disabled grants and unknown
  request scope. Do not assert source completeness from resolved references.
- **Risk routing:** correctness, cost and operability passes.
- **PR placement:** feasibility experiment first; supersedes the ordering in
  implementation section 40. No retention change is implied.

## CO-015 — structured stats and default ledger disposal

- **Classification:** clarification and lifecycle direction
- **Source:** requester
- **Claim:** The report is mechanically generated structured stats for logging,
  not prose or HTML. Keep effective sync options readable and preserve a compact
  stats artifact. The default direction is to drop detailed ledger rows once
  completion is durable and recovery no longer needs them; explicit diagnostic
  retention may preserve rows, with tokens scrubbed by default.
- **Motivation:** A 10–15 percent artifact-size increase needs demonstrated value;
  aggregate diagnostics do not by themselves require permanent page history.
- **Contract delta:** none in the prototype. Production summary persistence,
  disposal ordering, failure behavior and diagnostic retention require their own
  design and failure cuts before changing the store lifecycle.
- **Owning boundary:** stats format, readable request metadata and seal lifecycle.
- **Affected criteria:** C31, C35, C50; C49 measures generation/disposal cost.
- **Verification delta:** typed values, deterministic output, no token leakage;
  persisted summary/options survive later processing and ledger disposal.
- **Risk routing:** correctness, cost and operability passes.
- **PR placement:** this PR; lifecycle changes follow report qualification.

## CO-016 — one ledger walk and bounded report state

- **Classification:** constraint
- **Source:** requester
- **Claim:** Stats generation must not do quadratic work. Use one sequential
  ledger walk, bounded aggregation state and no per-page record/ledger lookups.
  Do not retain a map or latency list proportional to pages, resources or types.
  Stream larger complete breakdowns to an output sink. Measurements include
  million-row cases and memory, not just cumulative allocations.
- **Motivation:** The report must remain inexpensive on artifacts with millions
  of rows.
- **Contract delta:** no production store change in the experiment. Exact missing
  reference validation is unavailable under this pass; its fields must remain
  unknown rather than infer completeness from counts.
- **Owning boundary:** report iterator, aggregation and serialization.
- **Affected criteria:** C49, C50.
- **Verification delta:** iterator-only input; exactly one walk; no extra reads;
  long chains, broad resource/type counts and large child lists; sampled heap
  and RSS separated from post-GC retained heap and allocation totals.
- **Risk routing:** cost and correctness passes.
- **PR placement:** qualify before production report integration.

## CO-017 — default report scope and debug retention

- **Classification:** clarification and extension
- **Source:** requester
- **Claim:** Default stats preserve effective non-secret sync options, committed
  pages and writes by family/operation/resource type, SDK exclusions by known
  reason with connector-returned counts for context, empty responses and
  pagination, collection latency, observed waits, elapsed phase times, and
  connector attempts/errors across retries of a successfully committed page.
  Resume-reuse counts are omitted. Returned counts describe the connector
  interface, never upstream completeness. Retry observations survive individual
  discarded attempts but are committed only with the successful page; exhausted
  retries abort and uncommitted observations can be lost on crash.
  Preserve the report and options before safe default ledger disposal. Debug
  retains a scrubbed ledger and enables reference validation with bounded
  examples. Explicit no-scrub requires retention and warns about credentials.
  Validation inconsistencies warn and preserve evidence. Indexed debug checks
  may use O(N log N) work; one walk is preferred, not mandatory. Quadratic work
  and unbounded aggregation memory remain forbidden.
- **Motivation:** Explain SDK decisions and collection cost without claiming
  source completeness or building a durable history of failed attempts.
- **Contract delta:** additive page observations and saved report/options;
  lifecycle disposal requires crash-safe ordering and consumer tests.
- **Owning boundary:** sync page execution, report generation and seal lifecycle.
- **Affected criteria:** C31, C35, C49, C50.
- **Verification delta:** retry counters persist across discarded attempts and
  reset after each committed page; no row on exhausted retries; family counts
  survive projection/aggregation/serialization; empty response is distinct from
  filtered or internal work. Saved report survives disposal and reopen.
- **Risk routing:** correctness, cost and operability passes.
- **PR placement:** this PR.

## CO-018 — option metadata in differential comparisons

- **Classification:** clarification derived from CO-017
- **Source:** requester (record sync arguments); implementation consequence
- **Claim:** Attempt option snapshots are diagnostic history. Canonical comparison
  removes only their attempt identity and collapses identical option snapshots;
  it retains all flag values. Worker configurations have their own uninterrupted
  references because their recorded worker-count arguments intentionally differ.
  Record/index/digest/completion/accounting comparison remains intact. Raw
  artifacts and raw option history are retained as separate evidence.
- **Motivation:** Recording arguments must not force a four-worker artifact to
  falsely claim one worker, or hide changed behavior flags to satisfy equality.
- **Contract delta:** diagnostic metadata only; no execution behavior change.
- **Owning boundary:** canonical test oracle and option snapshot tests.
- **Affected criteria:** C16, C50.
- **Verification delta:** duplicate attempt-only snapshots normalize equally;
  changed skip flags remain unequal; unreadable option values fail comparison.
- **Risk routing:** correctness and operability passes.
- **PR placement:** this PR.

## CO-019 — use the current benchmark machine

- **Classification:** clarification
- **Source:** requester
- **Claim:** Run the remaining C49 measurements on the current environment;
  a dedicated or independently qualified unloaded machine is not required.
  Record the observed machine and load conditions with the results.
- **Motivation:** The requester accepts this machine for performance evaluation.
- **Contract delta:** measurement environment only; no durability or runtime change.
- **Owning boundary:** C49 cost harness and evidence.
- **Affected criteria:** C49.
- **Verification delta:** preserve interleaved baseline/ledger comparisons and
  metric definitions; describe observed host conditions without claiming isolation.
  This does not waive the remaining matrix, decomposition or tripwire explanation.
- **Risk routing:** cost pass.
- **PR placement:** this PR.

## CO-020 — accept collection performance with observed connector waits

- **Classification:** boundary decision.
- **Source:** requester.
- **Claim:** Collection performance is accepted given the reported local latency
  run: nine arms each verified one million resources; median sync time was about
  109 seconds, fresh/token 1.004 and resumed/token 0.998 with approximately 300ms
  per-page waits. The zero-latency regression alone does not block collection.
- **Motivation:** Representative waiting dominated completion time.
- **Contract delta:** performance acceptance only; no durability change.
- **Owning boundary:** collection cost acceptance.
- **Affected criteria:** C49.
- **Verification delta:** retain the measured zero-latency results and label the
  latency result requester-reported; raw local results have not been imported.
  Do not claim expansion was measured or that the original full matrix ran.
- **Risk routing:** cost pass.
- **PR placement:** this PR.

## CO-021 — deterministic expansion stays outside page-ledger execution

- **Classification:** scope correction.
- **Source:** requester.
- **Claim:** Page ledgers preserve collected sync data, not expansion batches.
  Expansion uses main's evaluator, store capabilities and whole-phase replay.
  An interruption may leave expansion output in the store; rerunning over the
  same collected input must converge to the clean result without extra grants.
  Collection pages already committed must not be fetched again. No expansion
  batch row or expansion page cursor is required. Final completion still follows
  successful processing; timing and completed-action accounting remain accurate.
- **Motivation:** Expansion is fast deterministic work with its own replay model.
  Adapting it to page transactions disabled existing Pebble optimizations.
- **Contract delta:** expansion is excluded from page atomicity/identity products;
  collection and terminal lifecycle proofs retain their requirements.
- **Owning boundary:** syncer expansion dispatch and existing expansion storage.
- **Affected criteria:** C04–C09, C13–C16, C20–C23, C31–C36, C39–C45, C48–C50.
- **Verification delta:** replace expansion-page fixtures with capability-use,
  no-expansion-row, interruption/reopen/replay and seal-accounting checks. Keep
  existing expander replay/segment tests. Extra output after replay is an
  expander defect, not a justification for adding ledger checkpoints.
- **Risk routing:** HIGH for the phase handoff; expansion algorithm unchanged.
- **PR placement:** this PR.

## CO-022 — discard once instead of scrubbing and then discarding

- **Classification:** correction.
- **Source:** requester.
- **Claim:** Default ledger disposal saves its report and recovery state, deletes
  the ledger and purges its residue once before the finished stamp. It does not
  scrub rows that will be deleted. Debug retention keeps the existing scrub
  policy. A report-write failure retains scrubbed history rather than losing it.
- **Motivation:** Scrub/purge followed by drop/purge repeats compaction work.
- **Contract delta:** EndSyncWithStats honors a durable discard-on-seal declaration;
  the archive can preserve terminal recovery state before ended_at is durable.
- **Owning boundary:** Pebble sealing, report archival and syncer recovery.
- **Affected criteria:** C11, C25, C31–C35, C43, C49–C50.
- **Verification delta:** assert one residue-purge invocation and zero scrub time
  for default disposal; assert report/counters survive failures around archival,
  deletion, purge and the finished stamp. Recovery must not restart collection
  from an empty ledger or overwrite its archived report with an empty report.
  Retained/debug modes and report-write failure remain separate cases.
- **Risk routing:** HIGH lifecycle change; existing credential-erasure obligations
  still apply before the finished stamp.
- **PR placement:** this PR.

## CO-023 — remove the external-import transaction now

- **Classification:** scope correction.
- **Source:** requester.
- **Claim:** External-resource import and matching use main's ordinary store
  writes, batching and deletion behavior. They are not accumulated in one ledger
  transaction. Removal does not wait for the independently reproduced replay bug
  in main, and this PR does not claim to fix that bug.
- **Motivation:** Buffering all imported and matched records is not viable and
  was introduced by this implementation, not required by main's behavior.
- **Contract delta:** external processing joins expansion outside page execution;
  completed local-phase accounting is recorded at terminal completion.
- **Owning boundary:** syncer external dispatch; remove its unused page APIs.
- **Affected criteria:** C04–C09, C16, C20–C23, C31–C36, C38–C45, C48–C50.
- **Verification delta:** prove main's writes occur before fetching the next
  external grant page; preserve its matching/deletion checks and migrate fault
  wrappers back to direct-store boundaries. No all-or-nothing import assertion.
  Debug references must not demand an external-processing page row.
- **Risk routing:** HIGH integration change; main's matching algorithm unchanged.
- **PR placement:** this PR, before CO-022 implementation.

### CO-022 implementation clarification

The purge removes history while retaining one token-free disposal declaration.
It is removed after finalization succeeds, without a second purge. This keeps a
failed seal distinguishable from a new request over an already-finished binding;
it does not reset the sync record's prior completion timestamp. Successful
completion has no live ledger keys. The declaration carries no page token.

### CO-024 — recover known quality for an empty unfinished sync

- Classification: clarification following final verification.
- Source: requester (fix the conflation of lost initial pages with unknown legacy collection).
- Claim: an unfinished bound sync with no surviving checkpoint, collection records or collection/replay state starts quality accounting cleanly when collection runs again. Surviving legacy progress without quality information remains unknown. This decision must require no store writes or new durable marker.
- Motivation: the public crash differential found that losing all initial pages incorrectly made the finished file ineligible for source-cache replay even after complete recollection.
- Contract delta: add a read-only bounded store query for an unstarted bound sync; session state alone is not collection progress. Errors cannot establish clean quality.
- Owning boundary: syncer restoration and Pebble bound-state inspection.
- Affected criteria: C10, C16, C17, C19, C30.
- Verification delta: restore exact quality equality in the 40-case public crash differential; test each surviving record/state family, legacy token/frontier, finished binding, session-only state and read failure. Assert unchanged raw keys and no writes.
- Risk routing: HIGH, unchanged.
- PR placement: this PR.

### CO-025 — bound static-entitlement materialization

- Classification: extension following independent review and reproduction.
- Source: independent review and parent verification.
- Claim: static-entitlement generation must not retain a resource type's entire population in one page writer. Each generated resource chunk commits records, continuation and counters atomically; recovery uses the already recorded template and preserves template order, including duplicate slugs and identical templates.
- Motivation: 21,002 resources produced 21,002 simultaneously staged entitlements; main wrote three batches capped at 10,000.
- Contract delta: a ledger-only materialization action represents each template's local work. The remote response commits its complete child list and remote continuation first. Memory is bounded by a remote definition response plus one stored-resource page and its generated records, not by a fixed byte cap on arbitrary connector replies.
- Owning boundary: static collection/materialization and existing scheduler dispatch. No new scheduler or storage schema.
- Affected criteria: C04–C09, C16–C23, C42, C46, C50.
- Verification delta: staged-count bound; duplicate/order comparison with token handler; failed local commit plus cold resume without repeated remote calls; process/durable cuts for parent and child; internal-cursor rejection; scrub/disposal coverage.
- Accounting: completed-action totals include materialization actions. Remote received counts, connector observations and progress remain on the remote action; generated writes and local duration belong to materialization rows. The report names the new operation and does not label it a connector call.
- Risk routing: HIGH, unchanged.
- PR placement: this PR, with follow-up independent review.

### CO-026 — preserve retention and account for local materialization

- Classification: corrections from independent implementation review.
- Source: independent reviewers and local reproductions.
- Claim: a durable request to retain tokens also prevents a later default resumer from discarding that history. Archived effective options describe the resulting artifact. Materialization time participates in named operation totals. Action log objects omit page tokens, including captured template payloads, without changing checkpoint serialization.
- Motivation: a default resumer discarded retained history; the static correction omitted its new operation from timing summaries and exposed encoded template bytes through debug action logs.
- Contract delta: none; honor durable retention and the existing timing/accounting contract.
- Owning boundary: syncer lifecycle, action logging and operation timing.
- Affected criteria: C16, C20–C24, C31, C37, C43, C50.
- Verification delta: public stop/reopen/resume with and without debug; debug admission/completion and pointer-action error logs with distinct payload markers; ordinary JSON token round-trip; summary total and named duration reconciliation. Each reproduction fails before its correction.
- Risk routing: HIGH, unchanged.
- PR placement: this PR.

### CO-026 finished-binding boundary

A retention declaration governs completion and retries of the current collection.
Once finished page history is cleared for further same-ID processing, its old
retain-token declaration is cleared atomically with that history. The invoking
process's current options choose discard, scrubbed debug retention or explicit
token retention for subsequent pages. This does not reset stored data, counters,
sync identity or lifecycle metadata. Unfinished recovery still inherits the
durable declaration; a pending seal is not a fresh policy boundary.

### CO-027 — durable pending work is the recovery authority

- Classification: architectural correction.
- Source: requester, following review of cumulative resume memory and repeated request semantics.
- Claim: each work instance has an ID and page revision independent of its request arguments. A page commits records, accounting, completion observations and pending-work updates atomically. Resume reads pending work directly in bounded windows, without traversing or remembering completed pages. Repeated request arguments may execute again, including an empty terminal response. No new cycle rejection or warning work is required for this change.
- Contract delta: separate pending-work and queue-metadata key ranges within the existing Pebble ledger family; an atomic page transition validates the expected work revision, replaces/deletes its pending entry and inserts children. The allocator advances in that batch. Completed rows are report input, not recovery input. Initial work and legacy takeover seed the queue atomically with their initialization/migration declaration. Finished processing preserves data and lifecycle metadata while clearing completed history and applying current retention options.
- Owning boundary: storage page batch, lifecycle/takeover and existing scheduler integration. SQLite remains on its existing path. No replacement worker pool.
- Affected criteria: C04–C16, C20–C36, C39–C50. Earlier history-walk and request-identity deduplication wording is superseded where incompatible.
- Verification delta: before/after durable page images include pending/record/history/fact/counter state; stale revisions and batch failures leave all unchanged; repeated-token then empty-terminal fixture; unique child instances for equal request arguments; checkpoint versions and takeover cuts; serial ordering/parallel phase barriers; bounded decoded work for growing completed history and pending backlog; default/debug disposal includes new ranges. Prototype artifacts establish feasibility, not integration closure.
- Cost evidence: raw Pebble prototype, 1m completed pages/10 pending entries resumed in approximately0.18ms;100k pending entries decoded in windows of64. Extra queue writes cost approximately1.6s per1m one-record pages; requester accepts this scale of overhead. Actual scheduler integration still needs comparison.
- Risk routing: HIGH; no production queue migration until the transaction and bounded-loader checks exist.
- PR placement: replace current recovery algorithm in this PR. No shipped ledgered syncer exists; incompatible unfinished experimental history-only artifacts must be diagnosed rather than silently treated as empty.


### CO-028 — default disposal also applies when reporting fails

- Classification: correction.
- Source: requester.
- Claim: a successfully sealed default-mode artifact contains no completed ledger history, including when report generation fails. Report failure is recorded as unavailable; it does not implicitly enable retention. Failure to persist recovery state prevents seal and remains retryable.
- Contract delta: none.
- Owning boundary: Pebble archive and seal.
- Affected criteria: C31, C32, C43, C50.
- Verification delta: report-generation failure through public full/partial sync and overlay/fold compaction asserts history absent and data/stats preserved; recovery-archive write failure asserts unfinished state and successful disposal on retry. Existing disposal crash cuts remain required.
- Risk routing: HIGH, unchanged.
- PR placement: this PR.


### CO-029 — diagnostic retention is independent of logging

- Classification: correction.
- Source: requester.
- Claim: log verbosity never enables ledger history retention or extra reference checks. Those require the explicit ledger debug option; durable token-retention policy on unfinished recovery remains honored.
- Contract delta: none.
- Owning boundary: sync configuration.
- Affected criteria: C37, C50.
- Verification delta: public sync crosses info/debug logging with explicit ledger debug enabled/disabled, checking effective saved options and actual row retention. Debug logging alone does not authorize token retention.
- Risk routing: unchanged.
- PR placement: this PR.

### CO-030 — bound metadata across attempts

- Classification: correction.
- Source: requester; preserve first and latest options rather than every attempt.
- Claim: an unfinished sync retains at most two option snapshots and one folded prior-attempt counter bucket plus current-attempt buckets. Increasing retry count alone does not increase live metadata keys or the amount loaded on resume. First options remain unchanged; latest options change only with a committed page. Folding preserves sums, maxima and OR flags across success, cancellation, repeated preparation and crash.
- Contract delta: `FoldLedgerCounters(ctx, currentRunID)` is a lifecycle write before the read-only restore. It atomically replaces older buckets with their folded total while preserving current-attempt buckets. A repeated call is idempotent. Page history and records are unchanged.
- Owning boundary: Pebble counter storage; sync lifecycle and report options.
- Affected criteria: C10, C18, C22–C28, C32, C33, C46, C50.
- Verification delta: thousands of attempts with fixed worker count have a fixed live bucket bound and exact folded totals; before/after synced-fold crash images, precommit failure, cancellation and repeated calls preserve accounting. A current bucket may be overwritten after fold without counting its prior value twice. Public resume preserves first/latest options through seal and saved-file reopen; intermediate attempts are not retained. Existing migration, quality, seal and lifecycle tests remain required.
- Risk routing: HIGH. Counter deletion and replacement are one batch, never separate writes.
- PR placement: this PR.

### CO-031 — preserve public EndSync lifecycle semantics

- Classification: compatibility correction.
- Source: requester; main preserves the checkpoint when ending unfinished collection.
- Claim: plain EndSync accepts a ledgered run with pending work, stamps its end and detaches it without destroying records, pending work, facts, counters, history or recovery tokens. Close/reopen and explicit binding preserve that recovery position. Starting a new sync retains main's existing reset behavior. Successful syncer completion alone enforces empty pending work and applies report/disposal policy.
- Contract delta: EndSyncWithStats remains the checked completion path; plain EndSync derives saved statistics from committed ledger accounting and preserves recovery state. The old plain-EndSync refusal requirement is superseded.
- Owning boundary: Pebble lifecycle and shared accounting conversion; SQLite unchanged.
- Affected criteria: C16, C24, C31–C36, C43.
- Verification delta: pending pages with tokens and records survive early end, cold reopen and explicit binding; C1's end/cleanup/start sequence works unchanged; pre-stamp failure and durable images around the end stamp preserve recovery and accounting. Existing successful-completion disposal and pending-work rejection tests remain.
- Risk routing: HIGH. No format relocation or new destructive recovery operation.
- PR placement: this PR.

### CO-032 — nil connector store preserves path fallback

- Classification: compatibility correction.
- Source: automated review, under requester's requirement to preserve existing caller behavior.
- Claim: WithConnectorStore(nil) combined with WithC1ZPath opens the path, independent of option order. With neither a store nor path, construction fails. Engine/capability refusals for non-nil stores remain unchanged.
- Contract delta: none; restore main's fallback.
- Owning boundary: syncer store attachment.
- Affected criteria: C01, C02.
- Verification delta: public constructor and path attachment across Pebble/SQLite and both option orders; missing-store-and-path refusal.
- Risk routing: bounded attachment correction; no storage mutation change.
- PR placement: this PR.

### CO-033 — service-mode connector rollback remains usable

- Classification: verification extension.
- Source: requester; use actual service mode, not a direct syncer invocation.
- Claim: after a new-SDK daemon is killed during collection or reports a sync error, an older SDK daemon with the same persistent directory and configuration can accept the redelivered task, upload usable data and report success. Redoing work and discarding partial output are acceptable. Check the optional previous-sync spare as well as default operation.
- Contract delta: none. C1 vendored-SDK artifact downgrade remains a separate accepted constraint.
- Owning boundary: connector runner, c1api task manager and full-sync handler; simulated C1 API.
- Affected criteria: C16, C25, C31, C33.
- Verification delta: actual daemon startup, polling, heartbeat, streaming upload and finish over local TLS/gRPC; replace processes and SDK versions without clearing their directory; verify new-SDK partial ledger premise, old-SDK successful upload contents, retryable failure classification and a subsequent task. Authentication/queue responses are simulated, not production C1 workflow evidence.
- Risk routing: HIGH compatibility assurance; test-only work, no production change proposed.
- PR placement: opt-in cross-version test and reproduction tool, with results before any compatibility claim.

### CO-034 — initialize requested work on ended, empty rebind

- Classification: correctness correction.
- Source: final review reproduction, confirmed by requester as the hosted expansion path for unexpanded connector uploads.
- Claim: reopening an ended sync with no pending work initializes the current request, including WithOnlyExpandGrants. Reopening with pending work resumes it. An unfinished empty queue completes its seal without restarting collection. The uploaded records, sync identity and accumulated accounting survive the processing boundary.
- Contract delta: ClearLedgerRows permits an initialized but empty queue on an ended sync; it still refuses unfinished syncs or any pending work. Queue emptiness is checked under the same write lock as the clear.
- Owning boundary: syncer lifecycle selection and Pebble history-clear precondition.
- Affected criteria: C16, C31–C36, C43.
- Verification delta: public collect-without-expansion/save/copy/reopen/expand-only must produce exact inherited grants for normal sealing and premature EndSync, with 1/4 workers and default/debug retention. An unfinished empty queue seals without recollection. Existing pending-resume and clear failure/crash checks remain required.
- Risk routing: HIGH. Use existing Init planning and atomic history-clear/queue-seed operations; no new scheduler or expansion implementation.
- PR placement: this PR.

CO-034 seal-retry clarification: an initialized declaration with seal-ready set
still has lifecycle work to finish, even if its page queue is empty and ended_at
is already present. Preserve its terminal marker and retention policy and retry
the prepared seal. Fully checked completion clears the declaration; a subsequent
ended rebind initializes the new request normally. Source: correction review.

### CO-035 — a prior seal does not fulfill a new expansion request

- Classification: caller-contract correction.
- Source: requester, following the prepared-seal review finding.
- Claim: an explicit expansion-only call on an empty recovery queue finishes any prior seal and then performs the requested expansion before returning success. It does not require a second identical call. Pending expansion work still executes directly. Ordinary recovery without a new expansion-only request may finish the prior seal alone. Existing records and sync identity survive the handoff; errors at either phase remain errors.
- Contract delta: none in storage. This supersedes CO-034's two-call expansion-only behavior. Repeating deterministic expansion for a new explicit request is allowed and is accounted as actual work; no file-level expanded-status flag is added.
- Owning boundary: syncer orchestration between sealing the prior pass and initializing the requested pass.
- Affected criteria: C16, C24, C31–C36, C43.
- Verification delta: copied unexpanded files, prepared seals before/after EndSync, normal recovery control, seal failure/retry and close/reopen must yield exact expanded grants from one expansion-only call. Preserve counters and prior retention through the first seal; apply current options to new work. No recollection, no replacement sync ID, no false success at a failed handoff.
- Risk routing: HIGH; reuse existing seal, binding and Init operations.
- PR placement: this PR.

### CO-036 — successful retries retain connector accounting

- Classification: accounting correction.
- Source: independent review reproduction; requester asks to fix.
- Claim: a committed page includes connector method calls, session usage and reported waits from every attempt in its successful retry sequence, exactly once. Live totals agree with committed totals. Failed attempts contribute no record, ingest or completion effects. A subsequent page starts a new observation accumulator.
- Contract delta: none; observations before a process crash remain best-effort.
- Owning boundary: syncer page retry observations.
- Affected criteria: C22–C24, C43.
- Verification delta: fail twice then succeed, assert exact counts/sums/maxima, session errors/timeouts, waits and page isolation; prove failure before correction and run focused race checks.
- Risk routing: HIGH for silent persisted accounting; bounded method-level aggregation, no per-attempt history or new writes.
- PR placement: this PR.

### CO-037 — one durable authority per lifecycle question

- Classification: lifecycle correction.
- Source: independent review of the seal and rebind lifecycle at 0bd0e5fb; requester asks for the full correction.
- Motivation: two defects share one cause. (1) A process killed after `ended_at` is durable but before the post-stamp cleanup batch leaves a finished file whose next `WithSyncID` rebind re-seals instead of starting the requested pass; a plain rebind reports success with no work done. (2) A rebind pass that drains its queue and dies before its terminal page is classified on resume as "ended, empty" and recollects, because `ended_at` from the prior pass is the only finished signal and the ledger's own markers (`sync.seal_ready`, the surviving `c1z.discard_ledger_on_seal` fact, presence of the work declaration) change meaning depending on it. Main has (2) but not (1).
- Claim, stated on the file:
  1. One record answers "what pass state is this sync in": the pending-work declaration carries a phase, `collecting` or `sealing`. Absent declaration with `ended_at` set means sealed; absent without `ended_at` means never started. The terminal page commits `sealing` in the same batch as its run bucket and terminal facts. No fact and no timestamp comparison answers this question.
  2. The `ended_at` stamp, the archive write, and removal of the remaining declaration are one atomic batch. After that batch is durable no write to the ledger family occurs until an explicit rebind. A process killed at any point after the stamp leaves a file byte-equal in the ledger family, archive and sync-run record to an uninterrupted seal.
  3. Resume selection is a function of phase, then the legacy token, then `ended_at`: `collecting` continues (an empty queue commits the terminal page, no connector collection call); `sealing` finishes the seal; absent with a legacy token takes it over regardless of `ended_at` (a finished unexpanded upload from the baseline SDK keeps its final token); absent, no token, `ended_at` set starts the requested pass, with or without an archive (a finished file the baseline SDK or the compactor produced has none); absent, no token, no `ended_at` seeds. A legacy token alongside any declaration is an error.
  4. Sealed default-mode files have an empty ledger family and no token; sealed retained-mode files keep scrubbed rows, facts and counters and no declaration. A baseline-SDK host can expand a default-mode upload by sync ID; this is unchanged.
  5. Producer/consumer pairs across the upload boundary: a finished unexpanded upload produced by the baseline SDK and expanded by this SDK (`WithSyncID` + `WithOnlyExpandGrants`) consumes the legacy token, makes no connector collection call, and yields the same expanded grant set as the same file produced and expanded by this SDK. A kill at any point during the host pass converges to that set on redelivery.
- Contract delta: `PendingWork`/`PendingWorkAfter` return a phase instead of an initialized boolean. `PageWriter` gains a terminal-transition method, validated at commit under the write lock: phase `collecting`, no pending entries, no continuation or children on the row. `EndSyncWithStats` requires phase `sealing`; an ended sync with no declaration remains resealable at engine level for low-level tools. `ClearLedgerRows` and `RestoreLedgerArchive` are replaced by one begin-pass operation that, in one synced batch, restores archived facts not already present and archived counters only when no counter bucket exists in the family (retained-mode seals keep theirs; both sources hold the same totals), removes rows, scheduling relations, frontier and the named facts, seeds the request and sets phase `collecting`; it requires `ended_at` and no declaration. `c1z.discard_ledger_on_seal` is a policy fact read at seal with no survival semantics. `sync.seal_ready` is removed. The archive keeps its engine-meta placement; it is written in the stamp batch. Plain `EndSync` (CO-031) is unchanged: it stamps `ended_at` and leaves the declaration and family intact.
- Owning boundary: Pebble pending-work state, seal finalizer, begin-pass; syncer lifecycle selection.
- Affected criteria: C11, C16, C25, C31–C36, C43, C50.
- Supersedes: the CO-034 seal-retry clarification (phase replaces "initialized with seal-ready on an ended sync"); CO-022's "the archive can preserve terminal recovery state before ended_at is durable" (the declaration does; the archive rides the stamp batch); CO-028's "failure to persist recovery state prevents seal" (the stamp batch fails atomically, leaving phase `sealing`). CO-034's end state and CO-035's handoff are preserved: an ended empty queue is a drained pass, sealed first, then the requested pass runs.
- Verification delta:
  - (a) Kill with the stamp as the last durable write (existing `endSyncPreFlushHook`); reopen; rebind with a recording connector: collection is called. Fails at 0bd0e5fb.
  - (b) Rebind pass; drain; kill before the terminal page; reopen; rebind: no collection call, the pass seals, expanded grants equal the uninterrupted reference. Fails at 0bd0e5fb.
  - (c) Raw ledger-family, archive and sync-run snapshot after (a) equals the uninterrupted seal's snapshot.
  - (d) Every existing disposal, seal, rebind, early-`EndSync` and takeover crash cut re-run under the phase model; the CO-034 and CO-035 public fixtures keep their asserted end states.
  - (e) Cross-version pair: baseline SDK completes an unexpanded full sync; this SDK expands by sync ID; grants equal the this-SDK reference; no `List*` call. Reverse pair: this SDK's default-mode sealed artifact expanded by the baseline SDK's syncer succeeds. Both opt-in, using the existing legacy-artifact tooling.
  - (f) Mutation adequacy: reintroduce a post-stamp ledger write and confirm (c) fails; skip the phase check in the terminal transition and confirm a page committed after `sealing` is refused.
  - (g) Commit-site registration: the stamp batch and the begin-pass batch each appear in `commitPointRegistry` with a hook route and in `seamFailureCases` with a failure test that observes the file after the injected failure (phase `sealing` retained; no partial begin-pass). The `RestoreLedgerArchive` and `ClearRows` entries are removed with their sites.
  - (h) Legacy finished file with no token and no archive rebinds into the requested pass.
  - (i) Retained-mode seal, then rebind: cumulative counters after begin-pass equal the sealed totals once, not twice; default-mode seal, then rebind: equal the archived totals.
- Risk routing: HIGH; silent + durable (a misclassified pass), version-pair dependence. Frozen by this entry; implementation obligations in implementation.md; instruments are the existing crash-cut harness, the raw-snapshot oracle and the legacy-artifact tooling.
- SQLite: no change. Every contract change is on `PageLedgerStore`/`PageWriter`, which SQLite must not implement (`setStore` refuses it); every engine change is under `pkg/dotc1z/engine/pebble`; every syncer change is in `ledger_*.go` or behind `s.ledgered`. The acceptance check is `git diff --stat` for this sequence showing no path under `pkg/dotc1z/*.go` other than `pebble_store.go`, and no hunk in `pkg/sync` outside `ledger_*.go` that is not inside an `s.ledgered` fork.
- PR placement: this PR, as its own commit sequence after CO-036.

### CO-039 — the pass is a state machine; writes are transitions

- Classification: lifecycle correction; completes CO-037.
- Source: requester's model of the sync as a state machine (start → collection complete → expansion, optionally skipped → seal), and a review finding at 10cb8c72: a process resuming a `sealing` declaration rewrites `c1z.report.latest_options` with its own configuration (`ledger_sync.go:35–39`) although it commits no terminal page and no work; `buildLedgerArchiveLocked` then reads `Requested.OnlyExpandGrants` from that fact to decide the archive's `preceding_collection` link, so a plain run resuming an expansion pass's seal drops the link.
- Motivation: CO-037 made the declaration the one authority for "is a pass open, collecting or sealing". Two questions still have no durable answer and are inferred: "is collection complete" (inferred from the queue's order, stamped into sync metadata as `supports_diff` at `parallel_syncer.go:384–400`) and "what kind of pass is this" (inferred from the options fact). And attempt-start writes are not tied to any transition, so an attempt that performs none still writes.
- Claim, stated on the file:
  1. States, durable, read from the declaration and the sync-run record:
     | state | declaration | `ended_at` | store accepts |
     |---|---|---|---|
     | Unstarted | absent | unset | `BeginCollecting`, `BeginCollectingFromToken` |
     | Collecting | `collecting` | any | page commits; `CompletePendingWork`; the transition to Expanding (`BeginExpanding`) or to Sealing (terminal page) |
     | Expanding | `expanding` | any | `CompletePendingWork` of the expansion entry; `PutCounterBucket`; the transition to Sealing (terminal page). No page commits: in ledger mode expansion writes grants through the store and its progress through the entitlement-graph store (`runPendingLocalStep`, `ledger_pending.go`), not through ledger pages |
     | Sealing | `sealing` | any | `PutCounterBucket`, `PutLedgerFacts`, `EndSyncWithStats` |
     | Sealed | absent | set | `BeginPass`; `EndSyncWithStats` (engine-level reseal) |
     Attempt accounting — `PutCounterBucket`, `PutLedgerFacts`, `FoldLedgerCounters` — is accepted in every state; it records the attempt, not the pass.
     Collecting and Expanding with an empty queue are "drained"; the syncer reads the queue to tell, the store does not need to.
  2. Transitions, each one synced batch, each validated under `writeMu` before staging:
     - Unstarted → Collecting: seed or takeover (as today).
     - Collecting → Expanding: `BeginExpanding(ctx)`, a synced lifecycle write by the coordinator when the expansion action is first picked up (the point that stamps `supports_diff`, `parallel_syncer.go:384–400`); guard under `writeMu`: phase `collecting` and the pending range holds only the expansion action's entry.
     - Collecting → Sealing and Expanding → Sealing: the terminal page carries `SetTerminal`; guard as CO-037.
     - Sealing → Sealed: the stamp batch (CO-037).
     - Sealed → Collecting: `BeginPass`, which also stages the presence fact `c1z.pass.follow_on`.
     Expansion is skipped by never entering Expanding: when `dontExpandGrants` or no grant needs expansion, the expansion action completes through `CompletePendingWork` in Collecting and the terminal page follows.
  3. A page commit in Expanding or Sealing is refused (`ErrLedgerQueuePhase`, carrying the phase). Collecting accepts any page. The rule is on the phase, not the op: expansion commits no pages, so an op-level rule would have nothing to distinguish.
  4. The archive's `preceding_collection` link is decided by `c1z.pass.follow_on`, not by the options fact. A first pass never carries the fact; every `BeginPass` pass does.
  5. An attempt writes `c1z.report.*_options` only for a pass it runs: it enters at `absent` (and seeds or begins the pass), `collecting` or `expanding`. An attempt entering at `sealing` writes its run bucket and nothing else. One stated exception: the CO-035 handoff, which seals a drained prior pass (committing its terminal page with this attempt's disposal facts) and then begins the requested pass, records options once, for the requested pass; the prior pass keeps the options of the attempt that ran it. Otherwise the rule is general: no transition, no write to the fact family. Attempt accounting (`PutCounterBucket`, `PutLedgerFacts`) is phase-independent by design and the table in §1 does not list it per state.
  6. Resume selection (CO-037 §3) gains one row: `expanding` continues; the queue holds the expansion entry, which the handler resumes from the entitlement-graph store as today. A resumer that finds `expanding` with an empty queue is drained and commits the terminal page.
  7. A resumer's expansion flags must be consistent with the pass's state; a conflict is refused before any write, with the state and sync ID in the error, and the pass is untouched (requester's ruling; both cells differ from `main`, which continues collecting in the first and seals a partial expansion in the second). `onlyExpandGrants` conflicts with Collecting when a collection entry is queued (any op other than the `Init` seed, the expansion step or the external import — C1 expands through `sdk.NewEmptyConnector`, so running connector-calling entries would seal a truncated sync as complete; expansion and the external import call no connector), with Collecting on an unfinished sync whose queue is the `Init` seed alone (nothing collected), and with Unstarted on a resumed sync (a caller-supplied sync ID, or the store's latest unfinished sync) with no legacy token. It is consistent with Collecting whose queue holds only the expansion step and the external import, or is drained (a completed collection: continue or seal, then expand — CO-035's handoff), with Collecting on a finished sync whose queue is the `Init` seed (a finished baseline upload after token takeover: its empty stack seeds `Init`, which plans the expansion-only pass — `TestLedgerFinishedLegacyFrontierKeepsPendingWork/empty`), with Expanding (continue), Sealing (finish, then the expansion pass) and Sealed (begin the expansion pass). `dontExpandGrants` conflicts with Expanding; in Collecting it decides at the expansion step's pickup, as on `main`. A legacy token is taken over as Collecting whatever its stack, including a stack that is exactly the expansion step: baseline tokens cannot say whether expansion ran (they clear its cursor and keep no graph), and the baseline resumer treats that stack as a step still to take, skipping it under `dontExpandGrants` and sealing. Taking it over as Expanding refused that resume, which a self-hosted connector with `dontExpandGrants` in its fixed configuration could never satisfy; `main`'s reading is kept. A resumer with neither expansion flag is bound by the collection rules below.
  8. Flags belong to phases. The collection flags (`skipEntitlementsAndGrants`, `skipGrants`, targets, resource-type selection, sync type, external source, traits and filter) are the pass's from the commit that planned under them, the `Init` page or the batch that seeds a legacy stack carrying collection work (CO-042): a resumer whose collection flags differ from the recorded options (`first_report_options`) while collection work is queued is refused before any write, naming the differing fields; a resumer that repeats them continues; an attempt that finds no recorded options continues. Collection happens once per sync ID: a finished sync with no open pass, a finished sync whose open pass is the `Init` seed alone, and a finished legacy token with an empty stack or `Init` alone accept `onlyExpandGrants` and refuse every other request (`TestLedgerFinishedSyncRefusesCollection`, `TestLedgerFinishedLegacyFrontierKeepsPendingWork/empty`). An expansion invocation's collection flags are not read: `initialActions` plans an expansion pass from the collection's facts, records none of the invocation's collection flags, and ignores its targets, because C1 runs expansion with no knowledge of how the file was collected (`TestLedgerExpansionOnlyIgnoresCollectionFlags`). Expansion flags are decided by whoever reaches expansion (`dontExpandGrants` at the pickup, as above) and locked once Expanding. An expansion pass queues only the expansion step and the external import, neither of which counts as collection, so a crashed expansion pass is finished by any expansion resumer and is never compared against the collection's recorded options that `BeginPass` carried over (`TestLedgerExpansionPassWithExternalImportResumes`). Both cells differ from `main`, which resumes under whatever flags the resumer passes and re-collects a finished sync under them, inheriting the earlier pass's `should_skip_*` facts into the new plan.
- Contract delta: the retained stamp batch also deletes scheduling relations and the frontier (the frontier holds a legacy token verbatim; only rows, facts and counters are retained history). `LedgerQueuePhase` gains `LedgerQueueExpanding`; `PageLedgerStore` gains `BeginExpanding(ctx) error`, a synced lifecycle write under the write lock with the guard in §2, registered as a commit site with a failure test; `BeginPass` stages `c1z.pass.follow_on`; `ErrLedgerQueuePhase` replaces `ErrLedgerQueueSealing` as the refusal for a page in a phase that accepts none. Nothing else on the interfaces changes. The archive placement, the stamp batch and `BeginPass`'s other duties are as CO-037.
- Owning boundary: Pebble page commit and pending-work state; syncer attempt start and the expansion action's pickup in `parallelSync`.
- Affected criteria: C11, C16, C31, C33, C50.
- Supersedes: CO-038 §1's "an attempt that commits no page still records its options" (now: an attempt that performs no transition records nothing) and the CO-038 implementation note claiming the sealing attempt's retention flag decides disposal (false: the terminal page's facts decide; the sealing attempt writes none). CO-035's "no file-level expanded-status flag" stands: `expanding` says what the store accepts now; `c1z.pass.follow_on` says the pass began on a sealed sync; neither says the grants are expanded.
- Verification delta, tests before code, red at 10cb8c72:
  - (a) Engine state × event table: for each state in §1 and each event (seed, takeover, page, `BeginExpanding`, `CompletePendingWork`, terminal page, stamp, `BeginPass`, `PutCounterBucket`, `PutLedgerFacts`), the expected next state or refusal, asserted on the declaration and `ended_at` after the call. One table test; the freeze's §1–§3 are its expected column.
  - (b) Syncer no-transition-no-write: every existing crash image × a resumer whose configuration differs from the original on debug, expansion-only, skip-grants, skip-entitlements and worker count. If the resumer committed no page and no terminal page, the fact family is byte-identical before and after; its own run bucket is the only allowed delta. Red on the `sealing` image via `latest_options`.
  - (c) Archive link: an expansion pass sealed by a plain resumer keeps `preceding_collection`; a first pass never has it.
  - (d) Skip path: `WithDontExpandGrants` seals from Collecting without the declaration ever reading `expanding`; a kill between the expansion action's local completion and the terminal page resumes in Collecting and seals.
  - (e) Expanding resume: kill after `BeginExpanding` is durable; reopen; the resumer's classification is continue; a page commit is refused if attempted (planted); expanded grants equal the uninterrupted reference.
  - (f) Mutation adequacy: remove the phase check on page commit and (a)'s refusal rows fail; write options on a sealing resume and (b) fails.
  - (g) Existing CO-034/CO-035/CO-037 fixtures keep their end states.
  - (h) Flag policy: kill mid-collection (entries beyond the seed remain), resume with `onlyExpandGrants` and a connector that refuses every list call: refused, no connector call, fact family and declaration unchanged, a plain resume then completes. Kill mid-expansion (after `BeginExpanding`), resume with `dontExpandGrants`: refused, same invariants, a plain resume completes expansion and the expanded grants equal the uninterrupted reference. Finished baseline-SDK upload (legacy token, empty stack) resumed with `onlyExpandGrants` and the refusing connector: taken over, `Init` plans expansion only, sealed, no list call — the C1 path. Drained Collecting (crash before the terminal page) resumed with `onlyExpandGrants`: sealed, then the expansion pass. The two refusals are red at 10cb8c72 (the first continues collecting; the second seals partially expanded); the other two rows are green at 10cb8c72 and must stay so.
- Risk routing: HIGH; silent + durable (a misattributed pass, a page in the wrong phase). Draft: frozen after a facts read by a reader other than the author; the resumer-flag policy is ruled (§7). Obligations in implementation.md.
- SQLite: no change; same acceptance check as CO-037. `MarkSyncSupportsDiff` keeps firing where it does; the phase is the ledger's own record of the same event.
- PR placement: this PR, after CO-037's sequence.

### CO-038 — attempt-scoped writes belong to the coordinator

- Classification: lifecycle correction, small.
- Source: the CXE-1358 synchronization review; requester asks for zero new primitives.
- Motivation: two once-per-attempt writes were placed on the page path and given locks so concurrent first pages could race for them. Neither has a second owner.
- Claim, stated on the file:
  1. The attempt's option snapshot (`c1z.report.latest_options`, and `c1z.report.first_options` when absent) is written once per attempt, before the attempt's first page, by the coordinator. An attempt that commits no page still records its options. `first_options` is preserved across finished same-ID passes as today. (CO-042 moves `first_options` to the `Init` page; the rest stands.)
  2. The prior ingest-invariant verification marker is cleared by the coordinator before the first page or batch of an attempt that has work to run; an attempt with no pending work performs no such write. A clear failure prevents every handler from running.
  3. No page stages either write; no page-path synchronization exists for them.
  4. The snapshot's effective flags reflect the request that is about to run: `EffectiveSkipGrants` is true when the option is set or the fact exists, likewise for entitlements-and-grants; `EffectiveLedgerDebug` reflects retention after the durable retain fact has been honored.
- Contract delta: one method on `PageLedgerStore`, `PutLedgerFacts(ctx, map[string]string)`, a synced blind write of named fact values outside any page under the write lock (the `PutCounterBucket` shape). First and latest snapshots are one batch. It is a new commit site and registers in `commitPointRegistry` with a failure test asserting neither fact lands.
- Owning boundary: syncer attempt start (`prepareLedgerState` / `syncLedger`).
- Affected criteria: C31, C33, C50.
- Supersedes: CO-030's "publish a runtime flag only after the page with latest options commits" — the flag and the page callback are removed; the snapshot is a lifecycle write with no page to fail.
- Verification delta: attempt commits no page → `latest_options` names that attempt; fresh sync with `WithSkipGrants` → snapshot has `EffectiveSkipGrants` true before any page; finished rebind → `first_options` unchanged, `latest_options` new; empty recovery (`current() == nil`) → no verification-clear write, checked by the write hook; injected clear failure → no connector call; injected `PutLedgerFacts` failure → attempt fails with no fact written. `TestSyncPrimitivesRegistered` loses its five `remove:` entries in the same change.
- Risk routing: bounded; existing options and verification tests plus the write-hook audit.
- PR placement: this PR, before the CO-037 sequence.

### CO-040 — a store without an engine takes the token path

- Classification: attachment correction, small. Supersedes CO-008's refusal arm.
- Source: requester, on the 75b38cd6 review of `setStore`.
- Motivation: `connectorstore.StoreMetadata` documents `Engine == ""` as the value for a store not backed by a c1z (mocks, in-memory wrappers, gRPC clients) and tells consumers not to switch behavior on unknown values. CO-008 refused both. `main` accepted any `c1zstore.Store` on the token path, so the refusal broke every store double outside this repo on upgrade with a runtime error and no migration path; six doubles in this repo had to grow `Metadata()` to keep passing.
- Claim, stated on the file:
  1. Pebble requires `PageLedgerStore` and is ledgered.
  2. Every other engine, including `""` and unknown values, checkpoints through the token path and must not implement `PageLedgerStore`; a ledger on a non-Pebble store is a misreported Pebble store and is refused at attach.
- Contract delta: none.
- Owning boundary: `setStore`.
- Affected criteria: C01; the E axis keeps its eight cells with the expected outcome of claim 2 for the empty and unknown engines.
- Verification delta: `TestLedgerPublicEngineAttachment` and `TestLedgerPublicRegisteredPathAttachment` flip the `""` and `other` without-ledger cells to attach; `TestNewSyncerStoreEngineRouting` attaches a nil-embedded double with the attach-time methods answered, the shape connector repos use.
- Risk routing: bounded; the token path on such a store is `main`'s behavior.
- PR placement: this PR.

### CO-041 — the layout stamp rides the batch it describes

- Classification: durability correction, small.
- Source: requester, on the b6834d01 review of the seal.
- Motivation: the in-flight stamp was its own synced write on both sides: set before a ledger batch, cleared after the purge and before the seal batch that writes `ended_at`. A crash between the clear and the seal left a v2 stamp over an unfinished sync with no token. A token-only SDK opened that file, seeded `Init` from the empty token, and collected again on top of the sealed records; this SDK then refused the file as a legacy checkpoint beside pending work. The arm side had the inverse image, a v3 stamp over no rows, which a token-only SDK refused although nothing ledgered existed.
- Claim, stated on the file: no durable image holds the in-flight stamp without a ledger row or pending-work key, and none holds a v2 stamp over an unfinished ledgered sync. The stamp is staged in the batch that writes the first ledger key (`Ledger.stageMarkInFlight`), and in the batch that removes the family or writes `ended_at` (`Ledger.stageClearInFlight`, from `Drop` and the seal).
- Contract delta: none. `Ledger.active` still reads rows, not the stamp alone: files from a build that cleared early can hold rows under a v2 stamp.
- Owning boundary: `pageUnit.Commit`, `Ledger.takeover`, `BeginCollecting`, `BeginExpanding`, `BeginPass`, `PutFacts`, `PutCounterBucket`, `Ledger.Drop`, `endSyncFinalize`.
- Verification delta: `TestLedgerDiscardDurableSealCuts` asserts the stamp at every seal image (v3 on `after-delete`, `after-purge`, `before-ended`; v2 on `ended`); the `before-ended` cell failed before the change. `TestLedgerTakeoverCrashImages` `mid` cell: a token-only SDK opens the image and the stamp is v2. `TestDropLedgerCommitFailureKeepsRowsAndStamp` covers the Drop batch's failure route.
- Risk routing: bounded; one write moved into an existing batch at each site.
- PR placement: this PR.

### CO-042 — the locked collection flags are the Init page's

- Classification: lifecycle correction, small. Supersedes the `first_options` half of CO-038 §1 and §3.
- Source: review at b6834d01 (`ledger_sync.go:39`).
- Motivation: `first_options` was the attempt snapshot's first write, before `parallelSync`, and `flagConflict` compared against it only once collection work was queued. An attempt that died between that write and the `Init` page commit left a record with no plan behind it; the next attempt, finding only the `Init` seed, was not compared, planned under its own flags, and left the first attempt's flags as the record. A third attempt was then held to flags the queue was never planned under.
- Claim, stated on the file: `first_options` exists iff a plan has committed, and names the flags it was planned under. It is staged in the commit that writes the plan: the `Init` page (`recordFirstReportOptions`), or, when a legacy stack already carries collection work (`legacyStackHasCollection`; a stack of the `Init` seed, the expansion step or the external import alone records nothing, and `Init` records when it runs), the batch that seeds it: the takeover (`loadLedgerResume`) or the frontier-only reseed (`prepareLedgerState`'s `ledgerSeedPending` arm, which records only when the file has no `first_options`). The attempt snapshot (`latest_options`) stays a coordinator write before any page. An expansion pass over a finished collection finds the collection's `first_options` carried by `BeginPass` and keeps it.
- Contract delta: `BeginFromToken` and `BeginCollecting` take `facts map[string]string` (a `""` value is a bare fact) so the seeding batch can carry the record. CO-038's concern was two first pages racing for an attempt-scoped write; `Init` is the single seed and runs alone, and the takeover and the reseed are each one batch, so each record has one owner.
- Owning boundary: `initializeAction`, `skipLedgerSync`'s `Init` page, `loadLedgerResume`, `prepareLedgerState`'s seed arm.
- Verification delta: `TestLedgerCollectionFlagsAreThePlanningAttempts`: attempt A dies at the `Init` commit (`latest_options` present, `first_options` absent); attempt B plans with `skipGrants` and dies on a resource page (`first_options` names B); a resumer with A's flags is refused naming `skip_grants`; a resumer with B's finishes without grants. Red before the change at the first assertion. `TestLedgerOptionsSnapshotBeforeAnyPage` and `TestLedgerReportOptionsPrecedePages` assert `first_options` absent before `Init`. `TestLedgerLegacyTakeoverArmsCollectionFlagLock` holds unchanged: the takeover's flags lock the next resume. `TestLedgerFrontierSeedArmsCollectionFlagLock`: a frontier with a grants stack and no declaration is seeded by an attempt with `skipGrants`; `first_options` names that attempt, and a resumer without `skipGrants` is refused. Red before the seed carried the record (review at 45ea871b, H1).
- Risk routing: bounded; one fact moved from a lifecycle write into the three planning commits.
- PR placement: this PR.
