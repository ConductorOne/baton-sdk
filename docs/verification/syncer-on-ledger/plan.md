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
| C33 | S5 | A finished WithSyncID rebind retains input data but does new requested work; old rows, facts, counters, frontier and retain declaration cannot suppress or pollute it. | O1,O2,O5; P8 sealed × retain on/off |
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

## 11. Change-order log

No change orders at freeze. Calibration entries will be appended as CO-001
onward, without revising §§0–10. Each records source, classification
(correction/clarification/extension), observable missing claim, motivation,
contract delta, owning boundary, affected criteria, verification delta, risk
routing and PR placement. Candidate tests supplied for an existing cell are
listed as candidates, not reported as measured evidence or missing claims.
