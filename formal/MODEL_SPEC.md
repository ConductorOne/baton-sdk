# Model spec — walker + source-cache replay calibration model (P)

Status: v11, FROZEN. The v10 addendum (overlay-flavor extension of
the atomic-unit variant: §9.6 V-OVERLAY-UNIT, cells 6-overlay and
6-overlay-naive, §5 buffer row) received the targeted round-7 spot
review: REJECT with 3 majors + 2 minors + 2 notes, ALL
fix-without-re-review, applied as v11
(`formal/reviews/model-spec-round7-overlay.md`); no cell verdict
overturned, no round 8 warranted. v11 also bundles the deliverable-2
de-scope edits (the `compositionEnum` and `annotationBinding` toggles
stay documented but are NOT BUILT in P — the atomic unit is the
single fix story for the scoping bug family; §10.5 kill obligations
adjusted) and the case-3 re-run extension of V-ATOMIC (§9.6). All
prior content is reviewed through round 6.
The v8 addendum (§9 scenario 7 — sessions × replay elision, P6-R,
session-taint toggles) received the targeted round-6 spot review:
REJECT with 4 majors + 5 minors + 3 notes, ALL fix-without-re-review,
applied as v9 (`formal/reviews/model-spec-round6-scenario7.md`); all
cell verdicts and shipped-system claims verified, no round 7
warranted. The v6 addenda (§9 cells 1d and 6, §7 fold-totality
clauses) passed the round-5 spot review similarly (2 majors + 6
minors → v7, `formal/reviews/model-spec-round5-addenda.md`). Review history: round 1 REJECT (15
findings → `formal/reviews/model-spec-round1.md`); round 2 of v2 REJECT
(2 blockers, both scenario-reachability defects → §9 re-scripted on the
graceful-stop premise generator, standing reachability-walk obligation
in §10.0 — `formal/reviews/model-spec-round2.md`); round 3 of v3 REJECT
(1 blocker: scenario 5b scripted a loud failure where the B1 contract
and code silently ignore annotations; all 18 §9 cells walked, 17
confirmed — `formal/reviews/model-spec-round3.md`); round 4 of v4
REJECT with 0 blockers / 0 majors — all seven round-3 dispositions
verified genuine, three fix-without-re-review minors applied as v5,
and per the round-4 verdict no further full round is warranted
(`formal/reviews/model-spec-round4.md`). On signoff this spec freezes
and is committed as the diffable baseline; deviations discovered while
writing P code are appended as change orders (MS-CO-NNN), never
silently absorbed. No green checker run is trusted before the freeze.

Covers deliverables 2 and 3 of `docs/tasks/sync-formal-model-brief.md`:
the tiered walker + 6b source-cache replay orchestration, its calibration
cases, and the staged composition-enum mitigation. The graph-runtime
model (deliverable 4) gets its own spec addendum, with its own
adversarial review; the machine interfaces below are designed so the
graph scheduler variant slots in without remodeling
Upstream/Store/SessionStore.

Vocabulary is `formal/GLOSSARY.md`. Behavioral ground truth is
`docs/verification/sync-replay-6b/plan.md` (frozen contract B1–B10,
CO-6b-001..007); code anchors are cited where the contract is silent.

## 1. What is modeled, what is abstracted

In the model:

- The dispatch loop: LIFO action stack, batched same-op dispatch to
  bounded workers, loop-top checkpointing, restart-from-root under hard
  crash (CO-6b-002), graceful-stop forced checkpoints
  (`checkpointOnStop`, run-expiry — `parallel_syncer.go` 195, 204,
  453–493; pre-seal forced site `syncer.go` 1162, Init sites 232/268).
- Source-cache orchestration: warm/cold install, hit recording, the warm
  gate, hit-validator binding, once-per-scope replay dedup, per-scope
  locks, page-op ordering (copy → upserts → tombstones → publish),
  poison, replay-blocked marking at its real durability.
- Multi-sync chains (up to 3 syncs): a sealed artifact becomes the next
  sync's previous artifact; gates G4/G7 modeled; G6 (capability) is a
  PER-ATTEMPT bit (CO-6b-003's withdrawal-across-attempts case needs
  it); G1/G2/G5 collapsed to one "previous artifact usable" boolean.
- Per-attempt compat config k ∈ {K1, K2} (what G7 byte-compares and B4
  cross-attempt-compares); rows carry a ghost config tag (§7 P1c).
- Two interruption modes: hard crash (volatile state lost, last durable
  checkpoint wins) and graceful stop (batch aborts, forced checkpoint of
  live state — including mid-chain cursors, admitted-but-undrained
  spawns, and hits recorded during the aborted batch — attempt ends).
- Upstream mutation between attempts (always available) and mid-attempt
  (per scenario); previous-artifact swap between attempts (case 3).
- The session store as shipped (variant A: free-form sync-scoped KV,
  durable across attempts). Modeled surface: Get/Set only; the shipped
  service also has Delete/DeleteMany/Clear — mutating ops the taint
  toggles must cover in production (§6) but the model cannot reach.

Abstracted away (soundness arguments in §8):

- Storage internals: Pebble, batching, index synthesis, bytes. The store
  is "partitions with atomic page commits and a manifest of scope →
  validator entries". Batched clear/copy is abstracted to two atomic
  steps; sub-batch partial-commit crash convergence was closed at the
  store level in Phase 6a and is registered here as an exclusion.
- Row kinds: one STORAGE row kind (every calibration mechanism is
  scope-level). Scenario 7 introduces a declared KIND axis on top: a
  kind is an (action op, scope) pair — W and R are distinct ops over
  distinct scopes, which is what gives sequential phases under §3's
  same-op batching; the storage row-kind axis is unchanged (the
  glossary's scope stays (row_kind, scope_key)), and per-kind
  produce/consume state (session taint, §6) is scoped by this axis.
  Scenario 7 is the only two-kind scenario.
- The G1/G2/G5 decision table (R2 chaos coverage, not scheduling
  semantics).
- Grant expansion, external resources, targeted sync, static
  entitlements, lookup ask/answer continuation, subprocess transports,
  the compactor.
- Produce-side block triggers beyond §3's two: page-arrival shape
  guards (child-resource declarations, `InsertResourceGrants`),
  ingest-filter drops (B6), unknown-prior-checkpoint conservatism —
  unreachable in the model (no child resources, no filter drops, every
  modeled token carries the ingest-quality snapshot).
- Connector cross-call memory: scenario policies are pure functions
  (§3 MWorker); the proto permits within-sync connector memory, and this
  under-approximation is a recorded boundary (§8), not a claim about
  connectors.
- Real pagination breadth: at most 2 pages per scope per round.

Small-scope configuration (every known bug fits; the inductive bet is
stated in the brief's Honest limits):

| dimension | bound |
|---|---|
| scopes | 1–2 |
| upstream epochs per scope | ≤ 3 |
| row identities per scope | ≤ 2 |
| syncs per scenario | ≤ 3 |
| attempts per sync | ≤ 3 (crash budget ≤ 2, stop budget ≤ 1) |
| workers | 2 |
| session keys | ≤ 1 |
| pages per scope per round | ≤ 2 |
| compat configs | ≤ 2 |
| kinds (action ops) per scenario | 1 (2 in scenario 7 only) |
| batch cap | nondet ∈ {1..2} per dispatch |

Batch-cap scaling argument: reality's cap is 100
(`maxPeekActionsCount`). The model's per-dispatch nondet cap {1..2}
explores the split shapes relevant at the model's action counts;
partitions into ≥ 3 batches are unexplored per split, but batch width
beyond the cap is independently reachable because same-op spawns are
admitted to the live batch uncapped, and multi-way splits decompose into
successive two-way splits at loop tops. CO-6b-006's 102-action two-batch
construction shows cap-induced splits are a real premise generator; a
model trace corresponds to a real scenario by inflating action counts,
never by changing the mechanism.

## 2. Ground rules the encoding must obey

Properties of the ENCODING (not the system) that the adversarial review
checks:

1. **Genuine choice points.** Worker interleaving at atomic-step
   granularity, crash/stop placement, checkpoint-or-skip at loop tops,
   upstream mutation timing, batch cap per dispatch, and store-op
   arrival order across senders are all resolved by checker-explored
   nondeterminism — never fixed by encoding order.
2. **Crash wipes volatile state and only volatile state.** §5's table is
   mechanically enforced: attempt-owned machines die at crash; the Store
   survives with exactly its committed state; resume constructs a fresh
   MSyncAttempt from the checkpoint token alone.
3. **Mitigations are guards, not behavior.** Every §6 toggle disables a
   check/lock that exists in the shipped design; toggles never add
   transitions.
4. **Monitors observe, never steer.** Spec machines subscribe to
   announce events; ghost state only. Ghost labels attached to events
   (§7) must be honest labels of decisions the model already made for
   behavioral reasons, never scenario foreknowledge.
5. **One semantics, one place.** Connector policy, page-op ordering, and
   gate logic are each encoded once and parameterized per scenario.
6. **Premises must be walked, not asserted.** Every §9 scenario premise
   must be mechanically reachable under §3's semantics; the reachability
   walk is part of the spec (§9 states each route) and of the review
   charge (§10.0).

## 3. Machines

### MEnv (test driver, per scenario)

Owns the scenario configuration (toggles, policy table, crash/stop
budgets, chain length, swap schedule, per-attempt capability bit and
compat config schedule). Runs the sync chain:

- start attempt → await one of {seal, crash quiesce, stop quiesce,
  attempt failure};
- on crash/stop quiesce: start the resume attempt;
- on ATTEMPT FAILURE (a loud cold verdict — §4): resume-on-failure up to
  the attempt budget; with `abandonLadder` on, after k identical
  failures abandon the sync and start the next sync cold (6c ladder);
  with it off, budget exhaustion with the P4 livelock rule (§7) firing
  is the recorded outcome;
- on seal: roll the artifact (it becomes the previous artifact) and
  start the next sync; perform the scheduled artifact swap, upstream
  mutation, capability withdrawal, or compat-config change between
  attempts when the scenario declares one.

### MUpstream

Per scope: epoch counter `ep[s]`, content function `rows(s, e)` (fixed
small table per scenario), validator `V(s, e)` truthful by construction
(`V(s,e1) == V(s,e2) ⟺ rows(s,e1) == rows(s,e2)`). Serves
`eValidate(s, v)` (match against current epoch) and `eFetch(s)` (rows,
validator, ghost epoch). `eMutate(s)` from MEnv; mid-attempt mutation
only when the scenario enables it.

### MSyncAttempt (walker scheduler, one per attempt)

Volatile state, initialized from the checkpoint token: action stack
(op, page token, scope binding, spawned flag), hit map (scope →
validator), replayed set, ingest-quality snapshot (incl. replay-blocked
reason flags), warm flag — NEVER from the token; recomputed per attempt
(B1, CO-6b-003; `sourceCacheWarm` is a syncer field,
`source_cache_orchestration.go` 363).

Warm install at attempt start (`installSourceCacheLookup`): usable-prev ∧
this attempt's G6 capability bit ∧ G4(prev not replay-blocked) ∧
G7(compat byte-match: this attempt's config vs prev artifact's compat
record) ∧ no drift this attempt → warm; else cold. Produce-side
blocking — exactly two triggers WITHIN THE MODELED FRAGMENT:

1. compat config recomputed differently across attempts of this sync
   (B4);
2. this attempt runs without source-cache handling (G6 bit off) over a
   store carrying prior-attempt produce state (CO-6b-003
   capability/shape withdrawal), fail-closed on compat-read error.

The system has further block triggers the model cannot reach and
excludes (§1): page-arrival shape guards (child-resource declarations,
`InsertResourceGrants` — `ingestQualityReasonSourceCacheShapeUnsupported`),
ingest-filter drops (B6), and unknown-prior-checkpoint conservatism on
restore. "Two triggers" is a claim about the modeled fragment, not the
walker.

Consume-side degradation alone (previous artifact withdrawn, blocked, or
mismatched) NEVER blocks the current artifact (R13: routine resumes must
not become chain breaks).

Dispatch loop (mirrors `parallelSync`):

1. Loop top: stack empty → seal sequence → report seal.
2. Checkpoint decision: nondet {checkpoint, skip}, with forced
   checkpoints after Init and before seal (`syncer.go` 1162), and the
   graceful-stop forced checkpoint below. `eCheckpoint` snapshots
   (stack, hit map, replayed set, ingest-quality) — §5.
3. Batch: take the consecutive same-op prefix of the stack top, bounded
   by this iteration's nondet cap; dispatch to free MWorkers; await
   batch end (all dispatched actions finished or batch aborted).
4. Batch bookkeeping (mirrors the queue contract, `parallel_syncer.go`
   608–716): same-op spawns are committed to LIVE scheduler state
   first, then admitted to the live batch queue and drained within the
   batch (`outstanding` gates batch end) — so a loop-top checkpoint can
   never contain an unfinished same-op spawn; cross-op spawns are
   pushed to the stack. Spawn admission is deduplicated by identity
   digest for ALL spawns, same-op included (`transitionAction`'s
   spawnedAdmitted guard — re-mentions of finished work are legal
   connector behavior and are skipped, not errors), plus a commit-local
   duplicate-cursor rejection within one transition (loud failure;
   SAME-OP children only — cross-op children bypass it in code); the
   dedup index is volatile and rebuilt from the checkpointed stack on
   resume. Completed actions pop AT COMPLETION — the pop is a
   live-state transition committed mid-batch, not batch-end
   bookkeeping (the 2-stop cell depends on a stop checkpoint capturing
   one action popped and its batchmate mid-chain). An aborted batch
   leaves its unfinished actions on the stack — a
   dispatched-but-unfinished action's stack entry retains the token of
   its LAST COMMITTED transition (mid-chain), because
   `transitionAction` writes page advances into live state as they
   commit; admitted-but-undrained spawns likewise remain in live state.

Graceful stop (`eStop`): the in-flight batch aborts (workers stop at the
next atomic-step boundary; the failing/stopped actions stay unfinished);
MSyncAttempt force-checkpoints the LIVE state — including hits recorded
during the aborted batch, admitted-but-undrained spawns, and mid-chain
cursors of unfinished actions — then the attempt ends
(`checkpointOnStop`, run-expiry force; spawn checkpointability is pinned
by `TestSpawnedActionsSurviveCheckpoint`). This is the one path by which
mid-batch state becomes durable; hard crash never checkpoints. The stop
path is therefore the model's premise generator for "stranded carrier +
recorded hit" states (§9).

### MWorker (×2)

Owns one action at a time and runs its ENTIRE page chain to completion
within the batch (mirrors `syncOneAction`): per page — connector call,
page ops, then either continue with the next page token (committing the
advance to live state via the transition, staying inside the same
worker) or finish (the pop commits at completion, mid-batch). A
loop-top checkpoint can
therefore only ever contain root tokens for never-dispatched actions;
mid-chain tokens reach durability only via the stop path.

Connector call (synchronous request/response): consult = `eLookup` on
the Store's previous-artifact surface, then on hit `eValidate` against
upstream. HIT RECORDING IS PINNED: the hit and its validator are
recorded into the volatile hit map at LOOKUP-HIT time, before and
regardless of the revalidation outcome
(`previousSyncSourceCacheLookup.LookupPreviousSourceCache` fires `onHit`
at lookup time, `source_cache_orchestration.go` 211–214); a later hit
for the same scope OVERWRITES (last-write-wins,
`state.RecordSourceCacheHit`).

Verdict per the scenario policy table — a pure function of (action,
page position, lookup result, upstream response, session reads). This
purity is a modeling simplification, not a connector contract (§8):

- unchanged → replay page(s); the annotation carries a validator or not
  per scenario cell (both shapes are legal per proto);
- changed-with-diff → replay + overlay record pages;
- changed / miss → fetch → record pages under the new validator;
- planning shape: consult k scopes, spawn same-op carriers via
  `EnqueuePageTokens` with (scope, verdict) baked into their page
  tokens — the token is the verdict channel across attempts; policies
  may also consult on later pages of the same chain.

Session ops (`eSessionGet/Set`) happen inside page handling when the
policy says so.

### MStore (durable; one per scenario, holds the whole chain)

Durable state per sync: partitions (scope → set of (row id, ghost
provenance)), manifest (scope → validator [+ sealed count]), poison set,
replay-blocked flag (via checkpointed ingest quality — see §5), compat
record, session KV, checkpoint token, sealed flag. Plus the
previous-artifact binding, rebindable by MEnv between attempts (case 3).

Atomic ops (one event each, processed to completion; announce on
commit): `eCheckpoint(token)`, `eLookup(s)` (miss when poisoned/absent,
or when the scope's kind is session-tainted in the previous artifact's
produce state — degradation, never a loud verdict; §6),
`eGateRead`, `eClearScope(s)` (acting-scope semantics: never poisons the
scope being replayed), `eCopyScope(s)` (preflight: entry present ∧ not
poisoned; count consistency is a 6a store guarantee out of scope; ghost
provenance: replay lineage fields (V_base, epoch_base, config_base) are
ADDED to copied rows, and embedded session stamps, when present, are
copied UNCHANGED — the single semantics P6-R's stamp travel refers to),
`eUpsertPage(s, rows)` (a put whose row identity is stamped with a
different scope poisons the losing scope; out-of-scope delete/overwrite
poisons likewise), `eTombstones(s, ids)`, `ePublishEntry(s, v)`,
`eSessionGet/Set`, `eSeal`. SCENARIO-LOCAL ops (§9.6 design variants
only; registered here per MS-CO-001 so §5's crash protocol and §2.1's
arrival-order choice points quantify over the full op vocabulary):
`eReplayUnit(s, v)` (V-ATOMIC's one-op {clear, copy, marker, publish}),
`eOverlayUnit(s, v, pages)` (V-OVERLAY-UNIT's one-op unit), the
marker ops `eMarkerPut(s)` / marker read (the per-scope marker row all
§9.6 variants share), and `eGroundScope(s, boundV)` (MS-CO-003
record-round grounding: one atomic check-and-clear — clears the
scope's partition when this sync's manifest has no entry for it, or,
under the `groundValidatorBound` candidate (boundV ≥ 0), when the
entry's validator differs from the record round's incoming validator;
mirrors the shipped `groundRecordScope`, whose lookup and clear run
under the scope lock inside the destination batch).

Scheduler-state mutations (hit recording, replayed marking, warm flag,
stack transitions, ingest-quality/replay-blocked flags) are NOT store
ops; they reach durability only inside `eCheckpoint` (CO-6b-006
semantics: state recorded in the batch that crashes is lost with it).

### MCrashInjector

Emits `eCrash` (hard) or `eStop` (graceful) between any two atomic
steps, within the scenario budgets. Crash delivery races in-flight store
ops per §5's prefix rule.

## 4. Page-op sequence (the replay/record hot path)

For one scoped page, each named check a separate atomic step.
(Scenario 6's design variants — §9.6 — REPLACE this sequence for
unit-mode scopes: the variant declarations there are the authority;
MS-CO-001.)

```
acquire scope lock (s)                 [toggle: scopeLocks]  — record pages too (CO-6b-006)
if replay page:
    warm-gate check                    [toggle: warmGate]
    hit check: s ∈ hit map             (B5 provenance; loud cold on absence)
    [toggle: annotationBinding] annotation validator, when present, == hit validator (case-3 fix, PROPOSED)
    if s ∉ replayed set:               [toggle: oncePerScope]
        base-binding check: current base manifest[s] == hit validator   [toggle: hitValidatorBinding]
        eClearScope(s)                 (atomic)
        eCopyScope(s)                  (atomic)
        mark replayed                  (atomic, volatile scheduler state)
eUpsertPage(s, page rows)              (atomic)
eTombstones(s, ids)                    (atomic, when present)
ePublishEntry(s, v)                    (when the page carries a validator)
release scope lock
```

Failure semantics:

- A COLD verdict (gate/provenance/binding failure) fails the ATTEMPT
  loudly; MEnv's resume-on-failure rule applies. Because the offending
  cursor is checkpointed, the failure recurs deterministically — the P4
  livelock shape — until `abandonLadder` (when on) abandons the sync.
- A WARM page failure (destination write error) is modeled ONLY in the
  lock scenario that needs it: the page fails after `beforeUpserts`
  acquired the lock; the WORKER retries the action in-attempt (mirrors
  `syncOneAction`'s retry loop), which re-enters the page sequence and
  re-acquires the scope lock. With the model's release-on-error edge
  removed (mutation check), the retry deadlocks — the CO-6b-007 hang.
- A replay-annotated page arriving in an attempt WITHOUT source-cache
  handling (G6 bit off) is SILENTLY IGNORED — no-op page ops, no
  failure (B1: absent capability means every source-cache annotation is
  ignored for the whole sync; `sourceCachePageOps` returns nil ops when
  `sourceCacheEnabled()` is false, `source_cache_orchestration.go`
  463–468). Produce-side blocking trigger 2 fires at ATTEMPT START
  (install time) over prior-attempt produce state, fail-closed only on
  compat-READ error — never at page arrival.

Deliverable 3 (staged composition enum) — INPUT ASSUMPTION to confirm
against the PR discussion, at its REAL durability: record pages carry a
wire intent {OVERLAY, REPLACES} (the word "intent" is reserved for this
wire enum; the P1 ghost label is called "verdict class" and exists
regardless of this toggle). Detection is SYNCER-SIDE: when an attempt
observes, in its own (volatile + checkpoint-restored) state, both a
completed replay copy and a REPLACES-intent record for one scope in one
sync, it sets the replay-blocked reason flag — which reaches durability
only at the next checkpoint/seal, like the rest of ingest quality. The
model must therefore exhibit BOTH weaknesses as findings, not
surprises: (a) crash between detection and checkpoint loses the block;
(b) the carrier-less phantom (§9 case 1c) leaves no syncer-visible
evidence a copy ran, so detection never fires. The deliverable-3 claim
("propagation bounded to the one corrupted artifact") is verified ONLY
for the shapes where detection is reachable, and the calibration report
must state that boundary; an op-commit-durable store-side variant may be
modeled ADDITIONALLY for comparison, clearly labeled as stronger than
the proposal.

## 5. Durability, crash, and resume semantics

| state | owner | durability |
|---|---|---|
| partitions, manifest, poison, compat, session KV, sealed flags | MStore | durable at op commit |
| checkpoint token: action stack (incl. admitted spawns and, on the stop path, mid-chain cursors), hit map, replayed set, ingest-quality snapshot (incl. replay-blocked reason flags and session-taint marks) | MStore | durable at eCheckpoint commit |
| live stack, hit map, replayed set, ingest-quality/replay-blocked flags, session-taint marks | MSyncAttempt | volatile (checkpoint-cadence durability) |
| warm flag | MSyncAttempt | volatile AND never checkpointed (recomputed per attempt) |
| scope locks, in-flight batch, worker page position | MSyncAttempt/MWorker | volatile |
| spawn-dedup index | MSyncAttempt | volatile; rebuilt from the checkpointed stack on resume |
| overlay collect buffer (V-OVERLAY-UNIT, §9.6 only) | MWorker | volatile; never checkpointed — harmless to lose: page transitions defer to unit commit (§9.6 o-i, round-7 F1), so the stop checkpoint holds the consult-page token and a marker-less resume re-enters at consult |
| replay/overlay unit marker (§9.6 variants only) | MStore | durable at op commit (per-scope store row; rides INSIDE the unit for V-ATOMIC/V-OVERLAY-UNIT, trails as its own op for V-NAIVE/V-OVERLAY-LAST — the placement IS the bake-off variable); MS-CO-001 |
| upstream epochs | MUpstream | environment (unaffected by crash) |

Crash protocol (pinned): `eCrash` is enqueued to MStore like any op; its
POSITION in MStore's queue partitions the dead attempt's outstanding
ops — ops the store processed before the crash event are committed; ops
behind it are DROPPED, never processed. Per-sender FIFO delivery holds,
so each worker's committed ops are a prefix of its issue order (the WAL
property); interleaving ACROSS senders remains a genuine choice point.
MEnv starts the resume only after quiesce: every attempt-owned machine
halted and the store's queue contains no dead-attempt ops (dropped, not
processed). No dead attempt's op can commit after the resume attempt
starts.

Resume: fresh MSyncAttempt from the MOST RECENT durable checkpoint,
alone. In crash-only histories that checkpoint is a loop-top/forced
one containing only root tokens (§3 MWorker) — restart-from-root
(CO-6b-002) is the structural consequence for those histories.
Restart-from-root is a property of WHICH checkpoint survives, not of
the crash itself: a stop-forced checkpoint that survives a LATER hard
crash legally carries mid-chain cursors and admitted spawns, and
resume restores them as-is. Under graceful stop the forced checkpoint
may contain mid-chain cursors and admitted-but-undrained spawns;
whether a
replay annotation can be resumed mid-chain WITHOUT a fresh consult is
deliberately left reachable and checked as conformance question C1 (§9)
with a concrete probe script — CO-6b-002 calls that shape "unreachable
in practice", and the model either confirms that or produces the trace
that refutes it. Either outcome is recorded.

## 6. Mitigation toggles

OFF never adds behavior; it removes a check or a lock.

| toggle | guard | contract | default (shipped) |
|---|---|---|---|
| `warmGate` | replay requires this attempt's warm flag | CO-6b-003 | on |
| `hitValidatorBinding` | copy requires base manifest[s] == recorded hit validator | CO-6b-004 | on |
| `scopeLocks` | per-scope mutex incl. record pages, held across the page, released on every path | CO-6b-003/005/006/007 | on |
| `oncePerScope` | replayed-set dedup of the replacement copy | B5 | on |
| `annotationBinding` | replay annotation's validator (when present) must equal the recorded hit | case-3 fix, PROPOSED — DE-SCOPED v11: not built in P, no kill obligation; V-ATOMIC subsumes it (§9.6 case-3 re-run) | off |
| `compositionEnum` | syncer-side REPLACES+replay detection → replay-blocked flag (checkpoint-durable) | deliverable 3, PROPOSED — DE-SCOPED v11: not built in P, no kill obligation; the atomic unit is the single fix story for the scoping family | off |
| `abandonLadder` | after k identical resume failures, abandon and start the next sync cold | 6c ladder | off |
| `sessionTaintWrites` | produce-side: a connector session WRITE during a replay-capable kind's phase marks that kind non-replayable in this artifact's produce state (checkpoint-durable, ingest-quality-style) | sessions×replay fix, PROPOSED (partial) | off |
| `sessionTaintAll` | produce-side: ANY connector session traffic (read or write) during a replay-capable kind's phase marks that kind non-replayable (checkpoint-durable, ingest-quality-style) | sessions×replay fix, PROPOSED (isolation) | off |
| `recordGrounding` | record round's first write to a scope this attempt clears the partition WHEN this sync's manifest has no entry for the scope (one atomic check-and-clear; the published-entry skip is part of the shipped rule) | the SHIPPED tc1c fix (`groundRecordScope` + `ClearSourceCacheScope`); MS-CO-003 | off in the model (post-freeze fix; cells opt in) |
| `groundValidatorBound` | grounding ALSO clears when the published entry's validator differs from the record round's incoming validator | MS-CO-003 candidate closure of the tc1cGround residual, PROPOSED — collection-scope safety argument outstanding | off |

Taint pins (round-6): REPLAY-CAPABLE KIND = a kind in the declared
source-cache flow — its rows are eligible to seed future replays from
this artifact — INDEPENDENT of the recording attempt's warm/cold state
(the taint records in cold attempts too; scenario 7's sync N is cold by
construction and the 7a fix run depends on it). WRITE = any mutating
session op (Set/SetMany/Delete/DeleteMany/Clear; the model reaches Set
only, §1). Durability is checkpoint-cadence (§5) and SELF-HEALING under
at-least-once re-execution: any checkpoint capturing the writer's pop
captures the taint in the same snapshot, and a crash losing the taint
also loses the pop, so the re-run re-records it — a stronger story than
`compositionEnum`'s detection evidence, claimed explicitly. Consume
side: a tainted kind's scopes read as lookup MISSES (degradation, never
a loud verdict — §3 `eLookup`). A capability-level OPT-OUT (the
connector attests emission-irrelevance: "my session traffic during
replay-capable listings does not influence emitted rows") disables the
taint detector for those kinds; it is trust-boundary machinery like the
truthful-validator assumption and needs no new cells — a dishonest
opt-out reproduces 7a/7b exactly, an honest one is green by definition
of emission-irrelevance.

Kill obligations (§10.5): every BUILT toggle has at least one §9 cell
whose verdict it flips — `warmGate` in 5a, `hitValidatorBinding` in 3B,
`scopeLocks` in 4, `oncePerScope` in 4, `abandonLadder` in the P4
cells, `sessionTaintWrites` in 7a (its 7b residual is a REQUIRED
finding), `sessionTaintAll` in 7a and 7b, `recordGrounding` in the
MS-CO-003 tc1cNoPub pair (its tc1cGround residual is a REGISTERED
finding), and `groundValidatorBound` in tc1cGround vs tc1cGroundV
(MS-CO-003). The DE-SCOPED toggles
(`annotationBinding`, `compositionEnum` — v11) carry no kill
obligation: they remain documented as reviewed design records, their
fix duty discharged by the atomic-unit story (§9.6's 1a/1b/1c and
case-3 re-runs under V-ATOMIC).

## 7. Properties (checkable forms)

Announce events double as the deliverable-6/7 trace vocabulary:
`consult(s, hit?, v, validated?)`, `replay(s, v_base, e_base, k_base)`,
`record(s, v, e, k, wire_intent?)`, `upsert(s)`, `tombstone(s)`,
`publish(s, v)`, `checkpoint`, `stop`, `seal(n)`, `crash(k)`,
`session_read/write(k, stamp)`, `blocked(reason)`. Ghost fields (epochs,
verdict classes, configs, stamps) are labels of decisions the model
already made for behavioral reasons (the policy verdict, the upstream
response, the attempt's config) — §2.4.

- **P1 — binding integrity (safety).** Per (sync, scope) the monitor
  folds the ROUND LOG. A ROUND is the maximal run of pages of one
  action chain for one scope under one verdict; its ghost label is
  (verdict class, consult epoch, attempt config); a round is TORN if
  its pages committed in more than one attempt; only COMPLETE rounds
  enter the fold (torn or INCOMPLETE rounds' debris surfaces as
  content divergence, which is the intended alarm — 6-naive's verdict
  rests on the incomplete class). Log legality rules: at most
  one replacement copy per scope per sync; an overlay round composes
  only onto this sync's completed replay of the base its verdict was
  computed against. Deterministic fold over complete rounds, ordered by
  ROUND COMPLETION — the commit of a round's last page. A page COMMITS
  when the last of its prescribed STORE ops commits (announce-visible);
  action transitions and pops are scheduler events, NOT fold events; a
  round that commits no store ops contributes no fold entry (round-5
  F1 pin — the only reading the §2.4 announce-subscribed monitors can
  implement). Rounds enter
  the fold when they complete, so completion order IS the fold order;
  pages of different rounds may interleave in commit order without
  affecting it (pinned so out-of-script counterexample logs fold
  identically for every implementer): replacement → rows(s, e_base); overlay(e_from → e_to) —
  requires current fold value = rows(s, e_from), yields rows(s, e_to);
  a COPY-SKIPPED duplicate overlay round (B5 legal: the replacement
  copy is skipped for an already-replayed scope and the page's
  upserts/tombstones apply normally) folds as a NO-OP when the fold
  value already equals rows(s, e_to); an overlay round whose OWN
  replacement copy committed is SELF-GROUNDING — folds as rows(s,
  e_to) regardless of prior fold value, its copy counting toward
  replacement legality (case 4's locks-on surviving round is this
  shape); a COPY-SKIPPED REPLACEMENT round commits no rows and folds
  as a NO-OP — its publish, when validator-bearing, participates in
  the attestation checks only (1d's stale carrier); anything else is
  a legality violation; fresh(e) → rows(s, e) (REPLACES in the fold
  even though
  the store accumulates — divergence between an accumulating store and
  a replacing contract is precisely the union pathology).
  [Post-freeze editorial, two-track seam: the fold algebra these
  rules implement — REPLACES absorption, OVERLAY composition,
  tombstone ordering, replay-copy idempotence — is mechanically
  proved in `formal/occult/LAWS.md` (L1–L6 plus negative controls);
  the model consumes the laws as assumptions per the brief's
  division of labor. No semantic change.]
  Replacement-count legality counts committed copies WITHIN COMPLETE
  ROUNDS, not verdict labels and not raw copy commits (round-7 F2
  pin): a copy inside a round that never completes is pre-committed-
  classification debris and surfaces through content divergence, never
  through the count — this keeps cell 4's locks-off run red (two
  complete rounds, two counted copies), keeps 6-naive red (its debris
  copy is uncounted; the alarm is content), and keeps the benign
  cross-attempt at-least-once re-copy green (plan B5's "worst case …
  re-runs an idempotent copy": attempt 1's incomplete-round copy plus
  attempt 2's completed re-copy count as ONE); the locks-on run
  copy-skips the duplicate and folds green. The FOLD'S INITIAL VALUE
  IS THE EMPTY PARTITION (round-7 F3 pin): a scope with committed
  store ops and no complete round diverges from the empty fold by
  construction (never vacuously green), and a published manifest
  entry for a scope whose fold result is EMPTY is an ATTESTATION
  violation — the entry attests a composition the round log does not
  contain. Checks: at SEAL — (a) CONTENT: partition equals the fold
  result; (b) ATTESTATION: the manifest entry's epoch (via truthful
  validators) equals the fold result's epoch, and an entry over an
  empty fold violates outright; (c) CONFIG: every row's
  ghost config tag equals the sealing attempt's compat config. At
  `publish(s, v)` — ATTESTATION ONLY: v's epoch equals the publishing
  round's verdict epoch (plan B5 permits the replay page to publish the
  new delta token before overlay pages land, so no content check at
  publish). The phantom union fails content or attestation in every §9
variant; case 4's duplicate copy violates the at-most-one-replacement
rule directly; case 5a's drift copy fails config. BOUNDARY: TORN
  completed rounds are outside P1-content's designed domain — under
  between-attempt mutation a torn fresh round legally mixes epochs
  (pure smear) and the per-round fold would false-alarm. AMENDED per
  MS-CO-002 (build-out find; decision 1 of the calibration log's
  "Model decisions of record"): this spec originally argued no §9
  config can tear a round (each config's single stop consumed by its
  premise), but in the model the stop's PLACEMENT is genuinely
  explored, so torn rounds ARE reachable (graceful stop mid-fresh-
  round, resumed in attempt 2; crash-based configs still cannot tear —
  crash-only histories resume from root-token checkpoints, §3/§5).
  The exclusion is therefore enforced MONITOR-SIDE, not config-side:
  P1 tracks attempt ghosts per round across every round op
  (clear/copy/upsert/tombstones/publish) and excludes torn scopes
  from the content and attestation folds. KNOWN NARROWING, registered
  here: P3′'s torn tracking observes only overlay writes
  (upsert/tombstones), so a replacement-only tear is excluded from
  P1-content but not from P3′'s domain; no calibrated cell reaches a
  replacement-only tear that survives to a P3′-asserted seal (the
  P3′-asserting interrupted cells seal empty or use atomic/overlay
  shapes), and widening P3′'s tracking to the P1 op set is the
  registered follow-up if one ever does. Widening any config to a
  second interruption plus a second mutation still REQUIRES a fold
  extension for torn rounds (per-page folding) via change order
  first.
- **P2 — bounded staleness (safety, multi-sync).** Ghost provenance per
  row: origin epoch and the chain of syncs it traveled by replay.
  "Consulted against upstream during sync N" is pinned as: the scope
  had, during sync N (any attempt), a verdict that included an upstream
  VALIDATION MATCH (`eValidate` == true), a fresh fetch, or a
  CHANGED-WITH-DIFF verdict (revalidation occurred and the diff is an
  upstream fetch; round-5 F8 pin) — a lookup
  hit alone does NOT qualify. At each seal: every row's scope was
  consulted-against-upstream this sync. Staleness counter per row;
  corollary runs assert staleness ≤ 1 while P1 holds and exhibit
  unbounded growth (to the chain bound) in the case-1 corrupted chain.
- **P3′ — per-scope epoch coherence (safety), doubly scoped.** Checked
  ONLY (i) in scenarios without mid-attempt upstream mutation, and
  (ii) for scopes with no TORN round this sync (a round crossing an
  attempt boundary via mid-chain stop-resume observes two epochs and
  would false-alarm). At seal, for every in-scope manifest scope:
  partition content equals `rows(s, e)` for the epoch e of the scope's
  last consulted-against-upstream verdict this sync ("last" by
  announce order across attempts). Justification (valid in this class):
  with upstream fixed within an attempt and no torn rounds, every page
  of a round observes one epoch; truthful overlay composition preserves
  coherence. Scenarios outside the class rely on P1/P2; a per-page
  refinement is future work, not silently assumed.
- **P4 — progress.** Liveness form: after crash/stop budgets exhaust
  and upstream mutation stops, the chain eventually seals (P liveness
  monitor, hot while unsealed) — meaningful only with `abandonLadder`
  on. Livelock DETECTION is a safety rule checkable in bounded runs:
  two consecutive resume attempts that fail from byte-identical
  restored checkpoint state with the same verdict at the same step
  constitute the deterministic re-failure finding (CO-6b-004's
  stuck-resume contract). The leaked-lock hang is checked via §4's
  in-attempt retry: with the release edge removed, the retry deadlocks
  (the model's own mutation check); with it present, the scenario
  seals.
- **P6-A — session laundering witness (safety, variant A).** Ghost:
  session values carry (writer action, attempt, derivation id);
  committed outputs embed the stamps they read. At seal: violation iff
  an embedded stamp's derivation differs from that writer's FINAL
  derived value for the key — the artifact holds conclusions from
  premises the final sync state does not hold, and no mechanism marked
  it. Same-value re-derivation (d1→d2→d1) does not alarm. DOMAIN
  (round-6 pin): P6-A quantifies ONLY over stamps embedded by session
  reads performed within the sealing sync; traveled (replay-copied)
  stamps and ⊥/miss stamps are outside its domain and belong to P6-R —
  P6-A is vacuously green on 7a/7b/7c.
- **P6-R — replay-session coherence (safety, scenario 7; signoff
  addendum).** Extends variant A's ghost vocabulary two ways: (a)
  session-derived stamps TRAVEL WITH COPIED ROWS (replay copies ghost
  provenance unchanged); (b) per (sync, key) the model carries a
  COUNTERFACTUAL session value — the producer policy's PHASE-FINAL
  value under an all-fresh execution at this sync's epoch, its reads
  evaluated against the empty per-sync namespace (non-circular: the
  namespace starts empty, the key budget is 1, and the producer phase
  runs first). The counterfactual is computable, not a second
  execution, because policies are deterministic and kinds run in
  sequential phases with no cross-op spawns (a scenario-7 config
  constraint) — which also makes it independent of reader timing:
  every R read happens after W's phase completes (§8 note). It is
  defined ONLY for scenarios whose upstream mutations are scheduled
  BETWEEN SYNCS (a single epoch per (sync, scope) per sync;
  between-attempt mutation within a sync makes "this sync's epoch"
  multivalued and is excluded). At seal, for every committed row whose
  scripted derivation includes a session input: violation iff the
  row's embedded stamp differs from the counterfactual value of that
  key this sync. Covers both duals: a fresh reader deriving from a
  READ-MISS whose scripted producer was elided (7a — counterfactual
  v1, embedded miss), and a replayed row carrying a stamp the producer
  re-derived differently this sync (7b — counterfactual v2, embedded
  v1). The both-warm control (7c) is green by construction: unchanged
  upstream makes the counterfactual equal the carried stamp. P6-A is
  unchanged and keeps its final-value form for within-sync laundering;
  P6-R is the cross-sync/replay form.

P5 (sweep) and P7 (sealed cuts) are graph-runtime properties — addendum.

## 8. Abstraction soundness arguments (for the adversarial review)

- **Clear/copy as two atomic steps.** Every calibration window is
  BETWEEN named steps: gate↔clear (case 4), clear↔copy (record-page
  wipe, CO-6b-006 N1), copy↔mark (carrier-less phantom, §9 1c),
  stop↔spawn-drain (§9 stop-stranding premise). Sub-batch
  partial-commit convergence: 6a store-level exclusion.
- **Checkpoint-or-skip nondeterminism** over-approximates the timer
  throttle; forced sites (Init, pre-seal, stop) are preserved exactly.
  Every model checkpoint placement is a real placement.
- **Batch cap {1..2}**: §1 scaling argument (two-way splits compose;
  in-batch spawn admission makes width > cap reachable).
- **Verdict-as-data**: connector obligations (replay only a this-sync
  hit, partition discipline) are deliberately violable by scenario
  policy; SDK guards must catch the violations the contract says they
  catch, and honest policies must not trip them. Policy PURITY
  under-approximates legal connectors (the proto permits within-sync
  connector memory, and the CO-6b-006 chaos connector uses it); all §9
  premises route verdicts through page tokens or the session store, so
  no premise depends on cross-call memory. A future scenario needing it
  is a change order, not a silent extension.
- **Session KV in MStore's durability domain**: the model gives session
  writes op-commit durability and the store's crash cut. The production
  session store is a separate service whose commits are NOT
  prefix-ordered with c1z writes (and noop/in-memory variants are
  lossy). The brief pins variant A as durable, so this is recorded as
  the trust boundary of P6-A's verdicts, not modeled.
- **Counterfactual session ghost (P6-R)**: scripted-policy determinism
  plus sequential kind phases (no cross-op spawns in scenario-7
  configs) make "the session value an all-fresh sync would hold" a
  computable ghost label, not a second execution — no ∀∃ obligation —
  and reader-timing-independent (every read follows the producer
  phase). Pinned as the producer's PHASE-FINAL value (§7). Sound only
  while scenario-7 configs schedule upstream mutation between syncs,
  never within one (mid-attempt or between attempts); a config
  violating that requires a P6-R scoping extension by change order.
- **One STORAGE row kind, 2 row ids**: sufficient to distinguish base
  rows from fresh rows so unions and resurrections are content-visible.
  Scenario 7's KIND axis ((op, scope) pairs, §1) rides on top without
  widening storage row kinds.
- **Session-derived content divergence is under-approximated to ghost
  stamps (scenario 7)**: real session-derived enumerations diverge in
  CONTENT; §3's policies never choose row content, so the model carries
  the divergence in the embedded stamp alone. The finding survives the
  abstraction — the real system has no content oracle for fresh rounds
  either, so the corruption is invisible to shipped checks for the
  same structural reason.
- **Seal as one step**: the real counts-before-ended_at fence is
  store-level, closed in 6a/6b.
- **Root-tokens-at-loop-tops is a claim about the modeled population.**
  Every annotation-bearing listing (resources/entitlements/grants) runs
  inside batches via `syncOneAction`, where it holds. Sequential
  non-fanned ops (e.g. `SyncResourceTypesOp`) process one page per loop
  iteration and CAN checkpoint mid-chain at loop tops in crash-only
  histories; they are outside the model, and future scenarios touching
  them must not inherit the batched-population claim.

## 9. Calibration scenarios

Expected-fail runs use the shipped design unless stated; every FIND must
be a checker counterexample trace (regenerable on demand from its
CALIBRATION.md cell row; sweep summaries are archived under
`walker/traces/`), rendered per deliverable 6. Every premise below
states its reachability
route; the standard premise generator is the STOP-STRANDING pattern: a
planning page's transition commits (hit recorded, same-op carrier
admitted to live state), the stop lands before the carrier's first
atomic step completes — undequeued or dequeued-but-unstarted both
qualify (genuine interleaving choice) — and the forced stop-checkpoint
makes {parent cursor, pending carrier, hit map} durable together.

1. **Phantom union** (2 syncs + 1 verification sync, 1 scope, shipped
   toggles ON — the residual exists in the shipped design). Premise for
   1a/1b: sync N+1 attempt 1 runs planning action P (2 pages): page 1
   consults S at epoch 1 (lookup hit V1 recorded, validation MATCHES,
   verdict replay) and spawns carrier C (same-op, replay annotation V1
   in its token); stop-stranding: forced checkpoint captures P
   mid-chain (page-2 cursor), C pending, hit {S: V1}. Between attempts
   upstream → epoch 2. Attempt 2 restores both actions; P's page 2
   re-consults per policy: lookup hit V1 (overwrite, same value),
   revalidation vs epoch 2 FAILS → verdict fetch-fresh → P's chain
   continues with a 2-page fresh round under V2. C and P interleave
   (same batch, 2 workers, or cap-1 orderings — genuine choices):
   - **1a**: C drains between P's fresh pages → clear wipes the first
     fresh page, copy installs base(e1), second fresh page lands on
     top; P publishes V2 at round end → partition = base(e1) ∪ partial
     fresh(e2) ≠ fold (fresh round replaces) → **P1 content violation**
     at seal; chain continues, unchanged upstream → union replays →
     **P2 staleness 2** (unbounded branch).
   - **1b-i** (C drains after P's complete fresh round; C's annotation
     publishes V1): clear wipes the fresh round, copy installs
     base(e1), entry V1 → fold (fresh(e2) then replacement(e1)) =
     rows(e1); content and attestation coherent → P1 green, **P3′
     violation** (last consulted verdict epoch 2, content epoch 1; no
     torn round for S — P's consult pages and the fresh round each
     commit within one attempt).
   - **1b-ii** (C drains last, defers publish — validator-less page,
     legal per proto): partition rows(e1) under entry V2 → **P1
     attestation violation**.
   - **1c — carrier-less variant** (priority trace for the chaos
     bridge; needs NO stop and NO spawn): attempt 1's single root
     action consults S (hit V1, validation matches at epoch 1) and
     replays in the same page; `eCopyScope` COMMITS; hard crash before
     any post-Init checkpoint — durable base(e1) partition debris, hit
     and replayed mark lost. Epoch 2; attempt 2 restarts from root,
     re-consults, revalidation fails → fetch-fresh → upserts land over
     the debris (fresh never clears) → publish V2 → **P1 content
     violation** with ONE crash and NO replay in attempt 2 (warm gate
     and binding never evaluated).
   - **Fix runs — DE-SCOPED v11, not built** (`compositionEnum` on,
     its real checkpoint-durable
     syncer-side semantics): 1a/1b — detection fires, `blocked` reaches
     the seal, sync N+2 runs cold, P2 restored from N+2. 1c — detection
     CANNOT fire (no syncer-visible copy evidence); the propagation
     bound fails; this is a REQUIRED finding of the run and bounds the
     deliverable-3 claim. Crash-between-detection-and-checkpoint is a
     second required finding. Kept as the reviewed design record of
     the staged mitigation; the built fix story is §9.6's atomic unit
     (1a/1b/1c re-runs green under V-ATOMIC, no detection machinery).
   - **1d — overlay-flavor control (signoff addendum; content-green
     under shipped toggles)**: same stranding premise as 1a/1b, but
     attempt 2's failed revalidation yields CHANGED-WITH-DIFF
     (delta-overlay flavor), not fetch-fresh: P's chain continues with
     an overlay round — replay page (annotation V1, base e1), then
     overlay pages (e1→e2) — while stale carrier C (pure replacement,
     base e1) interleaves freely. V2's publish placement is pinned,
     and BOTH placements are explored as sub-configs: (i) round-end
     (final overlay page carries V2); (ii) B5 early publish (replay
     page publishes V2, overlay pages validator-less). Expected:
     CONTENT GREEN in EVERY schedule UNDER SHIPPED TOGGLES — both
     copies draw the same base(e1) and collapse under `oncePerScope` ∧
     `scopeLocks` (the check-then-mark collapse is atomic only under
     the lock; this is case 4's dual-replay shape, whose cell kills
     each leg — a `scopeLocks`-off 1d mutant is content-red), and the
     overlay composes legally onto either copy; the union cannot form.
     The protection is MITIGATION-DEPENDENT, not structural
     (V-OVERLAY-UNIT, §9.6, pilots the structural alternative — v10).
     ATTESTATION is schedule-dependent by PUBLISH order, not drain
     order: when a validator-bearing C's V1 publish is the LAST
     publish for S (§4 runs `ePublishEntry` even on a copy-skipped
     page), the seal sees entry epoch e1 under content rows(e2) →
     expected ATTESTATION-STALE-BEHIND finding. Under sub-config (ii)
     a C draining MID-schedule can still publish last (V1 after the
     round's early V2); under (i) only C-drains-after-round-end
     schedules alarm. Classification: stale-BEHIND is the self-healing
     direction (the next sync's V1 consult re-delivers the e1→now
     changes) only under idempotent absolute-record overlay
     application — a connector-semantics assumption OUTSIDE the pinned
     trust boundary — so P1 stays direction-blind and the cell records
     the finding rather than weakening the property. A validator-less
     C is rowless and legal (B5: a round that never publishes leaves
     no entry — the replay itself remains valid); whether its copy
     commits is SCHEDULE-dependent, not a property of
     validator-lessness: C-first schedules COMMIT C's copy (the
     annotation validator plays no part in the hit or base-binding
     checks) — a committed replacement folding to rows(e1), counting
     toward replacement legality, with P's round then folding as a
     copy-skipped overlay to rows(e2); C-later schedules copy-skip C —
     the §7 no-op, with no publish. Fully green in all schedules
     either way. P2: GREEN (changed-with-diff qualifies as
     consulted-against-upstream per §7's round-5 pin; staleness ≤ 1 —
     base rows one replay hop, overlay rows fresh). Flavor-coverage
     note (EXTERNAL-BOUNDARY commentary, §8-style — a
     connector-population claim outside the model): the protection is
     FLAVOR-conditional, not connector-conditional — delta connectors
     degrade to fetch-fresh on token expiry (Graph 410), so 1a/1b/1c
     cover every connector's degraded path. Fold totality over 1d logs
     is pinned by §7's self-grounding-overlay and
     copy-skipped-replacement clauses (added with this cell).
     Provenance: this cell replaced a conversational "every ordering
     converges" claim that had checked content only — the attestation
     edge was caught while scripting the cell, which is the point of
     scripting it.
2. **Session laundering** (1 sync, 2 same-op actions H (session writer)
   and G (session reader), sessions variant A). Both cells are expected
   findings; the property must alarm on exactly the schedules below and
   stay green elsewhere.
   - **2-stop** (deterministic script): one batch, two workers; the
     explored schedule has H's session write of d1 commit (op-commit
     durable), G read d1, emit a row embedding d1, and finish; graceful
     stop aborts the batch while H is mid-chain; stop-checkpoint
     captures G popped, H on the stack. Resume: H alone re-runs;
     upstream mutated between attempts → H derives d2 ≠ d1; G never
     re-runs → its committed row embeds d1 ≠ final d2 → **P6-A
     violation** at seal.
   - **2-crash** (interleaving-dependent, real shipped behavior): hard
     crash before any post-batch checkpoint → BOTH re-run
     (at-least-once). The schedule where G re-runs BEFORE H's
     re-derivation exists (worker interleaving is a genuine choice):
     G re-reads the DURABLE stale d1, re-emits embedding d1, H then
     derives d2 → **P6-A violation**. The complementary schedule
     (H re-derives d2 first, G re-emits embedding d2) stays green —
     both outcomes are required calibration results.
   - No fix run (variant B is the graph addendum's obligation).
3. **Artifact swap + hit rebind** (2 sealed artifacts A/B, equal compat
   records, validators V_A ≠ V_B; upstream unchanged throughout, so
   truthful validators give rows(B) ≠ rows(up) = rows(A)).
   - **3A** (shipped: `hitValidatorBinding` ON, `annotationBinding`
     OFF — the residual hole): premise = stop-stranding with a 2-page
     P: page 1 consults base A (hit V_A, validation matches), spawns C
     (annotation V_A); stop; checkpoint {P mid-chain, C, hit V_A};
     MEnv swaps the previous artifact to B. Attempt 2: P's page 2
     re-consults — lookup hit V_B OVERWRITES the hit map (lookup-time
     recording, last-write-wins) even though revalidation of V_B
     FAILS → verdict fetch-fresh (a 2-page round under V_up = V_A
     content). C drains: hit check ✓, base-binding compares hit (V_B)
     to base B's manifest (V_B) — PASSES — clear+copy installs
     rows(B); C's annotation publishes V_A. Interleavings: C last →
     partition rows(B) under entry V_A → **P1 attestation violation**;
     C first or interleaved → fresh upserts land over rows(B) (fresh
     never clears) → **P1 content violation**. **P2 is GREEN** in every
     cell — attempt 1's validation match of V_A qualifies the scope as
     consulted this sync (corrected expectation; v2 wrongly claimed a
     P2 violation).
   - **3B** (pre-CO-6b-004: `hitValidatorBinding` OFF): 1-page P
     (consult+spawn, pops at the stop checkpoint); no re-consult in
     attempt 2, hit map stays V_A; C drains against swapped base B with
     NO binding check → copy proceeds, publish V_A → **P1 attestation
     violation** on an even weaker premise. Binding ON flips this cell
     to loud cold (V_A ≠ V_B) — the CO-6b-004 kill.
   - **Fix runs — DE-SCOPED v11, not built** (`annotationBinding` ON,
     shipped toggles): 3A —
     annotation (V_A) ≠ recorded hit (V_B) → loud cold, no wrong data;
     with `abandonLadder` on the chain completes cold (P4). Required
     extra cell: a carrier whose annotation validator is EMPTY (legal
     per proto) — the run must surface the fix's coverage boundary
     (fail cold on absence, or the fix is incomplete for validator-less
     connectors) rather than overstate it. Kept as the reviewed design
     record; V-ATOMIC subsumes the fix (no carrier, no annotation to
     bind — §9.6 case-3 re-run), including the validator-less coverage
     boundary, which is structurally absent there.
4. **Once-per-scope TOCTOU** (1 sync, 1 scope, 2 workers, dual replay
   carriers spawned same-op — both drain within one batch, no stranding
   needed; delta-overlay round). Premise constraint (§3 dedup is
   semantics, always on): the carriers carry BYTE-DISTINCT page tokens
   encoding the same (scope, verdict) — distinct identity digests, so
   both are admitted; this is the realized shape in the CO-6b-003/005
   chaos instruments (pages from different resources targeting one
   scope). Literal byte-identical duplicates would be rejected
   commit-locally or skipped by the spawned-admitted guard and CANNOT
   produce this premise. With `scopeLocks` OFF, both
   carriers pass the replayed-set check before either marks →
   clear/copy runs twice; the second replacement wipes the first's
   overlay upserts → **P1 violation** (two replacement copies violate
   log legality; content check catches the resurrection). With locks
   ON: single copy, overlays preserved, green under bounded
   exploration. Lock-release mutation check per §4/§7 P4.
5. **Warm-drift** (1 sync, 1 scope; the `warmGate` kill and the produce
   triggers). Premise = stop-stranding: attempt 1 (warm, config K1)
   records hit {S: V1} and strands carrier C; between attempts MEnv
   changes the declared drift input:
   - **5a — compat drift (trigger 1)**: attempt 2 computes config K2 →
     G7 mismatch → COLD, and B4 marks produce-blocked. `warmGate` ON:
     C's warm-gate check fails → loud cold; artifact blocked; with
     `abandonLadder`, next sync runs cold — no wrong rows. `warmGate`
     OFF: C passes hit (restored) and binding (base unchanged, V1) →
     copies K1-tagged rows into a K2 attempt → **P1 config violation**
     (clause c). This is the warmGate kill (§6).
   - **5b — capability withdrawal (trigger 2)**: attempt 2's G6 bit is
     off (no source-cache handling). At attempt start, install observes
     prior-attempt produce state without handling → produce-side block
     (trigger 2) marks the artifact replay-blocked (checkpoint-cadence
     durability; the crash-window finding is required here too). C's
     replay-annotated page then arrives in the handling-less attempt
     and is SILENTLY IGNORED per §4 (B1): no failure, no rows for S,
     and the sync SEALS GREEN with partition[S] empty and the artifact
     blocked. The green seal, cold consults, compat-record retention,
     and blocked marking match the CO-6b-005 capability-withdrawn chaos
     cell; the EMPTY-PARTITION DROPOUT is NOT pinned by that cell (its
     connector adapts cold on miss — no stranded carrier exists there),
     so the model's scripted seal-state expectation is the dropout's
     only executable oracle today.
     The silent scope dropout is a REQUIRED DESIGN FINDING of this
     cell: it is green under P1 — the empty partition equals the empty
     fold, and no entry exists to check (round-7 F3 wording pin; NOT
     "vacuously green": a scope with no complete round is still
     checked, against the empty fold) — and invisible to P2 (which
     quantifies only over rows present), so the
     cell's oracle is the scripted seal-state expectation itself.
     Whether a completeness/coverage oracle should exist is recorded as
     a deliverable-6 chaos-bridge question, not invented at freeze
     time.
   - **C1 probe** (CO-6b-002 conformance question): action A: page 1
     consults S (hit recorded at lookup) and records fresh; page 2
     carries a replay annotation (policy places replay mid-chain); stop
     between the pages → checkpoint holds A's mid-chain cursor + the
     hit map. Resume: page 2's connector call performs NO fresh
     consult; the SDK hit check passes on the RESTORED hit map → the
     replay runs on a mid-chain resume without a fresh consult. The
     model thus answers C1 "reachable via the stop path" — a
     conformance finding against CO-6b-002's "unreachable in practice"
     wording, to be confirmed or refuted against the real
     implementation through the chaos bridge (deliverable 6), not a
     model bug.
6. **Atomic-unit design variant — collect-and-commit (signoff
   addendum; bake-off pair, deliverable-4 pilot).** Not a §6 toggle
   (it alters commit structure rather than removing a check): a
   scenario-local variant of §3/§4/§5. V-ATOMIC pins the discipline
   "complete one request's work before any derived work, marker
   included": (i) replay executes INLINE on the consulting page — no
   carrier spawn — and the page's own transition commits only after
   (ii) ONE atomic store op `eReplayUnit(s)` = {clear, copy, marker,
   publish} (implementation shape: single WriteBatch under a memory
   threshold, grouped SST ingest above it — range-del + rows per key
   family + marker row in one manifest edit; the model checks the
   unit's CONTENTS, not the mechanism); the marker is a PER-SCOPE
   STORE ROW, durable at op commit — AUTHORITATIVE consult provenance
   (the marker) leaves checkpoint-cadence durability; (iii) a
   re-executed action checks the marker BEFORE consulting and
   suppresses re-consult and re-derivation for marked scopes. V-NAIVE
   is the internal kill: marker-after-work WITHOUT the unit — shipped
   §4 steps, then a separate marker op after `eCopyScope`; clause
   (iii) applies to BOTH variants (V-NAIVE's defect is solely the
   marker landing outside the unit). Under both variants the §5
   checkpoint token is UNCHANGED — the hit map and replayed set are
   still recorded and checkpointed as shipped, but a restored hit
   NEVER authorizes replay without a fresh consult: replay is
   consult-inline, and marked scopes suppress the consult. A stop
   between a lookup-hit and the unit can checkpoint {S: V1} with
   nothing materialized; that stranded hit is INERT. Scope of the
   pilot: originally FETCH-FRESH flavor only; the v10 addendum extends
   it to the changed-with-diff (overlay) flavor via V-OVERLAY-UNIT and
   cells 6-overlay / 6-overlay-naive below, which answer the two
   obligations this paragraph previously deferred to deliverable 4
   (what the unit publishes for a diff verdict; the stale-AHEAD hazard
   of a marker committing before overlay pages land — the latter is
   6-overlay-naive's kill). Boundary note
   (round-5 N4): clause (iii)'s marker check precedes the consult
   OUTSIDE the scope lock — itself a check-then-act window; two
   concurrent consulting actions for one scope would both pass and
   commit two units (two committed copies → P1 legality alarm).
   Unreachable in this scripted single-consulting-action family;
   recorded as a real 2-worker hazard for the deliverable-4 bake-off
   boundary notes.
   - **6-naive (expected RED — the debris union)**: 1 sync + resume,
     1 scope, fetch-fresh flavor, no post-Init checkpoint before the
     crash. Attempt 1: consult S → verdict replay → clear+copy commit;
     the hard crash lands in MStore's queue BETWEEN `eCopyScope` and
     the marker op (same-sender FIFO prefix, §5 — reachable). One
     between-attempt mutation (e1→e2). Attempt 2 restarts from root:
     no marker → re-consult → revalidation fails → fetch-fresh; the
     fresh round accumulates over the COMPLETE unmarked copy. Fold =
     fresh(e2) (the interrupted replay round never completed; its copy
     is debris) → **P1 content violation**, rows(e1) ∪ rows(e2) under
     V2 — the phantom union rebuilt from debris with no carrier and no
     stranding. (Shipped batched commits make PARTIAL-copy debris the
     same way through an intra-copy window — 6a store-level
     vocabulary, deliberately NOT §7's cross-attempt TORN; the
     complete-copy window is the minimal witness and needs no
     refinement of §8's copy-as-one-step abstraction.)
   - **6-atomic (expected GREEN across this schedule and the 1a/1b/1c
     re-runs)**: same schedule under
     V-ATOMIC — a crash before the unit leaves nothing durable (resume
     re-consults over an empty scope; the fresh round lands clean); a
     crash after it leaves {rows(e1), entry V1, marker} together, and
     resume suppresses re-derivation → seals coherent rows(e1)@V1,
     with the e1→e2 changes arriving next sync via V1's consult.
     Round-completion pin (round-5 F1): under V-ATOMIC the replay
     round is COMPLETE at `eReplayUnit` commit — even when the
     action's transition never commits and the re-execution is
     marker-suppressed, the fold sees replacement(e1) and the seal is
     green as scripted; under V-NAIVE the marker op is one of the
     round's prescribed store ops, which is exactly what leaves the
     6-naive round incomplete.
     Additionally re-run the 1a/1b/1c premises under V-ATOMIC with
     `compositionEnum` OFF — expected green across those premises: the
     stranding premise is unreachable (no carrier; durable consult
     PROVENANCE — the marker — is atomic with materialization; a
     checkpointed hit-map entry may precede it but is INERT, see
     above), and 1c's debris cannot exist (the
     marker rides the unit, so attempt 2 suppresses the second verdict
     instead of unioning over debris). Also re-run the CASE-3 premise
     (v11 extension, part of the de-scope decision): stop after
     attempt 1's consult, base swapped to sibling B between attempts,
     `annotationBinding` OFF — expected GREEN, because the variant
     SUBSUMES the annotation-binding fix: there is no carrier and no
     annotation to trust, restored hits are inert, and the marker
     lives as a per-scope row in the CURRENT sync's artifact (not the
     swapped previous one) — if attempt 1's unit committed, the seal
     is that unit's coherent contents; if it did not, attempt 2
     re-consults against whatever base is ACTUALLY current (swapped
     B's V1 fails validation against upstream e2 → fetch-fresh). The
     3A residual hole (hit-map rebind) is structurally closed for the
     same reason: no restored hit ever authorizes replay. The pair
     pins the two
     load-bearing lines — the unit's contents (6-naive red shows the
     marker must ride inside it) and marker-suppresses-re-execution —
     and is the walker-side pilot of the deliverable-4 bake-off: the
     graph runtime takes (i) as native scheduling semantics (derived
     work is downstream of its premise's commit).
   - **V-OVERLAY-UNIT (v10 addendum — the overlay-flavor extension of
     V-ATOMIC).** Scenario-local variant of §3/§4/§5, declared here in
     full before its cells (round-6 process lesson). The discipline
     generalizes clause (i) — BOTH halves of it (round-7 F1): the
     "request" whose work must complete is
     the CONSULT VERDICT, not one wire page — for a CHANGED-WITH-DIFF
     verdict the prescribed work is the base replay PLUS every overlay
     page the diff yields. Pins: (o-i) replay-and-overlay executes
     INLINE on the consulting chain — no carrier spawn (1d's stale
     carrier cannot exist) — AND the transitions of every page in the
     verdict's prescribed work commit only AT UNIT COMMIT (clause
     (i)'s second half, generalized): intermediate overlay cursors
     never enter live state, so a stop-forced checkpoint captures the
     chain AT ITS CONSULT-PAGE TOKEN, which is the token resume
     re-enters with. The deferral is scoped PER VERDICT, which keeps
     multi-scope chains well-defined (MS-CO-001, parallel-review F4):
     a page belonging to scope S1's committed unit has its transition
     committed WITH that unit, so a chain stopped while consulting S2
     checkpoints at S2's consult token — never inside either scope's
     prescribed work; (o-ii) under the variant, `eUpsertPage`/
     `eTombstones` for the scope are BUFFERED in a per-scope volatile
     collect buffer (§5 row; bounded by pages-per-round ≤ 2, within
     small scope) — no store op commits per page; when the FINAL
     overlay page is collected, ONE atomic store op `eOverlayUnit(s)`
     = {clear, copy(base e_from), overlay upserts/tombstones in
     prescribed page order, marker, publish(V_to)} commits
     (implementation shape as V-ATOMIC: WriteBatch or grouped SST
     ingest; the model checks contents, not mechanism). Announce
     vocabulary is unchanged: the unit announces its constituent ops
     at commit, so every page of the round commits simultaneously and
     round completion IS unit commit — the round folds as §7's
     existing SELF-GROUNDING OVERLAY (own copy committed, folds to
     rows(s, e_to) regardless of prior fold value, copy counting
     toward replacement legality); no new fold clause. (o-iii) THE
     UNIT ANSWERS THE DEFERRED PUBLISH QUESTION: it publishes V_to,
     the verdict's post-diff validator (the new delta token),
     attesting e_to. The publish constituent is present IFF the round
     supplied a non-empty validator (round-7 F4: validator-less diff
     rounds are legal per §3/B5) — a publish-less unit commits
     {clear, copy, overlays, marker}, leaves NO entry (a miss next
     sync, B5-consistent), and its marker still suppresses
     re-execution within the sync. Timing (round-7 F5 rewording): the
     variant DEFERS THE RUNTIME'S MANIFEST WRITE into the unit — a
     deviation from B3/B5's frozen per-page publish timing, change-
     order scope if adopted; the wire contract is untouched (the
     connector still returns the token on whichever page carries it —
     B5 permits either leg) and the deferral is connector-invisible
     because the consult surface is the PREVIOUS artifact only
     (`previousSyncSourceCacheLookup`). (o-iv) resume rule: the marker
     suppresses
     re-consult exactly as clause (iii); for a scope with NO marker,
     resume restarts the scope's work FROM CONSULT — under (o-i)'s
     transition deferral this is DERIVED, not decreed: the stop
     checkpoint holds the consult-page token (no mid-chain cursor
     exists for a unit-mode scope), the collect buffer is volatile,
     and the honest price
     is at-least-once re-fetch of the diff, lost work but never
     debris. The stranded-hit inertness carries over from V-ATOMIC
     unchanged; the marker-check-outside-lock two-worker hazard
     carries over IN CLASS but with a MATERIALLY WIDER WINDOW
     (round-7 F6): V-ATOMIC's window spans one page's handling, the
     overlay variant's spans the whole collect phase (marker check at
     consult → unit commit after every overlay page), multiple
     connector calls wide. Because each unit is internally atomic,
     the racing schedules' final content is the last unit's coherent
     rows(e_to) — the alarm is legality-only (two committed copies),
     never a wipe-mosaic. Recorded for the deliverable-4 bake-off
     boundary notes.
   - **6-overlay (expected GREEN across the re-scripted 1d premise
     family — with `oncePerScope` AND `scopeLocks` OFF)**: the
     structural claim that 1d could not make. 1d's stranding premise
     is unreachable under (o-i) — no carrier — so the family is
     re-scripted: planning chain consults S (hit V1, revalidation vs
     e2 fails → CHANGED-WITH-DIFF), collects the 2-page ROUND inline
     (the replay/first-overlay page plus the final overlay page —
     NOT consult + 2 overlay pages, which would break §1's
     pages-per-scope-per-round ≤ 2 bound; MS-CO-001),
     `eOverlayUnit(s)` commits {base(e1) copy, overlay e1→e2,
     marker, publish V2}. Sub-cases: (a) uninterrupted — fold =
     self-grounding overlay → rows(e2), entry V2, content and
     attestation green; (b) stop mid-overlay-chain — stop-checkpoint
     captures the chain at its CONSULT-PAGE token (transition
     deferral, o-i; round-7 F1 corrected this premise — no mid-chain
     cursor exists to capture) and hit {S: V1}; buffer lost;
     resume re-enters at the consult (o-iv, derived), re-consults
     (restored hit is
     inert), re-collects, one unit commits → green; (c) crash before
     the unit — nothing durable for S, clean re-consult → green; (d)
     crash after the unit — {rows(e2), entry V2, marker} durable
     together, marker suppresses re-derivation → seals coherent. Both
     mitigation toggles are OFF in every sub-case: `oncePerScope` is
     unneeded (the marker inside the unit is the dedup; a re-executed
     chain suppresses at the marker check) and `scopeLocks` is
     unneeded within the scripted single-consulting-chain family (the
     two-worker marker-race boundary note still applies and stays a
     recorded hazard, not a scripted cell). 1d's
     attestation-stale-behind cannot occur: publish order equals unit
     order because no publish exists outside a unit. P2 GREEN
     (changed-with-diff qualifies per §7's round-5 pin; staleness ≤
     1). P3′ applies: no torn round is possible for unit-mode scopes —
     every round commits within one attempt by construction.
   - **6-overlay-naive (expected RED — the stale-AHEAD kill; the
     inherited deliverable-4 obligation made concrete)**: unit
     misdrawn at the consult boundary instead of the verdict boundary:
     `eOverlayUnit'(s)` = {clear, copy, marker, publish(V2)} commits
     at consult time, overlay pages then commit per-page via the
     shipped §4 path. 1 sync + resume + 1 verification sync, 1 scope.
     Hard crash lands in MStore's queue between the unit' commit and
     the final overlay upsert (same-sender FIFO prefix, reachable as
     in 6-naive). Attempt 2 restarts from root; the MARKER IS PRESENT
     → clause (iii) suppresses re-consult and re-derivation → the
     remaining overlay pages NEVER land; seal: partition = base(e1) +
     partial overlay under entry V2. Fold: the round is INCOMPLETE
     (prescribed overlay store ops never committed) → contributes no
     fold entry → the fold for S is EMPTY, and the committed prefix
     diverges from it by the §7 empty-fold pin (round-7 F3) → **P1
     content violation** at seal, plus **attestation violation**
     outright: entry V2 is published over an empty fold (the entry
     attests a composition the log does not contain). The
     verification sync makes the direction asymmetry
     1d classified CONCRETE: consult of V2 revalidates CLEAN (upstream
     unchanged at e2 — the per-seal consult clause of P2 is GREEN
     there) → the mosaic replays warm → the **P2 staleness COUNTER
     grows without bound** (round-7 F7 wording: the growth is on the
     stale RESIDENT base(e1) rows whose e1→e2 updates never landed —
     case 1's "unbounded branch"; the never-landed rows are in no
     partition) and P1 content stays RED in the verification sync
     itself (the warm mosaic copy folds as rows(e2) via truthful V2;
     the partition is the mosaic) — stale-AHEAD is
     the NON-self-healing direction (V2 attests changes the artifact
     never absorbed; no future consult re-delivers e1→e2), the dual of
     1d's self-healing stale-BEHIND. The marker-before-pages placement
     is the sole defect: 6-overlay commits the identical contents one
     boundary later and is green.
   - **6-overlay-last (round-7 F2 — the third placement, its own
     cell)**: per-page commits with marker+publish LAST outside any
     unit: clear+copy commit at the replay page, overlay
     upserts/tombstones per-page via the shipped §4 path, then marker
     and publish(V2) as two trailing separate ops. The round-7 review
     REJECTED the v10 dismissal ("reduces to 6-naive's unmarked-debris
     class"): the reduction is impossible in the overlay family — a
     crash before the marker leaves UNMARKED debris, but attempt 2's
     re-consult yields CHANGED-WITH-DIFF by premise, whose prescribed
     work BEGINS with clear+copy — the clear WIPES the debris and the
     round rebuilds; no schedule unions (6-naive's class requires a
     NON-clearing fetch-fresh re-verdict). The placement's real
     windows are its own: (w1) crash anywhere before the marker →
     converging rebuild, GREEN — and the FIRST history in the spec
     where replacement-count legality meets a cross-attempt double
     copy (attempt 1's copy in an incomplete round + attempt 2's
     completed re-copy), legal under §7's complete-rounds counting
     pin (B5's idempotent re-run); (w2) crash BETWEEN marker and
     publish → marked, entry-less, content-complete scope whose
     re-execution clause (iii) SUPPRESSES → seals correct rows(e2)
     with NO entry; the publish was a prescribed round op that never
     committed → round incomplete → empty fold → **P1 content
     violation** (non-empty partition vs empty fold) — a
     suppression-window shape that is NOT 6-naive's union class
     either. Expected: P1 RED (w2 is the witness; w1 must NOT alarm —
     it exercises the counting pin).
7. **Session elision under replay — the sessions × source-cache
   product (signoff addendum; shipped design, pure two-sync scripts —
   no interruption machinery).** Two kinds: W (producer, in the
   declared replay flow) whose fresh enumeration writes session key K
   as a side effect, and R (reader) whose fresh enumeration derives
   emitted rows from reading K (ghost stamp). Kinds run in sequential
   phases (W's op before R's). Sync N is all-fresh: W writes K=v1, R
   emits rows stamped v1, seal green. The shipped design has NO
   coupling between sessions and source-cache
   (`source_cache_orchestration.go` has no session awareness;
   `BatonSessionService` is callable during any listing), so every
   cell below runs shipped toggles unless stated. R's connector
   violates NO pinned obligation in any cell — its validator
   truthfully attests R's upstream scope; the session dependency is
   un-attested because no contract clause requires attesting it. That
   missing clause is the scenario's design finding.
   - **7a — write elision (expected RED)**: upstream unchanged for W;
     R runs fresh in sync N+1 (its policy fetches fresh; round-6 F9
     struck the per-kind flow-membership route as unmodeled). W's
     consult → validator match → warm
     replay; W's enumeration — and its session write — is ELIDED, so
     sync N+1's namespace never holds K. R's fresh derivation reads K
     → MISS → emits rows stamped ⊥. Row CONTENT stays on the rows
     table by construction (§3: policies choose verdicts and session
     ops, never row content), so the divergence is carried entirely in
     the embedded ghost stamp. Counterfactual: v1 (an
     all-fresh sync's W would have written it) → **P6-R violation**.
     P1 and P2 are GREEN — every row is individually well-formed and
     every scope consulted; the corruption is invisible to
     content/attestation checks, which is itself a required finding —
     and holds in the real system for the same structural reason
     (fresh rounds have no content oracle; §8).
   - **7b — stale-read replay (expected RED, the dual)**: between
     syncs upstream W-data changes (W fresh in N+1, writes K=v2) while
     R's upstream scope is unchanged (validator match → R warm). R's
     copied rows carry ghost stamp v1; the counterfactual this sync is
     v2 → **P6-R violation**. No elided write anywhere — the writer
     ran fresh; the READER was replayed with rows derived from last
     sync's session state. This is the cell that kills write-only
     bans.
   - **7c — both-warm control (expected GREEN)**: nothing changes
     upstream; W and R both replay; carried stamps v1 equal the
     counterfactual v1. Required so P6-R does not overfit to "replay
     near sessions alarms".
   - **Fix runs**: fix runs re-execute the FULL two-sync script with
     the toggle ON — the toggles are produce-side, so the fix run's
     sync-N artifact DIFFERS from the red run's (the first toggle
     family that acts a sync earlier than the red verdict). Sync N is
     COLD (first sync, no previous artifact) and the taint records
     anyway: replay-capable is flow membership, not warm state (§6
     pins). `sessionTaintWrites` ON — sync N's produce observes
     W's write during a replay-capable phase and marks W's kind
     non-replayable in the artifact; sync N+1 runs W cold (consults
     MISS — degradation, not a loud verdict) → 7a GREEN;
     7b stays RED (R's hazard is a READ) — the residual is a REQUIRED
     finding: the write-only rule is half a fix. `sessionTaintAll` ON —
     R's read during its capable phase (sync N) taints R's kind too →
     7a and 7b both GREEN; 7c runs cold for every session-using kind
     (replay forfeited exactly where sessions are used — the toggle's
     honest price, recorded not hidden). Taint granularity is per KIND
     per artifact (phase attribution — kinds run sequentially, so no
     wire change is needed); taint-to-cold vs loud-reject is a
     severity choice outside the model — the detector is identical.
     Out-of-model enforcement layers of the same detector
     (change-order implementation work, recorded for the calibration
     report): a static analyzer (call-graph from replay-capable
     listing entry points to the session API, suppressible only by
     the §6 opt-out attestation), a pre-release conformance assert
     (per-response `SessionStoreUsage` attribution), and the runtime
     taint itself — one detector, three timings.

Fix-verification runs are bounded-exploration green runs (schedule
budget recorded in the calibration report), not proofs.

Pre-committed classification (round-2 residual risk): out-of-script P1
counterexamples of the cross-attempt fresh-debris shape (fresh pages
commit, crash, mutation, re-fetch unions over debris — no replay
involved) are DESIGN FINDINGS of the shipped walker, not model noise;
P1 must not be weakened to silence them.

## 10. Adversarial review charge (minimum questions)

0. Reachability walk: for each §9 premise, walk the route mechanically
   under §3's semantics (spawn admission, batch draining, stop/crash
   windows, checkpoint contents) and confirm the state is reachable
   without hand-placement; confirm each cell's expected verdict follows
   from §7's definitions (fold legality, P2's consult pinning, P3′'s
   double scoping).
1. Interleaving: did the encoding serialize any calibration-relevant
   pair (carrier drain vs fresh pages; record page vs replacement copy;
   duplicate carriers; H vs G re-runs in 2-crash; crash vs in-flight
   ops across two senders)?
2. Crash/stop: does crash wipe exactly §5's volatile column; is the
   warm flag demonstrably not restored; is the prefix rule (§5)
   enforced; do stop-checkpoints capture exactly the live state the
   stop path captures in code (mid-chain cursors, admitted spawns,
   batch-recorded hits)?
3. Emergence: do the §9 findings arise from stack/batch/stop/checkpoint
   mechanics rather than scenario hand-placement? (Delete each
   finding's trace; check no scenario step writes the corrupt state
   directly.)
4. P1/P3′: is the fold deterministic and total over every §9 log; is
   the publish-time check attestation-only; is P3′'s double scoping
   (mid-attempt mutation, torn rounds) enforced by ghost state rather
   than scenario labeling; is the truthful-validator assumption used
   only inside the trust boundary?
5. Mutation adequacy: each BUILT §6 toggle's kill cell flips as tabled
   (the v11 de-scoped toggles carry no obligation); the
   P1 monitor alarms on 1a/1b-ii/1c, 3A/3B, 4, 5a, 6-naive, and 1d's
   carrier-publishes-LAST schedules (attestation only; publish order,
   not drain order), and stays green on honest delta overlays, 1d's
   remaining schedules, the 6-atomic runs, and the case-3 V-ATOMIC
   re-run (v11 — the subsumption claim); a `scopeLocks`-off 1d
   mutant is content-RED (case 4's dual-replay TOCTOU); 6-overlay is
   GREEN across its whole sub-case family with `oncePerScope` AND
   `scopeLocks` OFF (the structural claim — the same toggles-off
   configuration that turns 1d content-red); 6-overlay-naive is
   content-RED and attestation-RED at seal (entry over an empty fold —
   round-7 F3) and exhibits unbounded P2-counter growth in the
   verification sync with P1 content still red there (stale-AHEAD,
   non-self-healing);    6-overlay-last is content-RED via the w2
   suppression window and must NOT alarm legality on w1's
   cross-attempt re-copy (the complete-rounds counting pin — round-7
   F2); an o-iv-REMOVAL mutant (resume honors a restored mid-chain
   cursor for a unit-mode scope) is content-RED in 6-overlay
   sub-case (b)'s schedule — the mutant collects only the final page
   into an empty buffer and commits a unit missing the first page's
   overlay ops (MS-CO-001, parallel-review F6; the round-5 F2
   precedent); P6-R alarms on
   7a and 7b, stays green on 7c and on fix runs as tabled (7b must
   stay RED under `sessionTaintWrites`); the
   lock-release edge's removal deadlocks §4's retry.
6. Composition enum: does the encoding match the PR-discussion
   semantics, INCLUDING detection-state visibility and mark durability
   across attempt boundaries? Are the 1c and detection-crash findings
   reported as claim boundaries, not suppressed?

## 11. Change-order log

(Append-only after freeze.)

- (pre-freeze) v2: round-1 adversarial review dispositions — see
  `formal/reviews/model-spec-round1.md`.
- (pre-freeze) v3: round-2 adversarial review dispositions — see
  `formal/reviews/model-spec-round2.md`. Headline changes: §9
  re-scripted on the stop-stranding premise generator (round-2 blocker
  1); scenario 2's hard-crash cell inverted to an expected finding
  (blocker 2); P1 fold fully pinned with publish-time checks reduced to
  attestation (finding 3); P3′ torn-round exclusion (finding 4);
  scenario 3 expectations re-derived — P1-attestation, P2 green
  (finding 5); scenario 5 added for warmGate and both produce triggers
  (findings 6, 7); per-attempt G6 bit and compat config with P1 config
  clause (findings 6, 7); oncePerScope toggle marker (finding 8);
  glossary corrected (finding 9); C1 probe script (finding 10); §8
  notes for session-KV durability domain and connector purity (notes
  i–iii).
- (pre-freeze) v4: round-3 adversarial review dispositions — see
  `formal/reviews/model-spec-round3.md`. Headline changes: §4/§9-5b
  corrected to B1's silent-ignore semantics with trigger 2 at install
  time, and 5b's silent scope dropout (green seal, empty partition,
  blocked artifact) promoted to a required design finding with a
  scripted seal-state oracle (blocker F1 + note F7); P1 fold order
  pinned to round completion (F2); action pops commit at completion,
  mid-batch (F3); restart-from-root restated as a
  which-checkpoint-survives property, §5 and glossary (F4); spawn
  dedup extended to all admissions with commit-local duplicate
  rejection (F5); stop-stranding window widened to
  first-atomic-step-incomplete (F6).
- (pre-freeze) v5: round-4 adversarial review dispositions — see
  `formal/reviews/model-spec-round4.md`. Cell 4's premise pinned to
  byte-distinct page tokens encoding one (scope, verdict) (finding 1);
  P1 fold gains the copy-skipped duplicate-overlay no-op clause and
  committed-copies replacement counting (finding 2); produce-blocking
  "two triggers" scoped to the modeled fragment with the excluded
  triggers registered in §1 (finding 3); 5b's CO-6b-005 attribution
  narrowed — the empty-partition dropout is pinned only by the model's
  scripted oracle (note 4); root-tokens-at-loop-tops recorded as a
  modeled-population claim in §8 (note 5); commit-local duplicate
  rejection qualified as same-op-scoped (note 6).
- (pre-freeze) v5 signoff-discussion addendum: P1 torn-round fold
  boundary recorded in §7 (torn completed fresh rounds under
  between-attempt mutation are legal smear the per-round fold would
  false-alarm on; unreachable in all §9 configs; config widening
  requires a fold extension by change order). Surfaced while walking
  the P3→P3′ narrowing during user signoff.
- (pre-freeze) v6 signoff-discussion addendum: overlay-flavor control
  cell 1d added to §9 case 1 (content-green structural protection —
  same-base copies collapse under `oncePerScope`; expected
  attestation-stale-behind finding when the stale carrier's V1 publish
  lands last; flavor-coverage note: token expiry degrades every delta
  connector to the fetch-fresh flavor covered by 1a/1b/1c). §7 P1
  fold extended for totality over 1d logs: self-grounding overlay
  rounds; copy-skipped replacement rounds fold as attestation-only
  no-ops. Scenario 6 added — collect-and-commit design-variant pair
  (V-NAIVE marker-after-copy → expected-red debris union; V-ATOMIC
  inline replay + one {clear, copy, marker, publish} atomic unit with
  the marker as a per-scope store row + marker-suppressed re-execution
  → expected green across the 1a/1b/1c re-runs; deliverable-4 pilot).
  Provenance: user signoff discussion (smear-vs-union attestation
  distinction, copy atomicity, SST-ingest collect-and-commit). The 1d
  attestation edge was caught WHILE SCRIPTING the cell, refuting the
  conversational "every ordering converges" claim. These addenda have
  NOT passed an adversarial round; a targeted spot review before
  freeze is the recommended option.
- (pre-freeze) v7: round-5 targeted spot review of the v6 addenda —
  see `formal/reviews/model-spec-round5-addenda.md`. REJECT, 2 majors
  + 6 minors, all fix-without-re-review per the verdict; reachability,
  fold totality, and 1d's content-green-under-shipped-toggles claim
  otherwise confirmed, no contradictions with rounds 3–4. Headline
  dispositions: page/round COMPLETION pinned to store-op commit
  (announce-visible; transitions and pops are not fold events; a
  round committing no store ops contributes no fold entry), with
  V-ATOMIC's round complete at `eReplayUnit` commit and V-NAIVE's
  marker op a prescribed round op (F1); 1d's protection re-attributed
  to `oncePerScope` ∧ `scopeLocks` and demoted from "structural" to
  mitigation-dependent, `scopeLocks`-off mutant added to §10.5 (F2);
  validator-less-C sub-case re-scripted per schedule class — C-first
  commits the copy, C-later copy-skips, green either way (F3); 1d
  publish placement pinned as two explored sub-configs with
  publish-order ≠ drain-order noted (F4); torn-round boundary
  justification extended to crash-based configs and incomplete-round
  debris named in the fold text (F5); V-ATOMIC's relationship to the
  §5 checkpoint token pinned — hit map still checkpointed but never
  authorizes without a consult, clause (iii) applies to both variants
  (F6); 6-atomic's green claim de-scoped to the 1a/1b/1c re-runs and
  the overlay flavor declared an explicit deliverable-4 obligation
  (F7); P2's consult pin extended to changed-with-diff verdicts and
  1d's P2 expectation stated (F8). Freeze-record notes: partial-copy
  vs torn vocabulary (N1); the self-grounding clause retroactively
  closes a latent v5 fold-totality gap on case 4's locks-on surviving
  round (N2); the flavor-coverage note labeled external-boundary
  commentary (N3); V-ATOMIC's marker-check-outside-lock
  check-then-act window recorded as a 2-worker bake-off hazard (N4).
- (pre-freeze) v8 signoff-discussion addendum: scenario 7 added — the
  sessions × source-cache PRODUCT, a coverage gap both subsystems
  individually modeled but no cell exercised. Shipped design has no
  coupling (`source_cache_orchestration.go` session-blind;
  `BatonSessionService` callable during any listing; sessions
  sync_id-scoped so each sync's namespace starts empty). Cells: 7a
  write elision (warm producer's elided enumeration never writes K;
  fresh reader derives from the miss), 7b stale-read replay (fresh
  producer rewrites K; warm reader's copied rows carry last sync's
  stamp — the dual that kills write-only bans), 7c both-warm control
  (green). New property P6-R (replay-session coherence): embedded
  session stamps vs a counterfactual session ghost (scripted-policy
  determinism + sequential kind phases make it computable — §8 note);
  ghost stamps travel with copied rows. New PROPOSED toggles
  `sessionTaintWrites` (partial — flips 7a only, 7b residual is a
  required finding) and `sessionTaintAll` (isolation — flips both;
  cost: replay forfeited for session-using kinds, recorded). Glossary
  gains Elision and Session taint. Provenance: user signoff
  discussion — the elided-session-write question, refined through the
  read-side dual to per-kind isolation; taint-to-cold vs loud-reject
  recorded as a severity choice outside the model. NOT yet
  adversarially reviewed; targeted round-6 spot review required
  before freeze.
- (pre-freeze) v9: round-6 targeted spot review of the v8 addendum —
  see `formal/reviews/model-spec-round6-scenario7.md`. REJECT, 4
  majors + 5 minors + 3 notes, all fix-without-re-review; every cell
  verdict and all six shipped-system claims verified clean. Headline
  dispositions: the KIND axis declared in §1 as an (op, scope) pair —
  storage row kinds unchanged, small-scope row added, §8 note updated
  (F1); P6-A's domain pinned to stamps read within the sealing sync —
  traveled and ⊥ stamps belong to P6-R; P6-A vacuously green on
  7a/7b/7c (F2); taint durability pinned checkpoint-cadence in §5/§6
  with the self-healing-under-re-execution argument claimed
  explicitly (F3); REPLAY-CAPABLE pinned as flow membership
  independent of warm/cold — the taint records in cold attempts,
  which the 7a fix run depends on (F4); consume side pinned as
  lookup-miss degradation in §3 (F5); P6-R scoping aligned to
  between-sync-only mutation (F6); counterfactual pinned to the
  producer's phase-final value with empty-namespace evaluation and
  reader-timing independence stated (F7); 7a's ghost-only divergence
  stated plus the §8 under-approximation note (F8); 7a's unreachable
  flow-membership premise route struck (F9); modeled session surface
  registered as Get/Set with WRITE covering all mutating ops in
  production (F10); two-sync fix-run re-execution stated (F11); §3
  `eCopyScope` ghost tuple extended for stamp travel (F12). Folded in
  from the same signoff discussion: the capability-level OPT-OUT
  (attested emission-irrelevance; dishonest opt-out ≡ 7a/7b, honest
  green by definition) and the out-of-model enforcement layers
  (static analyzer, pre-release conformance assert, runtime taint —
  one detector, three timings).
- (pre-freeze) v10: overlay-flavor extension of the atomic-unit
  variant, per user direction at signoff — the walker-side pilot now
  covers the changed-with-diff flavor rather than deferring it wholly
  to deliverable 4. §9.6 gains V-OVERLAY-UNIT (unit boundary = the
  consult VERDICT's prescribed work: base copy + all overlay pages +
  marker + publish(V_to) in one atomic op; per-page ops buffered in a
  volatile collect buffer, §5 row added; publish deferred into the
  unit — B5 early publish not exercised; marker-absent resume
  restarts from consult, mid-chain cursors ignored for unit-mode
  scopes), cell 6-overlay (expected GREEN across the re-scripted 1d
  premise family with `oncePerScope` AND `scopeLocks` OFF — the
  structural claim 1d could not make; folds as §7's existing
  self-grounding overlay, no new fold clause), and cell
  6-overlay-naive (expected RED — marker+publish at the consult
  boundary, crash before the final overlay page; marker suppression
  seals base(e1)+partial-overlay debris under entry V2, P1 content
  violation, and the verification sync shows unbounded P2 growth:
  stale-AHEAD is the non-self-healing direction, answering the
  hazard scenario 6 had named and deferred). 1d cross-referenced;
  §10.5 obligations extended. NOT yet adversarially reviewed;
  targeted round-7 spot review required before freeze.
- (freeze revision) v11: round-7 targeted spot review of the v10
  addendum — see `formal/reviews/model-spec-round7-overlay.md`.
  REJECT, 3 majors + 2 minors + 2 notes, all fix-without-re-review;
  no cell verdict overturned; fold treatment, structural claim,
  crash-window constructibility, and all shipped-system claims
  verified clean. Headline dispositions: (o-i) generalizes BOTH
  halves of V-ATOMIC's clause (i) — page transitions in a verdict's
  prescribed work commit only at unit commit, so stop checkpoints
  hold the consult-page token and o-iv's restart-from-consult is
  derived; sub-case (b)'s premise corrected (F1); the third-placement
  dismissal replaced with cell 6-overlay-last — the overlay
  re-verdict CLEARS unmarked debris so no reduction to 6-naive
  exists; its w2 (marker-committed, publish-lost suppression) is the
  P1 witness and its w1 pins replacement counting to committed copies
  within COMPLETE rounds (F2); the P1 fold's empty case pinned —
  initial value is the empty partition, an entry over an empty fold
  is an attestation violation outright, 5b's "vacuously green"
  reworded (F3); the unit's publish constituent conditional on a
  supplied validator — publish-less units leave no entry,
  B5-consistent (F4); the publish-timing claim reworded as a
  runtime-side B3/B5 deviation (change-order scope if adopted),
  connector-invisible via the previous-artifact-only lookup surface,
  "whichever page carries it" (F5); the two-worker marker-race note
  extended with the widened collect-phase window, legality-only alarm
  (F6); 6-overlay-naive's P2 wording corrected to the resident-row
  staleness counter with the verification sync's P2 seal clause green
  and P1 content still red there (F7). Bundled de-scope edits (user
  signoff): `compositionEnum` and `annotationBinding` marked
  DE-SCOPED — documented as reviewed design records, not built in P,
  kill obligations removed (§6/§10.5); their fix duty is discharged
  by the atomic unit as the single fix story, witnessed by the §9.6
  re-runs incl. the NEW case-3 re-run extension (V-ATOMIC subsumes
  annotation binding: no carrier, no annotation, markers live in the
  current artifact, restored hits inert). FREEZE: this revision is
  the diffable baseline; subsequent changes are MS-CO-NNN change
  orders, never silent edits.
- MS-CO-001: dispositions for the PARALLEL round-7 review
  (`formal/reviews/model-spec-round7-overlay-parallel.md` — a second
  independent spot review of the same v10 addendum, surfaced after
  the v11 freeze; 1 major + 5 minors + 4 notes). Its major (empty
  fold) and two minors (validator-less publish; publish-timing
  wording) were independently found by the primary review and were
  already fixed in v11 — the double-hit confirms those pins. Newly
  applied here: `eReplayUnit`/`eOverlayUnit` and the marker ops
  registered in §3's MStore op list as scenario-local, with a §4
  pointer to the variant override (F5) and a §5 durability row for
  the marker (N1); o-i's transition deferral explicitly scoped per
  verdict, making the resume rule well-defined for multi-scope
  chains (F4 — v11's deferral pin already dissolved the ignore-rule
  ambiguity the finding targeted); the collect pinned as the 2-page
  ROUND (N4); and a NEW §10.5 kill obligation — the o-iv-removal
  mutant is content-RED in 6-overlay sub-case (b) (F6), built as a
  model cell. DISAGREEMENT OF RECORD: the parallel review judged the
  third-placement reduction SOUND where the primary called it false
  (round-7 F2); the built cell 6-overlay-last settles it
  mechanically for the primary — the w2 marked-entry-less window is
  red in a shape that is NOT 6-naive's union class, and w1 exercises
  exactly the legality-counting gap the parallel review itself
  flagged as its N3 tripwire. Its F3 (degenerate publish-placement
  axis) is subsumed by v11's F5 rewording ("whichever page carries
  it"); noted here that both B5 token placements collapse into the
  single unit publish by construction.
- MS-CO-003: the record-round grounding tranche. The code base
  shipped a DIFFERENT fix for the scenario-1 tc1c flavor than the
  fix family this model verified (V-ATOMIC / V-OVERLAY-UNIT):
  record-round grounding (`groundRecordScope` +
  `ClearSourceCacheScope` — a record round's first write to a scope
  this attempt clears a partition holding rows with no published
  manifest entry this sync). This change order closes the model-side
  gap: `recordGrounding` encodes the shipped rule FAITHFULLY,
  including its published-entry skip and its
  once-per-scope-per-attempt volatile grant (realized as the
  fresh-round page-0 placement: crash-restarts re-decide, stop-
  resumes do not re-ground); the store op `eGroundScope` commits the
  check-and-clear atomically (the real check and clear run under the
  scope lock inside the destination batch). Registrations: §3 store
  op (scenario-local), §6 toggle rows (`recordGrounding` and the
  `groundValidatorBound` candidate), §10.5 kill obligations, and
  six §9 scenario-1 cells (the ladder). ADJUDICATION: the toggle's
  kill pair is the validator-less flavor (tc1cNoPub red →
  tc1cNoPubGround green); the faithful shipped design remains RED
  (tc1cGround_P1/P2) — a REGISTERED RESIDUAL found by this change
  order, not seeded: a replay round that completed and PUBLISHED
  before a crash is skipped by grounding, and a verdict-flipped
  re-run then accumulates its record listing over the completed
  replay's rows (phantom union under the fresh validator; the
  calibration log carries the audited trace and the real-code
  reachability argument). The `groundValidatorBound` candidate
  (also clear when the published entry's validator differs from the
  record round's incoming validator) greens both properties
  (tc1cGroundV_P1/P2) and is PROPOSED, not shipped: the naive form
  is unsafe for multi-contributor collection scopes.
