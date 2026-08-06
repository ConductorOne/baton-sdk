# RFC 0007: Scheduler cursor accounting — bound width and bytes, not history

Status: phase 1 (hotfix) in progress · phases 2–3 planned
Risk routing (REVIEW_CHECKLIST §2): parallel scheduler + checkpoint state —
silent/combinatorial subsystem, declared hot path. Phase 1 is a minimal
hotfix with targeted verification and explicitly named test skips; the full
BUG_CATCHING step-up runs with phase 2.

## 1. Incident and root cause

A large tenant's sync has failed for several days with:

    spawned cursor batch exceeded the maximum of 100000 unique cursors

The failing check (`pkg/sync/parallel_syncer.go`, `transition()`) compares
`len(q.seen) + len(keys)` against `maxSpawnedCursorsPerBatch` (100k). `q.seen`
is a **batch-lifetime** set of every unique cursor identity ever encountered:
every spawned sibling *and every ordinary pagination continuation* of every
action adds a permanent entry; nothing is removed when work completes.

The cap was intended to bound fan-out **width** (in-flight actions, which is
what drives checkpoint token size and memory — see
`checkpoint_cost_bench_test.go`). It actually bounds **cumulative unique work
per batch**. A batch that processes >100k pages+spawns over its lifetime fails
deterministically on every retry, even with only dozens of actions in flight
at any moment. Type-scoped fan-out (one spawned cursor per resource, e.g. the
experimental Entra enterprise-application planners) crosses the threshold on
large tenants in ordinary operation.

### Prior behavior (the world in which prod worked)

Before the Phase 5 scheduler work (#1029) there was **no identity machinery
at all**: no dedup, no cycle detection, no cap, anywhere in the pipeline. A
cyclic token chain of any period (including period-1) spun until run-duration
expiry and was treated as a connector bug. Width was uncapped: whale-scale
per-resource fan-outs marshalled unbounded action sets into every checkpoint
for years. The identity/dedup machinery and its tests (e.g. the cyclic
termination pinned by
`TestChaosConnectorCyclicPageTokensTerminateWithoutDuplicateRows`) shipped
in the same body of work as the cap; they pin new obligations, not prior
prod behavior — so the hotfix regresses nothing prod relied on by removing
them.

Two adjacent design flaws are recorded here and fixed in phase 2:

- `state.spawnedAdmitted` retains a 32-byte digest + action-ID string per
  spawned admission for the **process lifetime**, by design. Same
  unbounded-accumulation shape, in state form. Untouched by the hotfix.
- Nothing bounds **accumulated in-flight page-token bytes**. Per-response caps
  exist (1024 tokens, 1 MiB/token, 16 MiB/response), but a connector may
  legally hold 100k cursors × 1 MiB tokens outstanding — resident in
  `state.actions` and re-marshalled into the checkpoint token every ~10–20 s.

## 2. Design principle: observe, never predict

Whether an action will retire or spawn more work is **runtime data owned by
the connector**: `EnqueuePageTokens` is a per-response annotation; any
grants/entitlements action may attach it to any page; spawned children may
themselves spawn (covered by `checkpoint_cut_test.go`). No static
planner-vs-leaf classification exists, so no mechanism below relies on one.

- Accounting is incremental and exact at each `transition()`: width delta is
  `+len(admitted children)`, `−1` if the parent's continuation is empty
  (retire), `0` otherwise. No prediction needed.
- Phase 3's parking gates on *observed* outstanding. Overshoot is bounded by
  protocol, not prediction: one response admits ≤ 1024 children, so peak
  width ≤ high watermark + workerCount × 1024.

## 3. Phase 1 — hotfix: delete the queue's identity machinery

The hotfix is a deletion that restores the pre-identity-machinery posture
prod ran on for years: **remove `q.seen` and the cap computed over it**.
The queue performs no cross-commit identity accounting of any kind — no
batch-lifetime seen set, no cap, no dedup skip, no continuation
re-convergence, no cycle detection. Cycle detection is explicitly a
non-goal here: a cyclic continuation chain is a connector bug that runs
until the run-duration budget expires, exactly as before the machinery
existed.

What remains, deliberately:

- **The commit-local duplicate check** (`keys`, discarded per transition):
  the same cursor twice within ONE response is a loud protocol violation.
  Zero accumulation, and its tests pass unchanged.
- **`state.transitionAction`'s `spawnedAdmitted` guard** (untouched code):
  spawned re-mentions are still skipped process-wide, so spawn cycles
  still terminate — at worst one extra idempotent re-run when the first
  bearer of an identity was not spawned.
- Abort semantics, I10 drain evidence, per-response spawn validation.

Consequences, accepted: re-convergent continuations re-walk pages
(idempotent duplicate work); continuation cycles of ANY period spin until
the budget; queue memory is zero per-identity (strictly less than main).

### Mechanical notes

- Deleted from `transition()`: the seen-set seeding, the re-convergence
  finish, the re-mention skip, and the cap rejection, with the RFC named
  in the function's doc comment. The constant stays as a benchmark
  fixture only.
- Skipped with a reference to this RFC (they pin the removed behaviors):
  `TestParallelActionQueueRejectsCursorLimitBeforeCommit` (the cap),
  `TestSyncParallelBreaksCyclicSpawnedCursorIdempotently` (queue-level
  spawn-cycle break; the state layer still terminates it one re-run
  later), `TestContinuationReconvergenceFinishesParent` (re-convergence
  finish), and `TestChaosConnectorCyclicPageTokensTerminateWithoutDuplicateRows`
  (period-2 continuation cycle termination).
- The interleaving stepper drops the seen set from its state signature,
  and its two scenarios pinning seen-set behavior ("re-mention-and-cycle",
  "continuation-reconvergence") are removed until phase 2: the stepper's
  fake commit has no state layer, so without the seen set a spawn cycle
  would spin forever there, unlike production.

### Verification (hotfix scope)

Full `pkg/sync` suite green with exactly the named skips above; the
same-commit protocol tests
(`TestSpawnedCursorCannotCollideWithParentContinuation`,
`TestFailedSiblingAdmissionDoesNotAdvanceParentCursor`) pass unchanged.
Cost contract: strictly less work and memory per transition than main;
checkpoint shape untouched.

### Parked: the working-set implementation (branch `kans/drop-scheduled-sync-actions-limit`)

A first hotfix attempt replaced lifetime accumulation with working-set
dedup (`q.live`/`q.identOf` over outstanding actions only, retirement via
`transition()` and `done()`, the audit checker's C4 moved to
concurrently-live semantics, the interleaving stepper modeling the state
layer's `spawnedAdmitted` guard). It is correct and verified — incident
regression at 100_001 cursors, all scheduler/stepper/audit suites green —
but it *adds* an inverse-index and audit machinery where a hotfix should
only delete. It is parked on the branch above and folds into phase 2
item 1, to be reshaped by the simplification item (e.g. the Action carrying
its own identity digest deletes `identOf` outright) rather than merged
as-is.

## 4. Phase 2 — bounded accounting redesign

Everything time-decoupled from the incident, with the full step-up process:

1. **Working-set identity tracking**: reintroduce the dedup the hotfix
   deleted, as a map over *outstanding* actions only — add on admit, swap
   on continuation, remove on retire — restoring concurrent-duplicate
   gating, live re-convergence, and period-1 self-loop detection at
   O(width) memory instead of the removed set's O(cumulative unique
   cursors). Implemented and verified on the parked branch
   (`kans/drop-scheduled-sync-actions-limit`: `q.live`/`q.identOf`,
   `done(action)` retirement, C4 moved to concurrently-live semantics,
   stepper models `spawnedAdmitted`); reshape it through item 7 before
   merging — the parked shape adds an inverse index and audit plumbing that
   the simplification should dissolve (e.g. the Action carrying its own
   identity digest deletes `identOf`). Semantic delta, unchanged from the
   parked branch: a continuation onto a *retired* identity continues
   (idempotent duplicate work) instead of finishing the parent.
2. **Width cap on outstanding** (100k, the renamed constant): pre-commit
   rejection in `transition()`. Bounds checkpoint marshal cost and all
   O(width) structures.
3. **In-flight token byte budget** (~128 MiB, confirm against production
   token sizes): running sum over outstanding actions; pre-commit rejection
   directing connectors to chain spawns / shrink tokens.
4. **Brent's cycle detection per lineage** (in-memory, O(1)/action):
   restores prompt period ≥ 2 cycle termination — warn and drop the edge
   (finish parent / skip child), same graceful semantics as the old
   re-convergence path. Not serialized; re-anchor on resume (restarting
   inside a cycle is the best case). Re-enables the phase 1 skips that pin
   prompt cycle termination, including the chaos cyclic-page-token test.
5. **Persisted depth ceiling** (`Action.Depth uint64`, `omitempty`, ~10M):
   inherited+1 across continuation and spawn edges; monotonic across
   resume; classified hard failure naming the lineage. Catches
   endless-unique-token runaway and cross-batch mutual re-admission. Sized
   against chain length, not batch size. Placed in `state.transitionAction`
   (the shared path), which also covers the sequential root listings that
   have never had runaway protection.
6. **Remove `state.spawnedAdmitted`**: re-mentions of retired spawns re-run
   idempotently; termination owned by the depth ceiling. Removes the last
   process-lifetime accumulator.
7. **Simplify the abstractions.** The scheduler carries more machinery than
   the problem now needs, and it accreted rather than being designed:
   dedup/termination is split across two layers with different lifetimes and
   key types (the queue's per-batch set, `spawnedAdmitted`/`spawnedInFlight`
   per process in state — item 6 removes one, but the queue/state split
   itself deserves a hard look); `transition()` threads a commit callback
   through three files to keep two mutexes coordinated; identity is computed
   in one layer and re-derived in another. Deliverable: name the
   load-bearing concepts (working set, identity, retirement, admission) once
   each, collapse layers that exist only for historical reasons, and delete
   anything obsoleted. A reader should be able to answer "who owns
   termination?" and "who owns dedup?" from one file. This is a refactor
   gate for the rest of phase 2: land it before or with items 1–5 so the new
   accounting attaches to the simplified seams, not the accreted ones.
8. Whole-store equivalence oracles for cycle/re-mention scenarios; mutation
   adequacy on the accounting arithmetic; randomized-topology soak with the
   width bound as oracle; permutation pass over
   (retire/continue/spawn × cap states); compat harness both directions
   (token gains `depth`); independent evidence audit.

## 5. Phase 3 — watermark parking (lazy fan-out)

With phase 2's width cap, width ≈ plan size for planner-shaped connectors,
because the worker loops a planning action to completion (`syncOneAction`)
while the plan fans out — the cap is effectively a limit on
resources-per-type (an Entra-shaped tenant with >100k apps would
legitimately hit it).

Parking removes that coupling:

- High/low watermarks on outstanding. Over the high watermark, a transition
  commits the parent's continuation as a *parked* queued action (tail)
  instead of returning it to the looping worker; parked continuations resume
  below the low watermark.
- **Liveness**: head-of-line exemption — the oldest queued action always
  runs; the queue cannot wedge with everything parked.
- **Overshoot bound, no prediction**: width ≤ high watermark +
  workerCount × `maxEnqueuePageTokensPerResponse`.
- **Resume-safety**: a parked continuation is an ordinary checkpointed
  action holding its page token; no new token state.
- Peak width and checkpoint cost become independent of plan size; the cap
  becomes a buffer size, not a scale limit. Revisit whether the constant can
  then *shrink*.

### Ordering and the continuation-immediacy contract

**Phase ordering is an explicit invariant**: all resources complete before
entitlements, entitlements before grants (referential integrity — I7/I8
depend on it, and grants planning reads stored resources). It is enforced
structurally and must remain so: (1) the action stack layers phase roots
(grants below entitlements below resources); (2) `PeekMatchingActions` takes
only a consecutive same-op prefix, so every parallel batch is single-phase by
construction; (3) `syncParallel` returns only at `outstanding == 0`, so a
batch — including every spawned cursor and every parked continuation — fully
drains before the next phase's root can surface. Parking reorders work only
*within* one batch's queue; a parked continuation still counts in
`outstanding` and cannot outlive its batch. Phase-3 test obligation: assert
batch containment of parked continuations at the queue-audit batch-end event.

**Parent→child resource ordering is per-edge causal, not phase-like**: a
child listing action is created only by processing its fetched parent
(`childResourceActions` reads `ChildResourceType` annotations off the stored
parent; exactly-once via the I4 `childSchedule` record), so parent-row-before-
child-work holds by construction under any scheduling policy. The stronger
"all parents before all children" has never held in either mode (sequential
sync is depth-first via the LIFO stack; parallel workers interleave child
listings with later parent pages) and is not a contract.

Within a phase, parking changes execution order, and the three ordering
properties have different contract status:

- **Cross-action order**: never promised — parallel workers already
  interleave arbitrarily. Parking changes nothing a connector could validly
  observe.
- **Within-chain sequentiality**: page N+1 never runs before or concurrently
  with page N. Fully preserved; a parked continuation remains the unique next
  step of its chain.
- **Within-chain immediacy**: page N+1 runs promptly after page N. **This is
  what parking breaks.** Server-side pagination state expires (Graph
  nextLinks/delta links, LDAP paged-result cookies, DB cursors); today's gap
  is milliseconds, a parked gap is "drain the fan-out first".

Immediacy is already not formally guaranteed — run-duration expiry
checkpoints mid-chain and resumes the token minutes or hours later in a cold
process, and connectors nominally must tolerate that. But parking would turn
that rare edge path into the routine hot path: a de facto contract change
even though the de jure contract permits it. Spawned-cursor tokens are
already delay-tolerant in practice (children queue behind the whole fan-out
today); it is specifically *continuation* tokens that currently enjoy
immediacy.

**Decision: parking is opt-in, declared by the connector** — a marker
asserting its continuation tokens are durable/self-contained and may be
deferred freely (shape TBD: per-resource-type annotation vs. a field on
`EnqueuePageTokens`). Default scheduling semantics are unchanged for
undeclared connectors; they keep today's immediacy and today's width
behavior. Connectors built for large fan-out (the type-scoped planners) opt
in to get plan-size-independent width.

Phase 3 carries the full BUG_CATCHING step-up: frozen behavioral plan,
implementation-obligation addendum, mutation adequacy over the parking
arithmetic, randomized-topology soak with the width bound as oracle, chaos
stage-4 budget assertions, cross-version compat pair, independent evidence
audit.

## 6. Rejected alternatives

- **Raise the 100k constant**: linear checkpoint marshal cost for everyone;
  moves the wall without fixing lifetime-vs-width conflation.
- **Prune `seen` on completion only, keep it as the cap metric**: still
  conflates the cap with history; and cycle termination for period ≥ 2 is
  lost anyway — dropping the history explicitly (hotfix) is more honest.
- **Bloom/sketch as a gate**: false positive silently skips legitimate work —
  silent omission, worst failure class.
- **XOR-of-hashes replay detection**: no membership query; duplicates cancel
  — destroys exactly the signal needed; useful only against an independent
  reference multiset, which doesn't exist here.
- **Probabilistic detectors (rotating Bloom, exact window) as warn-only**:
  nothing may depend on a detector that fires timing-dependently; once the
  deterministic core exists they reduce to a sometimes-log-line. Revisit the
  exact window only with evidence that cross-lineage duplicate work is a real
  cost.
- **Global persisted transition counter** instead of per-lineage depth: must
  be sized against total batch work (tension with huge legitimate tenants);
  worse diagnostics.
- **Backpressure by blocking `transition()`**: producers-only deadlock;
  parking (phase 3) is the non-blocking form.

## 7. Open questions

- TODO (investigate only): stop holding the entire action working set in
  memory. `state.actions` + the batch queue keep every queued action fully
  resident — connector-controlled size, potentially GBs at spawn fan-out with
  large tokens — yet queued-but-not-started actions are cold data: only
  in-flight actions (≤ workerCount) plus a small ready buffer need RAM.
  Candidate: spill the queued tail to the store we already have
  (pebble/c1z), page it back in as workers drain. Interactions to assess:
  checkpoint marshal currently reserializes the whole map every ~10–20 s
  (spilling could make checkpoints incremental instead of O(width));
  phase 3 parking reduces the pressure by bounding width but does not
  remove the per-entry cost; both the queue and state layers hold the set
  today. No commitment — scope and cost/benefit first.

- Byte-budget constant (phase 2): 128 MiB is ~2 orders of magnitude above
  observed legitimate fan-out (100k × ~100 B tokens ≈ 10 MB). Confirm
  against the largest known checkpoint tokens in production before freezing.
- Should cross-op children (rare; not part of the same-op queue) count
  against the phase-2 byte budget via a state-level sum?
- Whether phase 3's low watermark should be worker-count-derived or fixed.
- Shape of the phase-3 parkability declaration: per-resource-type annotation
  (static, validated at registration like TypeScopedGrants) vs. per-response
  field on `EnqueuePageTokens` (dynamic, finer-grained). Registration-time
  validation favors the annotation.
