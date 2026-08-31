# RFC 0011: Demand-graph sync runtime — variant S over the shared chassis

Status: kickoff draft — design mapping and verification plan for review;
no code yet.
Risk routing (REVIEW_CHECKLIST §2): scheduler + checkpoint + storage
semantics — silent/combinatorial subsystem, HIGH; the full BUG_CATCHING
step-up applies to every implementation phase. This RFC implements the
verdict of the formal effort (`formal/REPORT.md`); the design itself was
adversarially reviewed and model-checked before this document existed
(`formal/GRAPH_MODEL_SPEC.md` v4 frozen, `formal/graph/BAKEOFF.md`).

## 1. Motivation

The current tiered walker composes per-scope artifacts (fresh rounds,
source-cache replays, overlays) by position in an action queue.
The formal calibration model reproduces the known failure classes of
that design mechanically: the phantom union (individually truthful
responses composing into a false sealed artifact), session laundering
(a reader embedding a dead writer's value), and the artifact-swap
rebind hole (`formal/walker/CALIBRATION.md`, scenarios 1–3). Phase 6b's
mitigations close the calibrated instances; the classes remain
structural, because nothing in the runtime ties an artifact to the
premises it was derived from.

The demand-graph runtime replaces positional composition with tracked
derivation: work is admitted by demand edges, every output carries its
lineage, and seal-time obligations are checked against a closure
oracle rather than assumed from queue completion. Two lineage variants
were modeled and baked off; the registered decision rule selected
**variant S — observable-causal stamps** (`formal/graph/BAKEOFF.md`).
This RFC maps that verdict onto `pkg/sync`.

## 2. What is being built (the frozen mechanism inventory)

From the spec's frozen tally (`formal/GRAPH_MODEL_SPEC.md` §7.5):

**Shared chassis** — frontier checkpoint carrying the
admitted-derivation set (death semantics, admitted-by edges, cursors);
forced resume checkpoint; generation table with the bump rule,
quiesce-before-bump, and the total mint fence; premise-validated
markers (premise digests, publishBearing, adopt-or-re-derive under
MATCH-only and writer-ineligibility); the supersession matrix and
poison rule; the seal-time sweep with the closure oracle; atomic
units.

**Variant S adds** — a per-output stamp field with the `eAdopt`
rewrite; stamp merge on read; three observation points (dispatch-time
refusal, session-read read-through with the dead-read count, the
pre-seal pass with an iteration budget); optional bucketed
compression, admissible only under the G9-CAL-1 minting rules.

Not built: everything on E's bill (durable edge checkpoint rows, the
∀-pending-purge, support rebuild + agreement check, the retraction
queue), and the constrained session primitives as a correctness
requirement — under S they demote to a stamp-width optimization
(BAKEOFF design consequence 1).

## 3. Model-to-Go mapping (proposed referents)

The model speaks of nodes, output keys, and generations. The proposed
concrete referents, chosen to coincide with the partition granularity
phase 6b already established:

| model concept | proposed Go referent |
|---|---|
| node | a scoped derivation task: one (action kind, rowKind, scopeKey) instance — the unit the tiered walker already dispatches |
| output key | the (rowKind, scopeKey) partition — the granularity of `sourcecache` replay, validators, and the trace oracle |
| generation | new durable table in the c1z store: (node, gen) rows with the mint fence; minting rides the existing checkpoint commit |
| frontier checkpoint | extends the existing checkpoint token (`pkg/sync` state marshalling): admitted-derivation set + admitted-by edges + cursors replace positional queue state |
| premise-validated marker | generalizes the source-cache manifest entry (`PutSourceCacheEntry` validators are proto-markers today); adds the premise digest and publishBearing bit |
| atomic unit | generalizes the 6b replay unit (clear + copy + marker + publish as one store transaction) to all derivation commits |
| per-output stamp | new column on partition rows or a per-partition sidecar (open question 2); merged on read, rewritten by adopt |
| dispatch-time refusal | frontier scheduler gate, replacing queue-order dispatch |
| session-read read-through | the session store consults the generation table on read; dead reads counted, not blocked |
| pre-seal pass | `EndSync` extension: bounded staleness chase over the demand closure before seal |
| closure oracle | seal-time check that every demanded output is present and stamped live — the sweep's authority |

## 4. Strategy: sibling runtime behind a capability, not in-place surgery

The tiered walker with 6b mitigations is shipping and calibrated. The
proposal is a parallel scheduler path selected per-sync (capability or
config flag), reusing the store, the connector protocol, and the
source-cache annotation surface unchanged — the demand graph changes
WHO dispatches work and WHAT lineage it records, not the wire contract
or row storage. Rationale: the runtime's correctness argument is
holistic (chassis mechanisms interlock; the spec's kill cells show
single-mechanism removals are silently unsafe), so incremental
in-place evolution would transit through states the model says are
broken. Rollout gates on dual-run conformance (§6).

## 5. Design consequences carried from the bake-off

Verbatim obligations from `formal/graph/BAKEOFF.md`:

1. Constrained session primitives are an optimization (stamp width),
   not a correctness gate; free-form session reads are safe under S.
2. Upstream validators and node generations get the same
   observation-point discipline — consult at observation time.
3. Compression, if shipped, ships only with the G9-CAL-1 minting
   rules (as first drafted it livelocked honest histories).
4. The dying-reader race kill needed feedback-PCT and a third worker
   to exhibit: implementation tests for that race MUST use perturbed
   or priority-based schedules, not uniform random chaos.

## 6. Verification plan (the bridge stays load-bearing)

- **Trace oracle first.** Extend the canonical vocabulary
  (`formal/occult/src/sync_trace_policies.occult`,
  `formal/occult/TRACE_BRIDGE.md`) with generation stamps and
  admission events BEFORE the runtime lands; the recorder pattern
  (`pkg/sync/sync_trace_audit.go`, nil in production) extends as-is.
  Every implementation phase exports fixtures; the oracle judges
  them. Note the engine's current trace-length ceiling (ask 8 of the
  Occult engine brief) bounds fixture size until fixed.
- **Refimpl as executable spec.** The demand-graph reference
  implementation (`formal/occult/host/refimpl/`) already produces
  oracle-green traces; production traces must be conformant to the
  same policies, and divergences route to whichever side is wrong.
- **Chaos parity.** The 6b chaos scenarios re-run under the new
  runtime; crash schedules must include the perturbed-schedule
  requirement from §5.4.
- **Dual-run conformance gate.** Same connector dataset, both
  runtimes, sealed artifacts compared row-for-row (the `diff`
  machinery exists). Ships only behind N green dual-runs on the
  chaos corpus plus real connector fixtures.
- **Review routing.** Every phase is HIGH; BUG_CATCHING's step-up is
  the floor, and the model's kill cells double as the adversarial
  test inventory (each calibrated mutant names a regression test the
  implementation must carry).

## 7. Open questions (to resolve in review before phase 1)

1. **Node identity vs dynamic fan-out.** The model's nodes are
   static; real syncs spawn scoped actions dynamically
   (`EnqueuePageTokens`, RFC 0007). Does a spawned child admit under
   its parent's demand edge, and is the admitted-derivation set's
   death semantics compatible with cursor-spawned siblings?
2. **Stamp storage.** Per-row column, per-partition sidecar, or
   in-manifest? Width is unmodeled (BAKEOFF honest limits); measure
   before choosing. Compression is the pressure valve, not the plan.
3. **Pass budget at scale.** The 3-iteration pre-seal budget is a
   small-scope declaration. Production policy on budget exhaustion
   (fail the sync loud vs degrade) needs an explicit decision —
   the model says exhaustion with dead stamps is a mechanism bug,
   which argues for loud failure.
4. **Generation-table growth.** Mint-per-bump with sync-scoped
   lifetime suggests table truncation at seal; confirm no
   cross-sync reader (warm consults read markers, not generations).
5. **Coexistence window.** How long do both runtimes ship? The 6b
   mitigations stay calibrated in the walker model for as long as
   the walker ships.

## 8. What this RFC is not

Not a phase plan with estimates (that follows once §7 resolves), not
a storage-schema spec (open question 2 gates it), and not a promise
that the model's guarantees transfer to code — the model arbitrates
the design; the trace oracle and dual-run gates are what tie the
implementation to it (`formal/REPORT.md`, "What is guaranteed").
