# Lineage bake-off verdict: eager edges (E) vs observable-causal stamps (S)

This is deliverable 4's written recommendation — the artifact the
demand-graph RFC cites. It is assembled under the decision rule
registered in `formal/GRAPH_MODEL_SPEC.md` §12 GS-CO-005 (declared
before the bake-off runs; the rule's v2 text was never committed and
GS-CO-005(a) repairs that provenance), from:

- the frozen 66-cell calibration matrix (10k schedules per cell,
  zero mismatches — `CALIBRATION.md`, freeze sweep 2026-08-30);
- the 12-cell bake-off phase (GS-CO-005(c)/(d) declarations, 12/12
  match — `CALIBRATION.md`, bake-off results);
- the frozen mechanism tally (spec §7.5, amended v3 BEFORE any
  bake-off run).

The decision rule is lexicographic — property satisfaction, then
mechanism count, then redo work — and the first non-tie decides.

## The candidates

Both variants ride the same shared chassis (spec §7.5 "shared"):
frontier checkpoint with the admitted-derivation set (death
semantics, admitted-by edges, cursors), generation table with
quiesce-before-bump and the total mint fence, premise-validated
markers (MATCH-only, writer-ineligible), the supersession matrix and
poison rule, the seal-time sweep with the closure oracle, and atomic
units.

- **Variant E (eager edges)** adds: durable admitted-by edge
  checkpoint rows, the resume ∀-pending-purge, derived-support
  rebuild plus the agreement check, the retraction rule and queue
  with pinned enqueue/drain — and it REQUIRES session variant B
  (the three constrained session primitives).
- **Variant S (observable-causal stamps)** adds: stamp merge on
  read, the per-output stamp field with the `eAdopt` rewrite, three
  observation points (dispatch-time refusal, session-read
  read-through with the dead-read count, the pre-seal pass with a
  3-iteration budget), and optional bucketed compression.

## Axis 1 — property satisfaction: TIE

Both variants, with their mechanisms on, are GREEN on every honest
cell of the freeze matrix, and every declared kill fires on its
designated monitor (P1-LEGALITY, P-GEN, P-ADOPT, P-MARK, P4-STUCK,
P5-UNDER/OVER, P6-G, SEAL-EXPECT, DEAD-DISPATCH, plus P6-E under E
and P6-S/GPASS under S). Neither variant is eliminated.

The one structural asymmetry Axis 1 surfaces: **E is correct only
with session variant B.** Under free-form sessions (E+A) the sealed
reader rows embed a dead writer's value with every artifact-level
oracle green — the calibrated laundering find (`tcG2ea_P6G`, the
G2 family's star cell). S is green under free-form sessions
(`tcG2s_All`): stamp merging makes arbitrary reads tracked without
constraining the session surface. Per GS-CO-005(b) this dependency
travels to Axis 2 as part of E's mechanism bill.

## Axis 2 — mechanism count under the frozen tally: S wins

The §7.5 counting rule weighs durable state classes over scheduler
rules over per-output overhead.

- E's bill: one added durable state class (admitted-by edges as
  checkpoint rows) plus the retraction queue, two recovery-path
  machineries (∀-purge; support rebuild + agreement check), the
  retraction rule with its pinned enqueue/drain and the GS-CO-004
  catch-up bound — and the session-B requirement, which is not a
  line item but an entire companion surface (three constrained
  primitives whose "covers all legitimate uses" claim is a separate
  SDK audit obligation).
- S's bill: no new durable state class. Its heaviest item is the
  per-output stamp field — the lowest-weight class in the counting
  rule — plus merge-on-read and the three observation points as
  scheduler rules. Compression is optional (an optimization lever,
  not a correctness mechanism).

First non-tie. **The decision rule terminates here: variant S.**

## Axis 3 — redo work: corroborates, does not decide

Citation unblocked by the G5d determination (no divergence; see
below).

- Metric floor: the zero-crash execution bound is 1 under both
  variants (four v1 controls GREEN) — the count oracle measures
  crash-caused redo, not variant overhead.
- Worst-case redo across the divergence scripts is SYMMETRIC:
  minimal green bound 2 (redo ≤ 1 per node) on the chain, fan-in,
  and mutation chassis under both variants; the bound-1 probes red
  under both. No chassis separates the variants on redo count.
- Each variant pays the at-least-once cost through its own
  mechanism — E's retraction-forced reader re-runs and S's
  pass/refusal-forced re-runs both exhibit as REDO-PROBE reds on
  their respective legs; where shared machinery forces the redo
  (writer-ineligibility), the cost is charged to both equally.
- S-specific costs are bounded and verified: the pre-seal pass
  converges within its 3-iteration budget in every honest cell
  (GPASS), and stamp compression is admissible only under the
  G9-CAL-1 rules (bucket-aligned heal minting + the ambiguity
  double-bump), trading bounded extra redo for stamp width.
- G5d (which-is-right): the reachable seal-world sets are IDENTICAL
  — {W1, W2} reachable and the sweep-failure world W3 unreachable
  under both variants, every reachable world inside the sync-scoped
  SealExpect envelope. The variants differ in HOW a world is
  reached (E purges the de-demanded child; S refuses its dispatch),
  never in WHICH worlds are sealable.

## Recommendation

**Adopt variant S — observable-causal stamps over the shared
chassis.** Note that the shared chassis already retains admitted-by
edges and the demand closure for the sweep, so this verdict IS the
hybrid the brief anticipated ("support counting retained solely to
compute the demand closure for the sweep, stamps replacing all eager
retraction") — expressible as shared column + S's adds, which
GS-CO-005(b) admits as a plain variant-S recommendation.

Design consequences the RFC should carry:

1. The constrained session primitives demote from correctness
   requirement to stamp-width optimization. Free-form session reads
   are safe under S (`tcG2s_All`); the primitives remain worth
   having to keep stamps narrow, but the SDK audit ("the three
   primitives cover all legitimate uses") stops being a correctness
   gate.
2. Internal causality gets the same discipline as external:
   upstream validators are consulted at observation time, and
   stamps apply exactly that observation-point pattern to node
   generations (the brief's unification note, now checker-backed).
3. Compression ships only with the G9-CAL-1 minting rules
   (re-mints land even, first admissions odd, floor-stale bumps the
   owner, parity-ambiguous entries also bump the named node) —
   as first drafted it livelocked honest histories into the pass
   budget.
4. The quiesce-before-bump pin is shared machinery, but its kill
   (`tcG1d_P6G`) needed feedback-PCT and a third worker to exhibit:
   implementation tests for the dying-reader race must not rely on
   uniform random schedules.

## Honest limits

- Small-scope bet as restated in spec §8: sessions × demand shrink
  and session-transitive reader-writer chains are excluded by
  declaration; first-order retraction and row-transitive support
  are exercised.
- Redo figures are checker-verified worst-case COUNTS at small
  scope, not throughput; stamp width in bytes is not modeled (G9
  models compression's decision effects, not encodings).
- The model arbitrates the design, not the Go implementation. The
  bridge is the trace-policy oracle set (`formal/occult/`, D7) and
  the chaos-scenario seam (D6) — keep them current as the RFC turns
  into code.
