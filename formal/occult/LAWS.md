# Equational laws — composition algebra and stamp lattice (deliverable 9)

Status: CHECKED — every law below is mechanically verified against the
engine (defining equations in `src/sync_laws.occult`, checks in
`host/laws_test.go` and `host/compression_test.go`; run with
`go test ./...` in `host/`). L1–L6 and L8 are closed by equality
saturation over free Skolem constants; L7 is an ASSUMPTION (stdlib
`vector_clock` semilattice axioms instantiated on our merge — loaded so
every other check runs in their presence); L9a/L9b are ground-exhaustive
over generations 0..7 (the small-scope envelope, bound stated). Five
saturation negative controls and one ground false-live control are
refused — notably the false-live control is violated at exactly the
predicted pairs (s = cur−1, cur odd): the G9-CAL-1 parity ambiguity the
P model found dynamically, rediscovered deductively. A refuted law is a
finding and goes to the P-model change-order log, not silently
reworded.

These are the algebraic assumptions the P models consume without
re-deriving them by state exploration (brief: "Prove them in Occult;
let P consume them as assumptions"). Each law cites the spec text that
pins it. The carrier vocabulary is `formal/GLOSSARY.md`'s.

## Composition algebra (P1 fold, MODEL_SPEC.md §7)

Carrier: per-(sync, scope) fold values — `empty` (the fold's initial
value, round-7 F3 pin) and `rows(e)` (scope content at upstream epoch
e). Operations are COMPLETE-round contributions (torn/incomplete
rounds never enter the fold):

- `fresh(e)` — fetch-fresh / REPLACES round
- `repl(e)` — replacement copy of the attested base e (replay verdict)
- `ovl(e_from, e_to)` — overlay round grounded on this sync's
  completed replay of `e_from`
- `ovl_sg(e_to)` — self-grounding overlay (its OWN replacement copy
  committed inside the round)
- `skip(e_to)` — copy-skipped duplicate (B5-legal)

| # | Law | Statement | Source |
|---|-----|-----------|--------|
| L1 | REPLACES absorbs | `apply(fresh(e), x) = rows(e)` for every fold value x; hence any composition suffixed by `fresh(e)` equals `rows(e)` — prior history is absorbed. | MODEL_SPEC §7 P1 fold clause "fresh(e) → rows(s, e) (REPLACES in the fold even though the store accumulates)" |
| L2 | Overlay grounds and composes | `apply(ovl(e1, e2), rows(e1)) = rows(e2)`; sequentially, `apply(ovl(e2, e3), apply(ovl(e1, e2), rows(e1))) = apply(ovl(e1, e3), rows(e1))` — truthful-diff transitivity. The truthfulness premise is the pinned trust boundary (validators truthful), NOT proved here. | MODEL_SPEC §7 "overlay(e_from → e_to) requires current fold value = rows(s, e_from), yields rows(s, e_to)"; trust boundary §"Honest limits" of the brief |
| L3 | Self-grounding absorbs | `apply(ovl_sg(e), x) = rows(e)` for every x — same absorption shape as L1; the round's own copy re-grounds the fold. | MODEL_SPEC §7 "SELF-GROUNDING — folds as rows(s, e_to) regardless of prior fold value" |
| L4 | Copy-skip is identity | `apply(skip(e), rows(e)) = rows(e)`; a copy-skipped round on any OTHER fold value is a legality violation, not a law. | MODEL_SPEC §7 "COPY-SKIPPED duplicate overlay round … folds as a NO-OP when the fold value already equals rows(s, e_to)" |
| L5 | Replay-copy idempotence | `copy(e) ∘ copy(e) = copy(e)` — an at-least-once re-run of one round's replacement copy contributes ONE counted copy (attempt-1 incomplete + attempt-2 completed count as one). | MODEL_SPEC §7 round-7 F2 pin; plan B5 "worst case … re-runs an idempotent copy" |
| L6 | Round-local tombstone ordering | Within one round, a page's upserts apply before the page's deletions; under the coalesced-delta precondition (at most one add-or-tombstone per id per round) operations on DISTINCT ids commute, so the round's result is page-order independent: `t(id1) ∘ u(id2) = u(id2) ∘ t(id1)` for `id1 ≠ id2`. Same-id cross-page re-adds are the connector's ordering responsibility (outside the algebra). | annotation_source_cache.proto `SourceCacheRecord.deleted_ids` precondition; MODEL_SPEC §"page-op ordering (copy → upserts → tombstones → publish)" |

## Stamp lattice (variant S, GRAPH_MODEL_SPEC.md)

Carrier: causal stamps — finite maps node → generation, merged on
read. Occult's stdlib `vector_clock` already axiomatizes the merge; the
laws below instantiate it and add the baton-specific facts.

| # | Law | Statement | Source |
|---|-----|-----------|--------|
| L7 | Merge is a join-semilattice | `merge` is commutative, associative, idempotent (stdlib `vc_merge` axioms instantiated by our stamp carrier). | model/stdlib/vector_clock.occult (sibling repo); brief §"Division of labor" |
| L8 | Dead-membership is absorbing | `hasDead(x) → hasDead(merge(x, y))` — a merge never loses a dead generation; staleness is monotone under read-propagation. This is what makes the seal's no-dead-stamps check sufficient over merged stamps. | GRAPH_MODEL_SPEC P6-S ("no sealed output carries a stamp containing a dead generation"); §4c |
| L9a | Compressed definite-stale is sound | `floor2(s) < floor2(cur) → s < cur` (monotonicity of `floor2(g) = g − g mod 2`) — the bucketed pass never declares a LIVE entry definitely-stale. | GRAPH_MODEL_SPEC G9; graph/CALIBRATION.md find G9-CAL-1 |
| L9b | Bucket-aligned liveness is provable | If `cur` is even (bucket-aligned re-mint) and `s ≤ cur`, then `floor2(s) ≥ floor2(cur) → s = cur` — on bucket boundaries the compressed comparison proves liveness exactly; with `cur` odd the case `floor2(s) = cur − 1` is ambiguous, which is WHY the pass double-bumps (G9-CAL-1's admissible rule set). Error direction: false staleness (redo) only, never false-live. | graph/CALIBRATION.md G9-CAL-1; brief §"Variant S" ("lossy stamp compression … error direction is false staleness → redone work, never wrong data") |

## What is assumed vs proved

Definitional equations (the fold clauses, the merge definition,
`floor2`) are the axioms; L1–L9 are DERIVED equalities/implications the
engine must close by equality saturation (or refute). The trust-model
premises (truthful validators, coalesced deltas, non-lying connectors)
remain assumptions of the whole effort and are marked as such where a
law depends on one (L2, L6).

Proved renderings (exact forms the engine closed):

- L1's "any composition suffixed by fresh(e)" is closed for op-list
  suffixes up to length 2 (`foldops` over the round log with a
  universal preceding op) — bounded, not inductive; the single-apply
  absorption is closed universally over Skolem constants.
- L5 is proved as APPLY-LEVEL idempotence (`app(repl(e)) ∘
  app(repl(e)) = app(repl(e))`); the counts-as-one bookkeeping is the
  P side's F2 pin, not an algebraic statement.
- L6 is proved OBSERVATIONALLY over the two-id envelope: `get(i, ·)`
  agrees on both op orders for every observer i ∈ {id1, id2}; the
  coalesced-delta precondition is what makes two ids sufficient.
- L7 is loaded as an assumption, never proved; all other checks run
  with the ACI axioms present, so the controls confirm the assumption
  set does not collapse the algebra.
- L9a/L9b are ground-exhaustive over 0..7 × 0..7; the false-live
  negative control is violated at exactly {(cur−1, cur) : cur odd},
  the parity ambiguity G9-CAL-1 hit dynamically.
