# Round 5 — targeted spot review of the v6 signoff-discussion addenda

Scope: §9 cell 1d, §9 scenario 6 (collect-and-commit variant pair),
§7 fold-totality clauses (self-grounding overlay rounds, copy-skipped
replacement rounds), §10.5 update — per the §11 v6 change-order entry.
Not a fifth full round; v1–v5 material was out of scope except where
the addenda destabilize it.

Method: mechanical reachability walk of each new premise under
§3/§4/§5; hand-application of the extended §7 fold over every schedule
class each cell can produce; coherence sweep against rounds 3–4;
verification of the copy-skip publish and validator-less replay
citations against `pkg/sync/source_cache_orchestration.go` and plan B5.

Verdict: **REJECT — 2 majors (F1, F2) + 6 minors, ALL
fix-without-re-review** per the round-4 convention; no further round
warranted. Positive results: all three premises reachable without
hand-placement; the extended fold is total and deterministic over
every addenda log once F1's pin lands; 1d's
content-green-every-schedule claim survived adversarial schedule
construction under shipped toggles; no contradictions with the
round-3/round-4 dispositions.

Findings and dispositions (all applied as v7):

- **F1 (MAJOR)**: "round completion — the commit of a round's last
  page" never defined page COMMIT; store-op vs scheduler-transition
  readings diverge for the first time on 6-atomic's verdict (unit
  committed, transition lost, re-execution marker-suppressed →
  ops-reading green as scripted, transition-reading false-alarms).
  → §7 pins: a page commits when the last of its prescribed STORE ops
  commits (announce-visible); transitions/pops are not fold events;
  no-store-op rounds contribute no fold entry. Scenario 6 pins the
  variant-specific consequences (V-ATOMIC round complete at
  `eReplayUnit` commit; V-NAIVE's marker op is a prescribed round op).
- **F2 (MAJOR)**: 1d attributed the same-base-copy collapse to
  `oncePerScope` alone and called it "structural"; the check-then-mark
  collapse is atomic only under `scopeLocks` (case 4's dual-replay
  TOCTOU — a locks-off 1d schedule is content-red, walked concretely).
  → re-attributed to `oncePerScope` ∧ `scopeLocks`, "structural"
  demoted to mitigation-dependent, locks-off 1d mutant added to §10.5.
- **F3 (minor)**: validator-less-C sub-case mis-scripted — whether C's
  copy commits is schedule-dependent, not a property of
  validator-lessness (C-first commits; verdict green either way).
  → re-scripted per schedule class, aligned with §7's no-op vocabulary.
- **F4 (minor)**: 1d left V2's publish placement unpinned, so the
  green/alarm schedule partition wasn't enumerable (B5 early publish
  makes publish order ≠ drain order). → both placements pinned as
  explored sub-configs; partition stated per sub-config.
- **F5 (minor)**: the §7 torn-round boundary justification argued only
  from stop-consumption; scenario 6 is crash-based. → extended:
  crash-only histories resume from root-token checkpoints, cannot
  tear; incomplete-round debris named in the fold text (6-naive rests
  on it).
- **F6 (minor)**: V-ATOMIC under-specified its relationship to the §5
  checkpoint token; "hit durability can never precede materialization"
  was false for the still-checkpointed hit map (true of the marker).
  → pinned: token unchanged, restored hits never authorize without a
  consult, stranded hits inert, clause (iii) applies to both variants;
  wording corrected to durable consult PROVENANCE (the marker).
- **F7 (minor)**: §11 over-scoped 6-atomic's green claim to "the
  case-1 family", which now contains 1d, whose overlay flavor V-ATOMIC
  doesn't define (stale-AHEAD hazard of marker-before-overlay).
  → de-scoped to the 1a/1b/1c re-runs; overlay-flavor unit semantics
  declared an explicit deliverable-4 obligation.
- **F8 (minor)**: §7 P2's consult pin (validation match or fresh
  fetch) excluded 1d's changed-with-diff verdict, making P2 alarm
  every 1d schedule unscripted. → pin extended (revalidation occurred
  and the diff is an upstream fetch); 1d's P2 expectation stated
  (green, staleness ≤ 1).

Notes to the freeze record: **N1** partial-copy debris renamed to
avoid colliding with §7's cross-attempt TORN. **N2** the
self-grounding clause retroactively closes a latent v5 fold-totality
gap (case 4's locks-on surviving round satisfied no v5 clause);
round-4's fold-walk coherence is now documented rather than
accidental. **N3** the 1d flavor-coverage note (delta connectors
degrade to fetch-fresh on token expiry) labeled external-boundary
commentary — a connector-population claim outside the model. **N4**
V-ATOMIC's marker-check-before-consult sits outside the scope lock —
a check-then-act window that two concurrent consulting actions for
one scope would race (two committed copies → P1 legality alarm);
unreachable in the scripted family, recorded as a 2-worker hazard for
the deliverable-4 bake-off boundary notes.
