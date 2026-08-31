# Round 1 — adversarial review of GRAPH_MODEL_SPEC v1 (deliverable 4 draft)

Scope: the entire `formal/GRAPH_MODEL_SPEC.md` v1 DRAFT — ground rules
G-RULE-1..4, machines (§3), the node-execution hot path and supersession
(§4), the durability table (§5), variant axes and mutation toggles (§6),
properties P1–P4 carried plus P5/P6-E/P6-S and the bake-off metrics
(§7), budgets (§8), cells G1–G7 (§9), adequacy obligations (§10), and
the §11 review charge. Baseline anchors: `formal/MODEL_SPEC.md` v11
FROZEN + MS-CO-001 (conventions, property pins, §9.6 design variants,
durability vocabulary), `formal/GLOSSARY.md` (pinned vocabulary),
`docs/tasks/sync-formal-model-brief.md` (charter, deliverable 4),
`docs/tasks/demand-graph-sync-brief.md` (settled decisions and design
requirements), `formal/walker/CALIBRATION.md` (decisions 19–24, the
6-atomic / 6-overlay / 6-overlay-naive / 6-overlay-last / 3-atomic
hand-off cells).

Method: mechanical reachability walks of G1 (both crash placements plus
the `suppressionOff` mutant), G2 (all four axis legs plus both kills),
G4 (honest and mutant), G5 (all four sub-cells plus the sweep toggles),
and G7 under the draft's §3–§5 semantics — checking (a) reachability
without hand-placement, (b) that each declared expected verdict derives
from the declared mechanisms and the inherited §7 pins applied by hand,
and (c) that each kill/mutant leg flips for the stated reason;
independent re-derivation of the P1 fold, P2 consult pin, and P3′
scoping over every walked history; a durability sweep of §5 against
crash/stop placements at each op boundary (two-crash histories
included, per the §8 budget); coherence sweep against MODEL_SPEC §7/§9.6
pins (complete-rounds replacement counting, empty-fold attestation,
marker-inside-the-unit, clause (iii) suppression, the N4/F6 marker-race
boundary notes) and the glossary; charter-coverage check of deliverable
4's obligations against the §9 cell set. Vocabulary checked against the
glossary term by term. This review does NOT relitigate: P as the
language, the walker model's frozen verdicts, unit-mode
materialization's superiority over the naive/last placements (settled
by cells 6-atomic/6-overlay/6-overlay-naive/6-overlay-last), or the
demand-graph brief's settled decisions. Findings against the unit-mode
HAND-OFF below are findings about the graph-side adaptation of the
settled discipline, not about the walker verdicts.

Verdict: **REJECT — 11 majors + 7 minors + 3 notes. F1
re-review-required: its repair introduces new suppression/adoption
semantics that must come back for a targeted round 2 together with the
F2/F3 pins it interlocks with. All other findings are
fix-without-re-review.** No checker verdict from this model is citable
until the round-2 disposition. The draft's skeleton is sound — the
machine decomposition, the axis structure, the inherited discipline
(expected-verdicts-before-first-run, kill obligations, announce-only
monitors) all carry over correctly, and most cells walk. The majors
concentrate in exactly the seam the draft claims as its purpose: what
happens to walker-pinned machinery (the unit marker, the P1 counting
pins, P4's fingerprint) when generations, death, and a frontier
scheduler are placed under it.

## Findings

- **F1 (MAJOR, re-review-required) — the unit marker's suppression
  scope is per-sync (inherited) but the graph is a generation world;
  the collision flips scripted verdicts on G1 and G2 and is never
  reconciled.**
  - Claim under attack: §4's consult step ("suppressed if the unit
    marker for this output key exists this sync"), adopted verbatim
    from V-ATOMIC clause (iii); G1's "Expected GREEN on E and S … a
    committed attempt-1 unit is superseded per §4"; G2's E+B and S
    legs ("H's re-publish retracts G; G re-runs", "G's output
    observably stale at seal → G re-run").
  - Evidence, walked. (i) G1, crash-after-unit-commit placement
    (reachable in G1's config: crash placement is armed injection,
    §1/§2; the scripted before-commit placement is one choice among
    several): the unit commits {rows, entry V1, marker} durably; the
    checkpoint predates it; resume finds S1 pending → G-RULE-3 bumps
    to g2 and marks g1 DEAD; (n, g2) consults, the marker exists this
    sync → suppressed; the seal contains g1's rows. Under variant S
    those rows carry stamp {S1: g1} with g1 dead → P6-S as written
    ("no sealed output carries a stamp containing a dead generation")
    fires RED on the leg declared GREEN. The walker's 6-atomic sealed
    the identical content green because the walker has no death
    concept; the graph's death semantics condemn the exact content
    the marker exists to keep. (ii) The same suppression defeats
    variant E's retraction: in G2's E+B leg, G@g1's unit committed a
    marker for G's output key; H's re-publish retracts G; G's forced
    re-execution (n_G, g2) consults, finds the marker → suppressed →
    never re-derives → the sealed artifact keeps the d1-embedding
    rows → P6-E RED on the leg declared GREEN. Identically for the
    S legs: the seal-time observation demands G re-run, the marker
    refuses the re-derivation, and the staleness cannot be cleared
    before seal. (iii) A marker-suppressed re-execution announces
    NOTHING, so its demand (spawn tokens, session ops) is never
    re-derived by a live generation — the closure and starvation
    consequences are taken up in F7.
  - Why it matters: this is the exact hazard the review charge was
    told to hunt ("what does 'this sync' mean across generations?").
    The draft adopts unit-mode as settled — correctly — but the
    settled pins were proven in a model with at-least-once redo and
    NO death/retraction machinery. Marker suppression and generation
    death now issue contradictory instructions on the same state
    ("do not re-derive" vs "this content's producer is dead;
    re-derive or sweep"), and three of the four G2 green legs plus
    G1's committed-unit leg are underivable as written.
  - Disposition (re-review-required): the repair must pick and pin a
    reconciliation — candidates: (a) generation-adoption: a
    marker-suppressed re-execution ADOPTS the marked unit's outputs
    as its own (stamps rewritten to the live generation, emissions
    re-announced from the store), making the dead round's content
    live; or (b) generation-scoped markers: the marker suppresses
    only within its producing generation's lifetime, and retraction/
    death clears it (at-least-once redo cost returns); or (c) death
    redefinition: a generation whose prescribed work fully committed
    is not marked dead by a restart (completion inferred from the
    marker at resume). Each candidate changes §3/§4/§5 semantics and
    the P5/P6 monitors' evidence base; each interacts with F2, F3,
    and F7. This is new mechanism, not a wording pin — it requires a
    targeted round-2 spot review before any G1/G2/G5d verdict is
    citable.

- **F2 (MAJOR) — the P6 property vocabulary is incomplete and
  mechanism-referential: the E+A leg of G2 has no declared property
  that can fire, P6-S reads the same stamps the mechanism computes
  (so `stampMergeOff` cannot flip), and the S-variant's seal-time
  observation step exists in no machine.**
  - Claim under attack: G2's "E+A RED (P6-E has no edge to see …
    the model exhibits the miss)"; §6's kill row "`stampMergeOff` …
    kill cell G2-S"; §7 P6-S; §3's seal sequence ("seal when the
    frontier drains: run the SWEEP … then `eSeal`").
  - Evidence: (i) §7 carries P1/P2/P3′/P4 and adds P5/P6-E/P6-S. The
    walker's P6-A (ghost session stamps, final-derived-value form) is
    NOT carried. P6-E is declared for "variant E + session B"
    explicitly. So in the E+A leg no declared monitor can alarm: the
    expected RED has no property attached — an implementer building
    only the declared monitors seals E+A green and the headline
    bake-off fact ("S makes the shipped session store safe; E
    requires the session rework") loses its degenerate-leg exhibit.
    (ii) `stampMergeOff` removes the read-side stamp merge. P6-S's
    evidence IS the stamps: with the merge off, G's rows carry only
    {G: g_live}, no dead generation appears in any sealed stamp, the
    consistent-cut clause compares only comparable same-node
    generations — P6-S is GREEN and the kill cannot flip. A monitor
    that reads the mechanism's own bookkeeping cannot detect the
    mechanism's mutation; this is a mutation-adequacy failure baked
    into the property's definition, and §10.1 says exactly this
    halts any recommendation. (iii) P6-S additionally alarms on
    same-value re-derivation (H re-derives an identical d1; G's
    rows carry the dead g1 stamp; clause 1 fires) — the walker
    pinned d1→d2→d1 as non-alarming for P6-A, and P6-E carries the
    counterfactual ghost discipline, but P6-S is value-blind with no
    stated decision on whether that red is intended (forced-redo
    conformance) or a false alarm. (iv) The mechanism half of the S
    story — "observably stale at seal → G re-run" — is scheduler
    machinery, and §3's dispatch loop has no observation step in the
    seal sequence: as declared, seal is drain → sweep → `eSeal`, and
    nothing re-runs anybody. Two honest readers diverge on every
    S-leg verdict of G2.
  - Why it matters: G2 is the calibration case the charter names
    (case 2, session laundering) and the bake-off's headline cell;
    as drafted its four legs rest on an unnamed property, an
    unkillable kill, and an undeclared scheduler step.
  - Disposition (fix-without-re-review, interlocks with F1): carry
    the walker's P6-A ghost discipline forward as the LAUNDERING
    ORACLE for all four legs — ghost dependency labels on emissions
    (which session value, which writer generation, value identity),
    violation = a sealed output embedding a value that differs from
    the key's final live derived value — re-grounded on generations.
    Demote lineage edges (E) and stamps (S) to MECHANISM whose job
    is to make the oracle green; then `stampMergeOff` and
    `retractionOff` both flip against the oracle, not against
    themselves. Declare the S-variant's observation-driven re-run
    rule in §3 (demand-derivation, session-read, and pre-seal
    observation points as scheduler transitions), and pin the
    value-blindness decision for P6-S's clause 1.

- **F3 (MAJOR) — the walker P1 pins do not transfer ungrounded:
  fold membership of a DEAD generation's complete round is unpinned
  (two readings diverge on G1's committed-unit leg via the
  empty-fold attestation pin), and G1's "superseded per §4" claim
  is false for unit rounds (marker suppression preempts supersession
  — supersession is unreachable for unit-mode keys).**
  - Claim under attack: §7 "P1 binding integrity … all v11 pins,
    carried unchanged in form … re-grounded on executions"; G1's
    "a committed attempt-1 unit is superseded per §4".
  - Evidence: take F1's history (unit commits, crash, generation
    bump, marker-suppressed re-execution, seal). The only round in
    S1's log belongs to the DEAD generation g1. Reading A ("fold
    over complete rounds" quantifies over all executions' rounds):
    the dead round folds replacement(e1), content matches, entry V1
    attests e1 — green, the 6-atomic analog. Reading B (the fold
    quantifies over LIVE generations' rounds — the natural companion
    to P5's "announced emissions of LIVE generations only"): the
    complete-round set is EMPTY, the fold is the empty partition,
    the sealed partition is non-empty → P1-CONTENT red, AND entry V1
    sits over an empty fold → the inherited v11 attestation-
    over-empty-fold pin (round-7 F3, CALIBRATION decision 20) fires
    outright. Both readings are honest; they diverge on a scripted
    cell's verdict — the round-5 F1 / round-7 F3 divergence class
    exactly. Separately: for unit-mode keys, dead rows durable ⟺
    the unit committed ⟺ the marker committed ⟺ the re-execution is
    suppressed — so §4 supersession (a live commit replacing dead
    rows) can NEVER fire for a unit round; G1's cell text attributes
    its green to a mechanism that is unreachable in its own config.
    Supersession's actual domain is the per-page record path (F6).
  - Why it matters: replacement counting and empty-fold attestation
    are the two v11 monitor pins the walker's freeze sweep
    calibrated (decisions 19–20); importing them "unchanged in
    form" into a world where complete rounds can belong to dead
    executions leaves the flagship confirmation cell underivable.
  - Disposition (fix-without-re-review, contingent on F1's repair):
    pin fold membership and replacement-count legality with
    generation grounding — recommended: a dead generation's
    COMPLETE round remains in the fold and the count until
    superseded (its content is what the artifact factually
    contains; supersession removes both the rows and the round's
    fold/count contribution), which keeps the marker-suppressed
    seal green under adoption-style F1 repairs and keeps G1's
    mutant-leg legality alarm intact; and correct G1's cell text
    (marker suppression, not supersession, is the committed-unit
    mechanism). Re-derive G1/G5 verdicts under the chosen pin.

- **F4 (MAJOR) — generation bookkeeping is unsound across two
  crashes: the bump-at-resume rule derives the new generation from
  the restored checkpoint alone, so an un-checkpointed resume REUSES
  a generation id and the dead set mis-derives — dead debris is
  laundered as live.**
  - Claim under attack: §5's generation-table row ("the LATEST
    generation per pending node is in the frontier checkpoint; the
    dead set is derivable (every generation below latest is dead);
    no separate durable row"); §3's resume rule; G-RULE-3.
  - Evidence, walked within the §8 budget (≤ 2 crashes): checkpoint
    cp1 captures S1 pending @ g1. Crash 1 → resume bumps to g2
    (in-memory). Attempt 2 as (S1, g2) commits durable output —
    record-path pages, or a session publish stamped {S1: g2} under
    variant S. Crash 2 lands before any post-resume checkpoint
    (checkpoint-or-skip is a genuine choice point). Resume 2
    restores cp1 AGAIN: latest = g1 → bumps to g2 — the SAME id.
    Attempt 3's execution (S1, g2)′ is a different execution with
    the same identity; attempt 2's g2-stamped durable debris is now
    indistinguishable from attempt 3's live output; "every
    generation below latest is dead" classifies attempt-2's debris
    as LIVE. G-RULE-3's "an output's producing generation is
    stamped at emission time and never reassigned" is violated in
    effect (one stamp, two producers), P6-S cannot see the dead
    debris (its generation reads as live), and E's supersession
    cannot fire on it (no dead rows for the key). Whether §3's
    "checkpoint placement is a choice point AS IN THE WALKER"
    silently imports the walker's forced Init-checkpoint is exactly
    the kind of unstated inheritance §2 forbids relying on — and
    even a forced resume checkpoint only closes the hole if it is
    pinned to commit BEFORE any resumed execution's first durable
    emission.
  - Why it matters: every death-keyed mechanism in the spec — the
    dead set, purge, supersession, stamp validation, P5's live-only
    closure — keys off generation identity; a reusable identity is
    a silent-corruption channel of the class the calibration
    protocol exists to catch, and it would be hardcoded away by any
    encoding that happens to checkpoint eagerly.
  - Disposition (fix-without-re-review): pin a forced resume
    checkpoint that commits the bumped generation table before the
    resumed attempt dispatches anything (ordering stated
    explicitly), or make the bump derive from max(restored latest,
    highest generation observed in durable stamps/store for the
    node) + 1 with the scan rule declared. Add the two-crash
    generation-reuse history as a required probe (expected
    unreachable after the pin) — the §11 question 2 obligation made
    executable.

- **F5 (MAJOR) — variant E's lineage state does not survive the
  crash seam as declared: admitted-by edges are in no durable row
  (the purge is unimplementable post-restore), the support
  rebuild-agreement check has no well-defined target when the crash
  lost announcements the store already reflects, the scripted G5
  family cannot reach a state where the purge does anything, and
  therefore the `purgeOff` kill cannot flip.**
  - Claim under attack: G-RULE-4 and §5 (checkpoint contents:
    pending nodes, completed-derivation set, closed-cut facts,
    session index — no spawn/admitted-by edges); §5's
    rebuild-agreement row ("rebuilt on resume … the rebuild
    agreement check is a monitor obligation"); §3's resume ("Variant
    E additionally purges the dead generation's spawn-subtree from
    the frontier"); §6's kill row "`purgeOff` … kill cell G5-E crash
    window"; G5d's E-leg ("E: purge + no re-demand → swept, GREEN").
  - Evidence: (i) the purge needs, post-restore, the admitted-by
    edge (WITH the admitting generation) for every pending node; the
    declared checkpoint carries none. G-RULE-4's own rule says
    "state the checkpoint does not carry is a FINDING about the
    variant" — this review so finds: E requires generation-qualified
    admitted-by edges in the durable checkpoint, which is durable
    state the mechanism tally must then count (F19). (ii) The
    rebuild-agreement check's declared target is "the pre-crash
    value"; in the standard window (unit commits, announce processed,
    demand derived, THEN crash — no checkpoint between) the restored
    frontier predates the derivations the pre-crash support included,
    so rebuild ≠ pre-crash value on legal histories and the check
    false-alarms; the only implementable target is the
    checkpoint-consistent value, which must be said. (iii)
    Reachability: for S1's unit-mode single-commit rounds, demand
    derivation (C's admission) and S1's completion enter scheduler
    state from the SAME announce, so no checkpoint can hold
    C-pending ∧ S1-pending; either the checkpoint predates the
    announce (C absent from the restored frontier — the purge has
    nothing to purge; the SWEEP alone produces G5d's E-leg green) or
    it postdates it (S1 completed — no re-execution, no premise).
    The purge's real domain is a paginated parent that spawns
    mid-round and then restarts — a shape no §9 cell scripts. (iv)
    Consequently `purgeOff` cannot flip in the tabled kill cell: in
    every reachable G5 history the honest sweep independently drops
    the dead-admitted partition, so the mutant leg seals the same
    artifact and stays green — a tabled kill that cannot fire, which
    §10.1 makes a recommendation-halting defect.
  - Why it matters: E's entire mechanism story (purge + derived
    support + retraction) is one of the two bake-off contestants;
    as drafted its crash behavior is unimplementable from declared
    durable state, unprobed by any reachable cell, and its
    signature toggle is unkillable — the bake-off would compare S
    against an E whose machinery never ran under fire.
  - Disposition (fix-without-re-review): add generation-qualified
    admitted-by edges to G-RULE-4/§5 (and to the frozen mechanism
    tally); pin the rebuild-agreement target as
    checkpoint-consistent; re-script the G5-E crash window (and the
    `purgeOff` kill) on a paginated record-path parent whose child
    admission IS checkpointed while the parent remains pending —
    that premise makes the purge load-bearing (with `purgeOff`, the
    stale child executes and its redo/closure effects are
    observable) — and decide/declare whether the purge also kills
    COMPLETED descendants or only pending ones (the two readings
    give different redo counts and different G6 outcomes; see F9).
    Also correct G5's premise wording — C is fetch-fresh in a
    first-sync config and commits a record round, not a "unit".

- **F6 (MAJOR) — supersession (§4) is not total: the two-live-
  derivations-one-output-key interaction that G-RULE-2 explicitly
  legalizes is undefined (and alarms P1 legality on legal behavior),
  the overlay-intent record path composes dead debris with live rows
  (the "structural under E" claim is false to the declared
  mechanisms), G4's no-residual-job expectation overreaches, and
  G5b's probe is not the strongest premise — with no dedicated
  supersession cell anywhere.**
  - Claim under attack: G-RULE-2's parenthetical ("two distinct
    derivations targeting one output key must both run; their store
    interaction is supersession, §4"); §4's supersession definition
    ("when execution (n, g′) commits … and a DEAD generation's rows
    for K already sit in the current partition, the commit REPLACES
    them … under variant E the sweep plus supersession make it
    structural"); G4's declared expectation ("NO residual job");
    §1's "fresh-artifact supersession" modeling claim.
  - Evidence: (i) §4 defines replacement only against a DEAD
    generation's rows. Two distinct derivation hashes with one
    output key are both LIVE; G-RULE-2 sends the reader to §4,
    which is silent. Compose (union — the partition invariant's
    double-stamp poison shape, per the demand-graph brief's silent
    partition-poisoning warning) or last-commit-wins (silent loss of
    a live derivation's output)? Two honest implementations diverge
    — and both commit two complete unit rounds with two copies for
    one scope in one sync, so the inherited P1 legality rule alarms
    on behavior the spec declares legal. (ii) The 2-worker marker
    race (walker N4/round-7 F6, carried over by §4's own boundary
    citation) makes this reachable with suppression ON: the two
    nodes' consults both pass the absent-marker check and both
    commit units. G4's declared expectation — scope locks have NO
    residual job — is therefore overreached: it holds for same-hash
    duplicates (suppression) but is refuted by the same-key
    distinct-hash shape, which no cell scripts. (iii) Record path:
    "clear only on REPLACES semantics" means an OVERLAY-intent
    fetch-fresh re-run commits over a dead generation's partial
    record debris without clearing — dead rows compose with live
    rows in one partition, falsifying §4's "never compose …
    structural" claim for E. The composition IS detected (the
    inherited fresh-replaces fold makes it a P1-CONTENT alarm when
    epochs differ — verified by hand), but detected-by-alarm is
    precisely not "structural", and when upstream does NOT move
    between the attempts the debris is content-identical and the
    claim is untested vacuously. Under S the same history is caught
    only by P6-S's dead-stamp clause — same-node generations are
    causally COMPARABLE, so the consistent-cut clause passes it;
    one more reason F2's oracle grounding matters. (iv) G5b probes
    only sweep-OFF-with-no-re-demand; the §11 question 5 asks
    whether that is the strongest premise — it is not: the
    overlay-intent re-demand history above defeats BOTH mechanisms
    with the sweep ON.
  - Why it matters: supersession is the graph's replacement for the
    walker's replay-blocked/supersede machinery and one third of
    the three-invariant framing's consume story; an interaction
    matrix with undefined and self-contradicting rows would be
    silently resolved by whatever the encoding happens to do.
  - Disposition (fix-without-re-review): define the supersession
    matrix totally over {dead vs live} × {unit vs record} ×
    {REPLACES vs OVERLAY intent} in §4, including the two-live-
    derivations-one-key rule (recommendation: make same-key
    distinct-derivation admission a declared connector-contract
    violation surfaced as poison, per the partition invariant, OR
    pin last-commit-wins with a legality exemption — either way the
    P1 counting pin must be updated in the same edit, F3); add a
    dedicated supersession cell family: (a) record-path dead-debris
    re-run under both intents (expected: REPLACES green,
    OVERLAY-intent alarm — the §4 claim corrected to
    "detected, not structural"), (b) the same-key distinct-hash
    2-worker race; retitle G4's expectation accordingly.

- **F7 (MAJOR) — P3′ is cited under the walker pin name but
  described as the full walk-equivalence property the walker
  explicitly did not build; under the implementable reading, demand
  starvation seals green under EVERY declared property — the P5
  ghost closure is circular (monitor and scheduler consume the same
  announce stream), no property owns demand-closure completeness,
  and no toggle kills under-admission.**
  - Claim under attack: §7's "P3′ smear-equivalence (complete
    crash-free walk equivalence)"; P5's ghost-closure definition
    ("computed by the monitor from announced emissions of LIVE
    generations only"); §6's kill table (no under-admission mutant);
    the absence of any 5b-style scripted seal-content oracle.
  - Evidence: (i) MODEL_SPEC's P3′ is per-scope epoch coherence,
    doubly scoped — the full "equal to some crash-free walk"
    quantifier was deliberately left as future work because it is an
    ∃-over-walks obligation a P monitor cannot discharge. The draft
    reuses the name with the stronger description; an implementer
    building the walker form checks something materially weaker than
    the words. (ii) Under the walker form, walk the starvation
    history: S1's spawn announce is processed, C is admitted but no
    checkpoint captures it; crash; resume re-runs S1, whose
    re-execution is marker-suppressed (F1) and announces nothing; C
    is never re-demanded; C never runs. Seal: P1 green for C (no
    round, no partition, no entry — empty equals empty), P2 green
    (quantifies over rows present), P3′-as-walker green (no manifest
    scope), P5 green — the monitor's ghost closure, built from the
    same live announces the scheduler consumed, ALSO excludes C.
    The artifact silently lost a subtree and every monitor agrees.
    P5 as declared can catch a sweep that disobeys the closure, but
    never a closure that is wrong — the oracle validates the
    scheduler against itself. (iii) Generation death itself is not
    an announce event; the monitors' "LIVE generations only" and
    "dead generation" predicates are implementable only via an
    inference rule (any announce from (n, g′) kills all (n, g < g′))
    that the spec never states — and F1's suppressed restarts emit
    no announce, so even the inference goes blind exactly where it
    is needed. (iv) The kill table's coverage is asymmetric:
    `suppressionOff` kills over-execution, nothing kills
    under-admission (a demand-derivation rule that drops tokens),
    so §10.2's reachability discipline has no mutation-side
    counterpart for the graph's single most load-bearing new
    mechanism.
  - Why it matters: the walker's 5b taught this exact lesson — a
    silent dropout green under every property, whose only executable
    oracle was the scripted seal-state expectation — and the graph
    spec inherited the lesson's vocabulary but not its oracle.
  - Disposition (fix-without-re-review): pin P3′'s actual checkable
    form (the walker form, honestly named, with the full-equivalence
    form recorded as not built); give P5's ghost closure an
    independent evidence base — the MEnv/MUpstream-side
    counterfactual closure (which children an honest uninterrupted
    walk at the scripted epochs would demand — computable per the
    P6-R counterfactual precedent, since policies are deterministic
    and content tables are scripted) — or, minimally, a scripted
    `SealExpect` oracle per cell in the 5b style; state the
    death-inference rule; add a `demandDropOff` (or equivalent
    under-admission) mutant with a kill cell.

- **F8 (MAJOR) — the cross-variant P3′ claim in G5d is not
  well-defined as a checker-checkable property: it compares sealed
  artifacts ACROSS two runs (two scheduler modes), which no single-
  run P monitor can express, and as a general claim it is false-
  by-design under smear (the variants may legally seal different,
  both-P3′-equivalent artifacts).**
  - Claim under attack: G5d's "Both legs must converge to the SAME
    sealed artifact (P3′ smear-equivalence across variants — the
    strongest cross-variant claim; declare it and let the checker
    try to break it)"; §10.6.
  - Evidence: the lineage axis is a per-run configuration; a P
    monitor observes one run. "E's seal equals S's seal" quantifies
    over pairs of runs from different configs — there is no monitor
    to fire, so "let the checker try to break it" has no executable
    meaning, and §10.6's "if it fails" has no failure event. Deeper:
    P3′-equivalence is membership in a SET of legal artifacts
    (smear admits many); two correct variants may select different
    members whenever their redo behavior differs — which is the
    bake-off's own premise (S re-runs what observation finds stale;
    E purges and re-derives). In G5d's tight config the legal set
    is plausibly a singleton and the claim holds trivially; in any
    config where it is not, the claim is false without either
    variant being wrong. The strongest cross-variant statement the
    charter needs is: each leg satisfies P3′ per-run, and the
    enumerated seal-artifact SETS (a meta-comparison over the two
    runs' reachable seals, produced by the harness, not a monitor)
    either coincide or the recommendation states which artifact is
    right — which is what §10.6 already gropes toward.
  - Why it matters: the strongest declared cross-variant claim in
    the spec is currently unfalsifiable, the precise pathology
    ("a badly built model passes its checker") the charter's
    execution notes warn against.
  - Disposition (fix-without-re-review): reformulate G5d as (a)
    per-leg P3′ monitors as usual, plus (b) a harness-level
    artifact-set comparison recorded in the bake-off table, with
    the divergence rule of §10.6 attached to (b) and honestly
    labeled a meta-analysis, not a property.

- **F9 (MAJOR) — the G6 bake-off scripts cannot distinguish the
  variants: both scripted interruptions land where E and S redo
  identical work by construction, the §8 envelope contains no
  history where purge-vs-stamps can diverge (no depth, no fan-in),
  and variant S's refusal rule is operationally unpinned, so the
  redo metric is ill-defined — the arbitration deliverable fails as
  scripted.**
  - Claim under attack: G6's two scripts (stop after S1's unit
    before S2 executes; crash during S2's execution) and its
    declared falsifiable expectation ("E re-executes MORE under
    deep spawn trees"); §8's envelope; §3's variant-S resume rule
    ("re-derived or refused at the demand-derivation observation
    point"); §11 questions 6 and 10.
  - Evidence: (i) the crash script interrupts S2 — a LEAF. Purge of
    a leaf's spawn-subtree purges nothing; S re-observes nothing
    (no descendants carry S2's dead generation). Both variants
    re-execute exactly S2 once. The stop script interrupts before
    S2 ever runs — the forced checkpoint is current and total, and
    resume re-executes S2 once on both variants. Neither script
    produces a between-variants delta; the declared expectation is
    not merely undecided but UNDECIDABLE by these cells, and a
    bake-off table built from them would report a tie manufactured
    by script choice — the fairness failure mode of §11 Q6, in the
    opposite direction from the one the question anticipates. (ii)
    The divergence cases need a parent restarting OVER completed
    descendants (purge discards C's completed work vs stamps keep
    or force-redo it) and support fan-in (a child demanded by two
    parents; a row supported twice — the refcount and transitive-
    retraction machinery of E, and the invalidate-everything
    degeneration the session brief worries about, never execute in
    a 1-child, depth-1, no-fan-in envelope). §8 has no such
    configuration; Q10's answer is therefore NO for the bake-off
    cells (the envelope is adequate for the safety cells G1–G5).
    (iii) Whether E's purge discards completed descendants (F5's
    open reading) and when S "refuses" a dead-admitted pending node
    (before or after it executes — the draft's "re-derived or
    refused" names no rule and no timing) each swing the
    executions-per-node metric; a metric that depends on unpinned
    scheduler choices cannot arbitrate anything.
  - Why it matters: the charter says this model exists to arbitrate
    edges-vs-stamps BY CHECKER OUTPUT; G6 is the redo-work axis of
    that arbitration.
  - Disposition (fix-without-re-review): add the divergence scripts
    — (a) crash while a mid-tree parent with ≥ 1 completed,
    checkpointed descendant restarts, identical re-derivation; (b)
    the same with a changed re-derivation; (c) a fan-in config (one
    child demanded by two live parents; one session publish read by
    two readers) — and add the needed envelope rows (depth 2, one
    fan-in edge) as an explicit, justified widening of the
    inductive bet; pin S's refusal timing and E's purge domain
    (F5) first; keep the current two scripts as the
    control (expected delta ≈ 0, now an honest calibration point
    rather than the whole axis).

- **F10 (MAJOR) — the stampCompression admissibility obligation has
  no cell: the axis is declared in §6, the safety-unchanged claim is
  stated in §7, §10 hangs a refutation condition on it — and no §9
  cell declares the axis, exercises `bucketed`, or could refute the
  claim; every §9 cell also violates §6's "every cell declares all
  three" rule outright.**
  - Claim under attack: §6's axis table and its preamble; §7 P6-S's
    "Under `stampCompression = bucketed` the SAFETY claim must hold
    unchanged and only REDO WORK may grow … a cell where compression
    changes any sealed content refutes the admissibility claim";
    §9's cell set.
  - Evidence: verified by exhaustive read — G1 through G7 nowhere
    mention stampCompression; no cell declares a value for it (nor,
    for that matter, for the session axis outside G2 — see F21);
    §10.1 covers toggles, and stampCompression is an axis, so it
    escapes the kill table too. The admissibility of lossy
    compression is not a nicety: it is the charter's stated reason
    variant S's mechanism cost is bounded ("error direction is
    redone work, never wrong data") and therefore a load-bearing
    input to the bake-off's mechanism-count and redo axes. An
    unexercised admissibility claim would enter the recommendation
    as prose — exactly what §10.5 forbids.
  - Why it matters: missing kill/coverage obligation for
    load-bearing new machinery, per the severity definition.
  - Disposition (fix-without-re-review): add the compression cell
    family — re-run G2's S legs and the G5 family under
    `stampCompression = bucketed`, expected: every safety verdict
    unchanged, redo count recorded (growth permitted and expected —
    bucketing makes unrelated nodes' deaths look observable); the
    refutation condition of §7 becomes those cells' declared
    mismatch criterion. State in §6 which axis values every §9 cell
    runs by default.

- **F11 (MAJOR) — the P4 tranche does not transfer as claimed:
  the walker's stuck-detection fingerprint (byte-identical restored
  checkpoint state) can NEVER fire under generation bumping, so
  G7's stuck leg flips green as written; attempt-failure semantics
  for a frontier scheduler are undeclared; and §8 has no attempt
  budget for the ladder to consume.**
  - Claim under attack: G7's "expected: without an abandon rule the
    re-fail loop is P4-STUCK RED (same shape as the walker's)"; §7's
    "P4 progress (no stuck re-failure; the walker's ladder analog if
    a scenario needs one)"; §8.
  - Evidence: the walker's livelock detection (MODEL_SPEC §7 P4,
    CALIBRATION decision 17) fires on two consecutive failures from
    byte-identical restored checkpoint state. In the graph, every
    failure-forced checkpoint captures the failing node at a HIGHER
    generation than the last (the bump is the resume rule), so no
    two restored states are byte-identical and the detector is
    structurally silent — the generation counter is a Zeno counter
    under the fingerprint. Reader A (byte-identical, as inherited):
    G7's stuck leg seals... never — the monitor simply doesn't fire
    and the leg is GREEN, flipping the declared RED. Reader B
    (fingerprint modulo generations): RED as declared. Whether a
    failure forces a checkpoint at all, and indeed what an "attempt
    failure" IS under a frontier scheduler (does one node's loud
    failure fail the attempt walker-style, or does the frontier
    route around it and the node alone re-fails?), is undeclared in
    §3 — the walker's decision-16 machinery has no graph analog in
    the draft. And §8's interruption budget ("≤ 2 crashes or 1 stop
    per cell") has no attempt-failure/resume-ladder row, while G7
    needs at least three attempts for k = 2 detection.
  - Why it matters: G7's whole point is that the P4 findings carry
    over "rather than dissolving"; as drafted the detector
    dissolves and the cell cannot show it.
  - Disposition (fix-without-re-review): pin the stuck fingerprint
    as generation-blind (failure point, reason, restored state
    modulo the generation table); declare attempt-failure semantics
    for MGraphScheduler (recommended: node-loud-failure fails the
    attempt, walker-parity, so the ladder analog is meaningful);
    add the attempt-budget row (≤ 3 attempts, walker parity).

- **F12 (MINOR) — demand-derivation timing is unpinned.** §3's
  dispatch loop reads batch-shaped ("collect its announced
  emissions, derive demand") while §4 announces emissions at commit;
  whether the scheduler derives demand per-announce (incrementally,
  atomic with completion bookkeeping) or after execution completion
  determines which checkpoint contents are reachable — F5(iii)'s
  walk turned on it. One sentence pins it (recommended:
  per-announce, atomic with the announce's completion effects, which
  is what makes the F5 reachability analysis stable).
  Fix-without-re-review.

- **F13 (MINOR) — "completed-derivation set" is admission-keyed in
  G-RULE-2 but completion-named everywhere else, and the resume-side
  completed-vs-never-admitted distinction is never stated.** G-RULE-2
  suppresses on "already ADMITTED this sync"; §3/G-RULE-4 call the
  same state the "completed-derivation set". A resume must
  distinguish a node absent-from-pending-because-completed from
  never-admitted, presumably via admitted ∧ ¬pending — but that rule
  is nowhere written, and it is load-bearing for every restart walk
  in this review. Rename to admitted-derivation set (or split the
  two sets) and state the resume rule. Fix-without-re-review.

- **F14 (MINOR) — record-path clearing and the intent enum arrive in
  a parenthesis, not a declaration.** §4's "(shipped record path;
  clear only on REPLACES semantics)" quietly imports the deliverable-3
  wire-intent enum as BUILT graph semantics and adds a record-path
  clear the walker's §4 never had, without §3 op-vocabulary
  registration (which §5's crash protocol and the arrival-order choice
  points quantify over) and without pinning clear placement
  (first page of the round only, presumably). This is the round-6 /
  MS-CO-001 registration lesson verbatim. Register the ops and
  intents in §3, pin clear placement, cross-reference from §4.
  Fix-without-re-review.

- **F15 (MINOR) — the aggregate node X exists in the budget and in
  no cell.** §1 promises X "executes only in configurations where
  its precondition is scripted true"; §8 budgets it; no §9 cell
  contains such a configuration, so the promise is about an empty
  set and Q8's honest-labeling check has nothing to inspect. Either
  add the scripted-precondition configuration (X executes last,
  P1–P5 checked around it — cheap, and it gives deliverable 5 its
  hand-off) or strike X from v1's budget and state the deferral.
  Fix-without-re-review.

- **F16 (MINOR) — cross-sync stamp scoping is undefined for variant
  S.** Generations are per-sync restart counters; G1's sync-2 replay
  copies rows whose stamps reference sync-1 generations, which the
  current sync's generation table cannot classify (ill-typed dead-set
  lookups; a refuse-on-unknown reading would false-alarm every warm
  replay under S). Pin the rule — recommended: replay re-stamps
  copied rows with the replaying execution's stamp (the sealed
  source artifact was observation-clean by its own seal), with an
  explicit note that cross-sync session-stamp travel remains the
  walker P6-R machinery's jurisdiction. Fix-without-re-review.

- **F17 (MINOR) — the sessions × replay product (the walker's
  scenario-7 class) is neither covered nor excluded.** The graph
  model replays inside node executions, so replay elision of session
  writes is structural here too; the draft's §1 abstraction list is
  silent on the product, and silence is how the walker model lost it
  the first time (round-6 lesson). Either declare it out of scope
  with the argument (taint machinery is verdict-side and orthogonal
  to the lineage axis; the walker cells own it) or script the 7a/7b
  analogs on the graph runtime. Fix-without-re-review.

- **F18 (MINOR) — P6-E's retraction-liveness form does not bind the
  re-run to the live value.** "Every reader execution that consumed
  the dead value must re-run before seal" is satisfied by a re-run
  that itself read the stale KV value (reachable in G2's crash
  config: G's crash-forced re-run reads d1 from the durable KV
  before H re-publishes) — the re-run happened, the sealed output
  still embeds d1. The mechanism plausibly closes this (retraction
  keyed on the value/key, retracting readers of any dead value on
  re-publish), and F2's final-value oracle closes it at the monitor
  — but the retraction rule's keying must be pinned so the fan-in
  quantification of Q7 is over reader-executions-of-dead-values,
  not reader-nodes. Fix-without-re-review, subsumed if F2's oracle
  disposition is taken.

- **F19 (NOTE) — the frozen mechanism tally must absorb this
  round's machinery before the cells run.** F5 adds durable
  admitted-by edges to E's column; F2 adds the scheduler-side
  observation/re-run step to S's column (it was implicitly free);
  F6's supersession matrix belongs to the shared column. The tally
  is the pre-hoc fairness device — freeze it in the spec revision,
  not in the calibration report after the numbers exist.

- **F20 (NOTE) — the recommendation's decision rule is unpinned.**
  §10.5 fixes the table's provenance but not the procedure: what
  wins when properties tie, mechanisms differ by definition-sensitive
  counts, and redo work splits by script family. Record the
  arbitration rule (and where the written recommendation lives —
  the CALIBRATION.md analog for `formal/graph/`) before the first
  bake-off run, for the same reason the tally is frozen.

- **F21 (NOTE) — the session axis is vacuous outside G2; say so
  once.** Only G2 declares session machinery (H and G exist "scenario
  G2 only"), so §6's every-cell-declares-all-three rule is
  unsatisfiable as written for the session axis in six of seven
  scenarios. One sentence ("cells without session actors run
  variant A, axis vacuous") plus F10's default declaration makes §6
  total.

## Mechanical walks (the §10.2 obligation, executed for this review)

- **G1.** Crash-before-unit-commit leg: reachable (armed injection at
  the pre-commit boundary; nothing durable for S1; resume bumps,
  re-consults, revalidation vs epoch 2 fails, fetch-fresh lands
  clean); GREEN derives on both variants — confirmed as scripted.
  Crash-after-unit-commit placement (same config, different armed
  position): verdict underivable — F1 (marker suppresses the live
  generation; P6-S fires on the dead stamp under S) and F3 (fold
  membership of the dead round; "superseded per §4" false — the
  supersession mechanism is unreachable for unit keys). Mutant
  (`suppressionOff`): the duplicate-admission race schedule (both
  consults pass the absent-marker check before either unit commits —
  the inherited N4/F6 window) reaches the double unit; last unit
  coherent; legality-only alarm derives as declared; the sequential
  schedule is marker-suppressed and green, which is consistent with
  a first-find RED. Flip confirmed for the stated reason.
- **G2 (all four legs).** Premise reachable in all legs: H's session
  write is a store op committed mid-execution (durable KV), a
  nondet checkpoint captures G completed ∧ H pending, crash, H alone
  restarts — no hand-placement. E+A: the miss is real, but no
  declared property fires — F2; the leg's RED is currently
  property-less. E+B: retraction → G re-run is defeated by G's own
  unit marker — F1; GREEN underivable as written. `retractionOff`
  kill: flips RED via the sealed-d1 witness, PROVIDED the monitor is
  grounded per F2 (as drafted the witness form works; confirmed).
  S+A and S+B: the merge → dead-stamp → re-run story requires a
  seal-time observation step absent from §3 (F2) and the re-run is
  marker-suppressed (F1); GREEN underivable as written.
  `stampMergeOff` kill: CANNOT flip — the monitor's evidence is the
  stamps the mutant removes (F2). The headline bake-off fact
  survives IN DIRECTION under the F1+F2 repairs (nothing found
  contradicts S-makes-A-safe once the observation step exists and
  the marker is generation-aware), but no leg of it is currently
  derivable.
- **G4.** Honest leg: root emits S1's token twice in one announce;
  second admission suppressed by derivation hash; single unit;
  GREEN derives. Mutant: double admission → two workers → both
  consults pass the absent-marker window → double unit → P1
  legality alarm; RED derives via the race schedule for the stated
  reason (the sequential schedule is suppressed by the first unit's
  marker and stays green — worth stating in the cell so the
  first-find expectation is explicit). Kill flip confirmed. The
  cell's FURTHER claim — no residual job for scope locks — is
  refuted by the distinct-derivation same-key shape the cell does
  not script: F6.
- **G5a.** Reachable via the checkpoint-predates-S1's-announce
  window (F5(iii)): restored frontier holds S1 pending, C absent; C's
  committed partition is orphan debris; S1 re-runs at epoch 2, no
  child marker; honest sweep drops C's partition (outside the
  live-announce closure). GREEN derives on both variants — via the
  SWEEP alone; purge contributes nothing in any reachable schedule
  (F5). Note C's commit is a record round, not a "unit" (F5
  wording).
- **G5b.** `sweepOff`: C's dead partition seals; P5-UNDER RED
  derives on both variants (S additionally alarms P6-S on the dead
  stamp — multiple alarms, direction consistent). The E-leg
  candidate-hole probe is honest as far as it goes: no re-demand of
  K_C means supersession cannot fire and the leg cannot be green.
  But it is not the strongest premise — F6(iv)'s overlay-intent
  re-demand history defeats sweep and supersession with the sweep
  ON. Kill flip confirmed; probe strength finding stands.
- **G5c.** `sweepOverreach`: S2's in-closure partition dropped;
  P5-OVER RED and P1-CONTENT (fold has rows the partition lost)
  both derive. Confirmed as scripted.
- **G5d.** E-leg premise as scripted (purge removes C from the
  restored frontier) is UNREACHABLE — no checkpoint can hold
  C-pending ∧ S1-pending for a single-unit parent (F5(iii)); the
  green that derives comes from the sweep, so the cell's mechanism
  attribution is wrong even though its verdict direction survives.
  S-leg: derives as scripted (C's rows carry the dead S1 stamp;
  outside the closure; swept) modulo F1's death/announce blindness.
  The cross-variant same-artifact claim: not checkable as declared
  — F8.
- **G7.** Premise requires attempt-failure machinery (§3 has none —
  F11); under the inherited byte-identical fingerprint the stuck
  detector never fires across generation-bumped checkpoints, so the
  declared RED does not derive (F11). Ladder leg contingent on the
  same pins plus the missing attempt-budget row. Direction
  plausible after the F11 pins; underivable as written.

## Answers to the §11 review charge (ten questions)

1. **G-RULE-1 enforcement**: CLEAN. Every scripted demand source in
   G1–G7 is an announced emission (spawn tokens, the child marker
   row, session values); mutation timing is env-side state, not
   structure; no script routes structure through response-loop
   position. Residual: the derivation TIMING ambiguity (F12) is a
   §3 gap, not a purity leak.
2. **Generation/death under two crashes**: FINDING — F4 (generation
   id reuse from un-checkpointed resumes; dead-set mis-derivation).
   The during-checkpoint-commit half is clean (eCheckpoint is one
   atomic op; either token wins wholly).
3. **E's derived-support rebuild well-definedness**: FINDING — F5
   (no well-defined target under the announce-lost/store-reflects
   gap; admitted-by edges missing from the durable checkpoint
   besides).
4. **S stamp durability leaks**: QUALIFIED CLEAN. The §5 row is
   internally coherent — no path drops or narrows a stamp while
   keeping its output (a lost un-checkpointed spawn token loses the
   whole output, which is consistent). The leaks found are
   evidentiary, not durability: death is not announce-visible
   (F7(iii)) and cross-sync stamps are unscoped (F16).
5. **Supersession sufficiency / two-key interleavings**: FINDING —
   F6. The two-live-derivations-one-key row is undefined; the
   overlay-intent record path composes dead with live under E with
   the sweep on; G5b's probe premise is not the strongest.
6. **G6 script fairness**: FINDING — F9. The scripts do not
   pre-decide the bake-off; worse, they cannot decide it (identical
   redo by construction on both scripted interruptions).
7. **P6-E under fan-in**: the per-reader quantification is correct
   as written (one non-re-run reader among two is a witness), but
   the form does not bind re-runs to the live value — F18 (and the
   envelope contains no fan-in to exercise the question — F9).
8. **Aggregate node honesty**: no vacuous P7 is smuggled (no cell
   claims or approaches P7), but the honesty check is vacuous for a
   different reason: no configuration containing X exists — F15.
9. **P1 pins transfer**: FINDING — F3. Complete-rounds counting and
   the empty-fold attestation pin need generation grounding before
   they are well-defined over logs containing dead generations'
   rounds; debris-surfaces-through-content does survive supersession
   on the record path (verified by hand in F6's walk — the
   fresh-replaces fold alarms on dead∪live composition when epochs
   differ), which is the one clean transfer in this family.
10. **Small-scope adequacy**: SPLIT. Adequate for the safety cells
    (G1–G5, G7 — every walked premise fits the envelope). NOT
    adequate for the bake-off: no depth, no fan-in means
    purge-vs-support cannot diverge from stamps anywhere in the
    envelope, E's refcount/transitive-retraction machinery never
    executes, and the session brief's invalidate-everything
    degeneration is unreachable — FINDING, folded into F9.

## Charter-coverage check (deliverable 4 obligations vs the cell set)

Present and correctly shaped: frontier/suppression/generations/sweep
(G4, G5), both lineage variants as a shared-mechanism axis (per the
charter's "implement both and compare"), session variants A/B,
calibration case 2 re-run (G2), case 1/3/4 premise re-runs (G1, G3,
G4), P5 in both failure directions, redo/mechanism/property as the
declared comparison axes, refutation-is-success framing. Missing or
defective: the stampCompression admissibility cell (F10), a dedicated
fresh-artifact supersession cell family (F6 — §1 lists supersession as
modeled; no cell exercises it, and for unit keys it is unreachable),
bake-off scripts that can discriminate (F9), the recommendation's
decision rule and destination (F20), and the E-variant's durable-state
honesty in the mechanism tally (F19). The "expect the model to force
design decisions" clause is discharged early: F1's marker/generation
reconciliation IS such a forced decision, surfaced by review rather
than by checker — consistent with the charter's purpose, and the
reason the re-review is targeted rather than resented.

## Verified clean (for the draft's revision record)

- The no-relitigation boundary is respected: unit-mode is adopted
  without rebuilding the naive/last placements, the walker cells are
  cited as settled hand-offs exactly as CALIBRATION.md's pending note
  designed, and nothing in the draft re-opens P-as-language or the
  frozen verdicts. The findings above concern the graph-side
  adaptation, not the hand-off itself.
- §2's inheritance is genuine: arrival order, armed crash injection,
  truthful validators, announce-only monitors, and
  expected-verdicts-before-first-run all carry with correct graph
  additions; G-RULE-1's monitor discipline (ghost closure from
  announces only) is the right instinct even where F7 shows it needs
  an independent evidence base.
- G3 walks clean end to end: stop-forced checkpoint captures S1 at
  its consult-granularity cursor, resume bumps the stopped
  generation, the re-consult hits the ACTUALLY current (swapped) base
  and fails validation → fetch-fresh; the 3-atomic closure carries
  over; cheap confirmation cell as declared.
- G5b and G5c derive exactly as scripted; the P5-UNDER/P5-OVER split
  with separately named directions is well-formed and both kill
  flips are real (modulo nothing — these two work as written).
- The walker→graph mapping table (§5) is accurate: restart-from-root
  ↦ generation bump, hit-map ↦ nothing (in-unit marker as consult
  provenance, matching the pilots), EnqueuePageTokens ↦ demand-derived
  admission, NextPageToken ↦ same-node cursor advance.
- P2's consult pin transfers cleanly: a marker-suppressed seal's
  scope was consulted (validation match) by the dead generation
  within the same sync, which qualifies under the any-attempt
  wording; staleness hops compute correctly on replayed rows.
- Torn rounds are unreachable model-wide (a round is one execution's
  ops; executions never span attempt boundaries), and the draft
  keeps the monitor active anyway — the inherited discipline applied
  correctly, and stronger than the walker (where only unit-mode
  scopes had the by-construction guarantee).
- Vocabulary: glossary conformance is exact everywhere it was
  checked (node/execution/generation/derivation hash/output key/
  demand closure/sweep/supersession/causal stamp/consistent cut/
  sealed cut; session variant definitions; the three-invariant
  framing). The single drift found is "completed-derivation set"
  (F13) — a draft-internal term, not a glossary term.
- Budget shape (§8) inherits the envelope honestly and restates the
  inductive bet; the G2-only session actors and the scripted-config-
  only aggregate node are correctly fenced (their gaps — F15, F21 —
  are registration, not smuggling).
- Public-repo hygiene: clean. No customer names, tenant identifiers,
  or internal infrastructure anywhere in the draft.

## Summary

| finding | severity | one line | disposition |
|---|---|---|---|
| F1 | MAJOR | per-sync unit-marker suppression vs generation death/retraction is unreconciled; flips G1's committed-unit leg and three G2 legs; the imported walker assumption the charge predicted | pick and pin adoption/scoped-marker/death-redefinition semantics; **re-review-required** (targeted round 2) |
| F2 | MAJOR | P6 vocabulary incomplete: G2 E+A red has no property; P6-S is mechanism-referential so `stampMergeOff` cannot flip; S's seal observation step is in no machine | carry P6-A ghost oracle re-grounded on generations; declare observation machinery in §3; fix-without-re-review |
| F3 | MAJOR | P1 fold/count membership of dead generations' complete rounds unpinned (empty-fold pin fires under one honest reading); G1's "superseded per §4" false — supersession unreachable for unit keys | generation-grounded fold/counting pins; correct G1 text; fix-without-re-review |
| F4 | MAJOR | generation ids reused across un-checkpointed resumes (2 crashes); dead set mis-derives; debris laundered live | forced resume checkpoint ordering pin (or durable-max bump rule) + reuse probe; fix-without-re-review |
| F5 | MAJOR | E's lineage state: admitted-by edges in no durable row; rebuild-agreement target ill-defined; purge unreachable in scripted G5; `purgeOff` kill cannot flip | checkpoint-content + tally edit; checkpoint-consistent rebuild target; re-script the crash window on a paginated parent; fix-without-re-review |
| F6 | MAJOR | supersession not total: live-live same-key undefined (P1 alarms on legal behavior); overlay-intent record path composes dead+live (E "structural" claim false); G4 subsumption overreach; G5b not strongest; no supersession cell | total supersession matrix in §4 + dedicated cells; fix-without-re-review |
| F7 | MAJOR | P3′ name/form drift; P5 ghost closure is circular; demand starvation seals green under every property; no under-admission kill | pin P3′'s form; counterfactual closure or SealExpect oracles; death-inference rule; demand-drop mutant; fix-without-re-review |
| F8 | MAJOR | cross-variant P3′ (G5d) is a cross-run comparison no P monitor can check, and over-strong under smear | per-leg P3′ + harness-level artifact-set comparison with the §10.6 divergence rule; fix-without-re-review |
| F9 | MAJOR | G6 scripts cannot differentiate E/S (leaf crash, pre-execution stop); envelope has no depth/fan-in; S's refusal timing unpinned — the arbitration axis is vacuous | divergence scripts + envelope rows + refusal/purge-domain pins; fix-without-re-review |
| F10 | MAJOR | stampCompression admissibility: §7 obligation, §6 axis, no §9 cell, no §10 entry; all cells violate declare-all-three | add bucketed re-runs of G2-S/G5 with recorded redo; default axis declarations; fix-without-re-review |
| F11 | MAJOR | P4 stuck fingerprint never fires under generation bumps (G7 stuck leg flips green); attempt-failure semantics undeclared; no attempt budget | generation-blind fingerprint; declare attempt failure; budget row; fix-without-re-review |
| F12 | MINOR | demand-derivation timing (per-announce vs post-completion) unpinned; checkpoint reachability depends on it | one-sentence pin; fix-without-re-review |
| F13 | MINOR | "completed-derivation set" is admission-keyed; completed-vs-never-admitted resume rule unstated | rename + state the rule; fix-without-re-review |
| F14 | MINOR | record-path clear + intent enum arrive via parenthesis; §3 registration and clear-placement pin missing (MS-CO-001 lesson) | register ops/intents; pin placement; fix-without-re-review |
| F15 | MINOR | aggregate node X budgeted but appears in no configuration; §1's promise ranges over an empty set | add the scripted-precondition config or de-scope X; fix-without-re-review |
| F16 | MINOR | cross-sync stamp scoping undefined (per-sync generations vs replayed rows' stamps) | pin restamp-on-replay (+ P6-R jurisdiction note); fix-without-re-review |
| F17 | MINOR | sessions × replay product neither covered nor excluded in §1 | explicit exclusion with argument, or 7a/7b analog cells; fix-without-re-review |
| F18 | MINOR | P6-E's "re-run before seal" doesn't bind the re-run to the live value (re-run-before-re-publish schedules) | pin retraction keying / adopt F2's final-value oracle; fix-without-re-review |
| F19 | NOTE | mechanism tally must absorb F2/F5/F6 machinery before any bake-off run | freeze the amended tally in the spec revision |
| F20 | NOTE | recommendation decision rule and destination unpinned | record the arbitration procedure pre-run |
| F21 | NOTE | session axis vacuous outside G2; §6's all-three rule unsatisfiable as written | one-sentence default declaration |
