# Round 3 (TARGETED) — the round-2 repairs and their composition, GRAPH_MODEL_SPEC v3

Scope: the targeted round-3 spot review the round-2 disposition of
R2-F2 required, executing v3's §11 charge verbatim: (1)
writer-adoption ineligibility and its convergence argument; (2)
quiesce-before-bump composed with the retraction queue and the
pre-seal pass; (3) marker lifecycle totality (§4b marker column +
per-sync scoping + poison voiding + P-MARK); (4) admitted-set death
semantics under G-RULE-2's MUST-suppress; (5) MATCH-only adoption
and the G1c oracle; (6) the corrected G2 table, all legs and all
three new kills; (7) regression spot-check of round-2's
verified-clean results. Secondary (registration-grade): the round-2
minors/notes as applied. Anchors: `formal/MODEL_SPEC.md` v11 FROZEN
+ MS-CO-001 (§7 pins incl. round-5 F8 and round-7 F2/F3; §9.6
V-ATOMIC / V-OVERLAY-UNIT and their consult-surface and elision
inheritance), `formal/GLOSSARY.md`, `formal/walker/CALIBRATION.md`
decisions 19–24, `formal/reviews/graph-spec-round1.md`,
`formal/reviews/graph-spec-round2-adoption.md` (the acceptance
baseline — its verified-clean walks and its majors' dispositions as
applied are NOT relitigated; only their v3 composition is in scope).

Method: mechanical walks of every honest history the charge names —
announce-window placements, two-crash compositions, mid-attempt
deaths under the quiesce pin, the G-pending and same-value G2 legs,
the G8d flap-back and G8c poison probes, the G5f/G6c re-derived
baselines — under 2 workers, ≤ 2 crashes, ≤ 3 attempts, ≤ 3 epochs,
pass-iteration budget ≤ 3, per §8; independent re-derivation of
P1/P2/P3′/P-GEN/P-MARK/P6-G/P6-E/P6-S and the closure oracle over
every walked history; kill-flip derivation for `writerAdopt`,
`quiesceOff`, `markerCleanupOff`, `adoptOnFail`, plus regression
flips for the inherited toggles; a durability sweep of §5's new and
amended rows (marker, mid-attempt bumps, retraction queue / pass
state) against crash and announce-in-flight placements; a totality
sweep of §4b's marker column over every marker state a walk can
construct; a chassis-constructibility check for every leg the charge
names (can the scripted premise exist under the declared semantics —
the round-1 F5(iii) discipline).

Verdict: **REJECT — 2 majors + 4 minors + 3 notes. Both majors are
fix-without-re-review; NO finding is re-review-required.** All six
round-2 majors are soundly repaired as applied: the
writer-ineligibility core is sound (every scripted dead-writer
history re-derives live and the pass converges within budget — the
convergence argument the round-2 charge found missing now derives
mechanically on every scripted honest history), quiesce-before-bump
closes R2-F1's wipe in every schedule walked and cannot starve the
pass or deadlock, the marker lifecycle matrix is total over every
constructible marker state, the admitted-set death semantics close
the starvation/double-admission space under two workers, MATCH-only
adoption is classifiable in every scripted cell with SealExpect as
the correct oracle, and the corrected G2 table derives leg by leg.
The two majors are v3-composition seams the repairs newly expose,
not defects in the repairs as specified: (R3-F1) the
writer-ineligibility machinery — including its `writerAdopt` kill —
rests on an UNREGISTERED semantics choice about whether a
replay-verdict re-derivation re-performs session publishes, and the
two honest readings diverge on the kill's fireability and on the
convergence claim itself; (R3-F2) mid-attempt-minted generations are
not durably fenced, so a budget-legal two-crash history REUSES a
generation id and P-GEN fires red on an honest run — round-1 F4's
hazard resurrected through the retraction-queue path that v3's own
pins made walkable for the first time. Both repairs are one-pin
registrations of disciplines the spec already practices elsewhere;
the fixed spec needs a disposition registration check, not a fourth
adversarial round.

## Majors

- **R3-F1 (MAJOR) — whether a re-derivation whose re-consult MATCHes
  (a replay-verdict round) re-performs the node's session publishes
  is unregistered; the two honest readings diverge on the
  `writerAdopt` kill's fireability (under the glossary's elision
  reading the kill is structurally unfireable in every legal
  chassis), and under the same reading a budget-legal writer
  flap-back history strands a dead publish on an HONEST run —
  falsifying §4a's "transient by construction" convergence claim and
  breaking the R2-M1 writer-pending invariant the spec declares
  checkable.**
  - Claim under attack: §4a's convergence rationale ("the writer's
    forced re-derivation re-publishes under its live generation
    (same value or not)"); §6's `writerAdopt` row ("G2
    announce-window legs ... S: P6-S RED at seal ... E+B: P6-E
    RED"); G2's announce-window leg ("publish-bearing unit committed
    ... re-derives → re-publishes"); §3's session-read invariant
    ("the dead value's writer is pending by construction");
    `formal/GLOSSARY.md`'s elision pin ("session writes (and reads)
    the elided enumeration would have made do not occur this sync").
  - Evidence, walked. The model must pick one of two readings of
    what a node execution's session publishes are. READING E
    (elision, the glossary/walker anchor): publishes ride the
    enumeration; a MATCH verdict's replay round elides them, so
    publish-bearing markers arise only from CHANGED-WITH-DIFF units
    (fetch-fresh rounds carry no marker, G8a). READING B (body-op,
    the reading round 2's accepted walks silently used — R2-F2's own
    evidence history has H committing a publish-bearing unit under a
    no-mutation premise, constructible only if publishes are
    node-body store ops independent of the verdict class):
    publishes execute on every non-adopted execution. The spec
    registers neither. Now the divergences, both within budget:
    (i) THE KILL. `writerAdopt` removes writer-ineligibility only;
    MATCH-only (R2-F3's pin) still applies, so the mutant fires only
    where an adoption is otherwise eligible: marker present, MATCH
    re-consult, digest equal. Under READING E every publish-bearing
    marker is a diff unit whose digest records a FAIL outcome; an
    adoption additionally needs a MATCH re-consult, whose digest
    then differs (FAIL ≠ MATCH) → re-derive regardless of the
    mutant. Exhaustive over chassis: stable premise (no
    between-attempt mutation) → re-consult FAILs again (the consult
    surface is the PREVIOUS artifact, §9.6 o-iii carried "as
    everywhere", so the current sync's V2 publish never converts the
    verdict) → MATCH-only blocks; flap-back mutation → MATCH but
    digest differs → blocks. The kill CANNOT fire in any legal
    history — a tabled kill for the round-3 re-review target's
    load-bearing machinery that cannot flip, the §10.1
    recommendation-halting class (round-1 F5(iv) precedent). Under
    READING B a replay-verdict publish-bearing unit is constructible
    (MATCH premise, body publish), the announce-window chassis
    exists, and the kill fires exactly as declared: H@g2 adopts →
    no re-execution → no re-publish → d1 stays writer-stamped
    g1-dead → the pass forces G, G's re-read returns (d1, g1)
    unchanged → digest EQUAL → G re-adopts → no progress per
    iteration → budget exhausts → sealed dead stamps → P6-S RED
    (R2-F2's loop as a kill, verbatim); E+B: no re-publish → no
    retraction → G never re-runs → P6-E RED. One toggle, one
    scripted leg, two honest readings, opposite fireability.
    (ii) THE HONEST HOLE. Under READING E, walk the writer
    flap-back (budget-legal: 1 crash, 2 mutations — the G8d shape on
    a writer): sync 2 warm; upstream e1→e2 before the sync; H's
    consult hits V1, revalidation FAILS → CHANGED-WITH-DIFF; the
    diff enumeration publishes d1; the overlay unit commits with
    marker(g1, (V1, FAIL), publishBearing=1); reader G completes
    embedding d1 with {H: g1}; checkpoint in H's announce window;
    crash; upstream mutates e2→e3 with content(e3) = content(e1).
    Resume: bump; H@g2 is adoption-INELIGIBLE (bit) → re-derives →
    re-consult: V1 vs e3 MATCHES (truthful validator; content
    reverted) → verdict REPLAY → the replay unit ELIDES the
    enumeration → NO re-publish. The dead publish d1@g1 is stranded
    exactly as in R2-F2's loop, with writer-ineligibility ON and
    honestly obeyed: G's premise re-read returns (d1, g1) unchanged
    → digest EQUAL → G adopts through unchanged premises forever →
    the pass budget-exhausts → P6-S RED honest; under E+B no
    re-publish means no retraction → P6-E RED honest. The §3
    session-read invariant ("the dead value's writer is pending by
    construction") is also violated on this history — H completed
    without clearing the value — so the model's own declared
    checkable invariant fires on an honest run. Under READING B the
    same history converges (the replay re-derivation's body
    re-publishes d1@g2; digest differs; G re-derives; two pass
    iterations). No SCRIPTED cell schedules a flap-back mutation on
    a session writer, so no scripted honest verdict flips today —
    the divergence that flips a scripted verdict is the kill leg,
    (i).
  - Why it matters: this is the round-3 charge's question 1 asked
    and answered in the negative under one honest reading — the
    convergence argument is sound only under a semantics the spec
    never states, and the repair's OWN kill (the evidence that the
    ineligibility bit is load-bearing) exists only under that same
    unstated reading. It sits on the F17 exclusion boundary the
    round-2 disposition was warned about: the within-sync
    death-and-re-derive machinery constructs a sessions × replay
    product (a replay-verdict re-run of a node that published this
    sync) that §1's cross-sync exclusion argument does not cover.
  - Disposition (fix-without-re-review): REGISTER READING B — one
    §3/§4a pin: session publishes are node-body store ops, executed
    by every non-adopted execution regardless of verdict class
    (adoption alone skips them, which is exactly what the
    eligibility bit exists to prevent); declared as a within-sync
    deviation from the walker's elision vocabulary, with the
    cross-sync sessions × replay product remaining excluded and
    walker-owned (extend the §1 F17 sentence). This is a
    registration of the reading round 2's accepted walks already
    used, not new mechanism — under it, every declared verdict in
    this review's walks derives (verified below): the
    announce-window honest leg, both `writerAdopt` flips, the
    writer flap-back history (converges), and the R2-M1 invariant.
    Also register the announce-window leg's chassis (a
    replay-verdict publish-bearing unit, MATCH-stable premise) so
    the kill's adoption-eligibility is constructible by script, and
    add the writer flap-back history as a probe leg (expected: pass
    converges, forced-redo count ≥ 1).

- **R3-F2 (MAJOR) — mid-attempt-minted generations are not durably
  fenced: a retraction-forced bump commits (volatile), the new
  generation dispatches and commits durable output, a crash lands
  before any checkpoint (placement is a genuine choice point), and
  the forced resume checkpoint re-mints the SAME generation id from
  the restored table — G-RULE-3's "durably fenced" is false on a
  budget-legal honest history, P-GEN fires red on it, and attempt-2
  debris reads as live in the window; round-1 F4's hazard
  resurrected through the retraction path v3's own queue/quiesce
  pins made walkable.**
  - Claim under attack: G-RULE-3 ("generations are per-node monotone
    counters, DURABLY FENCED: execution (n, g) exists only after
    every (n, g' < g) is dead"); §5's generation-table row
    ("latest-per-pending in checkpoints; death announce-visible")
    and its mid-attempt-bump row ("volatile scheduler state whose
    loss is self-healing"); §7 P-GEN ("no two attempts contain
    store-commit announces attributed to the same (node,
    generation)") as an every-cell monitor.
  - Evidence, walked (budget-legal: E+B, the G-pending chassis, 2
    crashes, 1 mutation, 3 attempts, 1 worker suffices). Attempt 1:
    H@g1 publishes d1; checkpoint captures G pending ∧ H pending
    (mid-read-window); crash 1. Resume 1: forced resume checkpoint
    cp1 commits {G@g2, H@g2H} (F4's pin, correctly applied — these
    resume-minted ids are durable before dispatch). G@g2 dispatches,
    reads stale d1, completes (unit commits, rows@g2). H@g2H
    re-derives d2 (upstream moved), re-publishes → retraction entry
    for reader G@g2 → G@g2 is quiesced (completed) → the bump
    g2→g3 commits as a derived announce on H's completion carrier —
    SCHEDULER STATE ONLY; §5 pins mid-attempt bumps volatile and no
    rule forces a checkpoint here (placement is a choice point, and
    the skip schedule is legal). G@g3 dispatches, re-derives, its
    unit COMMITS DURABLY — rows and marker attributed (G, g3).
    Crash 2, before any checkpoint. Resume 2: restore cp1 (the last
    durable checkpoint: G pending@g2, H pending@g2H); forced resume
    checkpoint bumps from the RESTORED table: G g2→g3, H g2H→g3H —
    (G, g3) is re-minted. Attempt 3's (G, g3)′ dispatches and
    commits (its marker check finds marker(g3) — its OWN id —
    recomputes, digest differs on the dead H stamp, re-derives, a
    second (G, g3)-attributed unit commits). Consequences, each
    checked: (a) P-GEN's recorded rule fires — attempts 2 and 3
    both contain store-commit announces attributed to (G, g3) — an
    honest RED from the monitor F4's repair installed; (b)
    G-RULE-3's identity uniqueness is violated in effect (one
    stamp, two producers — round-1 F4's exact phrasing); (c) in the
    window before attempt 3's clear, attempt-2's rows@g3 (embedding
    the dead {H: g2H} component) read as LIVE under the
    dead-set-derivation rule (every generation below latest is
    dead; g3 IS latest) — dead debris laundered live; content is
    eventually rescued only because every unit/record path is
    clear-based, which is luck of the mechanism, not a fence.
    Reachability requires nothing exotic: a mid-attempt bump (any
    retraction or observation re-admission), one post-bump durable
    commit, and a crash with the checkpoint skipped. The scripted
    G2/G1d configs arm one crash, so no CURRENT cell reaches it —
    but §8 budgets two crashes, P-GEN is an every-cell monitor, and
    the claim "durably fenced" is false as written. This walk was
    not constructible in round 2: R2-F1 left the retraction queue's
    enqueue/drain semantics unstated, so the post-bump dispatch
    path had no pinned behavior to walk; v3's queue and quiesce
    pins created the seam they now must fence.
  - Why it matters: every death-keyed mechanism keys off generation
    identity (round-1 F4's why-it-matters, verbatim applicable).
    The forced-resume-checkpoint discipline exists precisely so
    that no generation id is used before it is durable; v3 mints
    ids on a second path and forgot the discipline there.
  - Disposition (fix-without-re-review): extend the F4 discipline
    to mid-attempt minting — pin that a mid-attempt bump's
    generation-table delta is durable BEFORE the bumped generation
    dispatches. Cleanest form, F4-parity, no new state class: the
    bump forces an `eCheckpoint` commit (the checkpoint already
    carries the generation table per G-RULE-4) between the bump's
    carrier announce and the new generation's dispatch; the
    alternative durable-max bump rule (resume bumps to
    max(restored latest, highest generation in durable stamps for
    the node) + 1 — round-1 F4's option (b)) is also sufficient and
    touches only the resume rule. Verified under either pin: the
    walk above mints g4 at resume 2 (or restores g3 as durable) —
    no collision, P-GEN green, the dead-set derivation correct.
    Amend §5's two rows ("self-healing" is true for the LOSS of the
    bump's scheduling effect, not for id reuse — say so), add the
    two-crash reuse history as a probe cell (expected
    unreachable-after-pin), and give the pin a kill
    (`midBumpFenceOff` → P-GEN RED), mirroring G1b/`resumeCkptOff`.

## Minors

- **R3-M1 (MINOR)** — G8d's honest-leg mechanism attribution is
  wrong: attempt 3's consult HITS the previous artifact's V1 and
  revalidates against e3 whose content equals e1's, so the truthful
  validator MATCHES and the verdict is REPLAY, not "ordinary
  fetch-fresh" as the cell text says. The cell's own mutant text
  concedes this ("attempt 3 premise-matches it (V1 MATCH vs e3)") —
  the honest leg's text contradicts its mutant leg. SealExpect
  rows(e3) is verdict-equal either way (replay of the e1 base =
  rows(e3)); text correction only, the round-2 G1(ii) class.
  Fix-without-re-review.
- **R3-M2 (MINOR)** — eAdopt × poison has a check-then-act window
  the matrix's "no adoption of poisoned content is EVER legal"
  overclaims: the void bit is read at the worker-side marker check,
  and a second derivation's poisoning commit can land between that
  check and the `eAdopt` commit (2 workers; the N4/round-7-F6 window
  class). G8c's scripted leg forces the re-run post-poison, so no
  scripted verdict is affected, and the seal-exclusion bounds the
  hazard either way — but "ever" needs a mechanism: pin a store-side
  `eAdopt` precondition (refuse on a poisoned key — the same shape
  as R2-N1's fromGen-dead precondition) or reword to the scripted
  guarantee and record the window as a boundary note.
  Fix-without-re-review.
- **R3-M3 (MINOR)** — the pre-seal pass's iteration boundary is
  unstated: nothing says a scan begins only from a re-drained
  frontier. The drain-gated reading is strongly implied (the pass
  lives inside the seal sequence, whose precondition is a drained
  frontier, and a forced re-admission un-drains it), and under it
  every honest walk in this review converges in ≤ 2 iterations; an
  eager-re-scan reading would burn the budget observing the same
  dead stamp while the forced re-run is still in flight and make
  the §10.8 convergence assertion measurement-dependent. One
  sentence ("one iteration = one scan over a drained frontier").
  Fix-without-re-review.
- **R3-M4 (MINOR)** — §4a's convergence claim quantifies over "every
  honest history" but the mechanism only guarantees convergence
  when the dead component's writer is STILL DEMANDED: a writer
  legitimately de-demanded by an epoch shrink (its parent's live
  re-derivation no longer derives it; ∀-purge removes it) never
  re-publishes, and a still-demanded reader of its sync-scoped
  session value then carries an unclearable dead component — the
  pass budget-exhausts and P6-S reds on an honest history, while
  E+B seals green (a variant asymmetry worth recording). No
  scripted cell composes sessions with a demand shrink (sessions
  exist only in G2/G9, which script none), so no cell verdict is
  affected; the CHECKED form ("every honest cell converges") is
  true. Scope the prose claim to the scripted envelope and add the
  sessions × shrink exclusion to §1/§8's inductive-bet list (the
  R2-M6(ii) treatment), or script the leg. Fix-without-re-review.

## Notes

- **R3-N1 (NOTE)** — the at-least-once cost claim for
  writer-ineligibility is HONEST (charge question 1's second probe,
  answered affirmatively): a publish-bearing round's rows are
  re-derived, and every consequence is cost-shaped, not
  verdict-shaped — §4d's supersession removal keeps
  replacement-count legality at one (the dead round's copy leaves
  the count at the clear), G-RULE-2's MUST-suppress over
  pending∨completed prevents child re-execution (and a purged
  child's re-admission via the re-announce is the intended redo),
  P2 qualifies via the re-derivation's own consult, P3′/SealExpect
  see identical or fresher content, and P-GEN attribution is clean.
  No history was found where losing row-adoption changes a verdict.
  Recorded for the bake-off's forced-redo bookkeeping.
- **R3-N2 (NOTE)** — the deferral non-interference arguments
  (charge question 2) are sound and worth recording in §3 as two
  sentences: (a) a deferred bump cannot starve the pass budget
  because deferral requires an in-flight dying execution, which
  means a non-drained frontier, and the pass only runs and iterates
  from a drained frontier — every deferral resolves strictly before
  the pass's first scan; (b) two workers' deferrals cannot deadlock
  because a deferral waits on an execution's completion and no
  execution's completion ever waits on a bump — the wait graph is
  one-directional. Verified additionally: a crash that loses a
  pending deferral is self-healing exactly as §5 claims (the resume
  bump re-admits the reader regardless), MODULO R3-F2's fencing of
  bumps that had already COMMITTED and dispatched.
- **R3-N3 (NOTE)** — the round-2 minors and notes are all applied as
  dispositioned; registration sweep in its own section below. One
  cross-reference: R2-M1's registered invariant is the honest-red
  witness in R3-F1's reading-E history — the registration did its
  job (it made the hole checkable), which is evidence for the
  disposition discipline, not against the registration.

## Mechanical walks (the acceptance test for the round-2 repairs)

All walks under READING B where the reading matters (per R3-F1's
disposition — the reading round 2's accepted baseline used);
divergences under reading E are recorded in R3-F1 and not repeated.

- **Writer convergence, announce-window (G2 new leg, honest)**:
  checkpoint captures H pending, publish-bearing unit committed
  (d1@g1 durable); G completed embedding {G: gG, H: g1}. Crash.
  Resume: forced checkpoint commits H@g2; marker check —
  publishBearing set → INELIGIBLE → re-derive → re-publish d1@g2.
  E+B: retraction keyed (key, d1, g1) fires on the re-publish →
  G re-admitted → re-reads d1@g2 → digest differs (writer stamp) →
  re-derives → embeds live → queue drains → seal d2... seal d1@g2
  content, equal to the final live derived value → P6-G GREEN. S:
  frontier drains (H completed) → pass scan 1: G's rows carry
  {H: g1} dead → force G → re-read d1@g2 → digest differs →
  re-derive → scan 2 clean → CONVERGED in 2 ≤ 3. P6-S green.
  **Derives as declared.**
- **Writer convergence, two crashes**: the same chassis with crash 2
  in H@g2's announce window (checkpoint captures H pending, g2 unit
  + publish committed). Resume 2: bump g3 (resume-minted, durable at
  the forced checkpoint — fenced); marker(g2, publishBearing) →
  ineligible → re-derive → re-publish d1@g3 → readers cleared as
  above → pass converges in 2. Attempts: 3 ≤ 3. **Derives.** (The
  UNFENCED two-crash shape — crash 2 after a MID-ATTEMPT bump's
  post-dispatch commit — is R3-F2, a different placement.)
- **Writer mid-attempt death**: unreachable in-envelope — a writer
  is retraction-bumped only if it reads a re-published value
  (reader-writer, excluded §1/§8) and observation-bumped only via
  dead components in its own rows (same exclusion). Writer death is
  resume-path only; the convergence argument needs exactly the
  resume path. **Vacuity confirmed, correctly fenced by the
  declared exclusions.**
- **G1d honest (quiesce)**: retraction bump lands while dying reader
  G@g2 is in flight → DEFERS → G@g2 completes (late unit commits,
  embedding stale d1, marker g2) → bump commits atomically with the
  completion announce → G@g3 dispatches → marker(g2) found, reader
  bit clear → premises recompute: reads d2@g2H → digest differs →
  RE-DERIVES → its clear removes g2's rows → last writer → queue
  entry removed at G@g3's completion → queue empty → seal embeds d2
  → P6-G GREEN. **Derives as declared.**
- **G1d mutant (`quiesceOff`)**: bump commits immediately; G@g3
  re-derives d2 on the free worker and commits; the never-cancelled
  G@g2 then commits its late atomic unit — clear wipes g3's live
  rows, installs d1 — no further re-publish, queue drains, seal
  embeds d1 ≠ final live d2 → P6-G RED. **Flips for the stated
  reason.**
- **G8d honest (flap-back)**: attempt 1 replay unit + marker(g1,
  (V1, MATCH)); crash 1; e1→e2; attempt 2 marker found, FAIL →
  MATCH-only → re-derive → fetch-fresh RECORD round — `eClearScope`
  removes g1's rows AND DELETES the marker (the v3 pin) → rows(e2)
  + V2; crash 2 in the announce window; e2→e3 (content = e1's).
  Attempt 3: NO marker → ordinary consult → V1 MATCHES vs e3 →
  replay unit (not fetch-fresh — R3-M1) → clear removes g2's dead
  rows → seals rows(e3) → SealExpect GREEN, P-MARK vacuous-to-true
  throughout, P1 fold = replacement(e1) = rows(e3). **Derives;
  verdict as declared, mechanism attribution corrected.**
- **G8d mutant (`markerCleanupOff`)**: the clear leaves marker(g1,
  (V1, MATCH)); attempt 3 finds it, recomputes (V1, MATCH) → digest
  EQUAL, bit clear → ADOPTS — while the partition holds g2's
  rows(e2) that the marker never described → P-MARK RED (marker ⟹
  partition equals the marked round's outputs; announce-evidenced
  from attempt 2's record round onward); under E the adopting MATCH
  qualifies → P3′ expects rows(e3) = rows(e1) against sealed
  rows(e2) → P3′ RED. **Flips for the stated reasons, both
  monitors.**
- **G8c post-poison leg**: second distinct derivation's first commit
  poisons the scope AND voids the marker; the scripted forced
  re-run finds a voided marker → re-derive-or-refuse (no adoption)
  → post-poison rounds commit legally → scope seal-excluded
  (SealExpect) → P1 legality exempt (the poison is the alarm).
  **Derives as declared.** The unscripted marker-check/eAdopt
  commit race is R3-M2 (boundary window, no scripted verdict
  affected).
- **Marker-state totality hunt**: states {absent, present(g, D,
  bit), voided} × transitions {unit put (overwrite), REPLACES clear
  (delete), poison (void), eAdopt (generation update, digest and
  bit invariant — premises equal by definition), seal (drop —
  per-sync), crash (durable, no change), purge (no change; rows
  swept at seal, marker dropped at seal — P-MARK holds
  throughout)}. Live-rows record rounds for a marked key are
  unreachable except through the poison row (a unit round is
  single-op, so same-derivation continuation cannot follow it; a
  second execution of the node requires death → dead rows; distinct
  derivation → poison). OVERLAY-intent records over a marked key:
  only via the illegal dead-base row (mutant-only). **No
  unclassified marker state found; the matrix is total.**
- **G5f honest baseline (re-derived)**: no-shrink epoch; C admitted
  and checkpointed; crash; C's only edge dead → ∀-purge removes C
  AND its hash; the parent's live re-derivation (or adoption's row
  re-announce) re-derives the hash → not pending, not completed
  (hash removed, completion rule evaluated after removals) → MUST
  admit → C re-runs → closure complete → GREEN; `demandDropOff` →
  closure oracle RED. **Starvation unreachable, as the cell now
  asserts; both directions derive.**
- **G6c E leg (re-derived)**: C has edges from S1 (dead after the
  crash) and S2 (live) → ∀-purge does NOT fire → C survives, zero
  extra redo; S leg: C runs under S1@g2's live re-derivation of the
  hash (dispatch-time re-validation, no false refusal). **"Both
  keep C with zero redo" derives under the pins.**
- **Two-worker admitted-set composition**: admissions are derived
  per-announce, atomic with the carrier's bookkeeping (G-RULE-1),
  and the scheduler serializes announce processing — two parents
  deriving one hash admit once, suppress once (pending). Purge and
  refusal are scheduler steps (resume-time / dispatch-time), never
  concurrent with an admission of the same hash. Refusal-then-
  re-announce re-admits; re-announce-then-dispatch runs under the
  live re-derivation. **No suppress/starve gap, no double-admission
  gap.**
- **G1c honest**: marker digest (V1, FAIL, ⊥); attempt 2 re-consult
  FAILs vs e3 → MATCH-only forces re-derive → the re-derivation
  fetches at e3 → SealExpect rows(e3) GREEN. Mutant `adoptOnFail`:
  digest (V1, FAIL) equal → adopts rows(e2) across e2→e3 →
  SealExpect RED. P3′ CANNOT catch the mutant (the FAIL re-consult
  performs no fetch and does not qualify under the F8 pin, so the
  last qualifying verdict is attempt 1's diff at e2, which expects
  the mutant's own rows(e2)); P2 stays green via any-attempt
  wording. **SealExpect is not just the right oracle — it is the
  only oracle that flips; the walker 5b discipline correctly
  applied. Both legs derive.**
- **G2 full table**: E+A — nothing re-runs G; seal embeds d1 ≠
  final live d2 → P6-G RED, derives. E+B — retraction → re-run
  (quiesce-deferred where needed, G1d) → seal d2 → P6-G GREEN, now
  UNCONDITIONAL (the round-2 conditional reading is closed by the
  pin); `retractionOff` → P6-G RED. S+A/S+B — resume bumps H; H
  re-derives d2 (FAIL vs moved upstream; marker ineligible or
  absent), re-publishes; pass scan 1 forces G; digest differs;
  re-derive; scan 2 clean → P6-G GREEN, P6-S green, converged;
  `stampMergeOff` → the pass is blind, sealed d1 → P6-G RED
  (mechanism-independent oracle confirmed again). SAME-VALUE
  CONTROL — H@g2 re-derives d1 exactly, re-publishes under g2;
  pass forces G on the dead {H: g1}; the re-read's writer stamp
  differs → re-derive → all live → P6-G GREEN, P6-S GREEN at seal,
  FORCED-REDO COUNT ≥ 1 (the pass-forced re-run's derived
  announce) — the R2-F6 vocabulary now derives exactly as
  declared. ANNOUNCE-WINDOW — walked above, derives (chassis
  registration per R3-F1). `writerAdopt` — flips on both variants
  for the stated reasons UNDER READING B (S: budget-exhausted seal
  with dead stamps → P6-S RED; E+B: no re-publish → P6-E RED;
  P6-G stays green on the same-value content, correctly
  distinguishing oracle from mechanism); unfireable under reading
  E — R3-F1 is prerequisite to citing this kill. G-PENDING — G's
  pre-re-publish re-run reads stale d1 and completes; H re-publishes
  d2; the re-retraction clause fires on the completed re-run
  (quiesce trivially satisfied) → G re-runs again → embeds d2 →
  P6-G GREEN; witnesses the keying pin's clause as intended.
  **Every declared verdict derives; three kills flip (one
  conditionally on R3-F1's registration).**

## Regression spot-check (charge question 7)

- **G1 legs (i)–(iii)**: (i) unchanged, GREEN derives (no marker
  machinery engages). (ii) marker found → FAIL → re-derive →
  record REPLACES — the clear now also deletes the marker; nothing
  downstream in this history reads it; seal rows(e2)@V2 GREEN
  derives, and the round-2 latent stale marker is gone. (iii)
  MATCH, digest equal, publishBearing CLEAR (S1 neither reads nor
  publishes sessions) → ADOPT → GREEN derives — writer-ineligibility
  does not perturb the repair's flagship leg. `suppressionOff`
  mutant: unchanged, P1-LEGALITY first-find with the R2-N1 declared
  deviation. **Preserved.**
- **Fold back-port coherence (§4d)**: text unchanged and still
  death-gated verbatim (the walker cell-4 caveat honored); the v2
  transfer-of-a-removed-contribution incoherence is unreachable now
  that adoption requires a present marker and P-MARK ties the
  marker to the partition's actual outputs; replacement-count
  legality re-derived at one across every at-least-once redo walked
  (the superseded round's copy leaves the count at its clear).
  **Preserved.**
- **eAdopt atomicity**: still one store op; the v3 addition (marker
  generation update inside the op) does not tear it; re-announce
  attribution to the adopter unchanged; P5 ghost closure and
  G-RULE-1 timing unaffected. **Preserved.**
- **P-GEN checkability**: the R2-N3 rule is recorded (G-RULE-3, §7)
  and remains checkable; resume-path fencing (F4) intact — G1b and
  `resumeCkptOff` well-formed. The MID-ATTEMPT minting path breaks
  the rule's truth on an honest two-crash history — R3-F2, a new
  v3-composition seam, not a regression of the round-2 verification
  (which walked no post-bump-dispatch crash because the queue
  semantics did not yet exist to walk). **Preserved on the round-2
  evidence base; extended surface broken — R3-F2.**
- **Inherited kills** (`sweepOff`, `sweepOverreach`, `purgeOff`,
  `resumeCkptOff`, `overlayComposeDead`, `demandDropOff`,
  `retractionOff`, `stampMergeOff`): none interacts with the v3
  pins in any walked history; G5e's count-oracle premise is
  untouched by ∀-purge (its child has one dead edge). **Preserved.**

## Round-2 minors/notes registration check (lighter touch)

- **R2-M1** — session-read observation point registered in §3 as
  read-through + derived dead-read announce + the writer-pending
  invariant. APPLIED. (The invariant is load-bearing evidence in
  R3-F1's reading-E history; under the R3-F1 pin it holds on every
  honest history — re-verified.)
- **R2-M2** — derived-announce carrier pin registered in G-RULE-1;
  resume bumps ride the forced checkpoint. APPLIED. (Durability of
  the mid-attempt bump itself is R3-F2 — the carrier pin grounds
  the ANNOUNCE, which is what R2-M2 asked.)
- **R2-M3** — per-sync marker scoping pinned in §3 (current-sync
  store only; seal drops marker rows) and §4c's cross-sync wording
  deleted; §5 row amended. APPLIED.
- **R2-M4** — §4b dead-base OVERLAY row reworded to scripted policy
  + trust model, with `overlayComposeDead` as the load-bearing
  check. APPLIED.
- **R2-M5** — adopting executions' premise re-reads register
  normally, pinned in §3 and quantified into P6-E. APPLIED.
- **R2-M6** — (i) G-pending leg scripted and walked (derives); (ii)
  session-transitive chains excluded with the keying-uniform
  argument in §1 and restated in §8's inductive bet; (iii)
  publish-derived demand excluded from G-RULE-1's vocabulary with a
  change-order requirement. APPLIED.
- **R2-M7** — pass-iteration budget row in §8 (≤ 3), convergence a
  checked expectation, budget-exhausted seal announce-visible.
  APPLIED. (Iteration boundary wording — R3-M3.)
- **R2-M8** — poison voids the marker; post-poison rounds
  classified; G8c's post-poison leg added and walked. APPLIED.
  (Commit-order window — R3-M2.)
- **R2-N1/N2/N3** — eAdopt fromGen-dead precondition, digest-closure
  vacuity sentence, P-GEN monitor rule: all recorded where the
  dispositions specified. APPLIED.

## Answers to the §11 round-3 charge

1. **Writer-ineligibility convergence**: SOUND on every scripted
   honest history under the operative (round-2-baseline) reading —
   the dead writer is pending at resume, must complete before the
   frontier drains, is barred from adoption by the bit, re-publishes
   under its live generation, and retraction (E+B) or the
   writer-stamp digest delta (S) clears every reader in one forced
   re-run; the pass converges in ≤ 2 iterations in every walk,
   including two-crash and announce-window placements; mid-attempt
   writer death is unreachable in-envelope. The argument is
   UNREGISTERED at its load-bearing step (does a replay-verdict
   re-derivation re-publish) — R3-F1; and its "every honest
   history" quantifier overreaches the mechanism for de-demanded
   writers — R3-M4. Rows-owned-by-writers: cost-only, claim honest
   — R3-N1.
2. **Quiesce composition**: no pass-budget starvation (deferrals
   resolve strictly pre-drain), no two-worker deadlock (waits are
   one-directional), crash-loss of a pending deferral self-heals
   via the resume bump; G1d honest and mutant legs derive — R3-N2.
   The COMMITTED-bump durability seam is R3-F2.
3. **Marker lifecycle totality**: total — every constructible
   marker state and transition is classified by the §4b column plus
   the per-sync and poison pins; P-MARK is preserved by every row
   and fires exactly on the mutant; G8d honest and mutant derive
   (one text misattribution, R3-M1); the eAdopt×poison window is
   boundary-grade, R3-M2. No missing state found.
4. **Admitted-set death semantics**: compose cleanly — MUST-suppress
   over pending∨completed with purge/refusal hash removal and the
   after-removals completion rule closes both the starve and
   double-admission gaps under two workers; G5f and G6c re-derive
   as declared.
5. **MATCH-only adoption**: classifiable in every scripted cell —
   every legal adoption's consult is its own MATCH (P2), P3′
   coheres through truthful validators, ADOPT is a registered
   verdict class; G1c derives both legs and SealExpect is the only
   oracle that can flip the mutant — the right oracle, confirmed.
6. **The corrected G2 table**: all seven legs derive; `quiesceOff`
   and `markerCleanupOff` flip unconditionally for the stated
   reasons; `writerAdopt` flips for the stated reasons ONLY under
   the reading R3-F1 requires the spec to register — as written it
   is unfireable under the equally honest elision reading.
7. **Regression**: round-2's verified-clean results preserved (G1
   legs, G2 axis legs, fold back-port, eAdopt atomicity, resume-path
   P-GEN); the one break (R3-F2) is on a surface round 2 could not
   walk, introduced by v3's own — otherwise correct — queue/quiesce
   pins.

## Verified clean (for the revision record)

- The six round-2 majors are repaired as dispositioned and their
  repairs compose in every scripted history walked; nothing in this
  review re-opens a round-1 or round-2 disposition as applied, and
  neither major is a defect in a repair's specified mechanism —
  both are seams BETWEEN the repairs and pre-existing semantics
  (elision vocabulary; generation-fencing discipline).
- The pass-iteration budget (R2-M7) is honest: every honest walk
  converges in ≤ 2 of the budgeted 3 iterations, and the one
  constructed budget-exhaustion (reading-E writer flap-back) is
  exactly the class the budget-exhausted-seal red exists to catch.
- The §7.5 tally as amended reflects the v3 mechanisms accurately
  (quiesce, queue semantics, eligibility pins, marker column, death
  semantics, observation points + budget) and writer-ineligibility
  is correctly charged to both variants as shared machinery in G6.
- G3, G4, G7, G9 are untouched by the v3 pins in every walked
  history; G7's attempt-failure machinery composes with quiesce
  (a loud failure's checkpoint captures pending state; a
  never-committed deferral mints nothing and self-heals).
- Public-repo hygiene: clean. No customer names, tenant
  identifiers, or internal infrastructure in v3 or in this review.

## Summary

| finding | severity | one line | disposition |
|---|---|---|---|
| R3-F1 | MAJOR | session-publish semantics under replay-verdict re-derivation unregistered; `writerAdopt` kill unfireable under the elision reading and the convergence claim fails on a budget-legal writer flap-back; all declared verdicts derive under the round-2-baseline body-op reading | register the body-op pin (+ F17 boundary sentence), register the announce-window chassis, add the writer flap-back probe; fix-without-re-review |
| R3-F2 | MAJOR | mid-attempt-minted generations not durably fenced: bump → dispatch → durable commit → crash-skip-checkpoint reuses the id; P-GEN red on honest two-crash history; G-RULE-3's "durably fenced" false | force durability of the bump's table delta before dispatch (checkpoint-at-bump or durable-max resume rule), amend §5, add reuse probe + fence kill; fix-without-re-review |
| R3-M1 | MINOR | G8d honest leg says "fetch-fresh" where truthful validators derive REPLAY (its own mutant text concedes MATCH); verdict unchanged | correct the cell text; fix-without-re-review |
| R3-M2 | MINOR | eAdopt × poison check-then-act window; "never legal" overclaims relative to worker-side check | store-side eAdopt poison precondition or boundary-note wording; fix-without-re-review |
| R3-M3 | MINOR | pass iteration boundary (scan from a drained frontier) implied but unstated | one-sentence pin; fix-without-re-review |
| R3-M4 | MINOR | convergence prose quantifies over histories the mechanism doesn't cover (de-demanded writer × sync-scoped session value); unreachable in scripted cells | scope the claim; declare the sessions × shrink exclusion in the inductive bet; fix-without-re-review |
| R3-N1 | NOTE | at-least-once cost claim for writer row re-derivation verified honest (count-legal, suppression-safe, verdict-neutral) | record |
| R3-N2 | NOTE | deferral non-starvation / non-deadlock arguments verified; record the two-line argument in §3 | record |
| R3-N3 | NOTE | round-2 minors/notes all applied as dispositioned (sweep above) | record |
