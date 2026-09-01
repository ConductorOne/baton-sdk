# Round 2 (TARGETED) — premise-validated adoption (§4a) and its interlocks, GRAPH_MODEL_SPEC v2

Scope: the targeted round-2 spot review the round-1 disposition of F1
required. PRIMARY: §4a (premise-validated adoption — the marker as
memoization entry, adopt-on-equal-digest / re-derive-on-differing-
digest, `eAdopt` semantics, §4d fold transfer, row re-announcement),
answering v2's §11 round-2 charge questions 1–7 mechanically.
SECONDARY: composition of the F2 (P6-G + observation pass), F3 (§4d),
F6 (§4b matrix incl. poison exemption), and F7 (closure oracle +
`eAnnGenBump`) dispositions WITH §4a — not a re-review of those
dispositions as applied; and the acceptance walks: G1's three legs and
G2's four legs under v2 semantics. Anchors: `formal/MODEL_SPEC.md` v11
FROZEN + MS-CO-001 (§7 property pins incl. the round-5 F8 consult
qualification and round-7 F2/F3 counting/attestation pins; §9.6
V-ATOMIC / V-OVERLAY-UNIT), `formal/GLOSSARY.md`,
`formal/walker/CALIBRATION.md` decisions 19–24,
`formal/reviews/graph-spec-round1.md`. Round-1 dispositions v2 applied
as specified are NOT relitigated; only their composition with §4a is
in scope.

Method: mechanical walks of every §4a branch (adopt, re-derive, and
their interleavings) under 2 workers, ≤ 2 crashes, ≤ 3 attempts, ≤ 3
epochs, per the §8 budget; independent re-derivation of P1 (§4d
grounding), P2/P3′ qualification, P6-G/P6-E/P6-S, and the closure
oracle over every walked history; a durability sweep of the marker /
`eAdopt` / checkpoint rows of §5 against crash and announce-in-flight
placements (the G-RULE-1 window between a store commit and its
announce processing is load-bearing in several walks below); a
back-port thought experiment of §4d onto the v11 walker cells
(charge question 4); totality sweep of the G8a no-marker pin against
the §4b matrix and every marker state a walk can construct.

Verdict: **REJECT — 6 majors + 8 minors + 3 notes. R2-F2
(rows-only adoption strands dead session publishes) is
re-review-required: every candidate repair is new mechanism touching
the F17 exclusion boundary and the §7.5 tally. The other five majors
are fix-without-re-review, spot-checkable in the same targeted round
3 that R2-F2 forces.** The repair's core is sound in direction: G1's
three legs derive (leg (ii) with a mechanism-attribution correction),
G2's four axis legs derive (E+B conditionally on the R2-F1 pin), both
G2 kills flip against P6-G for the stated reasons, and the round-1
starvation hole (F7(ii)) is genuinely closed for completed children.
The rejection is for what the new mechanism composes into at its
seams: mid-attempt death with in-flight executions, session publishes
under adoption, the digest's lossy canonicalization of failed
revalidations, marker lifecycle on the re-derive-to-record path, and
the admitted-derivation set's behavior after purge/refusal.

## Majors

- **R2-F1 (MAJOR) — the fate of an in-flight execution whose
  generation dies MID-ATTEMPT is undeclared; under the
  run-to-completion reading a dead execution's late unit commit (its
  clear constituent included) wipes a live re-derivation's rows, and
  variant E seals stale content on a leg declared GREEN.**
  - Claim under attack: G2's E+B leg ("P6-G GREEN, P6-E green");
    §4a's adopt/re-derive dichotomy (implicitly assumes the dead
    generation's execution is gone); §3's retraction pin and the
    retraction queue (§7.5), whose enqueue/drain timing and effect on
    in-flight readers are nowhere stated.
  - Evidence, walked. §5 declares in-flight execution state volatile
    ONLY against crash ("a crash loses the execution"). Mid-attempt
    death — a retraction-forced bump (E+B) — has no such clause:
    G-RULE-3 gates dispatch of (n, g+1) on (n, g) being DEAD, and
    death is declared at the bump, which is scheduler bookkeeping.
    Nothing kills, cancels, or fences the dead execution's worker,
    and MStore has no death gate (§5 puts the generation table in
    MGraphScheduler; §3's op registration carries no liveness
    precondition). Budget-legal history, G2 E+B chassis, 2 workers:
    H's re-publish lands while reader G@g2 is in flight having read
    the prior value (the §3 keying pin itself contemplates exactly
    this reader: "re-runs that themselves read a stale value before
    the re-publish landed"). The retraction bumps G to g3; G@g3
    dispatches on the free worker, finds the marker, recomputes
    premises (reads the live value), re-derives, and commits its
    unit. THEN the dead G@g2 — never cancelled — commits ITS unit:
    an atomic {clear, rows, marker} whose clear removes g3's live
    rows and installs rows embedding the dead session value, with
    marker (g2, stale digest). No further re-publish occurs, so no
    further retraction fires; variant E's pre-seal condition is only
    that the retraction queue is empty (it is — g3 executed); E has
    no observation pass. Seal embeds a value differing from the
    key's final live derived value → P6-G RED on the leg declared
    GREEN. Under S the same schedule is caught (the late unit's rows
    carry the dead g2 stamp; the pre-seal pass forces a re-run) — the
    asymmetry is itself bake-off-relevant and currently invisible.
    Under the kill-on-death reading, none of this is reachable and
    the declared green derives. Two honest readings, one scripted
    cell verdict — the severity definition verbatim.
  - Why it matters: this is the inherited marker-race window the
    charge's question 1 named, relocated by the repair. Adoption
    removed the double-unit shape for same-generation races;
    the dead-vs-live race survives because §4a decides adopt vs
    re-derive per execution but never says the dead execution's
    store ops stop counting.
  - Disposition (fix-without-re-review, verified in the R2-F2 round
    3): pin ONE of — (a) QUIESCE-BEFORE-BUMP: a mid-attempt
    generation bump commits only after the dying execution's worker
    has quiesced (walker decision-16 precedent; scheduler rule, no
    tally change); or (b) a store-side DEATH FENCE: the bump commits
    a fence op and MStore refuses ops from fenced generations —
    which amends the §7.5 frozen tally (a new shared store rule) and
    incidentally closes R2-M2. Add the dead-in-flight interleave as
    a probe cell (expected unreachable-after-pin), and state the
    retraction queue's enqueue/drain semantics in §3.

- **R2-F2 (MAJOR, re-review-required) — adoption re-grounds ROWS
  ONLY; the dead generation's SESSION PUBLISHES stay stamped with
  the dead writer generation, so downstream readers can never clear
  the dead component: the S-variant pre-seal pass loses its progress
  guarantee (the "finite fixpoint" is budget exhaustion, not
  convergence) and seals dead stamps on an honest history, and
  P6-E's quantification alarms on the same honest history under E.**
  - Claim under attack: §4a ("Adoption re-announces ROWS ONLY, never
    session ops"); §3's seal sequence ("iterate to fixpoint; bounded
    by the §8 attempt budget times the node count, so the model's
    fixpoint is finite by construction"); G2's same-value control
    leg; G6a's "expected redo: both ≈ 1"; G6c's "both keep C with
    zero redo"; §11 question 5's premise that the bound is the
    question.
  - Evidence, walked (budget-legal: 1 crash, no mutation, the
    G-RULE-1 announce-in-flight checkpoint window). H's unit — H
    publishes d1 during its execution (the publish is its own store
    op, durable) — commits durably; the completion announce is in
    flight; a checkpoint commits capturing H PENDING (announce
    arrival order is a §2 choice point, so a scheduler step fits
    between commit and processing); crash loses the announce. G
    completed earlier, its rows embedding d1 with merged stamp
    {G: g_G, H: g1_H} under S. Resume: H pending → bump, g1_H dead.
    H@g2 finds its marker; no upstream mutation, H reads no
    sessions → digest EQUAL → ADOPT. Adoption re-grounds H's ROWS
    under g2 and re-announces them; d1 remains writer-stamped g1_H —
    a DEAD OUTPUT by the glossary's own definition, content-final
    but generation-dead, and nothing ever re-grounds it. Now walk
    the pre-seal pass (S legs): G's rows carry {H: g1_H} dead → the
    pass forces a re-run of the PRODUCING node, G. G's re-execution
    finds G's marker; its session re-read returns d1 with identity
    AND writer stamp unchanged (g1_H) → digest EQUAL → ADOPT — and
    `eAdopt` substitutes only G's OWN from/to generations in the
    row stamps; the H-component persists. The pass observes the same
    dead stamp, forces G again, G adopts again: NO PROGRESS PER
    ITERATION. Re-derivation does not help either — a re-derived G
    merges the re-read value's stamp, {H: g1_H}, back in. The
    iteration terminates only at the stated bound, and the sealed
    rows carry a dead generation → P6-S RED on an honest, no-mutant,
    no-mutation history. Under E+B the same placement alarms the
    conformance monitor directly: G is a "reader execution of a dead
    value" (d1 became dead at H's bump) and nothing re-runs it
    (retraction fires only on re-publish; adoption never
    re-publishes) → P6-E RED honest. P6-G stays green (content is
    final) — these are mechanism-conformance reds on honest legs,
    which §10.1's discipline cannot distinguish from real
    mutation-adequacy failures. Ripple: G6a's ≈ 1 and G6c's
    zero-redo expectations hold only in schedules where the
    checkpoint captured the writer's completion; the
    announce-in-flight placement makes them placement-dependent.
    Note also G-RULE-1 lists SESSION PUBLISHES as demand sources;
    adoption's rows-only re-announce therefore also fails to
    re-derive publish-derived demand — no scripted cell demands via
    a publish, so this facet is latent, but the asymmetry is real.
  - Why it matters: this is charge questions 2 and 5 jointly — a
    verdict input (the writer-generation liveness of embedded
    session values) that adoption changes the truth of but cannot
    repair, and a fixpoint claim that is false as a convergence
    claim exactly where adoption and sessions meet. It sits directly
    on the F17 exclusion boundary ("re-announces ROWS, never session
    ops" is §1's exclusion argument made load-bearing).
  - Disposition (RE-REVIEW-REQUIRED — every candidate is new
    mechanism): (a) publish-bearing units are INELIGIBLE for
    adoption (a writer that died re-derives always; its re-publish
    re-grounds the value and ordinary retraction/observation clears
    readers) — narrowest, keeps rows-only honest, costs adoption
    efficiency for writers only; (b) adoption extends to session
    publishes (writer-stamp rewrite + re-announce) — re-opens the
    F17 exclusion argument and G-RULE-1's demand vocabulary,
    heaviest; (c) a generation-alias table: `eAdopt` records
    from→to and every stamp/liveness read evaluates through the
    alias map — new durable state class, §7.5 tally change. Each
    changes §3/§4a/§7 and the G2/G6 expectations; pick, pin,
    re-derive the affected cells, and bring it back with the R2-F1
    pin for a targeted round 3.

- **R2-F3 (MAJOR) — the premise digest canonicalizes a failed
  revalidation as a bare outcome, so adopt fires across a genuine
  upstream change (fail-vs-e2 ≡ fail-vs-e3); ADOPT is a new verdict
  class the inherited P2/P3′ qualification never classifies, two
  honest readings diverge on the constructed history, and §4a/§7's
  "the adopting re-consult is strictly fresher" claim is false in
  the FAIL shape.**
  - Claim under attack: §4a's digest definition ("the consult result
    (previous-artifact entry + revalidation outcome)") and its
    walker-coherence paragraph ("the graph's re-consult is strictly
    fresher... strengthens, never weakens"); §7 P2 ("a
    marker-adopted scope's consult is the ADOPTING re-consult, this
    attempt — strictly fresher than the walker's suppressed case").
  - Evidence, walked (budget-legal: 1 crash, epochs e1→e2→e3, one
    scope, no sessions). Attempt 1: (S1, g1) consults — entry V1,
    revalidation vs e2 FAILS → CHANGED-WITH-DIFF; the overlay unit
    commits {copy base e1, overlay e1→e2, marker(g1, D), publish V2}
    with D = H(entry V1, outcome FAIL). Checkpoint predates
    completion (announce window); crash; upstream mutates e2→e3
    (between attempts — in scope per §1). Resume: bump g2; marker
    found; recompute: entry V1 unchanged, revalidation vs e3 FAILS →
    D′ = H(V1, FAIL) = D → ADOPT rows(e2) under upstream e3. The
    premise the verdict actually depended on — WHICH upstream state
    the diff fetched — changed; the digest cannot see it because a
    failed revalidation yields no canonical token for the state it
    failed against, and the diff content is fetched inside the
    round, after the verdict. Now the readings: the adopting
    re-consult performed a revalidation (FAIL) but NO fetch — under
    MODEL_SPEC §7's round-5 F8 pin it is neither a validation match,
    nor a fresh fetch, nor a changed-with-diff-with-fetch. Reading A
    (adopt re-consult does not qualify): P2 green via attempt 1's
    qualifying diff (any-attempt wording), P3′'s "last
    consulted-against-upstream verdict" is attempt 1's at e2 →
    expects rows(e2) → green. Reading B (the spec's own §7 sentence
    makes the adopting re-consult THE consult): its epoch is e3 →
    P3′ expects rows(e3) against sealed rows(e2) → RED. The sealed
    content itself is smear-legal either way (coherent rows(e2)@V2,
    staleness ≤ 1, self-healing next sync) — but the verdict
    diverges by reading, and the "strictly fresher" justification is
    false: in the FAIL shape the adopting re-consult qualifies
    nothing, and freshness rests on the DEAD generation's original
    fetch, exactly the walker's suppressed case, not stronger. Note
    the walker never calibrated this shape: §7's P1 boundary note
    confines every walker config to one interruption + one mutation;
    suppress-after-diff-unit under a SECOND mutation is outside the
    6-overlay envelope — the adaptation extended the hand-off
    without noticing.
  - Why it matters: charge question 2's named target ("does any
    input to a verdict escape it"). The escape is real, constructed,
    and budget-legal; no scripted cell reaches it (G1's original
    verdict is a MATCH), so it would be hardcoded away silently by
    whatever the encoding does.
  - Disposition (fix-without-re-review): pin adopt eligibility to
    MATCH-outcome premises — a re-consult whose revalidation FAILS
    re-derives regardless of the stored digest (cheap: FAIL means
    upstream moved; the re-derivation fetches the current state and
    "strictly fresher" becomes true), OR keep FAIL-adoption and pin
    ADOPT's P2/P3′ classification explicitly (attempt-anchored
    qualification; correct the §4a/§7 freshness prose). Register
    ADOPT as a verdict class in the announce vocabulary either way,
    and add the e1→e2→e3 FAIL-adopt history as a probe cell.

- **R2-F4 (MAJOR) — marker lifecycle under supersession is not
  total: the re-derive branch's `eMarkerPut` overwrite exists only
  for unit-verdict re-derivations, a record-round re-derivation
  (fetch-fresh) leaves the stale marker in place, and a later
  premise flap-back ADOPTS content the marker no longer describes —
  the exact hazard §11 question 6 hypothesized, constructed here
  within budget.**
  - Claim under attack: §4a's re-derive branch ("its new unit's
    clear constituent supersedes the dead rows (§4b), and its
    `eMarkerPut` overwrites the marker") — false for fetch-fresh
    re-derivations, which are record rounds and commit NO marker
    (the G8a pin); §4b's matrix (no marker column: no row says what
    any incoming round does to an EXISTING marker); G1 leg (ii)'s
    cell text (same misattribution: a failed revalidation yields
    fetch-fresh per leg (i), so leg (ii)'s superseding round is a
    RECORD round, not "the new unit").
  - Evidence, walked (budget-legal: 2 crashes, 3 attempts, epochs
    e1→e2→e3 with content(e3) = content(e1) — value flap-back is in
    scope per the walker's d1→d2→d1 precedent and truthful
    validators). Attempt 1: (S1, g1) consults (V1, MATCH) → replay
    unit + marker(g1, D = H(V1, MATCH)). Checkpoint in the announce
    window (S1 pending); crash 1; mutate e1→e2. Attempt 2: bump g2;
    marker found; recompute (V1 vs e2: FAIL) → differs → RE-DERIVE →
    fetch-fresh → record round: `eClearScope` (removes g1's rows AND
    fold contribution, §4b) + pages rows(e2) + publish V2 — and NO
    `eMarkerPut` (G8a pin); nothing removes marker(g1, D), which now
    describes a unit whose rows are gone. Checkpoint again in the
    final-page announce window (S1 pending); crash 2; mutate e2→e3,
    content reverting to e1's. Attempt 3: bump g3; marker check
    finds marker(g1, D); recompute: entry V1, revalidation vs e3
    MATCHES (truthful validator: content unchanged vs V1's) → digest
    EQUAL → ADOPT. Every constituent of `eAdopt` now operates on the
    wrong object: the stamp rewrite substitutes g1→g3 and touches
    nothing (the partition holds g2's rows, no g1 components — under
    S they remain DEAD-stamped g2); the §4d fold transfer transfers
    a contribution that was REMOVED at the §4b clear (incoherent
    bookkeeping — meanwhile g2's dead complete round persists in the
    fold per §4d); the re-announce announces rows(e2) as the adopted
    content of a marker attesting replay-of-V1 = rows(e1). Seal, E
    leg: partition rows(e2); the adopting re-consult was a MATCH —
    which QUALIFIES under the F8 pin — so P3′'s last
    consulted-against-upstream epoch is e3 and expects rows(e3) =
    rows(e1) → P3′ RED on an honest history. S leg: the pass sees
    the dead g2 stamps, forces re-runs that re-ADOPT through the
    same stale marker without ever rewriting g2's stamps → the
    R2-F2 no-progress loop shape again, from a different cause →
    P6-S RED honest. The marker described content the store no
    longer held, and adoption believed it.
  - Why it matters: totality (charge question 6). The G8a pin is
    correct in isolation; its ripple — who clears a marker a record
    round strands — was never walked, and §12's own parenthetical
    shows the pin was surfaced mid-draft. Adoption's soundness
    silently assumes the invariant "marker present ⟹ the key's
    partition IS the marked unit's outputs," and no rule maintains
    it.
  - Disposition (fix-without-re-review): add a MARKER column to
    §4b's matrix making marker lifecycle total: any superseding
    commit for a key removes-or-overwrites its marker (unit rounds
    via their `eMarkerPut` constituent — already true; record
    REPLACES rounds: `eClearScope` also deletes the marker, pinned
    in §3's clear-placement pin; OVERLAY-intent records: no dead
    base is legal, so no marker case arises — state it). Pin the
    invariant explicitly and monitor it (marker ⟹ partition equals
    the marked unit's outputs; announce-evidenced). Correct §4a's
    re-derive branch ("its new ROUND supersedes; a unit round's
    `eMarkerPut` overwrites the marker, a record round's clear
    removes it") and G1 leg (ii)'s text. Add the flap-back history
    as a probe cell (expected unreachable-after-pin).

- **R2-F5 (MAJOR) — the admitted-derivation set has no death
  semantics: a purged (E) or refusal-dropped (S) child's derivation
  hash stays in the set, so the live generation's re-announce — by
  adoption OR by at-least-once record redo — is suppressed and the
  child starves on honest legs; jointly, E's purge predicate is
  ∃/∀-ambiguous over fan-in edges and the two readings diverge on
  G6c's declared E-leg verdict.**
  - Claim under attack: G-RULE-2 ("the scheduler MAY suppress a node
    admission iff the same derivation hash is already in the
    ADMITTED-DERIVATION SET... this sync" — no removal rule
    anywhere, and "may" leaves the load-bearing branch to the
    encoding); §3's purge ("purges from the frontier every PENDING
    node whose admitted-by edge names a dead generation" — ∃ or ∀
    over a fan-in node's edges?); G6c's E-leg ("E must exercise
    refcounted support (C survives on S2's live support — no
    retraction)... both keep C with zero redo"); §4a's starvation-
    closure claim ("a lost child admission is re-derived from the
    adopted rows' content").
  - Evidence, walked. (i) G6c, E leg: C is pending, demanded by S1
    AND S2 (two generation-qualified admitted-by edges); the crash
    kills S1 mid-round; resume bumps S1 → C now has one DEAD edge
    and one LIVE edge. Under the ∃-reading of §3's purge predicate C
    is purged — contradicting the refcounted-support story the cell
    asserts; under the ∀-reading C survives. Two honest readings,
    one declared cell verdict. The S-side got the equivalent pin
    ("S must not false-refuse (C's admission stamp contains a live
    parent)"); E's purge predicate did not — asymmetric drafting.
    (ii) The starvation composition, either variant: C's hash
    entered the admitted-derivation set at ADMISSION (F13 pin) and
    the set is checkpoint-durable. C is purged (E) or comes up for
    dispatch before any live re-derivation and is refusal-dropped
    (S: "otherwise it is dropped from the frontier without
    executing"). The live parent's re-execution then re-derives the
    SAME hash — S1@g2's at-least-once page-1 re-announce in a
    paginated no-shrink config, or an adoption's row re-announce.
    Demand derivation consults the set: the hash is present →
    suppressed → C is in the frontier of no one, completed by no
    one, and never re-admitted. Post-purge, the F13 completion rule
    read against LIVE state (admitted ∧ ¬pending) even classifies C
    as COMPLETED. The final closure lacks C; the env-side
    counterfactual closure oracle fires RED on an honest,
    mutant-free run — the oracle works exactly as F7's repair
    intended, which is how we know the run is broken, not the
    oracle. Adoption's F7(ii) closure claim survives only for
    children whose admission was NEVER checkpointed (hash absent
    after restore → re-admitted fresh) and for COMPLETED children
    (suppression is then correct — G6a's ≈ 1 redo depends on it and
    still derives); the purged/refused middle case starves.
  - Why it matters: this is the composition of three round-1
    dispositions v2 applied individually and correctly (F5's
    pending-only purge, F13's admission-keyed set, F1's re-announce)
    — none wrong alone, jointly unsound. It is the graph's
    single most load-bearing new mechanism (demand derivation)
    failing silent-with-oracle on honest runs.
  - Disposition (fix-without-re-review): pin (a) the purge predicate
    as ∀ (purge only when EVERY admitted-by edge names a dead
    generation — the refcount-consistent reading G6c's text already
    assumes); (b) purge and refusal-drop REMOVE the node's
    derivation hash from the admitted-derivation set (making
    re-derivation re-admissible; G5e's count oracle is unaffected —
    its epoch-2 shrink never re-derives the hash); (c) G-RULE-2's
    "may" tightened to MUST-suppress iff the hash's node is pending
    or completed, so the checker cannot legally choose starvation.
    Re-derive G5f's honest baseline and G6c's E leg under the pins.

- **R2-F6 (MAJOR) — G2's same-value control leg declares "P6-S red
  ... recorded as redo," but P6-S's written form is a SEAL check and
  the honest observation pass clears the evidence before seal: under
  the at-seal reading the declared red is underivable (the leg walks
  green), and under the at-observation reading P6-S is not the
  property §7 registered — the F2(iii) value-blindness pin as
  applied is incoherent with the pass it rides on.**
  - Claim under attack: G2's control leg ("P6-S red on the S legs by
    the value-blindness pin — recorded as redo"); §7 P6-S ("no
    SEALED output carries a stamp containing a dead generation")
    plus its value-blindness pin ("P6-S alarming on same-value
    re-derivation is INTENDED — it checks the mechanism's
    forced-redo promise").
  - Evidence, walked (scripted control premise: checkpoint captures
    G completed ∧ H pending; crash; H@g2 re-derives d1 EXACTLY).
    H@g2 re-publishes d1 under writer generation g2 (G-RULE-3 stamps
    at emission; the value is identical, the writer stamp is not).
    Pre-seal pass: G's rows carry {H: g1} dead → forces G's re-run.
    G's re-execution finds its marker and recomputes premises: the
    session re-read returns d1 with WRITER STAMP g2 ≠ g1 → digest
    DIFFERS → RE-DERIVE → new rows carry {G: live, H: g2}, all live.
    Seal: no sealed output carries a dead generation → P6-S GREEN as
    written. The forced redo happened and is real — but it is a
    RECORDED METRIC, not a property violation; if the pass works,
    P6-S at seal cannot fire, and if P6-S fires at seal the pass
    failed, which is a conformance failure, not "overhead." The two
    readings (at-seal vs at-observation) flip a scripted leg's
    declared verdict, and §10.1's adequacy discipline cannot tell an
    intended red that never derives from a broken kill.
  - Why it matters: the control leg is the declared exhibit of the
    mechanism-vs-oracle distinction the F2 repair introduced; as
    written it exhibits the confusion instead.
  - Disposition (fix-without-re-review): recast the same-value
    mechanism-conformance signal as what it is — a forced-redo COUNT
    (observation-pass re-run events, announce-evidenced), recorded
    in the redo column — and reserve P6-S red for sealed dead stamps
    (mechanism failure). Correct the control leg's declared verdict
    to: P6-G green, P6-S green, forced-redo count ≥ 1 (the
    value-blindness pin's content, honestly stated). The
    value-blindness PIN itself survives — dead-stamp-forced redo on
    value-identical re-derivation remains intended behavior; only
    its verdict vocabulary was wrong.

## Minors

- **R2-M1 (MINOR)** — the SESSION-READ observation point (variant S)
  is named in §7.5's tally and the glossary but has no declared
  transition in §3: what an execution does when a session read
  returns a dead-stamped value (refuse? proceed and let the pre-seal
  pass catch it? force the writer?) is unstated. No scripted leg
  turns on it (reads are live at read time in every walked history),
  but the tally counts it as S mechanism, so it must be registered
  or struck. Fix-without-re-review.
- **R2-M2 (MINOR)** — `eAnnGenBump` at OBSERVATION/RETRACTION-forced
  re-admissions has no commit carrier: those re-admissions are
  scheduler steps, not store ops, while §2's monitor discipline is
  announce-AT-COMMIT. The resume-time bump rides the forced resume
  checkpoint's commit (clean); the mid-attempt bump's announce is
  evidentially ungrounded. R2-F1's fence option (b) closes this for
  free; otherwise pin the carrier. Fix-without-re-review.
- **R2-M3 (MINOR)** — marker store-scoping across syncs is
  unregistered: §5 makes the marker durable in the artifact, the
  budget runs 2-sync cells, and nothing says the §4a marker check
  reads the CURRENT sync's store only (the walker's 3-atomic
  precedent implies it; the graph spec never says it). A sealed
  artifact also retains stale markers (R2-F4's history seals one
  even after the fix, on the no-flap path) — pin that seal or sweep
  drops marker rows, or that they are per-sync namespaced; note
  §4c's "adopted-across-sync rows" wording invites the wrong
  reading. Fix-without-re-review.
- **R2-M4 (MINOR)** — §4b's dead-base OVERLAY row explains detection
  "via the marker's dead generation + no adoption," but record-path
  keys HAVE no markers (G8a pin): for record-over-record the stated
  detection channel does not exist and the precondition rests
  entirely on scripted connector policy + the trust model. One
  defensible intended meaning (the mutant, not the mechanism, is the
  load-bearing part); the wording overclaims. Fix-without-re-review.
- **R2-M5 (MINOR)** — whether the ADOPTING execution's premise
  re-reads register as tracked read edges (E+B) / merge into stamps
  (S) is undeclared. The walked histories self-heal (the dead
  generation's original read still grounds node-level retraction,
  and digest equality pins the re-read stamp to the original), but
  P6-E's quantification ("every reader execution of a dead value")
  silently includes adopting re-readers only under one reading.
  State it. Fix-without-re-review.
- **R2-M6 (MINOR)** — coverage gaps in the retraction machinery's
  distinctive clauses: (i) the §3 keying pin's re-retraction clause
  ("re-runs that themselves read a stale value... retracted again")
  has no witness — G2's scripted checkpoint placement (G completed)
  never produces a pre-re-publish re-run; add the G-pending
  placement as a leg. (ii) Session-TRANSITIVE chains (a
  reader-writer: retracted reader re-publishes, retracting
  second-order readers) exist in no envelope config — the widened
  envelope's readers do not write. Either script one reader-writer
  leg or restate the inductive bet to exclude the shape explicitly.
  (iii) No cell derives demand from a SESSION PUBLISH though
  G-RULE-1 names publishes as demand sources (interacts with
  R2-F2's rows-only re-announce). Fix-without-re-review.
- **R2-M7 (MINOR)** — the pre-seal pass's stated bound ("the §8
  attempt budget times the node count") is a category error: the
  attempt budget bounds crash/resume attempts, and nothing in §8
  bounds INTRA-attempt observation-forced bumps per node (each bump
  is a fresh generation; the state space is unbounded exactly where
  R2-F2's loop lives). Add an explicit pass-iteration budget row to
  §8 and re-word "finite by construction" to name the cutoff.
  Fix-without-re-review.
- **R2-M8 (MINOR)** — poison × marker is unclassified: G8c's second
  derivation poisons a scope whose first unit committed a marker;
  a later forced re-run of the first node would find the marker,
  recompute equal premises, and ADOPT a poisoned scope's rows —
  re-announcing them into demand derivation. The seal excludes the
  scope (SealExpect), so the direct content hazard is bounded, but
  the matrix should say poison VOIDS the key's marker (no adoption
  of poisoned content) and classify post-poison rounds for the key.
  Fix-without-re-review.

## Notes

- **R2-N1 (NOTE)** — `eAdopt`'s precondition (fromGen is DEAD) is
  unstated. A live-fromGen adoption is reachable only under
  `suppressionOff` (G1's mutant leg: the sequential schedule's
  second execution adopts a LIVE generation's unit), where the
  declared legality alarm still derives; record the precondition so
  the mutant's adopt path is a declared deviation, not an accident.
- **R2-N2 (NOTE)** — digest completeness is relative to the modeled
  input set: config/compat drift and warm-install state are
  unmodeled in every G cell (no drift scenario), so the digest omits
  them VACUOUSLY; a cold re-consult would miss and force re-derive
  (safe direction) if cold attempts were ever modeled. Add one
  closure sentence to §4a ("the digest is total over the model's
  verdict inputs: consult result and session reads; config/compat
  are constants in every cell") so the vacuity is declared, per the
  F15/F21 registration lesson.
- **R2-N3 (NOTE)** — P-GEN's monitor rule is implicit but derivable:
  the checkable form is "no two ATTEMPTS contain store-commit
  announces attributed to the same (n, g)" (attempt boundaries are
  announce-visible; adoption re-announces attribute to the ADOPTING
  execution, so they do not collide). Record the rule so the G1b
  probe's evidence base is pinned — this is the affirmative answer
  to charge question 7's F4 item.

## Mechanical walks (the acceptance test for the repair)

- **G1 leg (i)** (crash before unit commit): nothing durable; resume
  bumps; re-consult V1 vs e2 FAILS → fetch-fresh → record round
  (REPLACES: clear over empty, rows(e2), publish V2, no marker —
  G8a-consistent). Fold = fresh(e2); P1/P2/P6-S green both variants.
  **DERIVES as declared.**
- **G1 leg (ii)** (crash after unit commit): marker(g1, (V1, MATCH))
  found; recompute → FAIL vs e2 → digest differs → RE-DERIVE →
  fetch-fresh → RECORD round; §4b's dead-rows/record/REPLACES row
  removes g1's rows and fold contribution; seal rows(e2)@V2; fold =
  fresh(e2); P6-S live. **Verdict GREEN DERIVES on both variants —
  but via §4b's record row, NOT "the new unit's clear" (the cell
  text and §4a's re-derive branch misattribute, R2-F4), and the
  stale marker(g1) survives to seal (latent per R2-F4/R2-M3;
  harmless in this cell's remaining history).**
- **G1 leg (iii)** (no mutation): marker found; recompute (V1,
  MATCH) = D → ADOPT; `eAdopt` rewrites stamps g1→g2 (S: all live),
  §4d transfers replacement(e1) — fold equals content; entry V1
  attests e1; P2's qualifying consult is the adopting re-consult
  ITSELF (MATCH qualifies under the F8 pin — the "strictly fresher"
  claim is TRUE in the match shape); rows re-announced. **DERIVES
  as declared, both variants. This is the repair working.**
- **G1 mutant leg** (`suppressionOff`): racing schedule — both
  consults pass the absent-marker check, two units commit, two
  complete rounds with two copies → P1-LEGALITY RED first-find;
  sequential schedule — second execution finds the first's marker,
  premises equal, ADOPTS, seals coherent → green. **Flip confirmed
  for the stated reason; first-find expectation correctly stated
  in-cell. (Live-fromGen adoption in the sequential schedule:
  R2-N1.)**
- **G2 E+A**: nothing forces G's re-run; seal embeds d1 ≠ final live
  d2 → P6-G RED. **DERIVES as declared** (the F2 repair works: the
  red now has a property). Marker machinery never engages (G never
  re-runs) — no §4a interaction.
- **G2 E+B**: re-publish → retraction → G re-runs → marker found →
  recompute: session value identity+writer-stamp differ → RE-DERIVE
  → unit clears, embeds d2, `eMarkerPut` overwrites → P6-G GREEN.
  **DERIVES under the scripted checkpoint placement AND the
  kill-on-death reading of mid-attempt bumps; under the
  run-to-completion reading the dead-in-flight schedule seals d1 →
  R2-F1. Conditionally derivable.** `retractionOff` kill: no
  re-run, sealed d1 → P6-G RED — **flips, mechanism-independent
  oracle confirmed.**
- **G2 S+A and S+B**: resume bumps H (pending); H re-derives d2,
  re-publishes; pre-seal pass sees {H: g1} dead in G's rows → forces
  G → recompute differs (identity and writer stamp) → re-derive →
  seal embeds d2, all stamps live → P6-G GREEN, P6-S green; the
  pass CONVERGES here because the writer re-derived live (contrast
  R2-F2's adopt placement). **DERIVE as declared.** `stampMergeOff`
  kill: G's rows carry no H component; the pass is blind; sealed d1
  → P6-G RED — **flips against the oracle, not against itself —
  the F2 repair's exact design goal, confirmed.**
- **G2 same-value control leg**: walks P6-G green / P6-S GREEN at
  seal with forced-redo ≥ 1 — the declared "P6-S red" does not
  derive → **R2-F6.** In the announce-window placement (H's unit
  committed, H pending) the leg instead walks into R2-F2's adopt
  loop → honest P6-S/P6-E reds. **The control leg is the repair's
  soft spot, both directions.**

## Answers to the §11 round-2 charge

1. **Adoption/re-derivation interleaving under 2 workers**: row-level
   MIXED content cannot seal — units and `eAdopt` are single atomic
   store ops, and every §4b/§4a path is clear-based, so the store
   holds one round's rows at any commit boundary. The race that
   survives is WORSE than mixing: under the run-to-completion
   reading of mid-attempt death, a dead execution's late atomic unit
   (clear included) replaces a live re-derivation wholesale and E
   seals it — FINDING R2-F1. Adopt-vs-adopt and adopt-vs-re-derive
   orderings are otherwise sound: eAdopt-then-clear ends at the live
   unit; clear-then-eAdopt makes the substitution a no-op with stale
   marker metadata (subsumed by R2-F4's lifecycle rule).
2. **Premise-digest completeness**: NO for two inputs. (i) The
   upstream state behind a FAILED revalidation escapes (outcome-bit
   canonicalization) — adopt fires across e2→e3; concrete history
   constructed in R2-F3. (ii) The writer-generation liveness of
   embedded session values is changed by adoption itself and is
   unrepairable by the digest (R2-F2). The previous artifact's
   identity is adequately covered by entry + truthful validators;
   warm/cold and config/compat are vacuously absent (R2-N2).
3. **eAdopt stamp-rewrite vs eAnnGenBump ordering**: sound at resume
   — the forced resume checkpoint's commit carries the bump announce
   and precedes any dispatch, so a monitor processes death before
   any adoption's re-announce; commit-order delivery
   (announce-at-commit) makes the dead-set current when rewritten
   stamps arrive. Mid-attempt bumps lack a commit carrier (R2-M2).
   A monitor CAN see dead-stamped row announces after a death
   announce (the R2-F1 late commit); P6-S being a seal check means
   the window itself doesn't false-alarm — the defect is R2-F1's,
   not an ordering unsoundness.
4. **§4d back-port coherence**: COHERENT — verified by re-deriving
   the v11 cells under §4d. The removal clause (supersession removes
   a complete round's fold/count contribution) is DEATH-GATED and
   therefore vacuous in the walker (no death): cell 4's locks-off
   two-copy legality red survives (both rounds live, nothing
   removed); 6-overlay-last w1's ONE-count survives (the incomplete
   round was never counted — completion-counting, decision 19,
   untouched); 6-naive's content alarm survives (debris is
   incomplete either way); empty-fold attestation (decision 20)
   fires exactly as pinned — §4d's marker-adopted seal folds the
   transferred round, so the fold is non-empty precisely where the
   walker's suppressed seal was green. One caveat: keep the removal
   clause death-gated verbatim; a live-rows removal reading would
   dissolve cell 4's alarm. The transfer-of-a-removed-contribution
   incoherence in R2-F4's history is a §4a-composition defect, not
   a back-port defect.
5. **Pre-seal fixpoint finiteness**: the model TERMINATES only by
   cutoff, and the stated bound is not even the right budget
   (R2-M7). Convergence holds when every dead stamp component
   belongs to a node that re-derives live (all scripted main legs);
   it FAILS on honest histories where the dead component is a
   session writer's and the writer ADOPTED (rows-only re-grounding —
   R2-F2) or where re-adoption cycles through a stale marker
   (R2-F4): the pass forces the rows' producer, which cannot clear
   an embedded dead component, and iteration makes no progress.
   With `stampMergeOff` the pass is trivially convergent (blind);
   `retractionOff` is E-only and does not interact. The claim as
   written conceals the non-converging class.
6. **G8a pin + §4b joint totality**: NOT total. Round shapes are
   covered (every verdict class maps to unit or record; ADOPT is
   pinned as a transfer, not a round; poison-blocked rounds need a
   classification sentence — R2-M8). Marker STATES are not: the
   matrix has no marker column, the stale-marker-over-record-rows
   state is constructible within budget, and the charge's
   hypothesized walk — fetch-fresh leaves the stale marker; a later
   consult adopts content the marker no longer describes — is
   confirmed REACHABLE and misbehaves in both variants (R2-F4).
   The pin's same-sync consequences also misfire in §4a's own
   re-derive text (unit-only overwrite). Cross-sync, the marker's
   store-scoping is unregistered (R2-M3) but the intended walker
   reading (current-sync store only) makes the pin total there.
7. **Round-1 leftovers**: F4 — APPLIED CORRECTLY; ordering pin
   explicit in §5, G1b probe + `resumeCkptOff` kill well-formed;
   P-GEN is checkable via attempt-scoped announce attribution
   (rule recorded as R2-N3). F5 — APPLIED CORRECTLY; the
   checkpoint-consistent rebuild target is well-defined in G5e's
   mid-round premise (a deterministic function of restored frontier
   + generation-qualified admitted-by edges + durable store, all
   announce-reconstructible; the dead-edge child purges cleanly
   after the bump). The COMPOSITION defect found nearby (purge ×
   admitted-derivation set, purge predicate under fan-in) is new —
   R2-F5, not a mis-application of F5. F9 — MOSTLY SUFFICIENT:
   depth 2 exercises E's support-transitive retraction (G6b: S1's
   changed re-derivation drops C's support, C's death drops GC's —
   the machinery executes), fan-in exercises the refcount (G6c,
   modulo R2-F5's purge-predicate pin). Still unexercised:
   session-transitive (reader-writer) chains and the re-retraction
   clause — R2-M6; declare or script.

## Interlock spot-checks (F2/F3/F6/F7 dispositions × §4a)

- **F2 × §4a**: the laundering oracle is genuinely
  mechanism-independent — both G2 kills flip against P6-G with
  adoption active (walked above); adoption cannot defeat a
  retraction/observation-forced re-run whose premises changed (the
  digest sees the value change) — the round-1 F1 defeat is closed.
  Defects at the seam: the observation pass × adoption progress
  failure (R2-F2), the value-blindness pin's verdict vocabulary
  (R2-F6), the session-read observation point's missing transition
  (R2-M1).
- **F3 × §4a**: §4d composes with adopt (transfer) and re-derive
  (removal) correctly in every scripted cell; back-port coherent
  (charge answer 4). The one incoherence — transferring a removed
  contribution — arises only through R2-F4's stale marker.
- **F6 × §4a**: the matrix's four row-classes compose with
  adoption's clear-based branches; the poison exemption is coherent
  (G8c's alarm is the poison; counting exempted, legality preserved
  elsewhere). Gaps: the missing marker column (R2-F4), poison ×
  marker (R2-M8), the dead-base detection wording (R2-M4).
- **F7 × §4a**: adoption's row re-announce makes the ghost closure
  well-defined across deaths as claimed, and the env-side
  counterfactual closure is genuinely independent (it is what
  catches R2-F5's starvation). The starvation hole is closed for
  never-checkpointed and completed children; the purged/refused
  middle case re-opens it via the admitted-set composition (R2-F5).
  `eAnnGenBump` evidence base: sound at resume, carrier gap
  mid-attempt (R2-M2).

## Verified clean (for the revision record)

- The adopt-on-equal MATCH path is a faithful, strictly-stronger
  adaptation of the walker's clause (iii): same-sync consult
  provenance becomes this-attempt re-consult provenance, P2's
  qualification holds via the adopting MATCH itself, and the
  6-atomic hand-off content seals identically (G1 leg (iii)). The
  walker-coherence paragraph's degeneration argument is correct FOR
  MATCH premises; only the FAIL shape overclaims (R2-F3).
- Fold/count transfer and removal (§4d) re-derive P1 correctly in
  every scripted G1/G2/G5/G8 history walked; replacement-count
  legality never double-counts an adopted copy (adoption commits no
  copy) and never loses the mutant legs' alarms.
- `eAdopt` atomicity: single store op, announce-at-commit, rows
  re-announced under the adopting execution's attribution — P-GEN,
  P5's ghost closure, and G-RULE-1's per-announce timing all remain
  well-grounded; no torn adoption state is constructible.
- The no-relitigation boundary is respected in both directions: v2
  did not re-open the walker verdicts, and this review's findings
  are all about §4a's own seams or v2-new compositions, not about
  round-1 dispositions as applied.
- Public-repo hygiene: clean. No customer names, tenant
  identifiers, or internal infrastructure in v2 or in this review.

## Summary

| finding | severity | one line | disposition |
|---|---|---|---|
| R2-F1 | MAJOR | mid-attempt death of an in-flight execution unpinned; dead unit's late clear+commit wipes a live re-derivation; G2 E+B red under one honest reading | pin quiesce-before-bump (or store death fence, tally-amended) + retraction-queue semantics + probe cell; fix-without-re-review |
| R2-F2 | MAJOR | rows-only adoption strands dead session publishes; observation pass loses progress (budget-exhaustion seals dead stamps, honest P6-S/P6-E reds); G6 redo expectations placement-dependent | writer-adoption ineligibility vs publish re-grounding vs generation aliasing — new mechanism either way; **re-review-required** |
| R2-F3 | MAJOR | digest's FAIL-outcome canonicalization adopts across a real upstream change; ADOPT unclassified in P2/P3′ qualification (readings diverge); "strictly fresher" false in the FAIL shape | pin adopt-eligibility to MATCH premises (or classify ADOPT explicitly), register the verdict class, probe cell; fix-without-re-review |
| R2-F4 | MAJOR | marker lifecycle not total: record-round re-derive strands the stale marker; flap-back premise ADOPTS content the marker no longer describes (P3′/P6-S reds on honest history); §4a re-derive text false for fetch-fresh | marker column in §4b (supersession removes/overwrites), marker⟺content invariant + monitor, text corrections, probe cell; fix-without-re-review |
| R2-F5 | MAJOR | admitted-derivation set has no death semantics (purged/refused child's hash suppresses its own re-admission → honest starvation); E purge predicate ∃/∀-ambiguous over fan-in, flipping G6c's E leg | ∀-purge pin; purge/refusal remove the hash; suppression scoped MUST over pending∨completed; re-derive G5f/G6c; fix-without-re-review |
| R2-F6 | MAJOR | G2 control leg's declared "P6-S red" underivable at seal (the pass clears the evidence); value-blindness pin's verdict vocabulary conflates property with forced-redo metric | recast as forced-redo count; P6-S red reserved for sealed dead stamps; correct the leg's declaration; fix-without-re-review |
| R2-M1 | MINOR | session-read observation point in tally/glossary but no §3 transition | register or strike; fix-without-re-review |
| R2-M2 | MINOR | mid-attempt eAnnGenBump has no commit carrier (announce-at-commit discipline) | pin the carrier (free under R2-F1's fence option); fix-without-re-review |
| R2-M3 | MINOR | marker store-scoping across syncs unregistered; sealed stale markers as artifact debris; §4c wording | per-sync scoping pin + seal/sweep drops markers; fix-without-re-review |
| R2-M4 | MINOR | §4b dead-base detection "via the marker" overclaims for markerless record keys | wording (scripted policy + trust model); fix-without-re-review |
| R2-M5 | MINOR | adopting execution's premise re-reads: tracked-edge/stamp-merge status undeclared | one-sentence pin; fix-without-re-review |
| R2-M6 | MINOR | re-retraction clause, reader-writer session chains, and publish-derived demand unexercised by any leg | add legs or restate the inductive bet; fix-without-re-review |
| R2-M7 | MINOR | pre-seal pass bound is a category error (attempt budget ≠ intra-attempt bump budget) | §8 iteration-budget row + re-word "finite by construction"; fix-without-re-review |
| R2-M8 | MINOR | poison × marker unclassified (adoption of poisoned rows constructible) | poison voids the marker; classify post-poison rounds; fix-without-re-review |
| R2-N1 | NOTE | eAdopt's fromGen-dead precondition unstated (mutant-only reachable) | record it |
| R2-N2 | NOTE | digest completeness vacuous over unmodeled config/compat inputs | closure sentence in §4a |
| R2-N3 | NOTE | P-GEN's per-(n,g)-per-attempt monitor rule implicit | record it (charge Q7/F4 answer) |
