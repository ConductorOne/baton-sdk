# Round 7 — targeted spot review of the v10 addendum (V-OVERLAY-UNIT)

Scope: §9 scenario 6 V-OVERLAY-UNIT declaration (pins o-i..o-iv),
cells 6-overlay and 6-overlay-naive (including the "third placement"
dismissal), the §5 overlay-collect-buffer durability row, the 1d
cross-reference, and the §10.5 obligation extensions — per the §11 v10
change-order entry. Not a full round; v1–v9 material out of scope
except where the addendum destabilizes or newly depends on it.

Method: mechanical reachability walk of 6-overlay sub-cases (a)–(d)
and 6-overlay-naive's crash window under §3/§4/§5 (prefix rule,
stop/crash checkpoint contents, batch semantics); independent
re-derivation of every P1/P2/P3′ verdict from the §7 fold as written
(round-5 F1 pin applied by hand), including the dismissed third
placement, walked window by window; coherence sweep against the
round-5 V-ATOMIC pins (clause (i)–(iii), F1, F6, N4) and the round-6
process conventions; shipped-system claims verified against
`docs/verification/sync-replay-6b/plan.md` (B3/B5),
`pkg/sync/source_cache_orchestration.go` (lookup surface 163–215,
`beforeUpserts` 563+, `afterUpserts` publish 718–771), and
`pkg/synccompactor/pebble/overlay.go` (atomic range-del + rows batch
precedent, 353–385).

Verdict: **REJECT — 3 majors + 2 minors + 2 notes, ALL
fix-without-re-review**; no round 8 warranted. No cell verdict is
overturned under the recoverable readings, but two majors leave a
scripted verdict underivable from the text as written and one
dismisses a placement on a false equivalence. Verified clean (see
list after the findings): the unit-commit fold treatment is genuinely
F1-pin-consistent with no new fold clause; the toggles-off structural
green derives within the scripted family; the naive crash window is
constructible; all shipped-system claims are true in code; the §5
buffer row is coherent; the 1d cross-reference and §10.5 extensions
are consistent with round-5 F2.

## Findings

- **F1 (MAJOR) — pin o-iv is not realizable from the pinned durable
  state; 6-overlay sub-case (b) rests on the gap.**
  - Claim under attack: "(o-iv) … for a scope with NO marker, resume
    restarts the scope's work FROM CONSULT even when a stop-forced
    checkpoint restored a mid-chain cursor for it … mid-chain cursors
    for unit-mode scopes are restored but IGNORED"; sub-case (b)
    scripts exactly this ("stop-checkpoint captures the mid-chain
    cursor … resume ignores the cursor (o-iv), re-consults").
  - Evidence: §3 pins that a stack entry holds ONE page token,
    advanced in place — "a dispatched-but-unfinished action's stack
    entry retains the token of its LAST COMMITTED transition
    (mid-chain)" — and the glossary pins `NextPageToken` as in-place
    cursor advance. §5 pins "Resume: fresh MSyncAttempt from the MOST
    RECENT durable checkpoint, alone." In sub-case (b) the most
    recent checkpoint is the stop-forced one whose entry for the
    consulting chain holds the mid-overlay cursor; the consult-page
    token was destroyed by the in-place advance and exists in no
    restored state. After "ignoring" the cursor, the resume has NO
    re-entry token: it cannot construct the consult from the
    checkpoint alone, and dispatching the mid-chain cursor is the
    exact thing o-iv forbids ("cursor continuation without the buffer
    is undefined"). A third reading — the worker fails loudly on the
    forbidden cursor — produces a P4-shape livelock, turning (b) RED.
    Root cause: V-ATOMIC's clause (i) had TWO halves — inline
    execution AND "the page's own transition commits only after the
    unit". (o-i) generalizes the first half and silently drops the
    second; sub-case (b)'s "captures the mid-chain cursor" premise
    shows the dropped half was load-bearing.
  - Why it matters: the variant's marker-absent resume rule — the
    §5 buffer row cites it as the thing buffer loss "forces" — has no
    executable semantics, and (b)'s green is the sub-case that makes
    the structural claim non-vacuous under graceful stop. This is
    not a contradiction-in-principle with the glossary's
    stop-resume pin or CO-6b-002 (a declared scenario-local variant
    may deviate); it is a joint-unsatisfiability of o-iv with §3's
    in-place advance and §5's checkpoint-alone resume.
  - Disposition (fix-without-re-review; pick one and pin it):
    (1) generalize clause (i) faithfully — transitions of the pages
    in a consult verdict's prescribed work commit only at unit
    commit, so intermediate overlay cursors never enter live state,
    the stop checkpoint captures the chain AT its consult-page
    token, o-iv's ignore-rule becomes derivable/vacuous, and
    sub-case (b)'s premise text is corrected ("captures the
    consult-page cursor", not "the mid-chain cursor"); or
    (2) keep per-page transitions and declare that unit-mode stack
    entries additionally retain their consult-page token — an
    explicit checkpoint-content extension in §5 (and a stated
    departure from V-ATOMIC's "token UNCHANGED" pin). Option (1) is
    the smaller change and matches V-ATOMIC's discipline. Verdict of
    (b) survives either repair.

- **F2 (MAJOR) — the "third placement" dismissal is unsound: within
  the overlay family the reduction to 6-naive's unmarked-debris
  class cannot occur, and the placement's real windows are not
  covered by any existing cell.**
  - Claim under attack: "(The third placement — per-page commits
    with marker+publish LAST outside any unit — reduces to 6-naive's
    unmarked-debris class and needs no separate cell.)"
  - Evidence: 6-naive's class requires a NON-clearing re-verdict
    unioning over unmarked debris — its attempt 2 goes FETCH-FRESH,
    and "fresh never clears" (1c). In the re-scripted overlay family
    the re-consult's failed revalidation yields CHANGED-WITH-DIFF by
    premise, whose prescribed work BEGINS with
    `eClearScope`+`eCopyScope` (§4; the replayed set is volatile and
    lost in the crash, and the family runs `oncePerScope` OFF
    besides). Walking the placement's windows: (w1) crash anywhere
    before marker+publish → attempt 2 re-consults, the replay page's
    clear WIPES the debris, base+overlay rebuild, marker+publish
    commit → partition rows(e2)@V2 — no union in any schedule; the
    residual is attempt 1's committed copy PLUS attempt 2's committed
    copy in one sync — a replacement-count legality question, not a
    debris union. (w2) marker-before-publish ordering, crash between
    them → marked, entry-less, content-complete scope whose
    re-execution is SUPPRESSED by clause (iii) — seals correct
    rows(e2) with no entry, and P1-content alarms against the empty
    fold (publish was a prescribed round op that never committed) —
    a suppression-window shape that is not 6-naive's class either.
    Neither window reproduces the union; both are uncovered.
  - Why it matters: "needs no separate cell" is a coverage decision
    resting on a false equivalence — the flavor of the re-verdict is
    exactly what makes the debris classes differ, which is the
    addendum's own central insight (stale-AHEAD vs stale-BEHIND).
    Worse, (w1) is the FIRST history in the spec whose class depends
    on whether "at most one replacement copy per scope per sync"
    counts committed copies inside INCOMPLETE rounds: §7's
    "replacement-count legality counts COMMITTED copies" was pinned
    on cell 4's two-complete-rounds shape, while plan B5 pins the
    cross-attempt re-execution copy as legal ("the worst case …
    re-runs an idempotent copy"). Under the all-committed-copies
    reading (w1) alarms on a converging, B5-legal history; under a
    complete-rounds-only reading it is green. No scripted cell
    reaches cross-attempt double-copy today, so the pin is genuinely
    missing, and out-of-script counterexample logs must fold
    identically (round-5 F1's own motivation).
  - Disposition (fix-without-re-review): replace the dismissal with
    a correct argument or a cheap cell (same premise, shifted op
    placement — both windows above are the interesting rows), and
    pin the replacement-count rule's treatment of copies in
    incomplete rounds. Recommended pin: legality counts committed
    copies within COMPLETE rounds, plus the pre-committed-
    classification rule that incomplete-round copy debris surfaces
    through content divergence (which keeps cell 4 red, keeps
    6-naive red, and keeps the benign at-least-once re-copy green).

- **F3 (MAJOR) — the P1 fold value for a scope with ZERO complete
  rounds is unpinned; two readings exist in the spec's own text and
  they diverge on 6-overlay-naive's headline content verdict, while
  the cell's attestation claim is underivable under either.**
  - Claim under attack: 6-overlay-naive — "the round is INCOMPLETE
    … contributes no fold entry; the committed prefix is debris →
    P1 content violation at seal (and attestation: entry e2 over an
    e1-mosaic partition)"; §10.5 — "6-overlay-naive is content-RED
    at seal".
  - Evidence: §7 defines the fold "over complete rounds" and never
    pins its value when the complete-round set is EMPTY ("current
    fold value" is likewise presupposed by the overlay and
    copy-skipped clauses, never seeded). 6-overlay-naive is the
    first cell whose content verdict depends on the empty case:
    every prior red cell had at least one complete round (6-naive
    folds fresh(e2); 1b-ii folds rows(e1)). The spec's own 5b text
    supplies the competing reading — "invisible to P1 (no legal
    round for S — vacuously green)" — under which a scope with no
    complete round is simply not checked, and 6-overlay-naive seals
    content-GREEN. The intended reading (empty fold = empty
    partition; debris diverges) is recoverable from "torn or
    INCOMPLETE rounds' debris surfaces as content divergence", but
    recoverable-not-pinned with a divergent cell verdict is exactly
    the round-5 F1 shape. Separately, the attestation check is
    pinned as "the manifest entry's epoch … equals the fold
    result's epoch" — an empty fold HAS no epoch, so the comparison
    is undefined, not violated; the cell's parenthetical attestation
    claim does not follow from the check as written (again a first:
    1b-ii's entry-vs-fold mismatch compared two defined epochs).
  - Why it matters: the kill cell of the whole addendum — the one
    that answers scenario 6's deferred stale-AHEAD obligation — must
    be RED because the property text forces it, not because the
    scenario intends it; §10.5's obligation is unverifiable as
    written and a checker implementing the vacuous reading passes
    the naive variant.
  - Disposition (fix-without-re-review), three pins in §7: (a) the
    fold's initial value is the EMPTY partition, so a scope with
    committed store ops and no complete round diverges by
    construction; (b) a published manifest entry for a scope whose
    fold result is empty is an ATTESTATION violation (the entry
    attests a composition the log does not contain); (c) reword
    5b's "vacuously green" to "green — empty partition equals the
    empty fold, and no entry exists to check" so the vacuous reading
    loses its textual foothold. Collateral swept: 5b stays green
    under (a)+(b) (empty partition, no entry); no other cell has a
    non-empty partition or a published entry over an empty fold;
    1d's copy-skipped C publishes over a NON-empty fold
    (self-grounding rows(e2)) and keeps its scripted
    stale-BEHIND alarm.

- **F4 (MINOR) — (o-iii) pins the unit's contents unconditionally to
  include publish(V_to), but validator-less diff rounds are legal in
  the modeled population and the declaration is silent on them.**
  - Evidence: §3 MWorker pins "the annotation carries a validator or
    not per scenario cell (both shapes are legal per proto)"; plan
    B5 pins that a round which never supplies a non-empty validator
    gets no entry and the replay remains valid; 1d scripts a
    validator-less carrier as a first-class sub-case. V-OVERLAY-UNIT
    declares `eOverlayUnit(s)` = {…, publish(V_to)} with V_to "the
    verdict's post-diff validator (the new delta token)" —
    presupposing the connector supplied one. The round-6 process
    lesson ("declared here in full before its cells") makes the
    declaration, not the cells, the place where the legal input
    space must be total; the v10 §11 entry claims the variant
    answers "what the unit publishes for a diff verdict", and the
    answer is currently partial.
  - Why it matters: an implementer must know whether a
    validator-less diff round's unit omits the publish constituent,
    synthesizes a validator, or rejects the shape; the three differ
    observably (miss next sync vs forged attestation vs loud cold).
  - Disposition: one sentence in the declaration — the publish
    constituent is present iff the round supplied a non-empty
    validator; a publish-less unit commits {clear, copy, overlays,
    marker}, leaves no entry (miss next sync, B5-consistent), and
    the marker still suppresses re-execution within the sync. No
    scripted sub-case changes.

- **F5 (MINOR) — "B5's early-publish permission is NOT exercised by
  the variant" conflates the connector-side permission with the
  runtime's frozen publish timing; the deferral is a frozen-contract
  deviation on the runtime axis, though verified connector-invisible.**
  - Evidence: B5's publish rule is a runtime MANDATE with pinned
    timing — "if `SourceCacheReplay.cache_validator` is non-empty,
    the manifest entry is published after that page's operations
    complete" — and the code publishes per-page in `afterUpserts`
    (`source_cache_orchestration.go` 758–771). The "permission" is
    the CONNECTOR's option of which page carries the token (1d's
    reading: "plan B5 permits the replay page to publish … before
    overlay pages land"). Under V-OVERLAY-UNIT the connector still
    exercises that option — (o-iii) itself says "the connector still
    returns the token on the replay page" — so the permission IS
    exercised on the wire; what the variant changes is the runtime's
    manifest-write timing, a deviation from B3/B5's frozen per-page
    publish that would need a change order if adopted. The
    load-bearing half of the claim verifies clean in code: the
    consult surface is the PREVIOUS artifact only
    (`previousSyncSourceCacheLookup` wraps the previous store's
    entry reader, 163–215), so the current sync's manifest is never
    connector-readable mid-sync and the deferral is
    connector-invisible — "not a wire-contract change" is true.
  - Why it matters: the bake-off comparison must not book the
    deferral as free; it is a runtime-contract change with a CO
    obligation, and "permission not exercised" misattributes whose
    behavior changed. Also (o-iii)'s "on the replay page"
    over-narrows: B5's other leg (token on a later record/overlay
    page) is equally legal and equally buffered into the unit.
  - Disposition: reword — the variant defers the runtime's manifest
    write into the unit (a B3/B5 timing deviation, change-order
    scope if adopted; wire contract untouched, connector-invisible
    because the lookup surface is the previous artifact only), and
    replace "on the replay page" with "on whichever page carries
    it".

- **F6 (NOTE) — the two-worker marker-race carry-over is the same
  hazard CLASS but a materially wider WINDOW; the boundary note
  should say so for the deliverable-4 bake-off.**
  - V-ATOMIC's N4 window spans one page's handling (marker check →
    `eReplayUnit`). V-OVERLAY-UNIT's spans the whole collect phase —
    marker check at consult → unit commit after every overlay page
    is collected, multiple connector calls and atomic steps. The
    class carries over as claimed (two consulting actions both pass
    the absent-marker check, two units commit, two committed copies
    → P1 legality alarm; unreachable in the scripted
    single-consulting-chain family), and one difference from
    shipped case 4 is worth recording: because each unit is
    internally atomic, the racing schedules' final CONTENT is the
    last unit's coherent rows(e_to) — the alarm is legality-only,
    never a wipe-mosaic. Stranded-hit inertness carries over
    unchanged as claimed (consult-inline + o-iv make a restored hit
    inert). Disposition: extend the boundary-note sentence with the
    widened-window observation; no cell change.

- **F7 (NOTE) — 6-overlay-naive's P2 wording misplaces the staleness
  counter, and the verification-sync expectations are stated
  narrower than what re-derives.**
  - "P2 staleness grows without bound on the never-landed rows":
    staleness attaches to rows PRESENT in the partition — the growth
    is on the stale base(e1) rows whose e1→e2 updates never landed;
    the never-landed rows are in no partition. Recomputing P2 in the
    verification sync: the per-seal consult clause is GREEN (S is
    consulted via V2's validation match, which qualifies under §7's
    round-5 pin) — the unbounded growth is the staleness-counter
    corollary, case 1's "unbounded branch" vocabulary, and should be
    labeled as such. Also true but unclaimed: P1 content stays RED
    in the verification sync itself (the replacement fold of the
    warm mosaic copy is rows(e2) via truthful V2; the partition is
    the mosaic), so an implementer checking only P2 there would
    under-report. Disposition: two wording fixes; verdict direction
    unchanged.

## Verified clean (positive results, for the freeze record)

- Fold treatment of 6-overlay: genuinely consistent with §7 as
  written and the round-5 F1 pin — each page's prescribed store ops
  commit at unit commit, so round completion IS unit commit; the
  round is an overlay round whose own copy committed →
  self-grounding, folds rows(e2), copy counts once; no new fold
  clause needed. Sub-cases (a), (c), (d) re-derive green exactly as
  scripted ((b) pending F1's mechanism pin, after which it
  re-derives green too).
- The structural claim is non-vacuous and derivable within the
  scripted family with both toggles OFF: the marker-inside-the-unit
  is the dedup (at most one unit per scope per sync — a committed
  unit implies a marker that suppresses every later execution; an
  uncommitted unit implies no copy), and no interleaving partner
  exists for `scopeLocks` to matter (single consulting chain, no
  carriers by o-i, no record pages).
- 6-overlay-naive's crash window is constructible under §5's prefix
  rule (unit', overlay-p1 upsert committed; final upsert dropped;
  same-sender FIFO), attempt 2's marker suppression follows clause
  (iii) as declared, and the verification sync's warm mosaic replay
  is reachable (nothing marks the artifact blocked; V2 revalidates
  clean). Stale-AHEAD as the non-self-healing direction re-derives:
  V2 attests e2, so no future consult ever re-delivers e1→e2 — the
  exact dual of 1d's stale-BEHIND, correctly classified.
- Shipped-system claims: per-page publish at `afterUpserts` when the
  page carries a validator (so B5 early publish is real shipped
  behavior for validator-bearing replay pages); hit recording at
  lookup time via `onHit`; the consult surface is the previous
  artifact only. The implementation-shape sentence (WriteBatch /
  grouped ingest with range-del + rows in one commit) has an in-repo
  precedent (`overlay.go` fold batches combine `DeleteRange` with
  row writes in one atomic commit); the model checks contents, not
  mechanism, as declared.
- §5 buffer row: coherent — MWorker ownership matches §3 (one worker
  runs the entire chain, so the buffer never crosses workers);
  volatile-never-checkpointed is consistent with the stop
  checkpoint's pinned contents; bound (pages ≤ 2) inside small
  scope.
- 1d cross-reference and §10.5: consistent with round-5 F2's
  mitigation-dependent demotion; the "same toggles-off configuration
  that turns 1d content-red" claim checks out (locks-off 1d is
  content-red per round-5 F2; both-off is red a fortiori).
- P3′ claim in 6-overlay: correct — the family's mutation is between
  syncs, unit-mode rounds cannot tear, and rows(e2) matches the last
  consulted verdict epoch.
- o-iv vs the glossary's stop-resume pin and CO-6b-002: deviation is
  declared and legitimate for a scenario-local variant (precedent:
  V-ATOMIC's marker suppression, round 5); the defect is F1's
  realizability, not contradiction-in-principle.

## Summary

| finding | severity | one line | disposition |
|---|---|---|---|
| F1 | MAJOR | o-iv's restart-from-consult has no re-entry token under §3's in-place cursor advance + §5's checkpoint-alone resume; sub-case (b) rests on it | pin transition deferral to unit commit (or explicit token retention); correct (b)'s premise text; fix-without-re-review |
| F2 | MAJOR | third-placement dismissal reduces to 6-naive falsely — the overlay re-verdict CLEARS debris; real windows (cross-attempt double copy, marked-entry-less suppression) are uncovered and expose an unpinned legality-counting case | correct argument or cheap cell; pin replacement counting for incomplete-round copies; fix-without-re-review |
| F3 | MAJOR | empty-fold value unpinned; 5b's "vacuously green" wording supplies a reading that flips 6-overlay-naive content-GREEN; attestation-over-empty-fold undefined | pin empty-fold = empty partition, entry-over-empty-fold = attestation violation, reword 5b; fix-without-re-review |
| F4 | MINOR | unit contents pinned unconditionally with publish(V_to); validator-less diff rounds legal and unhandled | pin publish constituent conditional on a supplied validator; publish-less unit leaves no entry |
| F5 | MINOR | "B5 early-publish permission not exercised" conflates connector permission with the runtime's frozen B3/B5 publish timing (a CO-scope deviation, though verified connector-invisible) | reword; widen "on the replay page" to "whichever page carries it" |
| F6 | NOTE | marker-race carries over in class but with a materially wider window (whole collect phase); racing content is coherent, alarm legality-only | extend the boundary note |
| F7 | NOTE | P2 growth wording misplaces the counter (stale resident base rows, not "never-landed rows"); P2's seal clause is green in the verification sync; P1 also stays red there | wording fixes |
