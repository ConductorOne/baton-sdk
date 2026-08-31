# Round 7 (parallel) — second independent spot review of the v10 addendum

PROVENANCE NOTE: two independent round-7 spot reviews of the v10
addendum were run in parallel; this is the second, surfaced after the
primary (`model-spec-round7-overlay.md`) had been dispositioned as
v11 and the spec frozen. Its findings are dispositioned as MS-CO-001
(see MODEL_SPEC §11): the overlapping findings (its F1 ≈ primary F3,
F2 ≈ primary F4, F3 ⊂ primary F5) were already fixed in v11; the
non-overlapping items (F4, F5, F6, N1, N4) are applied by the change
order. Its "third-placement reduction: sound" verdict DISAGREES with
the primary's F2 (false reduction); the built cell `tc6overlayLast_P1`
settles the disagreement mechanically in the primary's favor — see the
MS-CO-001 entry. Text below is the review verbatim.

---

Scope: §9.6 V-OVERLAY-UNIT declaration (pins o-i..o-iv), cells
6-overlay and 6-overlay-naive, the §5 overlay-collect-buffer row, the
rewritten scenario-6 pilot-scope paragraph, the 1d cross-reference
sentence, the §10.5 obligation extensions, and the §11 v10 entry — per
the §11 v10 change-order entry. Not a full round; v1–v9 material out of
scope except where the addendum destabilizes it.

Method: mechanical reachability walk of 6-overlay sub-cases (a)–(d) and
6-overlay-naive under §3/§4/§5 as modified by o-i..o-iv (crash position
in MStore's queue per the §5 prefix rule, stop-checkpoint contents,
o-iv resume behavior, clause-(iii) suppression); hand-application of
§7's fold (round-5 F1 completion pin, self-grounding-overlay and
copy-skipped clauses), P2 (changed-with-diff consult pin), and P3′ over
every schedule the cells produce; adversarial schedule construction
against the toggles-off green claim; coherence sweep against rounds 3–6
and against 6-atomic, 1d, 5b, and the round-5 F7 disposition;
verification of the wire-contract and walker-mechanics claims against
`docs/verification/sync-replay-6b/plan.md` (B1–B10, CO-6b-001..007),
`proto/c1/connector/v2/annotation_source_cache.proto`,
`pkg/sync/source_cache_orchestration.go`, `pkg/sync/state.go`,
`pkg/sync/parallel_syncer.go`, and
`pkg/dotc1z/engine/pebble/source_cache.go`.

Verdict: **REJECT — 1 major (F1) + 5 minors + 4 notes, ALL
fix-without-re-review** per the rounds 4–6 convention (every
disposition is a pin or declaration; no scripted cell verdict changes
under the intended readings; no re-script). No round 8 warranted.

## Verified clean (for the freeze record)

- **Reachability.** All 6-overlay sub-cases (a)–(d) and
  6-overlay-naive's premise walk mechanically without hand-placement.
  6-overlay-naive's crash lands between `eOverlayUnit'` and the final
  page's `eUpsertPage` in one sender's FIFO stream — a genuine
  queue-position choice under §5's prefix rule, the same mechanism
  6-naive uses; restart-from-root holds (crash-only history, Init
  checkpoint survives, checkpoint-skip nondet at the sole loop top).
  Sub-case (b)'s stop-checkpoint contents (mid-chain cursor + hit
  {S: V1}, buffer lost) follow §3's stop semantics exactly; the
  restored hit is inert per the carried-over V-ATOMIC pin;
  clause-(iii) suppression in naive's attempt 2 is the declared marker
  semantics, not scenario hand-placement. Budgets hold: ≤ 2 syncs,
  ≤ 2 attempts per sync, 1 crash or 1 stop per sub-case, buffer
  bounded by the 2-page round.
- **Fold coherence for 6-overlay.** The unit's round folds as §7's
  EXISTING self-grounding-overlay clause with no new fold clause — own
  copy committed inside the unit, folds to rows(s, e_to), copy counts
  toward replacement legality (exactly one unit per scope per sync is
  constructible in the family: one chain, marker suppresses
  re-execution, crash cannot split the atomic op). Round completion =
  unit commit is consistent with the round-5 F1 pin (every prescribed
  store op of every page commits at the unit's queue position;
  announce in prescribed page order keeps the fold order
  well-defined). Sub-case (b)'s attempt-1 fragment commits no store
  ops and contributes no fold entry (existing pin); no torn round is
  constructible — P3′'s by-construction claim holds, and its (i)
  scoping holds (mutation is between syncs only).
- **6-overlay-naive's incompleteness.** Under the F1 pin, the round's
  prescribed store ops are the unit' constituents PLUS each overlay
  page's upserts/tombstones; the final page's ops never commit, the
  round is INCOMPLETE (not torn — all committed pages sit in attempt
  1), and marker suppression prevents any attempt-2 round. The
  committed prefix is debris. The publish-time check is
  attestation-only and green (V2's epoch equals the verdict epoch e2),
  so the alarm correctly lands at seal — modulo finding F1 below on
  what the seal checks are defined to compare against.
- **Toggles-off claim.** With `oncePerScope` AND `scopeLocks` both
  OFF, every schedule the scripted family allows was walked: the
  single consulting chain is the only actor on S (o-i removes the
  carrier; whole-chain worker ownership per §3), so no interleaving
  exists for a lock to serialize — the family does not secretly depend
  on one. `oncePerScope`'s function is genuinely subsumed by the
  in-unit marker (at most one unit per scope per sync in every
  constructible history). The two-worker marker-race boundary note is
  honestly scoped: the dual-consult shape is unreachable in the
  single-chain family and stays a recorded hazard, correctly carried
  over from round-5 N4.
- **P2/P3′.** 6-overlay's P2 green follows from the round-5 F8
  changed-with-diff pin (staleness ≤ 1: base rows one hop, overlay
  rows fresh). 6-overlay-naive's verification-sync claim follows from
  P2's pinned definitions: the mosaic's V2 entry revalidates clean
  (truthful validators, upstream fixed at e2), the per-seal consult
  check stays green, and the staleness corollary exhibits growth to
  the chain bound on the replayed stale rows — the same "unbounded
  branch" convention as case 1 (wording caveat in N2). The stale-AHEAD
  non-self-healing classification is correct: a future consult of V2
  delivers diffs from e2 onward only; the e1→e2 changes are never
  re-delivered — the genuine dual of 1d's stale-BEHIND.
- **Wire-contract claim (o-iii).** TRUE as stated, verified against
  the proto and the orchestration code:
  `SourceCacheReplay.cache_validator` documents both token placements
  (replay page, or deferred to the final record page's
  `cache_validator`); the manifest write timing is runtime-internal
  and unobservable on the wire within a sync — the warm lookup and the
  ask/answer continuation resolve against the PREVIOUS artifact only
  (`previousSyncSourceCacheLookup.prev`; the proto's continuation
  diagram). B5's early publish is a permission, not an obligation, so
  declining it is a runtime design restriction, exactly as the pin
  says. (Two adjacent looseness items: F2, F3.)
- **Third-placement reduction.** "Per-page commits with marker+publish
  LAST outside any unit reduces to 6-naive's unmarked-debris class" is
  sound: under a changed-with-diff re-verdict the next attempt's own
  clear/copy replaces the debris; the debris is harmful only under a
  fetch-fresh follow-up, which is exactly 6-naive's scripted kill. No
  separate cell needed. (An adjacent legality-counting boundary is
  recorded as N3.)
  [MS-CO-001 NOTE: overturned mechanically — see the provenance note.]
- **No destabilization.** 6-atomic's green claim stays de-scoped to
  the 1a/1b/1c re-runs; 1d's verdicts are untouched and its new
  cross-reference sentence is accurate; the round-5 F7 disposition is
  correctly historicized (the §11 v7 entry is unmodified, append-only
  discipline kept; the v10 entry states the supersession); the
  rewritten pilot-scope paragraph names exactly the two obligations
  round 5 F7 deferred (the unit's publish for a diff verdict; the
  marker-before-overlay stale-AHEAD hazard) — verified against the
  round-5 record. §5's crash-wipe column stays consistent with the new
  buffer row; the announce-at-commit story keeps the §2.4 monitor
  subscription implementable; the o-iv/§5 relationship is coherent
  (restore-as-is governs checkpoint restoration, o-iv governs
  dispatch, and the §5 buffer row cross-references the rule);
  conformance question C1 is a shipped-design probe and is untouched.

## Findings and dispositions

- **F1 (MAJOR) — the P1 seal checks are undefined over a log with ZERO
  complete rounds for a non-empty, attested scope, and the two live
  readings flip 6-overlay-naive's verdict.** Spec text at issue: §7's
  content check ("partition equals the fold result") and attestation
  check ("the manifest entry's epoch … equals the fold result's
  epoch"), versus the 6-overlay-naive script ("the round is INCOMPLETE
  … contributes no fold entry; the committed prefix is debris → P1
  content violation at seal (and attestation: entry e2 over an
  e1-mosaic partition)"). Reasoning: every prior cell's seal log
  contains at least one complete round per attested scope — 6-naive
  and 1c have the attempt-2 fresh round; the fold's value over an
  EMPTY round set is never pinned, and neither is the attestation
  comparison when the fold result carries no epoch. Worse, the spec
  itself contains the opposing precedent: 5b pins "invisible to P1 (no
  legal round for S — vacuously green)." An implementer following 5b's
  vacuous-domain reading returns GREEN on 6-overlay-naive's content
  check (no complete round → scope outside the check's domain),
  contradicting the scripted RED; the empty-fold reading (fold(∅
  rounds) = empty partition → mosaic ≠ ∅ → RED) gives the scripted
  verdict. This is precisely the round-5 F1 divergence class: a
  definitional gap two honest readings disagree on, surfacing for the
  first time on the new cell. The attestation parenthetical is doubly
  underdetermined (no fold epoch to compare V2 against). Disposition:
  pin in §7 — (a) the fold over zero complete rounds yields the EMPTY
  partition (initial fold value = ∅, stated once); (b) a
  validator-bearing manifest entry for a scope whose fold result
  carries no epoch is an ATTESTATION violation (the entry attests an
  epoch the fold cannot ground). Note that this pin RECONCILES 5b
  rather than disturbing it: 5b's partition is empty and it publishes
  no entry, so it is green by empty-equality and attestation vacuity —
  adjust 5b's "vacuously green" parenthetical to say so. State
  explicitly that 6-naive and 1c are unaffected (complete rounds
  exist). **Fix-without-re-review**: both scripted verdicts hold as
  written under the pin; the disposition is a fold pin, not a
  re-script. [MS-CO-001: already applied in v11 — primary F3.]

- **F2 (MINOR) — `eOverlayUnit`'s contents are not total over
  wire-legal validator-less rounds.** Spec text at issue: o-ii/o-iii
  declare the unit as "{clear, copy(base e_from), overlay
  upserts/tombstones …, marker, publish(V_to)}" with publish an
  unconditional constituent. Reasoning: B5 legalizes a replayed round
  that never supplies a validator ("no entry … a miss next sync — the
  replay itself remains valid"), and 1d's validator-less-C sub-case
  (round-5 F3) leaned on exactly this legality. Under the variant, a
  validator-less changed-with-diff round has no V_to; the declaration
  does not say what the unit then contains, so the variant's semantics
  are undefined over a legal wire shape. No scripted cell reaches it
  (the family always carries V2). Disposition: pin publish-when-present
  — the unit omits the publish constituent when the round supplies no
  validator; the scope gets no entry and is a miss next sync, the
  replay/overlay contents remain valid (B5's own language) — or
  explicitly declare validator-less rounds outside the pilot's scope.
  **Fix-without-re-review** (declaration pin, no verdict changes).
  [MS-CO-001: already applied in v11 — primary F4.]

- **F3 (MINOR) — the 1d publish-placement sub-config axis silently
  collapses in the re-scripted family, and o-iii's parenthetical
  asserts one wire shape where the proto permits two.** Spec text at
  issue: o-iii's "the connector still returns the token on the replay
  page"; the 6-overlay family text, which never mentions the placement
  axis that round-5 F4 required 1d to pin as two explored sub-configs.
  Reasoning: the proto permits the new token on the replay page OR
  deferred to the final overlay page's
  `SourceCacheRecord.cache_validator`. Under the variant both
  placements feed the same buffered collection and the same unit
  publish, so the axis is degenerate — but a family presented as "the
  re-scripted 1d premise family" that silently drops a pinned explored
  axis invites the round-5 F4 question all over again, and the
  parenthetical as written is factually over-narrow. Disposition: one
  sentence in o-iii or the 6-overlay cell — both B5 token placements
  are wire-legal and collapse to the single unit publish under
  o-ii/o-iii; the 1d sub-config axis is degenerate by construction.
  **Fix-without-re-review.** [MS-CO-001: subsumed by v11's F5
  rewording; degeneracy noted in the change-order entry.]

- **F4 (MINOR) — o-iv's cursor-ignoring rule is phrased per-scope but
  the restartable unit is the ACTION; multi-scope chains leave the
  operational rule undefined.** Spec text at issue: "mid-chain cursors
  for unit-mode scopes are restored but IGNORED … resume restarts the
  scope's work FROM CONSULT." Reasoning: an action has one cursor, not
  one per scope. The model's state space contains planning chains that
  consult k scopes (§3 MWorker) and 2-scope configs; a stop-checkpoint
  of a chain whose page 1 committed scope S1's unit and whose page 2
  consults S2 restores ONE cursor — "ignore it for unit-mode scopes"
  does not say what the scheduler dispatches. The scripted cells are
  single-scope, so no verdict diverges, but the pin as declared is not
  implementable as written. Disposition: pin o-iv operationally —
  under the variant, a restored mid-chain cursor for an action
  carrying unit-mode scope work is discarded and the action restarts
  from its ROOT token; clause-(iii) marker suppression provides
  per-scope idempotence for already-committed units (the at-least-once
  re-fetch price already stated). Alternatively restrict the variant
  to single-scope chains by declaration. **Fix-without-re-review.**
  [MS-CO-001: v11's transition-deferral pin (primary F1) dissolved the
  ignore-rule; deferral now explicitly scoped per verdict.]

- **F5 (MINOR) — the variant's atomic store op is not registered in
  §3's MStore op list, and §4 carries no pointer to the variant
  override; the round-6 process lesson is only partially complied
  with.** Spec text at issue: §3's MStore atomic-op enumeration
  (eCheckpoint … eSeal), which contains neither `eOverlayUnit` nor
  V-ATOMIC's `eReplayUnit`; §4, whose page-op sequence the variants
  supersede for unit-mode scopes with no cross-reference. Reasoning:
  the round-6 lesson — new machinery arrives through §1/§3/§5
  declarations, not scenario scripts — is the standing hunt pattern
  for this review. v10 complied at §5 (buffer row) and by declaring
  pins before cells, but a new MStore atomic op is §3 machinery: §5's
  crash protocol ("eCrash is enqueued … like any op") and §2.1's
  cross-sender arrival-order choice point quantify over the store-op
  vocabulary, which a reader assembles from §3. The semantics are
  fully pinned in §9.6, so no two readings diverge — hence minor, not
  major — but the registration gap is real, and `eReplayUnit` was
  grandfathered only because round 5 predates the lesson. Disposition:
  add both variant ops to §3's MStore list, marked scenario-local
  (§9.6), and add one line to §4 noting that scenario 6's design
  variants replace this sequence for unit-mode scopes.
  **Fix-without-re-review.** [MS-CO-001: applied.]

- **F6 (MINOR) — §10.5 carries no mutation obligation for o-iv, the
  one load-bearing line the overlay flavor adds beyond V-ATOMIC's
  two.** Spec text at issue: §10.5's new 6-overlay/6-overlay-naive
  obligations, which pin the unit boundary (naive red) and the
  toggles-off green, but nothing kills a resume that honors the
  restored mid-chain cursor. Reasoning: the pair pins "unit contents"
  and "marker suppresses re-execution" (scenario 6's own framing); the
  overlay flavor newly introduces the buffer-loss resume rule, and its
  kill is constructible — in sub-case (b)'s schedule, a
  continuation-without-buffer mutant collects only page 2 and commits
  a unit missing page 1's overlay ops → partition ≠ rows(e2) under the
  self-grounding fold → content-RED. Round-5 F2 set the precedent (the
  locks-off 1d mutant was added to §10.5 for exactly this reason).
  Disposition: add to §10.5 — an o-iv-removal mutant (resume continues
  the restored cursor for a unit-mode scope) is content-RED in
  6-overlay sub-case (b)'s schedule. **Fix-without-re-review.**
  [MS-CO-001: applied, and built as a model cell.]

## Notes to the freeze record

- **N1.** The marker's durability (per-scope store row, durable at op
  commit) is declared only inside §9.6's V-ATOMIC paragraph, while
  §2.2 keys mechanical crash-wipe enforcement to §5's table. v10 added
  a §5 row for the buffer but not for the marker, which
  6-overlay-naive's premise newly leans on (the marker must survive
  the exact crash that drops the overlay upserts). Grandfathered from
  v6, but a variant-scoped §5 line ("replay/overlay unit marker |
  MStore | durable at op commit — §9.6 variants only") would make the
  table total. Cosmetic-plus; fold into the F5 edit. [MS-CO-001:
  applied.]
- **N2.** "P2 staleness grows without bound on the never-landed rows"
  is imprecise: staleness grows on the PRESENT mosaic rows whose
  e1→e2 updates never landed; rows the lost final page would have
  ADDED are absent and outside P2's row quantification entirely (the
  5b dropout class — their only oracle is the content check). Reword
  so the two halves of the non-self-healing story (present-stale rows
  with growing counters; absent rows invisible to P2) are stated
  separately. [MS-CO-001: already applied in v11 — primary F7.]
- **N3.** §7's replacement legality "counts COMMITTED copies, not
  verdict labels" does not say whether copies belonging to INCOMPLETE
  rounds count. No §9 config reaches the shipped-benign shape (B5: a
  checkpoint cut between lookup and page commit "re-runs an idempotent
  copy" — copy commits, crash loses the replayed set, resume replays
  the same scope again), so no scripted verdict is affected; but under
  the all-committed reading that shipped-legal schedule would
  false-alarm. Record as a config-widening tripwire in the
  torn-round-boundary style: any future config scheduling a replay
  verdict on both sides of a crash for one scope requires a
  legality-count scoping decision (complete-rounds-only vs
  all-committed) by change order first. [MS-CO-001: superseded — v11
  pinned complete-rounds-only counting (primary F2) and the
  6-overlay-last cell reaches the shape this note called unreached.]
- **N4.** "Collects the 2-page overlay inline" should be pinned as the
  2-page ROUND (the replay/first-overlay page plus the final overlay
  page). The 3-page reading (consult page + 2 overlay pages) violates
  §1's pages-per-scope-per-round ≤ 2 bound; 6-overlay-naive's own
  premise confirms the 2-page reading (unit' at page 1's consult
  boundary, page 1's upserts committed, page 2's dropped). One clause
  fixes it. [MS-CO-001: applied.]

## Process observation

v10 is the first addendum that visibly internalized the round-6 lesson
— the pins precede the cells, the buffer reached §5, and the cells
cite the fold clauses they rely on — and it shows: the cells
themselves walked clean, and the one major lives in §7's check
definitions rather than in undeclared scenario machinery. The residual
pattern is narrower than round 6's: machinery declared in the right
ORDER but not in all the right PLACES (F5, N1), and a property-layer
totality gap that only a new log class could expose (F1). The
5b-vs-6-naive precedent collision behind F1 is worth remembering at
freeze time: two cells can each pin a locally-correct reading whose
union is contradictory, and only a cell that sits in the intersection
detects it.
