# Graph model calibration — run log

Status: COMPLETE. All scenario families (G1–G9) built and
calibrated; the frozen 66-cell full-matrix sweep PASSED (see
§"Freeze sweep — 2026-08-30" below; run of record
`PCheckerOutput/sweep/summary.txt`, `SWEEP-DONE cells=66
mismatches=0`; reproduce with `tools/sweep.sh`), and the 12-cell
bake-off phase matched its GS-CO-005 declared verdicts 12/12 (see
§"Bake-off results — 2026-08-30" below; run of record
`PCheckerOutput/bakeoff/summary.txt`; reproduce with
`tools/bakeoff.sh`). The verdict synthesis lives in `BAKEOFF.md`.
FLAP-BACK MACHINERY: the upstream reports CONTENT
epochs everywhere (raw epochs never escape it), so `flapBack`
(raw e>=3 serves content(e1)) keeps validators, manifests, folds,
and SealExpect worlds coherent with no monitor changes. Spec
baseline:
`formal/GRAPH_MODEL_SPEC.md` v4 FROZEN + GS-CO-001..005 (the first
four change orders were driven by calibration finds in this
build-out — see §12 of the spec and the "Model decisions of record"
section below; GS-CO-005 is the pre-registered bake-off decision
rule).

Toolchain: P 3.1.0 (`p compile` / `p check -tc <cell> -s <schedules>`),
same conventions as `formal/walker/CALIBRATION.md`: verdicts are
audited by counterexample trace-file presence (`tools/sweep.sh`), reds
are the calibration currency, and a green run means nothing except at
the stated budget.

Project layout mirrors the walker: `PSrc/` (Types, Events, Upstream,
Store, Sched, NodeExec, Env), `PSpec/Monitors.p` (GP1, GP2, GP3prime,
PGEN, PMARK, PADOPT, SealExpectG, GP6G, GP6E, GP6S),
`PTst/ScenarioG1.p`. Cell topology 11: parent P (node 0) → consult
node C (node 1). Sessions topology (cell 21, built, exercised from G2
on): P → writer W/H + reader G.

## G1 family — phantom-union premises + generation identity (sweep: 10/10 at 5000 schedules)

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tcG1i_All | leg (i): crash sync 2, fetch-fresh policy, e1→e2 between syncs | all core | GREEN | GREEN | 5000 |
| tcG1ii_All | leg (ii): diff unit, crash, e2→e3 between attempts, MATCH-only re-derive fresh | all core | GREEN | GREEN (after GS-CO-002; see finds) | 5000 |
| tcG1iii_E | leg (iii): crash-after-commit, no mutation → ADOPT, lineage E | all core | GREEN | GREEN | 5000 |
| tcG1iii_S | same, lineage S (+P6-S) | all core + P6-S | GREEN | GREEN | 5000 |
| tcG1b_All | two-crash generation-reuse probe, honest | all core | GREEN | GREEN | 5000 |
| tcG1cMut_P3 | adoptOnFail mutant, P3′ control | P3′ | GREEN | GREEN (FAIL consult does not qualify; last qualifying verdict expects the mutant's own rows) | 5000 |
| tcG1cMut_Seal | adoptOnFail mutant, SealExpect control | SealExpect | GREEN | GREEN (GS-CO-002: rows(e2) is inside the sync-scoped envelope — the declared artifact-blindness control) | 5000 |
| tcG1sup_P1 | suppressionOff on the leg-(iii) chassis | P1 | RED | RED: `P1-LEGALITY` (racing double-admission at one generation; second live complete-round copy) | first find |
| tcG1bMut_PGEN | resumeCkptOff | P-GEN | RED | RED: `P-GEN` (re-minted identity carries store commits in two attempts; fireable with ONE crash post-GS-CO-001) | first find |
| tcG1cMut_Adopt | adoptOnFail | P-ADOPT | RED | RED: `P-ADOPT` (adoption after a FAIL consult — MATCH-only eligibility violated) | first find |

Sweep summary archived at `PCheckerOutput/sweep/summary.txt` (rerun
`tools/sweep.sh` from `formal/graph`).

## G2 family — session laundering + G1d/G1e probes (topology 21: P -> writer H + reader G)

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tcG2ea_Core | E + variant A, value change across attempts | artifact-level core | GREEN | GREEN | 5000 |
| tcG2ea_P6G | same cell | P6-G | RED | RED: `P6-G` (THE FINDING: no retraction, no stamps — the sealed reader rows embed the dead value; artifact-level monitors all green, the blindness contrast is tcG2ea_Core) | first find |
| tcG2eb_All | E + variant B (retraction + quiesce) | all + P6-G/P6-E | GREEN | GREEN (after GS-CO-003; see finds) | 5000 |
| tcG2ebRetrOff_P6G | retractionOff | P6-G | RED | RED: `P6-G` | first find |
| tcG2s_All | S (stamps + pre-seal pass) | all + P6-G/P6-S | GREEN | GREEN | 5000 |
| tcG2sStampOff_P6G | stampMergeOff | P6-G | RED | RED: `P6-G` | first find |
| tcG2awE_All | announce-window/flap-back honest, E+B, premise-stable | all + P6-E | GREEN | GREEN | 5000 |
| tcG2awS_All | same, S | all + P6-S | GREEN | GREEN | 5000 |
| tcG2awE_Redo | flap-back existence probe | REDO-PROBE | RED | RED: `REDO-PROBE` (the at-least-once cost is real: retraction-forced reader redo exists) | first find |
| tcG2awS_Redo | same, S (pass-forced redo) | REDO-PROBE | RED | RED: `REDO-PROBE` | first find |
| tcG2awWA_E | writerAdopt, E+B | P6-E | RED | RED: `P6-E` (after GS-CO-004; the stranded dead publish is never retracted) | feedback-PCT(20), first find ~3.8k (random first-find sits at the 5k budget edge) |
| tcG2awWA_S | writerAdopt, S | P6-S | RED | RED: `P6-S` (pass budget exhausts against the stranded dead stamp) | first find |
| tcG2eb2c_All | honest two-crash, E+B | all + P6-E | GREEN | GREEN | 5000 |
| tcG1e_PGEN | two crashes + midBumpFenceOff | P-GEN | RED | RED: `P-GEN` (identity reuse; the first-find may ride the first-admission mint path — same toggle per GS-CO-001, same discipline) | first find |
| tcG2fbE_All | WRITER FLAP-BACK probe (R3-F1 second registration), E+B: attempt-1 DIFF publish-bearing unit (d2), crash, content(e3)=content(e1) | all + P6-G/P6-E | GREEN | GREEN (H ineligible -> re-consult vs prev artifact MATCHes -> REPLAY -> body-op re-publishes d1@g2 -> readers cleared; under an elision reading this history strands d2@g1 — the probe is the body-op pin's load-bearing witness) | 5000 |
| tcG2fbS_All | same, S | all + P6-S | GREEN | GREEN | 5000 |
| tcG2fbE_Redo | flap-back chassis | REDO-PROBE | RED | RED: `REDO-PROBE` (declared expected count >= 1) | first find |
| tcG2ebPend_Redo | honest E+B laundering chassis | REDO-PROBE | RED | RED: `REDO-PROBE` (the G-pending re-retraction clause fires: R2-M6(i) via GS-CO-004's catch-up + re-publish paths) | first find |
| tcG1dProbe_Redo | G1d chassis | REDO-PROBE | RED | RED: `REDO-PROBE` (the race arms: forced reader redo exists) | first find |
| tcG1d_P6G | E+B + quiesceOff, 3 workers | P6-G | RED | RED: `P6-G` (dying reader's stale round survives to seal; confirmed post-handshake — the first feedback-PCT run instead caught the harness deadlock below) | feedback-PCT(20), first find at 188 schedules |

### G1D-REACH — reachability ladder for the quiesceOff kill

The dying-reader race is DEEP under uniform random search: 165k
random schedules found nothing. Bisected by temporary existence
probes, each RED = milestone reached: forced reader redo on the
chassis (`tcG1dProbe_Redo`, kept, first find ~3k); same-node
concurrent dispatch (temp diag, common); a second reader round
CLEARING while another is open on the reader's key (temp diag,
reached under 30k); the full kill — the stale round's content
surviving to SEAL — never, at 120k random. Two aids resolved it,
both harness-level:

1. `nWorkers = 3` for this cell (spec §1 tagged note) so the
   retraction-forced re-run dispatches AT the bump instead of
   waiting for a completion carrier to free a worker (with 2, the
   race additionally needs the scheduler to starve the dying worker
   through H's entire completion).
2. Strategy: `--sch-feedbackpct 20` (feedback-mutating PCT).
   First find at 188 schedules, 0.53% buggy — the race is
   priority-inversion-shaped, exactly PCT's regime; the sweep runs
   this cell under that strategy (per-cell strategy column in
   `tools/sweep.sh`).

Diagnostics removed after resolution; the redo probe stays as a
sweep cell (the ladder's first rung documents the chassis arming).

### Harness find — crash-arm queue race (feedback-PCT bycatch)

The first feedback-PCT sweep run of `tcG1d_P6G` redded on DEADLOCK,
not P6-G: the env sends `eCrashArm` right after creating the
attempt scheduler, so a schedule that starves the env lets the
ENTIRE attempt (through seal) enter the store's queue ahead of the
arm — the store's at-seal resolution point sees `armed == false`,
seals clean, and the late-landing arm never resolves; the env
blocks on `eCrashAck` forever. Random search never starves one
machine for hundreds of steps, which is why no random sweep (graph
or walker) ever hit it; priority-based PCT does exactly that and
found it at 34 schedules. Fix: synchronous arm handshake
(`eCrashArm` → `eCrashArmed`) BEFORE the attempt is created, so the
arm's queue position precedes every attempt op by construction.
The walker env has the same latent race (arm sent after
`new MSyncAttempt`, random-only sweeps) — noted in the walker's
calibration log; harness-level, no bearing on either model's
verdicts.

## G5 family — sweep, purge, and the closure oracle (topology 24: paginated P -> consult C, named on page 1 only)

Cell 24 scripts UPSTREAM DEMAND SHRINK: the parent's own scope (key
0) mutates between attempts, and its page-1 row — the only row that
names C — is the row the e1->e2 mutation deletes. New monitors: GP5
(artifact demand-closure, both directions, self-contained over the
sealed artifact: CLOSED = every non-root sealed partition has a
living namer, COMPLETE = every named child's partition is sealed),
PURGEPROBE (existence probe on `eAnnPurge`, REDO-PROBE pattern),
GDEADDISPATCH (the G5e count oracle in checkable form: no node ever
dispatches with every admitted-by edge dead), GPASS (§10.8: honest S
cells seal within the pass budget; also asserted retroactively in
tcG1iii_S / tcG2s_All / tcG2awS_All / tcG2fbS_All).

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tcG5aE_All | honest sweep, E, shrink chassis | all core + GP5 + DEAD-DISPATCH | GREEN | GREEN (which parent world seals is schedule-dependent under sync-scoped freshness: P re-ran -> seal {0}; P completed at e1 -> C live, seal {0,1} — C's key EXCLUDED from the attempt-2 expectation, the structural question is GP5's) | 10000 |
| tcG5aS_All | same, S (dispatch-refusal leg) | + GPASS | GREEN | GREEN | 10000 |
| tcG5f_All | honest, NO shrink (R2-F5 no-starvation witness) | all core + GP5 | GREEN | GREEN (resume purges C through the mid-round window; P's live re-derivation re-names it; the purged hash is re-admissible; C re-runs — starvation would red SealExpect) | 10000 |
| tcG5bE_P5 | sweepOff, E | P5 (CLOSED) | RED | RED: `P5-UNDER` (C's dead partition seals with no living namer) | first find |
| tcG5bS_P5 | sweepOff, S | P5 (CLOSED) | RED | RED: `P5-UNDER` | first find |
| tcG5c_P5 | sweepOverreach, no shrink | P5 (COMPLETE) | RED | RED: `P5-OVER` (an in-closure key dropped at seal; the sealed parent still names it) | first find |
| tcG5e_Probe | honest E shrink chassis | PURGE-PROBE | RED | RED: `PURGE-PROBE` (the resume ∀-purge fires — the counterexample exhibits the mid-round C-pending & P-pending checkpoint window) | first find |
| tcG5e_PurgeOff | purgeOff, E | DEAD-DISPATCH | RED | RED: `DEAD-DISPATCH` (the restored C dispatches with its only admitted-by edge dead — dead demand executed) | first find |
| tcG5f_Drop | demandDrop, no shrink | SealExpect / P5 (COMPLETE) | RED | RED: `SEAL-EXPECT` (the dropped child never runs; the expected demand-closure key is missing) | first find |

### G5 build finds (model-level, no spec change)

1. **Per-announce demand timing implemented** (G-RULE-1 TIMING PIN).
   The G1/G2 build derived demand at the completion carrier only —
   observationally equivalent for those families, but the pin is
   load-bearing here: the mid-round checkpoint window (C-pending ∧
   parent-pending) that the resume ∀-purge exists for is UNREACHABLE
   under carrier-only derivation (exactly round-1 F5(iii)'s
   argument). A paginated record round now sends the scheduler a
   demand note per committed child-naming page (`eGDemandNote`,
   read-note pattern); each note is its own carrier under GS-CO-003's
   one-delta fence. The completion carrier still re-derives
   (idempotent via G-RULE-2 suppression + edge dedupe) — belt and
   braces, and a crashed note costs nothing (the row is durable; a
   pending parent re-derives, a completed parent's admissions rode
   the fence).
2. **Pass domain = demand closure** (S). The pre-seal pass chased
   dead stamps on OUT-OF-CLOSURE keys — debris the sweep is about to
   drop — force-bumping their owners into dispatch-refusal loops
   until the budget exhausted on an honest history (tcG5aS_All red
   before the fix). The scan now skips keys outside the current
   closure, recomputed per sealPhase entry. Spec §5's pass text
   ("one scan over a drained frontier") does not state the domain;
   flagged for an RFC-stage clarification line, not a change order.
3. **demandDrop models a lost PATHWAY, not a lost message.** With
   belt-and-braces derivation the original drop-one-admission inject
   is always healed by the second derivation of the same emission
   (tcG5f_Drop was green). The inject now drops every admission of
   the first hash it fires on.
4. **The count oracle is a dispatch-time announce.** "No post-resume
   C execution" is not directly assertable (C's post-resume run is
   HONEST when the completed parent's naming stays live —
   sync-scoped freshness again); the invariant form is
   DEAD-DISPATCH: no node dispatches while every admitted-by edge
   names a dead generation. Honest mechanisms (E resume purge, S
   dispatch refusal) make it unreachable on every cell, so it is
   asserted in the honest G5 greens, not just the kill.
5. **Digest session fields are DECLARED VACUITY in this envelope
   (SPEC §4a DIGEST CLOSURE discharge).** `tDigest` carries the
   session-read fields the spec requires
   (`sVal`/`sWriter`/`sWGen`/`hasSess`), but every digest the model
   constructs uses the no-session sentinel (`NodeExec.p`): no
   adopt-eligible node performs a pre-commit session read in any
   calibrated cell — readers always fetch fresh and never consult
   markers, writers are writer-ineligible (pubBearing), and the
   consult-kind adopt check precedes any session read. Per the
   spec's own closure clause this omission is registered vacuity,
   not an escape: any extension that gives an adopt-eligible node a
   pre-commit session read MUST populate the digest fields (and add
   a cell where a session-value change kills adoption) or it
   reopens exactly the E-only-laundering class the digest exists to
   refuse.

## G3 / G4 / G6-G9 — built and calibrated

G4 (duplicate admission) is REGISTERED COVERAGE, not new cells: cell
11's parent names C on EVERY row — the double-token fan-in premise —
so the honest suppression leg IS tcG1i_All and the `suppressionOff`
kill IS tcG1sup_P1 (round-1's walk shape: racing double admission,
P1-LEGALITY). G8a (record REPLACES over dead debris) is likewise G1
sweep coverage: the crash cells explore every mid-record-round
position and the REPLACES clear wipes debris on re-run.

New machinery for the remaining families (all announce-only):

- G3 (cell 25): scripted GRACEFUL STOP (interrupt 1) — the flagged
  consult execution stops after its consult announce, the scheduler
  checkpoints with the node pending at its cursor, the attempt ends
  unsealed with the store intact; `eGSwapPrev` rebinds the PREV
  artifact between attempts. nWorkers=1 (no straggler commits).
- G6 (cells 26 chain / 28 fan-in): GEXECBOUND count oracle —
  executions per node per sync <= cfg.execBound; minimal GREEN bound
  = checker-verified worst case; bound-minus-one RED probes are the
  redo existence exhibits (adequacy §10.1 count-oracle kills).
  Declared bounds: 2 everywhere (redo <= 1 per node).
- G7 (cell 27): loud deterministic failure (failNode/failSync) with
  a GENERATION-BLIND fingerprint (F11); env abandon ladder (give up
  after 2 identical fingerprints); GP4STUCK reds 3 in a row.
  tcG7_Ladder GREEN / tcG7_Stuck RED declared.
- G8b: `composeDead` on the overlay unit (skip clear + prev-copy).
  DECLARED FINDING (pre-run analysis): the mutant is
  CONTENT-INVISIBLE in this envelope — with 2 row ids and truthful
  TOTAL diffs, every diff from the debris's base overwrites/removes
  every debris id. [SUPERSEDED by G8B-CAL-1: content-invisibility
  confirmed, but the crash re-run leg is mechanism-visible to P1 —
  see calibration results. tcG8bMut_P1 is the kill; tcG8bMut_Ctl
  keeps the content oracles GREEN.]
- G8c (cell 29): keyOf maps two distinct derivations to one output
  key; the poison path (already store-side: void + adopt-refusal +
  SealExpect exclusion + P1 exemption) gets a POISONPROBE existence
  probe. GP5 is NOT asserted (its key = hash - 1 convention is what
  this cell breaks).
- G8d: marker flap-back (cell 11 + flapBack + interrupt 3 +
  between-attempt mutation): honest GREEN via R3-M1's REPLAY-verdict
  correction; `markerCleanupOff` -> P-MARK RED declared.
- G9: `stampCompression` — the S pass compares FLOOR-BUCKETED stamps
  (buckets of 2): stale-erring, never false-live. Safety verdicts
  must be identical to uncompressed legs (any change refutes
  admissibility). [Convergence rules and the growth-exhibit pair as
  first declared were wrong — see find G9-CAL-1 in the calibration
  results.]

Deferred to the bake-off phase: G6's v1 control scripts (delta ~ 0
calibration points; metric-only, no kill) and the G5d cross-variant
seal-artifact meta-analysis (harness-level, outside P; §10.6).

### Calibration results (10k schedules per cell; 28 cells, all
### matching declared verdicts after the finds below)

Kills, first pass: 7/8 red cells fired under uniform random search —
tcG7_Stuck [P4-STUCK], tcG8c_Poison [POISON-PROBE], and the
count-oracle probes tcG6{a,c}{E,S}_Redo + tcG9c_Redo [EXEC-BOUND].

**Find G8D-CAL (search depth, not model).** tcG8dMut_PMARK was 0/10k
under uniform search. The kill needs attempt 1's crash to land in the
[replay-unit commit, completion checkpoint) window — the LAST store
ops of the attempt — and `maybeCrash`'s per-op coin makes P(crash at
op k) = 2^-k: late crash positions decay geometrically, ~1e-4 before
attempt-2 requirements compound it. Same pathology as G1D-REACH;
same remedy: `--sch-feedbackpct=20` finds P-MARK reliably (0.02%
buggy schedules at 20k), pinned for the cell in sweep.sh. Trace
confirms the declared shape: the marker survives attempt 2's REPLACES
clear (mutant), and P-MARK fires on the foreign round mutating the
marked key's partition.

Honest legs, first pass: 13/17 GREEN; the four reds decomposed into
one harness gap and two genuine finds:

- Harness gap (not a find): cell 27 was missing from the env's
  SealExpect expectation script (key 1 unexpected on an honest
  sync-1 seal). One-line fix; tcG7_Ladder GREEN.
- **Find G8B-CAL-1 — the composeDead mutant is content-invisible
  but NOT mechanism-invisible.** The declared vacuity (pre-run
  analysis, logged below) only considered the no-crash leg. On the
  crash re-run the skipped clear leaves the DEAD attempt's copy
  round live under the composed diff, and P1's
  one-live-replacement-copy legality reds it — the §4b precondition
  has a kill after all. Disposition: tcG8bMut_P1 is the kill
  [P1-LEGALITY]; tcG8bMut_Ctl now asserts only the content-level
  oracles (GP2/GP3'/SealExpect/GP5) and stays GREEN as the
  registered content-invisibility evidence.
- **Find G9-CAL-1 — floor-bucketed stamp comparison is admissible
  ONLY WITH bucket-aligned heal minting and an ambiguity
  double-bump.** As first built (compressed comparison only, all
  mints +1), honest S histories redded PASS-BUDGET: every heal
  re-created odd generations one level down — the pass's owner bump
  left readers merging the writer's unchanged odd generation, and
  dispatch-refusal re-admissions re-minted children odd — so
  convergence needs O(demand-depth) iterations, not 3. The
  admissible rule set, now implemented and green: (i) every
  scheduler RE-mint (pass heal, retraction, resume, refusal
  re-admission) lands on an even generation; first-admission mints
  stay odd, so the mixed-parity stamp population and its redo cost
  remain modeled; (ii) a floor-stale entry always bumps the key's
  OWNER (the exact rule), and when the entry is parity-AMBIGUOUS
  (floor(s) = cur-1, cur odd, named node != owner) it ALSO bumps
  the NAMED node — the owner's re-read alone can never prove an
  odd live generation. Worst case converges in exactly the budget
  (detect/heal, raced-merge re-heal, verify). Exhibit reshaped: a
  crash script MASKS the redo growth (resume redo and heal-wave
  redo both peak at 2 per node), so the growth pair is the
  no-crash fan-in chassis — tcG9cBase_All (uncompressed)
  GREEN@bound1, tcG9c_All GREEN@bound2, tcG9c_Redo RED@bound1.

## Model decisions of record (already registered as spec change orders)

- GS-CO-001 — MINT-FENCE TOTALITY. First honest run of the leg-(iii)
  cell redded P-GEN: attempt 1 crashed before any checkpoint,
  attempt 2 cold-restarted and re-minted (P, 1), whose identity
  carried commits in both attempts. G-RULE-3's durable fence is
  total over all four minting paths: attempt-start root mint and
  first-admission mid-attempt mint (found here), resume bump (F4),
  mid-attempt bump (R3-F2). Encoded as forced checkpoints before
  first dispatch of any newly minted generation; the root-mint fence
  rides `resumeCkptOff`, the first-admission fence rides
  `midBumpFenceOff`.
- GS-CO-003 — CARRIER-DURABILITY ATOMICITY. First honest G2 run
  redded SealExpect (closure key missing): P's carrier admitted H
  (fence ckpt), crashed before G's fence — restore shows P
  COMPLETED with G's admission lost, and a completed parent never
  re-derives, so the demand starves. A carrier's derived effects
  (admissions, mints, edges) commit durably as ONE delta at the end
  of the demand loop; no checkpoint may separate a carrier's
  completion from its admissions.
- GS-CO-004 — RETRACTION CATCH-UP BOUND. Two coupled finds: (a)
  carrier-time session-read registration makes the R2-F1
  dying-reader race structurally unreachable (the scheduler can
  never see an in-flight reader), so reads register at READ time
  via a scheduler note; (b) an unbounded registration-side
  retraction rule livelocks the writerAdopt strand (the adopted
  writer never re-publishes; the reader re-runs forever; the
  frontier never drains; P6-E is structurally unevaluable and the
  kill cell goes GREEN-by-divergence). Registration-side retraction
  is a once-per-(reader, dead-wgen) CATCH-UP; all repeated
  retraction is re-publish-driven, per R2-M6(i) as written.
- GS-CO-002 — SYNC-SCOPED SEALEXPECT + P-ADOPT (find G1-CAL-1). The
  scripted single-epoch expectation redded an honest leg-(ii)
  schedule: C's diff round completed AND checkpointed in attempt 1,
  crash landed before seal, attempt 2 restored an all-completed
  frontier and sealed attempt-1 content (e2) while the expectation
  demanded attempt-2's live world (e3). That survival is licensed by
  G-RULE-2 (completed-across-crash) and the SYNC-scoped staleness
  contract, so SealExpect now accumulates the acceptable epoch set
  across attempt starts. Consequence: the `adoptOnFail` laundering
  (FAIL-consult adoption of e2) is inside the envelope — invisible
  to EVERY artifact-level oracle (SealExpect and P3′ both green on
  the mutant, kept as declared controls) — so the kill moved to the
  new P-ADOPT mechanism monitor: adoption requires a validated MATCH
  consult announced by the adopting (node, generation) BEFORE the
  adopt commits. The announce-order pin matters: the first build
  announced the justifying consult after the store's adopt announce
  and P-ADOPT redded three honest adopt cells; the justification
  precedes the act.

## Freeze sweep — 2026-08-30, PASSED

Full matrix, 66 cells at 10k schedules each (tools/sweep.sh, one
build: the G9-CAL-1 Sched + cell-27 expectation fix + G8b test
split): 66/66 match declared verdicts, zero mismatches. Every kill
fired on its designated monitor (P1-LEGALITY, P-GEN, P-ADOPT,
P-MARK, P4-STUCK, P5-UNDER/OVER, P6-G/E/S, SEAL-EXPECT, EXEC-BOUND,
REDO-PROBE, PURGE-PROBE, POISON-PROBE, DEAD-DISPATCH); every honest
leg is clean. Three cells carry the pinned `--sch-feedbackpct=20`
strategy (tcG1d_P6G, tcG2awWA_E, tcG8dMut_PMARK — deep-crash-window
reach, G1D-REACH/G8D-CAL). The graph model is CALIBRATED and FROZEN.

COVERAGE LIMIT of the frozen matrix (registered post-freeze; no cell
or expectation changed): eight of Monitors.p's 25 alarm strings fire
in no red cell — P1-CONTENT, P1-ATTEST-PUBLISH, P1-ATTEST-SEAL,
P1-ATTEST-EMPTY, P2-CONSULT, P2-STALENESS, P3'-COHERENCE,
PASS-BUDGET. They are the walker-inherited content/attestation/
staleness oracles restated over the graph store; the matrix's kills
deliberately target the graph's OWN mechanisms, and the walker leg
witnesses the same property statements with its own reds — but
against the walker's checking code. By this log's own doctrine those
eight clauses are asserted, not calibrated, in THIS model: a
green matrix says nothing about them. Inventory mirrored in
REPORT.md's standing limits so the 66-cell green is not over-read.

## Bake-off phase — declarations (GS-CO-005, registered BEFORE run)

The protocol, the restated decision rule, and the provenance repair
(the v2 §10.6/§10.7 text was never committed) are in the spec's §12
GS-CO-005 entry. Cells and declared verdicts:

| cell | config | property | declared |
|---|---|---|---|
| tcG6aE_Ctl | cell 26 chain, E, NO crash, execBound 1 | honest stack + EXEC-BOUND | GREEN (zero-crash redo floor is zero) |
| tcG6aS_Ctl | same, S (+GPASS) | honest stack + EXEC-BOUND | GREEN |
| tcG6cE_Ctl | cell 28 fan-in, E, NO crash, execBound 1 | honest stack + EXEC-BOUND | GREEN |
| tcG6cS_Ctl | same, S (+GPASS) | honest stack + EXEC-BOUND | GREEN |
| tcG6bE_Redo | G6b mutation chassis, E, execBound 1 | EXEC-BOUND | RED (mutation-chassis redo exists) |
| tcG6bS_Redo | same, S | EXEC-BOUND | RED |
| tcG5dE_W1 | G5a shrink chassis, E, target {0→2} | SEAL-WORLD | RED (world reachable) |
| tcG5dS_W1 | same, S | SEAL-WORLD | RED |
| tcG5dE_W2 | target {0→1, 1→1} | SEAL-WORLD | RED (completed-across-crash world) |
| tcG5dS_W2 | same, S | SEAL-WORLD | RED |
| tcG5dE_W3 | target {0→2, 1→1} | SEAL-WORLD | GREEN (sweep-failure world unreachable) |
| tcG5dS_W3 | same, S | SEAL-WORLD | GREEN |

Any variant-asymmetric outcome (a leg deviating from its declared
verdict under exactly one variant) is a divergence finding and
blocks Axis-3 citation until dispositioned (GS-CO-005(d)).

### Bake-off results — 2026-08-30, 12/12 match declarations

10k schedules per cell, uniform random except `tcG5dS_W2`, which
carries `--sch-feedbackpct=20` in the script (summary archived at
`PCheckerOutput/bakeoff/summary.txt`; reproduce with
`tools/bakeoff.sh`, which carries exactly these 12 cells — they are
deliberately NOT in `tools/sweep.sh`, so re-running the calibration
sweep cannot overwrite this phase's evidence and vice versa). Every
red records its FIRING MONITOR as an alarm tag, the same audit the
sweep applies: `[EXEC-BOUND]` on the two G6b probes, `[SEAL-WORLD]`
on the four reachable-world probes. The tag is what makes an
expected-RED cell auditable — counterexample presence alone matches
`expected=RED` even for a cell that redded on a deadlock instead of
its calibrated monitor — and in the bake-off it is ENFORCED, not just
recorded: each red cell declares its calibrated monitor in the script
and a red whose tag lacks it is a MISMATCH (sound here because every
bake-off red has exactly one pre-registered monitor; sweep cells can
legitimately red on more than one calibrated shape, so their tags
stay informational with CALIBRATION.md as the comparison surface).
All four v1 controls GREEN
(the zero-crash redo floor is bound 1 under BOTH variants — the
count oracle measures crash-caused redo, not variant overhead); both
G6b bound-1 probes RED on EXEC-BOUND (the mutation chassis's redo is
real and symmetric); all four reachable-world probes RED on
SEAL-WORLD and both W3 probes GREEN under both variants.

G5d WHICH-IS-RIGHT DETERMINATION: the reachable seal-world sets are
IDENTICAL across variants — {W1, W2} reachable, W3 unreachable —
and every reachable world lies inside the sync-scoped SealExpect
envelope (asserted by the honest G5a greens). No divergence finding;
Axis-3 citation is unblocked. Where the two variants differ is HOW
a world is reached (E purges C / S refuses C's dispatch on the W1
path), never WHICH worlds are sealable.

`tcG5dS_W2` STRATEGY PIN (2026-08-31): the cell's find is
seed-BIMODAL under uniform random — one full-10k seed explored 18
timelines and found nothing while five other seeds (three uniform,
two feedback) all found within ~500 schedules at a 0.23%
buggy-schedule rate, a miss that is ~e^-23 improbable if schedules
were independent draws. Some seeds evidently cannot reach the target
at all, so the cell carries `--sch-feedbackpct=20` in `bakeoff.sh` —
the same remedy the sweep gives its narrow cells (`tcG2awWA_E`,
`tcG1d_P6G`) and the same lesson BAKEOFF.md's methodology note 4
records: narrow-target kills must not rely on uniform random
schedules. The world's REACHABILITY is unaffected (found under both
strategies); only the gate's reliability needed the pin.

## Pending
- Nothing. The verdict document is `BAKEOFF.md` (assembled under
  GS-CO-005's registered decision rule).
