# Walker calibration — run log

Status: COMPLETE. CURRENT GATE: the 55-cell full-matrix sweep, 0
mismatches, every red on its calibrated alarm (`tools/sweep.sh`, 10k
schedules per cell; the run of record is
`PCheckerOutput/sweep/summary.txt`, ending
`SWEEP-DONE cells=55 mismatches=0`). Scenarios 1, 2 (including the
P6-C session-checkpoint-consistency tranche, decision 25 — the
CO-6b-009 root cause made executable), 3, 4, 5 (both triggers + crash
window + C1 probe), 6 (both flavors + the round-7 third-placement
cell + the MS-CO-001 o-iv mutant), 7, 8 (external principals,
decision 26), and the P4 progress cells built and calibrated. Spec
baseline: `formal/MODEL_SPEC.md` v11, FROZEN
(round-7 dispositions + de-scope edits applied; see the §11 v11
entry), plus change order MS-CO-001 (dispositions for the PARALLEL
round-7 review — `reviews/model-spec-round7-overlay-parallel.md` —
surfaced post-freeze; §3/§4/§5 registration edits, the per-verdict
deferral scoping, and the o-iv-removal kill cell). The v11 monitor
pins (F2 complete-rounds replacement counting, F3
attestation-over-empty-fold) and the two v11 cells (6-overlay-last,
3-atomic) are built and verified. Sweep history (superseded gates,
summaries archived under `traces/`): the 46-cell v11 freeze sweep
(`traces/freeze-sweep-v11-summary.txt`; the pre-v11 44-cell sweep was
also clean, isolating the P4 merge from the v11 monitor change), the
47-cell post-MS-CO-001 sweep
(`traces/msco001-sweep-summary.txt`), then +3 P6-C cells (50) and
+5 scenario-8 cells (the current 55).

Toolchain: P 3.1.0 (`p compile` / `p check -tc <cell> -s <schedules>`).
Counterexample traces are NOT committed (neither the human-readable
schedule nor the machine-replay `trace.json`): archived traces rot
silently as the model evolves, while every red REGENERATES on demand
— re-run the cell's `p check` line and a counterexample lands in
`PCheckerOutput/BugFinding/` at the stated find rate (first find is
seconds-to-minutes on every red below). Only the sweep summaries are
archived under `traces/`. GREEN rows state the explored budget. A
green run means nothing except at the stated budget — the reds are the
calibration currency.

NOTE on verdict reading: `p check` runs a small portfolio of search
strategies; the aggregate verdict is the "Checker found a bug." line.
A run can print a strategy block reporting 0 bugs AFTER another
strategy found one (observed on tc1c_P2 — red, found by the first
strategy at ~60 schedules, missed by the 100k random pass).

## Scenario 1 — phantom union (shipped toggles ON in every cell)

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc1a1b_P1 | stop-stranding, carrier publishes, 2 syncs | P1 | RED | RED: `P1-CONTENT` (carrier clear+copy composes with the fresh round; union sealed) | first find; 3000 explored |
| tc1a1b_P3 | same config | P3′ | RED | RED: `P3'-COHERENCE` (1b-i shape: carrier drains after the complete fresh round; content green, epoch incoherent) | first find |
| tc1a1b_P2 | 3 syncs, corollary-run scoping | P2 | RED | RED: `P2-STALENESS` (union replays warm in the verification sync; hops reach 2 — the unbounded branch) | first find |
| tc1bii_P1 | carrier validator-less | P1 | RED | RED: `P1-CONTENT` (the attestation-only 1b-ii edge is also live in this config; the checker surfaces the content shape first) | first find |
| tc1c_P1 | carrier-less hard crash | P1 | RED | RED: `P1-CONTENT` (copy debris survives restart-from-root; fresh round unions over it) | first find |
| tc1c_P2 | crash + verification sync | P2 | RED | RED: `P2-STALENESS` | first find |
| tc1c_P1_probe | tc1c's verification config (3 syncs), P1 asserted | P1 | RED | RED: `P1-CONTENT` — premise-liveness probe: the verify config still contains the sync-2 union, so tc1c_P2's red is fired by the staleness mechanism, not by the premise having drifted out of the config | first find |
| tcGreen_All | no interruption, no mutation, honest replay | P1+P2+P3′ | GREEN | GREEN | 10000 schedules |
| tc1c_P2_honest | crash config, P2 only, 2 syncs | P2 | GREEN | GREEN (the corrupted seal itself is staleness-legal; the alarm belongs to the verification sync) | 3000 schedules |

## Scenario 2 — session laundering (P6-A; sessions variant A, shipped)

One sync, root stack [H writer, G reader] (same op, one batch on the
2-cap schedules). H derives-and-writes the session key on EACH of its
two pages; G reads once and emits a row embedding the read value (a
read-miss emits nothing). Both reds are EXPECTED FINDINGS — no fix run
(variant B is the graph addendum's obligation):

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc2stop_P6A | graceful stop, mutate between attempts | P6-A | RED | RED: `P6-A` — H's d1 commits, G embeds d1 and pops; stop strands H mid-chain; H alone re-derives d2 on resume | first find (0.5% of 10k) |
| tc2crash_P6A | hard crash (at-least-once, both re-run) | P6-A | RED | RED: `P6-A` — the G-before-H interleaving re-embeds the durable stale d1 under H's re-derived d2; the complementary H-first schedule is green in the same config | first find (11% of 10k) |
| tc2green_P6A | no interruption, no mutation | P6-A | GREEN | GREEN | 10000 schedules |

### P6-C — session-checkpoint consistency (CO-6b-009 root cause)

The constraint P6-A never stated: post-crash session state must equal
the session state at the restored checkpoint, in BOTH directions —
no ZOMBIE (a dead attempt's beyond-checkpoint write observed by the
re-run: the cursor rolled back, the session did not) and no AMNESIA
(a checkpoint-committed value destroyed: its producing work will not
re-run, so deletion is unrecoverable). The axis is `cfg.sessVariant`,
the store's session semantics at the crash boundary: 0 = shipped
(durable at op commit), 1 = the rejected wholesale resume-clear,
2 = checkpoint-consistent sessions (state latched with each
checkpoint, restored at crash — the registered fix). The amnesia
cells run cell 21 (cell 2 with the ROOT ORDER REVERSED so the writer
pops first): under cell 2's LIFO order the reader pops before the
writer, so every checkpoint that still contains the reader predates
the writes and a committed-value-plus-re-run-read history is
structurally unreachable — verified green at 10k before the chassis
was added, which is a reachability fact, not evidence the rejected
fix is sound.

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc2crash_P6C | cell 2, hard crash, sessVariant 0 (shipped) | P6-C | RED | RED: `P6-C-ZOMBIE` — H's un-checkpointed d1 survives the crash; the re-run G reads it before H's re-derivation lands. The SHIPPED defect, now model-caught | first find (3.7% of explored) |
| tc2clear_P6C | cell 21, hard crash, sessVariant 1 (rejected resume-clear) | P6-C | RED | RED: `P6-C-AMNESIA` — H's batch completes, the loop-top checkpoint commits d1, the crash clears the namespace wholesale, G's re-run read misses committed data. The rejected fix, now model-killed | first find (20% of explored) |
| tc2consistent_P6C | cell 21, hard crash, sessVariant 2 (checkpoint-consistent) | P6-C | GREEN | GREEN — rollback to the checkpoint snapshot closes both directions; an un-checkpointed write legally vanishes (the re-run re-derives), a committed value survives | 10000 schedules |

## Scenario 3 — artifact swap + hit rebind

Upstream at e2 throughout (preMutate, then never moves); sync 1 seals
A = rows(e2) @ V2; the stop strands the carrier in sync 2 and MEnv
swaps the base to sibling B = rows(e1) @ V1 (equal compat, truthful
validators):

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc3a_P1 | shipped (`hitValidatorBinding` ON) — the residual hole | P1 | RED | RED: `P1-ATTEST-SEAL` (carrier-last shape: rows(B) sealed under entry V_A; the carrier-first/interleaved content shape is also live in the config) — requires the re-consult's lookup-hit V_B to OVERWRITE the hit map before the carrier's LIVE hit read (last-write-wins rebind) | first find (0.34% of 10k — narrow: needs C dispatched after P's re-consult transition) |
| tc3a_P2 | same config | P2 | GREEN | GREEN — attempt 1's validation match qualifies the scope as consulted; copied rows carry hops 1 (corrected expectation; spec v2 wrongly claimed a P2 red) | 10000 schedules |
| tc3b_P1 | pre-CO-6b-004 (`hitValidatorBinding` OFF), 1-page planning (cell 31) | P1 | RED | RED: `P1-ATTEST-SEAL` — no re-consult, hit stays V_A, NO binding check; carrier copies swapped B and publishes V_A | first find (100% of explored) |
| tc3bBindingOn_All | same premise, binding ON — the CO-6b-004 kill | P1+P2+P3′ | GREEN | GREEN — binding gate compares hit V_A to base V_B, fails LOUD-COLD (behavior, not assert); scope seals empty in premise schedules, no wrong data | 10000 schedules |
| tc3atomic_All | v11: same stop+swap premise under V-ATOMIC — the `annotationBinding` de-scope's subsumption witness | P1+P2+P3′ | GREEN | GREEN — no carrier and no annotation exist; either the unit committed (marker in the CURRENT artifact suppresses attempt 2; seal is the unit's coherent contents) or attempt 2 re-consults the actually-current swapped base (V1 fails validation vs e2 → fetch-fresh). The 3A rebind hole is structurally closed: no restored hit authorizes replay | 20000 schedules |

## Scenario 5 — warm-drift (both produce triggers + the crash window)

Upstream never moves (drift is config-side). Sync 1 seeds A = rows(e1)
@ V1 with compat record K1 (`baseConfig = 1`); sync 2 is the premise
sync: attempt 1 (warm, K1) consults, records hit {S: V1}, spawns
carrier C (cell-31 planning shape) and the stop strands C. The
scripted drift input (compat recompute / G6 withdrawal) applies to
attempt 2 ONLY in premise histories — see decision 13. Warm install
and both produce-block triggers run at attempt start
(`installProduceState`, MODEL_SPEC 3/4); the blocked flag is volatile
at checkpoint-cadence durability (§5):

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc5a_P1 | cell 51, compat drift K1→K2, `warmGate` OFF — the kill | P1 | RED | RED: `P1-CONFIG` — attempt 2 cold + B4-blocked, but C passes hit (restored) and binding (base unchanged V1) and copies K1-tagged rows into the K2 attempt; sealed rows carry config 1 under seal config 2 | first find (100% of explored) |
| tc5a_Gate_All | cell 51, shipped toggles (`warmGate` ON) | P1+P2+SealExpect | GREEN | GREEN — C fails LOUD-COLD at the warm gate (reason 2), no ops; seal blocked with partition[S] empty | 30000 schedules |
| tc5b_Dropout_All | cell 52, G6 withdrawn in attempt 2, stop script | P1+P2+SealExpect | GREEN | GREEN, and the green IS the required design finding (MODEL_SPEC 9.5b): trigger 2 blocks at install, C's replay-annotated page is SILENTLY IGNORED (B1 — no failure, no announce, no rows), the sync seals green with partition[S] EMPTY and the artifact blocked. P1/P2 are structurally blind (no rows, no round); the scripted `SealExpect` expectation (wantBlocked + wantScopeEmpty) is the dropout's only executable oracle | 30000 schedules |
| tc5b_CrashWindow | cell 52, interrupt 3 (stop attempt 1, crash attempt 2) | SealExpect | RED | RED: `SEAL-EXPECT sealed unblocked` — the crash-window finding: trigger 2's block lives only in attempt 2's volatile flag; the crash lands before any checkpoint carries it; attempt 3 (handling restored, withdrawal is attempt-2-exact) re-detects nothing, runs warm, and seals UNBLOCKED with replayed rows. Schedules where the crash lands after a flag-carrying checkpoint seal blocked and stay green | first find (100% of explored) |
| tc5c_C1Probe | cell 53: ONE action, replay annotation MID-CHAIN (page 0 consults, page 1 replays), plain stop | C1Probe (+P1, P2 clean) | RED | RED: `C1-PROBE` — the CO-6b-002 conformance answer, REACHABLE VIA THE STOP PATH: attempt 21 consults (hit V1 recorded at lookup), the stop strands the action between pages, the checkpoint holds the mid-chain cursor + hit map, and attempt 22's resumed replay page commits its copy with NO fresh consult (hit check passed on the RESTORED map). A finding to confirm against the real implementation via the chaos bridge (deliverable 6), not a model bug — P1/P2 stay green in the same cell (the replayed content is truthful) | first find |

## Scenario 6 — atomic-unit bake-off

Fetch-fresh flavor (shipped toggles ON):

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc6naive_P1 | V-NAIVE (marker a separate op after copy), crash script | P1 | RED | RED: `P1-CONTENT` — crash fires at the marker op's queue position; unmarked copy debris; resumed attempt re-consults, revalidation fails, fresh round unions over debris | first find |
| tc6atomic_All | V-ATOMIC (one `eReplayUnit`), same crash script | P1+P2+P3′ | GREEN | GREEN — every crash placement leaves nothing or the complete unit; the marker suppresses re-derivation | 10000 schedules |
| tc6atomicStop_All | V-ATOMIC, stop-stranding script (1a/1b premises) | P1+P2+P3′ | GREEN | GREEN — replay executes inline; the stranded-carrier premise is structurally unreachable | 10000 schedules |

Changed-with-diff (overlay) flavor, v10 addendum. 6-overlay runs with
`oncePerScope` AND `scopeLocks` OFF (the structural claim);
6-overlay-naive runs shipped toggles — its defect is the unit boundary,
which no toggle repairs:

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc6overlayNaive_P1 | unit misdrawn at the consult boundary ({clear, copy, marker, publish V2} at consult; overlay pages per-page), crash script | P1 | RED | RED: `P1-CONTENT (incomplete-round debris sealed)` — marker suppression seals base(e1)+partial-overlay under entry V2 | first find |
| tc6overlayNaive_P2 | same + verification sync | P2 | RED | RED: `P2-STALENESS` — V2 revalidates clean, the mosaic replays warm; stale-AHEAD is the non-self-healing direction | first find |
| tc6overlayLast_P1 | v11 (round-7 F2): the THIRD placement — no unit; clear+copy per-page at the replay boundary, marker+publish LAST as two trailing ops; crash script | P1 | RED | RED: `P1-CONTENT (incomplete-round debris sealed)` — the w2 window exactly: the trace shows the marker committing and the very next op (publish) dropped by the crash; attempt 2 suppresses on the marker; the scope seals CONTENT-COMPLETE but entry-less, diverging from the EMPTY fold (2.17% of schedules). NO `P1-LEGALITY` appears in any strategy's counterexample — w1's much wider crash window (cross-attempt double copy after the re-verdict's clear wipes the debris) runs green under the complete-rounds counting pin, where the pre-pin monitor would have alarmed on the converging B5-legal history | first find |
| tc6overlayMutO4_P1 | MS-CO-001 (parallel-review F6): the o-iv-REMOVAL mutant — stop script, `o4Mutant` removes the consult reset | P1 | RED | RED: `P1-CONTENT (sealed partition diverges from the round-log fold)` — the resume honors the restored mid-chain cursor with an EMPTY collect buffer, collects only the final overlay page, and commits a unit missing page 1's ops; the self-grounding fold says rows(e2), the partition disagrees (100% of explored). Kills the one load-bearing line the overlay flavor adds beyond V-ATOMIC; the non-mutant sibling stays green in the same config | first find |
| tc6overlay_All | V-OVERLAY-UNIT, crash script | P1+P2+P3′ | GREEN | GREEN | 10000 schedules |
| tc6overlayStop_All | V-OVERLAY-UNIT, stop script (mid-collect aborts; buffer loss; o-iv resume) | P1+P2+P3′ | GREEN | GREEN (re-verified post-MS-CO-001) | 10000 schedules |

## Scenario 7 — session elision under replay (P6-R; signoff addendum)

Pure two-sync scripts, no interruption machinery. Kinds W (producer,
scope 0) and R (reader, scope 1) in sequential phases (different ops —
the batch prefix never spans both). W's FRESH enumeration writes K as a
side effect; warm replay is the inline carrier-less path, so ELISION IS
STRUCTURAL. R stamps its fresh rows from its session read (0 = miss);
copied rows carry stamps unchanged. The counterfactual is announced by
MEnv as a computed ghost (= epoch of W's scope this sync). All reds are
deterministic (100% of schedules) — the premises are phase-ordered, not
interleaving-dependent:

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc7a_P6R | write elision: upstream unchanged, R policy always-fresh | P6-R | RED | RED: `P6-R` — W warm, session write elided; R reads MISS, stamps 0; counterfactual v1 | first find |
| tc7a_P1P2 | same config | P1+P2 | GREEN | GREEN — REQUIRED FINDING: the corruption is invisible to content/attestation/staleness checks | 10000 schedules |
| tc7b_P6R | stale-read replay: W's upstream moves between syncs, R warm | P6-R | RED | RED: `P6-R` — R's copied rows carry stamp v1 under counterfactual v2; no elided write anywhere (kills write-only bans) | first find |
| tc7c_All | both-warm control | P1+P2+P3′+P6-A+P6-R | GREEN | GREEN — carried stamps equal the counterfactual; P6-R does not overfit to "replay near sessions" | 10000 schedules |
| tc7aTaintW_P6R | fix run: `sessionTaintWrites` ON | P6-R | GREEN | GREEN — sync N taints W; sync N+1 consults W MISS, re-runs fresh, K present | 10000 schedules |
| tc7bTaintW_P6R | fix run: `sessionTaintWrites` ON, 7b premise | P6-R | RED | RED: `P6-R` — REQUIRED RESIDUAL: R's hazard is a READ; the write-only rule is half a fix | first find |
| tc7aTaintAll_P6R / tc7bTaintAll_P6R | fix run: `sessionTaintAll` ON | P6-R | GREEN | GREEN — replay forfeited exactly where sessions are used (honest price) | 10000 schedules each |

## Scenario 8 — external principals (P8; the deleteStaleExternalPrincipals contract)

One sync, one external-phase action (cell 8): page 0 LISTs the
source's current answer and commits the reconciliation op
(`eExtReconReq`); page 1 COPYs the answer's principals (`eExtCopy`).
The ext keyspace is separate from scope partitions (BatonID-annotated
rows beside connector rows). Committed copies are durable across
crashes — the debris premise — and a redispatched external phase
restarts from its root token. MEnv announces a truth ghost
(`eAnnExtTruth`) at sync start and after every between-attempt
mutation; the shrink drops principal 1 (e1 = {0,1} → e2 = {0}). P8
asserts two clauses: CURRENT — every round's listed answer equals the
source's answer at that moment (a resume must RE-LIST; the
`ResumeUsesCurrentExternalAnswer` chaos pin) — and SEAL — the sealed
ext keyspace equals the last-RUN round's answer exactly (STALE: a dead
attempt's copy survived reconciliation; MISSING: a listed principal
dropped). The seal clause deliberately compares against the last LIST,
not truth-at-seal: a completed-then-crash schedule seals attempt 1's
answer legitimately (sync-scoped freshness). The axes are `extRecon`
(TRUE = the shipped capable-engine path; FALSE = the warn-and-continue
degrade of a non-deleting engine) and `extStaleList` (the recency
mutant: attempts ≥ 2 consume the sync-start answer):

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tc8green_P8 | no interruption, no mutation | P8 | GREEN | GREEN — cold baseline | 10000 schedules |
| tc8crash_P8 | hard crash sync 1, shrink between attempts, capable engine | P8 | GREEN | GREEN — every crash placement heals: the resumed attempt re-lists the current answer and reconciliation deletes the dead attempt's stale copies before the fresh writes; completed-then-crash schedules seal attempt 1's answer without re-running the phase (deliberately green — sync-scoped freshness) | 10000 schedules |
| tc8stop_P8 | graceful stop + shrink, capable engine | P8 | GREEN | GREEN — restart-from-root re-lists; no mid-phase cursor can copy a fresh answer over a stale reconciliation | 10000 schedules |
| tc8reconOff_P8 | crash + shrink, `extRecon` OFF (non-deleting engine) | P8 | RED | RED: `P8-EXT-STALE` — the warn-and-continue degrade seals the dead attempt's principal 1 (the SQLite degradation pinned by `SQLiteExternalPrincipalResumeDegradesWithoutFailure`, now model-caught) | first find (20% of explored) |
| tc8staleList_P8 | crash + shrink, `extStaleList` ON (resume consumes the dead attempt's answer) | P8 | RED | RED: `P8-EXT-CURRENT` — the recency mutant the `ResumeUsesCurrentExternalAnswer` chaos pin forbids | first find (100% of explored) |

## P4 tranche — progress properties (stuck-resume, ladder, leaked lock)

Scenario-5 chassis (cell 51 premise: stop strands carrier C in sync 2,
compat drifts K1→K2 for the remainder of the sync). Attempt-level loud
failure is ON (`loudColdFailsAttempt`): a warm-gate/binding cold
verdict fails the ATTEMPT (checkpoint forced, `eAnnAttemptFailed`,
resume ladder) instead of completing the chain cold — the deviation
recorded in decision 10 is repaid here. The drift latch makes the
re-failure deterministic: every resume restores the same checkpoint
and meets the same drifted config:

| cell | config | property | expected | observed | budget |
|---|---|---|---|---|---|
| tcP4stuck_P4 | ladder OFF, 3 attempts | P4Stuck | RED | RED: `P4-STUCK` — attempts 2 and 3 fail at the same scope/cursor/reason from byte-identical restored checkpoint state (CO-6b-004 stuck-resume made executable) | first find |
| tcP4ladder_All | `abandonLadder` ON (k = 2), 2 syncs | P4Live+P1+P2 | GREEN | GREEN — after 2 identical failures the sync is abandoned (sealed=false, resume ladder ends), sync 3 starts COLD from root and seals; the liveness monitor confirms the eventual seal. P4Stuck is deliberately NOT asserted: k = 2 IS the detection event — the ladder's claim is the recovery, not the absence of re-failure | 20000 schedules |
| tcP4leak_P1 | `scopeLocks` ON, warm page fails once, `lockReleaseOnError` OFF | P1 (deadlock) | RED | RED: DEADLOCK — the failed page's retry re-requests the scope lock the dead first try never released; the retry parks forever on `eScopeLockGrant` (CO-6b-007 leaked-lock hang; surfaces as P's deadlock detector, not an assert) | first find |
| tcP4release_All | same premise, `lockReleaseOnError` ON | P1+P2 | GREEN | GREEN — the error path releases before retry; the retry acquires, replays, seals | 20000 schedules |

## Model decisions of record (change-order candidates for the spec)

1. **Torn-round exclusion is monitor-side, not config-side.** The spec
   (§7 boundary note) argued no §9 config can tear a round because each
   config's single stop is consumed by its premise. In the model the
   stop's placement is genuinely explored, so torn rounds ARE reachable
   (stop mid-fresh-round, resumed in attempt 2). The monitors track
   attempt ghosts per round and exclude torn scopes from P1-content and
   P3′ rather than trusting configs. Spec §7 should be amended by
   change order.
2. **P2 corollary-run scoping** is realized as a config flag
   (`verificationOnlyIfInterrupted`): the verification sync runs only in
   histories where the scripted interruption landed. Without it, honest
   double-replay chains reach hops 2 legally and the ≤1 bound
   false-alarms — the flag is the "corollary runs" language of §7 made
   operational.
3. **Crash injection is store-armed.** `eCrashArm` lets MStore fire the
   crash nondeterministically at any op boundary of the armed gen
   (resolution guaranteed at seal). Equivalent to the pinned
   queue-position semantics of §5, but every window is explored with
   useful probability — pure env-side racing missed the copy→marker
   window entirely at 3000 schedules.
4. **Op granularity** per §1: upsert pages are single atomic store ops
   ("partitions with atomic page commits"); clear and copy are two
   separate atomic steps; V-NAIVE's marker is a third; V-ATOMIC's unit
   and V-OVERLAY-UNIT's unit are single ops by design.
5. **Dead-machine parking**: ops from a crashed gen receive `eStoreDead`
   and the machines park in terminal states instead of blocking — model
   hygiene only (P would otherwise flag the pinned block-forever
   semantics as deadlocks).
6. **tc1bii surfaces the content shape first.** The validator-less
   config contains both the 1a content schedules and the 1b-ii
   attestation schedules; the checker reports the first violation per
   run. The attestation edge (`P1-ATTEST-SEAL`) remains an obligation to
   surface explicitly — either via a schedule-restricted sub-config or
   by inspecting further counterexamples.
7. **`scopeLocks` was inert in scenarios 1/6; load-bearing in 4.** The
   case-4 kills (check-then-mark TOCTOU) landed as scripted; the
   leaked-lock retry (CO-6b-007 hang) belongs to the P4 tranche.
8. **The hit map is read LIVE, not from the dispatch snapshot.** The
   carrier's hit check and binding compare round-trip to the scheduler
   at drain time (`eHitReadReq`), matching the shipped one-sync-level-
   map, lookup-time-recording, last-write-wins semantics. This is
   LOAD-BEARING for 3A: with dispatch-snapshot reads the rebind hole is
   structurally unreachable (the carrier can never observe the
   re-consult's V_B overwrite) and tc3a_P1 stays green. The replayed
   set was already live (lock grant / `eReplayedCheckReq`).
9. **Replacement rounds fold by the base they ACTUALLY copied.** The
   P1 fold uses the announce-side `vBase` (the copied base's manifest
   entry), not the carrier's believed validator, and a copy-skipped
   replacement folds as a NO-OP (round-4 F2). Equal in every scenario-
   1/4/6 cell (belief == base there); the distinction is exactly what
   scenario 3 tests — a swapped base folds as its own content and the
   belief mismatch surfaces as `P1-ATTEST-SEAL`.
10. **Loud cold is behavior, not an assert.** Binding-gate mismatch
   (and, when scenario 5 lands, warm-gate failure) makes the chain
   fail cold: no copy, no publish, `eAnnLoudCold` announced. Scenario-3
   schedules reach the gate legitimately in green histories (carrier
   drains before the re-consult), so an assert would report fake bugs.
   DEVIATION OF RECORD: the real system fails the ATTEMPT (resume
   ladder); the model completes the chain cold with no ops. Equivalent
   for P1/P2/P3′ (no wrong data either way); the attempt-failure form
   becomes load-bearing only in the P4/abandonLadder cells.
11. **P6-A is scoped to this-sync stamps.** Its assert covers rows with
   stamp >= 1 AND hops == 0: copied rows carry LAST sync's stamps
   (P6-R's domain — cross-sync), and the miss marker 0 is P6-R's 7a
   evidence. This realizes the spec's "P6-A is vacuously green on
   7a/7b/7c" without weakening the scenario-2 cells (whose reader
   emits hops-0 rows with stamps >= 1, or nothing on a miss).
12. **Produce-side taint is store-recorded, worker-attributed.** The
   session ops carry the acting kind's scope and the config's taint
   verdict (so MStore stays config-free); taint rotates with the
   artifact and a prev-tainted kind's consult MISSES. Checkpoint-
   cadence durability of taint marks is not modeled (op-commit) —
   accepted for scenario 7 (no interruption in its configs); revisit
   if a taint-crash cell is ever scripted.
13. **Scenario-5 drift inputs are premise-scoped.** MEnv applies the
   compat recompute / G6 withdrawal to attempt 2 only when the
   restored checkpoint holds a stranded (replay-annotated) carrier —
   the same spirit as `verificationOnlyIfInterrupted` (decision 2).
   Without this, a legal non-premise history (stop lands AFTER C
   drains; attempt 1's K1 rows are already sealed-bound) meets the
   drifted seal and P1's clause (c) false-alarms on the shipped
   design's own mitigated behavior (the artifact seals blocked). The
   premise witness (checkpoint introspection) is env-level test
   scripting, not modeled machinery.
14. **The seal carries the sealing attempt's blocked flag and compat
   config.** `eSealReq`/`eAnnSeal` gained `(blocked, config)`; P1's
   clause (c) compares every sealed row's ghost config tag to the seal
   config (NOT torn-scope-excluded: config drift is between-attempt by
   construction, so a mixed-config scope is exactly the alarm). The
   `SealExpect` monitor consumes the scripted `eAnnExpectSeal`
   expectation — MODEL_SPEC 9.5b's "scripted seal-state expectation"
   made executable. G6 silent-ignore (B1) is a worker-side early
   return BEFORE any gate: no ops, no announce, no marks.
15. **Warm install is computed only in the config-modeled cells.**
   `installProduceState` (produce read + gates G4/G6/G7 + triggers 1/2
   + compat record write) runs for cells 51/52; every other cell keeps
   the calibrated warm=true boot and op stream byte-identical. The
   full-sweep regression after the scenario-5 merge confirmed all 35
   prior verdicts unchanged.
16. **Attempt-level loud failure is opt-in per cell.** With
   `loudColdFailsAttempt` ON, a cold verdict at the warm/binding gate
   sends `eChainFailed`; the scheduler quiesces in-flight workers,
   forces a checkpoint, announces the failure (scope, cursor, reason,
   restored-state fingerprint), and ends the attempt failed. The
   scenario-3/5 gate cells keep the complete-cold-chain form (decision
   10) — their properties are seal-side and both forms are equivalent
   there; the P4 cells need the resume ladder itself.
17. **Stuck-detection and the abandon ladder are the same event.**
   `P4Stuck` fires on the SECOND identical failure (same restored
   checkpoint fingerprint, same failure point); the ladder abandons at
   k = 2 — i.e. exactly when detection fires. The ladder cell
   therefore asserts recovery (P4Live liveness: abandoned sync's
   successor seals) and NOT P4Stuck. `eAttemptEnded` carries `failed`
   so MEnv can count identical failures without store introspection.
18. **The leaked-lock hang surfaces as a deadlock, not an assert.**
   The failed warm page's retry (same worker, recursion in
   `replayPage`) re-requests its scope lock; with `lockReleaseOnError`
   OFF the grant never arrives and P's deadlock detector reports the
   parked worker — matching CO-6b-007's hang phenomenology (no wrong
   data, no progress).
19. **Replacement counting moved to round completion (v11, round-7
   F2 pin).** The P1 monitor counts a scope's committed copies when
   their round COMPLETES (same-attempt rounds only), not at copy
   commit. Verdict-equal in every pre-v11 cell (cells 4's rounds both
   complete, 6-naive's debris copy alarms via content either way);
   load-bearing exactly in 6-overlay-last's w1, where the
   cross-attempt at-least-once re-copy is B5-legal and must not
   alarm.
20. **Attestation over an empty fold is checked at seal (v11, round-7
   F3 pin).** New assert `P1-ATTEST-EMPTY`: a manifest entry for a
   non-torn scope with no fold epoch fires outright — covering the
   sealed-empty-partition case the content loop never visits. No
   pre-v11 cell reaches the shape (copy-skipped publishes always ride
   scopes another round grounded); in 6-overlay-naive the content
   assert fires first in observed traces, and the direction is red
   either way.
21. **o-iv is realized as worker-side cursor reset, equivalent to the
   spec's transition deferral (v11 deviation of record).** The spec
   pins option (1) of round-7 F1: unit-mode page transitions commit
   only at unit commit, so the checkpoint holds the consult token.
   The model keeps per-page transitions and instead RESETS a restored
   non-zero cursor to the consult page at dispatch — executable here
   because model cursors are page indices (the consult re-entry is
   always constructible), which is precisely what the real system's
   in-place opaque tokens lack (the review's F1 point). Equivalent
   for every property: no store op commits before unit commit in
   either mechanism, the buffer is volatile in both, and resume
   behavior is identical (restart at consult, marker suppression
   unchanged).
22. **The third placement commits shipped ops, no new store
   machinery.** VAR_OVERLAY_LAST reuses `eClearScope`/`eCopyScope`
   (replay boundary), per-page `eUpsertPage`/`eTombstonePage`
   (tombstones NOT lastOp), then `eMarkerPut` and `ePublishEntry`
   (lastOp) as two trailing queue positions — six crash windows, all
   explored by the armed crash.
23. **Two round-7 reviews ran; the model arbitrated their
   disagreement (MS-CO-001).** The parallel review
   (`reviews/model-spec-round7-overlay-parallel.md`) independently
   re-found the empty-fold major and two v11 minors (double-hit
   confirmation), judged the third-placement reduction SOUND where
   the primary called it false — and the built cell 6-overlay-last
   settled it for the primary (the w2 window is red in a non-union
   shape; w1 is the legality-counting case the parallel review
   itself flagged as a tripwire). Its genuinely new item is the
   o-iv-removal kill (decision 24); its registration/wording items
   are §3/§4/§5 spec edits with no model impact.
24. **The o-iv mutant is one flag on the realization line.**
   `o4Mutant` disables the worker's consult reset (decision 21's
   realization of the spec's transition deferral), which is exactly
   "resume honors the restored mid-chain cursor". The kill firing at
   100% of explored schedules in the stop config shows the reset is
   load-bearing, not vestigial — the strongest possible answer to
   "does the model actually depend on o-iv".
25. **Session durability variants are store-side, one field, crash-
   boundary-applied (P6-C).** `cfg.sessVariant` rides `eStoreReset`
   so MStore stays config-free in spirit (decision 12); variant
   semantics apply in `fireCrash` (variant 1 clears `sessionKV`,
   variant 2 restores the `sessCkpt` snapshot latched in
   `eCheckpointReq`). Applying at the crash commit is equivalent to
   acting at resume start: dead-gen ops arriving after the crash are
   dropped by the gen gate and can never observe the adjusted map.
   The P6-C monitor tracks write provenance from the announce stream
   (uncommitted → committed at `eAnnCheckpoint`; uncommitted →
   zombie at `eAnnCrash`, unless value-identical to the committed
   state, where survival is unobservable; a live rewrite reclaims
   the key) and judges reads via the new `eAnnSessionGet` announce.
   DISPOSITION LESSON recorded with the cells: scenario 2's original
   reds carried the zombie mechanism since calibration but were
   filed as a future-runtime obligation; the shipped-code defect
   (CO-6b-009) sat unrouted until re-found by hand. Expected-red
   findings against SHIPPED semantics must be routed as change
   orders on the shipped code, not only as obligations on the
   replacement design.
26. **External principals are their own keyspace, phase-shaped, with
   an env truth ghost (scenario 8 / P8).** The spec's announce
   vocabulary (§7) predates the external phase, so scenario 8 adds
   `eAnnExtTruth`/`eAnnExtRound`/`eAnnExtSeal` beside it rather than
   inside a scope partition: external rows carry no validator, no
   consult, no replay — the only mechanism is LIST (current answer),
   RECONCILE (delete what the answer no longer contains), COPY. The
   truth ghost is announced by MEnv at sync start and after every
   between-attempt mutation, so P8's CURRENT clause always compares a
   round's list against the answer live when the attempt listed —
   which is what makes the completed-then-crash carry legitimately
   green (the seal clause anchors on the last-RUN list, sync-scoped
   freshness). The reconciliation op is ONE atomic store pass
   (`eExtReconReq`); partial-delete debris reduces to the same
   stale-survivor class the crash-between-ops windows already cover,
   so no finer granularity is modeled. `extRecon` FALSE is a store
   CAPABILITY (the non-deleting engine), not a worker toggle —
   the round still announces, because the LIST truly happened; only
   reconciliation didn't.

## Known latent harness race (no bearing on verdicts)

The env sends `eCrashArm` AFTER `new MSyncAttempt`: a schedule that
starves the env for the entire attempt lets the seal enter the
store's queue ahead of the arm, the at-seal resolution point sees
`armed == false`, and the late arm never resolves — the env
deadlocks on `eCrashAck`. Unreachable in practice under this
project's random-strategy sweeps (needs hundreds of consecutive
starvation steps); found by feedback-PCT on the graph model's G1d
cell, which inherited the same env shape. The graph model fixes it
with a synchronous arm handshake (`eCrashArmed`) before attempt
creation — apply here if this project ever runs priority-based
strategies. Harness-level: no walker verdict depends on schedules
in that regime.

## Pending

- Nothing for deliverables 2–3. The 47-cell post-MS-CO-001 sweep ran
  clean (0 mismatches, every red on its calibrated alarm; summary
  archived at `traces/msco001-sweep-summary.txt`). Deliverable 4's
  spec is FROZEN: `formal/GRAPH_MODEL_SPEC.md` v4 after three review
  rounds (round 1: REJECT, 11 majors, dispositioned in v2; round 2,
  targeted on the adoption repair: core verified sound, REJECT on 6
  seam majors, dispositioned in v3; round 3, targeted on the v3
  repairs: all six round-2 repairs verified sound, REJECT on 2
  fix-without-re-review majors — session-publish body-op pin,
  mid-bump fence — dispositioned in v4, no fourth round required —
  see the spec's §12 log and `formal/reviews/`). The P project in
  `formal/graph/` is BUILT against the frozen v4 spec and its G1
  family is calibrated (10/10 sweep; two calibration-driven change
  orders GS-CO-001/GS-CO-002 — see `formal/graph/CALIBRATION.md`).
  The walker-side
  V-ATOMIC / V-OVERLAY-UNIT pilots (6-atomic, 6-overlay, 3-atomic
  re-runs) are its designed hand-off points and their verdicts are
  adopted there as settled.
