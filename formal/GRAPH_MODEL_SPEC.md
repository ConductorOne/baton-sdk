# Model spec — demand-graph runtime model (P), deliverable 4

Status: v4, FROZEN. Review history: round 1
(`formal/reviews/graph-spec-round1.md`) REJECTed v1 with 11 majors,
all dispositioned in v2. Round 2, targeted on the adoption repair
(`formal/reviews/graph-spec-round2-adoption.md`), verified the
adoption core sound and REJECTed on 6 seam majors, all dispositioned
in v3. Round 3, targeted on the v3 repairs
(`formal/reviews/graph-spec-round3-repairs.md`), verified ALL six
round-2 repairs soundly applied and REJECTed with 2 majors + 4 minors
+ 3 notes — every finding fix-without-re-review, no re-review
required ("the fixed spec needs a disposition registration check, not
a fourth adversarial round"). v4 applies the round-3 dispositions
(§12) and freezes: subsequent changes are GS-CO-NNN change orders,
never silent edits. Companion and baseline: `formal/MODEL_SPEC.md`
(v11 FROZEN + MS-CO-001); `formal/GLOSSARY.md`;
`docs/tasks/sync-formal-model-brief.md` (charter, deliverable 4);
`docs/tasks/demand-graph-sync-brief.md`.

Purpose, stated narrowly: (a) re-check the walker's calibration bug
premises against the graph runtime's candidate semantics, (b) check
the NEW property obligations the graph introduces (P5 sweep soundness,
the laundering oracle and its per-variant mechanisms), and (c)
ARBITRATE THE LINEAGE BAKE-OFF — variant E (eager edges) versus
variant S (observable-causal stamps) — by checker output on property
satisfaction, mechanism count (frozen tally, §7.5), and redo work
under the divergence scripts (§9 G6). The written recommendation is
the artifact the demand-graph RFC cites, assembled under §10.7's
frozen decision rule. Refutation of a candidate invariant is a
success.

## 1. What is modeled, what is abstracted

Modeled (the graph runtime's candidate scheduling semantics):

- A frontier scheduler: nodes as re-issuable requests, executions as
  (node, generation), revisit suppression by derivation hash (with
  the death semantics of G-RULE-2, round-2 R2-F5), demand derivation
  from emissions, frontier checkpoint, resume with generation bumps,
  seal-time sweep to the final demand closure, fresh-artifact
  supersession with a total interaction matrix incl. marker lifecycle
  (§4b).
- BOTH lineage variants as first-class scheduler modes sharing every
  other mechanism: E = spawn lineage pending-purge (∀-predicate, §3)
  + refcounted support DAG + session reads as tracked edges (requires
  session variant B); S = causal stamps merged on read, validated at
  the declared observation points (§3), nothing eagerly retracted.
- The session store as a dependency channel, variants A and B.
- Crash/resume at every boundary (armed injection), graceful stop,
  the forced resume checkpoint (§5), MID-ATTEMPT generation death
  under the quiesce-before-bump rule (§3, round-2 R2-F1), upstream
  mutation between attempts and between syncs, bounded workers (2;
  the G1d cell scripts 3 so the retraction-forced re-run dispatches
  AT the bump — with 2 the dying-reader race needs two consecutive
  starvation phases and random search cannot reach the declared
  alarm; calibration scripting note, logged).
- Source-cache consult/replay/record inside node executions using
  unit-mode materialization (MODEL_SPEC §9.6, settled hand-off),
  adapted by §4a's premise-validated adoption with the round-2
  eligibility pins (MATCH-only, writer-ineligible).

Abstracted, beyond MODEL_SPEC §1's list:

- Budgets and magnitudes (attempt-level failure and the ladder ARE in
  scope, §3/§8; per-node work budgets are not).
- Grant expansion / the aggregate node: de-scoped from this
  deliverable (v2; deliverable 5 owns sealed cuts).
- The sessions × replay product (walker scenario-7 class): the
  CROSS-SYNC product stays excluded with argument (v2, §12 F17
  entry); cross-sync stamp travel pinned in §4c; the walker cells
  own taint. The WITHIN-SYNC product is now governed by the §3
  SESSION-PUBLISH BODY-OP PIN (round-3 R3-F1): in this model,
  session publishes are node-body store ops executed by every
  non-adopted execution regardless of verdict class — a declared
  within-sync deviation from the walker's elision vocabulary,
  load-bearing for the writer-ineligibility convergence argument
  and for the `writerAdopt` kill's fireability (under the elision
  reading a replay-verdict re-derivation would strand a dead
  publish on an honest flap-back history and the kill could never
  fire). If the runtime design adopts elision-style replay for
  publish-bearing nodes, that is a change order with its own cells.
- SESSIONS × DEMAND SHRINK (round-3 R3-M4): a writer legitimately
  de-demanded by an epoch shrink never re-publishes, and a
  still-demanded reader of its sync-scoped value would carry an
  uncleanable dead component under S. No scripted cell composes
  sessions with a shrink; the shape is EXCLUDED from the envelope
  and stated in §8's inductive bet, and §4a's convergence claim is
  scoped to the scripted envelope accordingly.
- PUBLISH-DERIVED DEMAND (round-2 R2-M6(iii) + R2-F2 facet): demand
  derivation in this model is a pure function of ROW CONTENT only.
  Session publishes are dependency-channel machinery (P6-G's
  jurisdiction), never demand sources. This is a declared restriction
  of G-RULE-1's vocabulary: a runtime capability to demand work from
  a session publish is NOT modeled, and if the design adopts one it
  needs its own cells (change order).
- SESSION-TRANSITIVE RETRACTION CHAINS (reader-writer nodes; round-2
  R2-M6(ii)): excluded from the envelope and stated in the inductive
  bet (§8) — the retraction rule is keying-uniform, so chains add
  length, not mechanism; a reader-writer config is deferred to the
  RFC stage if the recommendation's session story needs it.
- The walker itself (mapping in §5; divergences are honest deltas).

## 2. Ground rules the encoding must obey

All of MODEL_SPEC §2 carries over verbatim (arrival order a genuine
choice point; crash wipes exactly §5's volatile rows; announce-only
monitors; truthful validators; non-lying connectors; expected
verdicts declared before first run).

Graph-specific rules:

- G-RULE-1 (structure rides emissions): demand derivation is a pure
  function of announced ROW CONTENT (the child-marker row; §1's
  publish-derived-demand exclusion). TIMING PIN: derivation is
  per-announce, atomic with that announce's completion-bookkeeping
  effects. DERIVED-ANNOUNCE CARRIER PIN (round-2 R2-M2): scheduler
  events with monitor significance that arise from processing an
  announce (mid-attempt `eAnnGenBump`, retraction re-admissions,
  observation-forced re-runs, forced-redo counts) are emitted as
  DERIVED ANNOUNCES within the atomic processing of their carrier
  announce, so monitors observe them in commit order; resume-time
  bumps ride the forced resume checkpoint's commit (unchanged).
- G-RULE-2 (derivation hash is the ONLY suppression key, WITH death
  semantics — round-2 R2-F5): the scheduler MUST suppress an
  admission iff the same derivation hash's node is currently PENDING
  or COMPLETED this sync ("may" is deleted — the checker cannot
  legally choose starvation). PURGE AND REFUSAL-DROP REMOVE the
  node's derivation hash from the admitted-derivation set, making a
  live re-derivation re-admissible. Output keys never suppress; the
  distinct-derivation same-key shape is §4b's poison row. RESUME
  RULE: completed iff admitted ∧ ¬pending, evaluated AFTER any
  purge/refusal removals of the resume.
- G-RULE-3 (generations are per-node monotone counters, durably
  fenced): execution (n, g) exists only after every (n, g' < g) is
  dead; an output's producing generation is stamped at emission and
  never reassigned (`eAdopt` is a recorded transfer). Identity
  uniqueness is monitored (P-GEN, §7; rule per R2-N3: no two
  attempts contain store-commit announces attributed to the same
  (n, g); adoption re-announces attribute to the ADOPTING execution).
  (GS-CO-001) The durable fence is TOTAL over every minting path: no
  generation dispatches before the table delta that minted it is
  durably committed — (a) the attempt-start root mint (rides the
  forced-resume-checkpoint discipline; a crash before any checkpoint
  otherwise cold-restarts and re-mints attempt 1's identities), (b)
  the first-admission mid-attempt mint of a newly demanded node
  (rides the mid-bump fence; a crash after the node's first store
  commit but before any checkpoint otherwise re-mints it), (c) the
  resume bump (F4), (d) the mid-attempt bump (R3-F2). Calibration
  found (a) and (b): P-GEN reds an HONEST single-crash history
  without them.
- G-RULE-4 (the frontier checkpoint is total): pending nodes (id,
  derivation hash, output key, generation, round-boundary cursor),
  the admitted-derivation set, generation-qualified admitted-by
  edges, closed-cut facts, the session index per variant. Variant
  E's support counts are derived; the resume rebuild target is the
  CHECKPOINT-CONSISTENT value; the rebuild-agreement monitor checks
  that value.

## 3. Machines

Reused from MODEL_SPEC §3 (re-declared in `formal/graph/`): MUpstream,
MStore, MCrashInjector, MEnv. MStore's registered op vocabulary:
`eCheckpoint`, `eLookup`, `eGateRead`, `eClearScope`, `eUpsertPage`
(with composition intent REPLACES/OVERLAY), `eTombstones`,
`ePublishEntry`, `eSessionGet/Set` + variant-B primitive ops, `eSeal`,
`eReplayUnit(key)` / `eOverlayUnit(key)`, marker ops
(`eMarkerPut(key, gen, premise, publishBearing)` / marker read — the
marker records whether its round performed session publishes, §4a),
`eAdopt(key, fromGen, toGen)` (PRECONDITIONS, store-side: fromGen is
DEAD (round-2 R2-N1; the one mutant-reachable live-fromGen adoption —
`suppressionOff`'s sequential schedule — is a declared deviation
whose legality alarm still derives) AND the key is NOT POISONED
(round-3 R3-M2: the poison-voids-marker rule is enforced at the
`eAdopt` commit, not only at the worker-side marker check, closing
the check-then-act window against a concurrently landing poison)). CLEAR-PLACEMENT PIN:
`eClearScope` commits only as the first store op of a REPLACES-intent
round; it also DELETES the key's marker (round-2 R2-F4 — marker
lifecycle rides the clear). MARKER SCOPING PIN (round-2 R2-M3):
markers are PER-SYNC scheduling state — the §4a marker check reads
the CURRENT sync's store only (the walker 3-atomic precedent made
explicit), and seal DROPS marker rows from the sealed artifact
(markers never travel cross-sync; consult provenance cross-sync is
the manifest, as everywhere).

### MGraphScheduler (one per attempt)

Owns: frontier, admitted-derivation set, admitted-by edges,
generation table, demand derivation, lineage state per variant,
dispatch to 2 workers.

Dispatch loop: pick a frontier node, issue (n, g), process announces
per-announce (G-RULE-1), checkpoint (placement a choice point), seal
when the frontier drains.

MID-ATTEMPT DEATH — QUIESCE-BEFORE-BUMP (round-2 R2-F1, the walker
decision-16 precedent): a mid-attempt generation bump (retraction- or
observation-forced) commits only when the dying generation's
execution is NOT in flight; if it is, the bump DEFERS until that
execution's completion announce and is processed atomically with it
(derived announce). Consequence, pinned: a deferred bump orders the
forced re-run AFTER the dead execution's late commits, so the
re-derivation is the last writer and no dead unit can wipe live rows.
The dead-in-flight interleave is a probe cell (G1d, expected
unreachable-after-pin; `quiesceOff` kills it). Non-interference
(round-3 R3-N2, recorded): a deferred bump cannot starve the pass
budget — deferral requires an in-flight dying execution, hence a
non-drained frontier, and the pass only scans a drained frontier, so
every deferral resolves strictly before the pass's first scan; two
workers' deferrals cannot deadlock — a deferral waits on an
execution's completion and no completion ever waits on a bump, so
the wait graph is one-directional.

MID-BUMP FENCE (round-3 R3-F2 — the F4 durability discipline
extended to mid-attempt minting): a mid-attempt bump's
generation-table delta is durable BEFORE the bumped generation
dispatches — the bump forces an `eCheckpoint` commit (the checkpoint
carries the generation table, G-RULE-4) between the bump's carrier
announce and the new generation's first dispatch. Without it, a
crash landing after the bumped generation's first durable commit
with the elective checkpoint skipped would re-mint the same id from
the restored table (round-1 F4's hazard through the retraction
path). Probe cell G1e; `midBumpFenceOff` kills it (P-GEN RED).

RETRACTION QUEUE (E; semantics pinned per R2-F1's disposition):
processing a re-publish announce ENQUEUES (as derived effects) a
retraction entry per reader execution of the now-dead value (keyed
per MSessionStore's pin); each entry re-admits the reader node
(bump + re-admit, quiesce-deferred as above) and is removed when the
re-admitted execution completes. The pre-seal condition for E is an
empty retraction queue.

OBSERVATION POINTS (S): (i) DEMAND-DERIVATION refusal — a pending
node whose admission stamp contains a dead generation is re-validated
at dispatch: run under a live re-derivation if one has re-derived the
hash, else dropped from the frontier (and its hash removed,
G-RULE-2). (ii) SESSION-READ (round-2 R2-M1, registered): a read
returning a dead-stamped value proceeds (read-through) and emits a
derived dead-read announce that feeds the forced-redo count; no
scheduling effect is needed — the dead value's writer is pending by
construction (its death was a bump that re-admitted it; invariant
stated here, checkable). (iii) PRE-SEAL PASS: scan sealed-bound
outputs; any dead-generation stamp forces the producing node's re-run
(bump + re-admit, derived announces); iterate up to the §8
PASS-ITERATION BUDGET (round-2 R2-M7 — the bound is an explicit
budget row, not "finite by construction"; convergence within budget
is a CHECKED expectation of every honest cell, via the no-dead-stamps
seal form of P6-S, and a budget-exhausted seal is announce-visible).
ITERATION BOUNDARY (round-3 R3-M3): one iteration = one scan over a
DRAINED frontier — a forced re-admission un-drains it, and the next
scan begins only after it re-drains; the budget counts scans, never
mid-flight re-observations.

SEAL SEQUENCE: frontier drained → pre-seal pass (S) / retraction
queue empty (E) → SWEEP → `eSeal`.

Resume: restore checkpoint; bump every pending node (prior generation
dead); commit the FORCED RESUME CHECKPOINT before any dispatch (§5);
then dispatch. Variant E purges pending nodes under the ∀-PREDICATE
(round-2 R2-F5: purge only when EVERY admitted-by edge names a dead
generation — the refcount-consistent reading), removing purged
hashes from the admitted-derivation set. Variant S refuses at
dispatch time per observation point (i).

ATTEMPT FAILURE (walker parity): a loud node failure fails the
attempt; the scheduler checkpoints, announces the failure with its
GENERATION-BLIND fingerprint (failure point, reason, restored state
modulo the generation table), and MEnv resumes or abandons per the
ladder.

### MNodeExec (worker-side execution body)

One execution = marker check (§4a) → consult → verdict (ADOPT is a
REGISTERED VERDICT CLASS in the announce vocabulary, round-2 R2-F3 —
alongside replay / changed-with-diff / fetch-fresh) → emissions.
Unit-mode materialization for replay and diff rounds; record rounds
for fetch-fresh (no marker — G8a pin). SESSION-PUBLISH BODY-OP PIN
(round-3 R3-F1): a node's session publishes/contributes are BODY
STORE OPS executed by every NON-ADOPTED execution regardless of
verdict class — a replay-verdict re-derivation re-publishes; only
adoption skips session ops, which is exactly what the
writer-ineligibility bit exists to prevent. This is the reading
round 2's accepted walks used, registered; it is a within-sync
deviation from the walker's elision vocabulary (§1's boundary
sentence), and it is what makes the writer flap-back history
converge (the MATCH-verdict re-derivation re-publishes under its
live generation, the writer-stamp delta re-derives every reader). Under variant S the
execution's stamp initializes {n: g} and merges every session value's
stamp it reads; ADOPTING executions' premise re-reads REGISTER
normally (round-2 R2-M5: tracked read edges under E+B, stamp merge
under S, attributed to the adopting execution).

### MSessionStore

Variant A: free-form KV. Variant B: scratch / publish / contribute;
publish-retraction keyed by (session key, value identity, writer
generation); a re-publish retracts every reader execution of a dead
value, including re-runs that read a stale value before the
re-publish landed (witnessed by G2's G-pending leg, round-2
R2-M6(i)).

## 4. Node execution and store interaction

### 4a. Premise-validated adoption (round-1 F1 repair + round-2 eligibility pins; round-3 review target)

- The marker is a memoization entry: `eMarkerPut` commits (key,
  producing generation, premise digest, publishBearing bit) inside
  the unit. The premise digest is the canonical hash of the consult
  result (previous-artifact entry + revalidation OUTCOME AND MATCHED
  VALIDATOR) and the identity+writer-stamp of every session value
  read before unit commit. DIGEST CLOSURE (round-2 R2-N2): the
  digest is total over the MODEL'S verdict inputs — consult result
  and session reads; config/compat and warm/cold state are constants
  in every cell of this spec, so their omission is declared vacuity,
  not an escape (a drift-modeling extension must extend the digest
  by change order).
- A re-execution finding a marker for its output key recomputes the
  premises (re-consult; session re-reads). ADOPTION ELIGIBILITY
  (round-2 pins, both load-bearing):
  - MATCH-ONLY (R2-F3): adoption requires the re-consult's
    revalidation to MATCH. A FAILED revalidation re-derives
    regardless of the stored digest — FAIL means upstream moved, the
    prior FAIL-verdict's fetched content is not re-attested by
    anything current, and a FAIL-vs-FAIL digest equality is an
    outcome-bit coincidence (the e1→e2→e3 escape, R2-F3's history;
    probe cell G1c). With this pin the §7 freshness claim is TRUE:
    every adoption's qualifying consult is the adopting MATCH
    itself, this attempt.
  - WRITER-INELIGIBILITY (R2-F2, the round-2 re-review target): a
    marker whose publishBearing bit is set NEVER adopts — a dead
    writer re-derives always. Rationale, pinned: adoption re-grounds
    ROWS ONLY; a dead generation's session publishes cannot be
    re-grounded by any rows-only mechanism, and a stranded dead
    publish defeats the observation pass's progress (readers re-adopt
    through unchanged premises forever — R2-F2's loop). Under this
    pin the dead-publish state is TRANSIENT by construction: the
    writer's forced re-derivation re-publishes under its live
    generation (same value or not — true for EVERY verdict class
    including replay, per the §3 body-op pin, round-3 R3-F1),
    retraction (E+B) or the stamp
    delta (S: writer stamp g_dead → g_live differs → reader digest
    differs → re-derive) clears every reader, and the pre-seal pass
    CONVERGES within the iteration budget on every honest history OF
    THE SCRIPTED ENVELOPE (round-3 R3-M4's scoping: the sessions ×
    demand-shrink shape, where a de-demanded writer never
    re-publishes, is excluded by declaration — §1/§8) —
    the convergence argument the round-2 charge question 5 found
    missing, verified mechanically by round 3 (every honest walk
    converges in ≤ 2 of the budgeted 3 iterations; the at-least-once
    cost claim verified honest, R3-N1: count-legal,
    suppression-safe, verdict-neutral).
    Cost: writers never amortize across death — recorded as
    forced-redo count in the bake-off, safety-neutral.
  - Digest EQUAL (and eligible) → ADOPT: one atomic `eAdopt` —
    stamps rewritten to the live generation (S), fold/count transfer
    (§4d), marker generation updated, stored rows re-announced under
    the adopting execution's attribution. Rows only; adoption-
    eligible rounds have no session writes by the eligibility bit,
    so rows-only re-announce is TOTAL over what the round emitted.
  - Digest DIFFERENT or ineligible → RE-DERIVE: the execution
    proceeds to its verdict as if unmarked. Its superseding round
    clears per §4b — a UNIT round's `eMarkerPut` constituent
    overwrites the marker; a RECORD round's `eClearScope` deletes it
    (round-2 R2-F4's text correction: fetch-fresh re-derivations are
    record rounds and carry no `eMarkerPut`).
- Walker coherence: for MATCH premises with no session inputs,
  adopt-on-equal is observationally the walker's clause (iii),
  strictly fresher (the adopting MATCH qualifies under the F8 pin) —
  round 2 verified this degeneration argument correct for the MATCH
  shape; the FAIL shape is now excluded by eligibility rather than
  overclaimed.

### 4b. Supersession and marker lifecycle — the total matrix

Rows and markers, total over {existing rows dead vs live} ×
{incoming unit vs record} × {REPLACES vs OVERLAY intent}, with the
MARKER COLUMN (round-2 R2-F4):

| existing | incoming | rows | marker |
|---|---|---|---|
| dead rows | unit (replay/overlay) | clear constituent removes them; fold/count contribution removed (§4d) | `eMarkerPut` constituent overwrites (new gen + digest + bit) |
| dead rows | record REPLACES | `eClearScope` removes them; contribution removed | `eClearScope` DELETES the marker (§3 pin) |
| dead rows | record OVERLAY-intent | ILLEGAL under the trust model (base-liveness precondition): overlay composes only over a live base; scripted connector policies never produce it and — for record-over-record keys, which carry NO marker — the precondition rests entirely on scripted policy + the trust model (round-2 R2-M4's honest wording; the `overlayComposeDead` MUTANT is the load-bearing check: expected P1-CONTENT red on epoch divergence + P6-S dead-stamp red under S) | n/a (no marker on record keys) |
| live rows (same derivation) | any | round continuation / at-least-once redo | unit rounds refresh it; record rounds have none |
| live rows (distinct derivation hash, same key) | any | CONNECTOR-CONTRACT VIOLATION: store poisons the scope on the second derivation's first commit; P1 legality EXEMPTS poisoned scopes (the poison is the alarm); seal excludes the scope (SealExpect) | POISON VOIDS the key's marker (round-2 R2-M8): no adoption of poisoned content is ever legal; post-poison rounds for the key commit legally but the scope stays seal-excluded |

MARKER INVARIANT (round-2 R2-F4, monitored as P-MARK, §7): a marker
present for a key ⟹ the key's current partition equals the marked
round's committed outputs. Every matrix row above preserves it; the
flap-back history that violated it in v2 (record re-derivation
stranding a stale marker, later premise flap-back adopting content
the marker no longer described) is unreachable after the
clear-deletes-marker pin — probe cell G8d, `markerCleanupOff` kill.

### 4c. Cross-sync stamps

RESTAMP-ON-REPLAY: REPLAYED rows (cross-sync copies) carry the
replaying execution's stamp; the source artifact's own seal
discharged its internal provenance. (v2's "adopted-across-sync"
wording deleted — adoption is WITHIN-SYNC only; markers are per-sync,
§3.) Cross-sync session-stamp travel remains walker P6-R
jurisdiction.

### 4d. Fold and count grounding over generations

Unchanged from v2 (round-2 verified the back-port coherent and the
removal clause correctly DEATH-GATED — a live-rows removal reading
would dissolve walker cell 4's alarm; the gate is load-bearing,
stated here per the round-2 caveat): a generation's complete round
stays in fold and count until ADOPTED (contribution transfers) or
SUPERSEDED (§4b removal). The v2 incoherence (transferring a removed
contribution) is unreachable once §4b's marker column exists.

## 5. Durability, crash, and resume semantics

Unchanged from v2 except: the marker row now reads "(key, generation,
premise digest, publishBearing)" and is per-sync (dropped at seal,
§3); the forced-resume-checkpoint row is unchanged (round-2 verified
F4 applied correctly and P-GEN checkable per R2-N3's recorded rule);
mid-attempt bumps' SCHEDULING EFFECT is volatile and its loss is
self-healing (the triggering re-publish or observation re-fires on
the resumed attempt — at-least-once), with their announces carried
per G-RULE-1's derived-announce pin — but the bump's
GENERATION-TABLE DELTA is NOT self-healing against id reuse and is
durably fenced by the §3 MID-BUMP FENCE before the bumped
generation's first dispatch (round-3 R3-F2: "self-healing" is true
for the loss of the bump's scheduling effect, never for the minted
id).

| state | machine | durability |
|---|---|---|
| partitions, manifest, poison, session KV, seal flag | MStore | durable at op commit |
| unit marker (key, gen, digest, publishBearing) | MStore | durable at op commit; per-sync (seal drops); deleted by REPLACES clear; voided by poison |
| frontier checkpoint (per G-RULE-4) | MStore | durable at eCheckpoint commit |
| forced resume checkpoint | MStore | restore → bump → eCheckpoint commit → dispatch (P-GEN's ground) |
| in-flight execution state | MNodeExec | volatile vs crash; vs mid-attempt death, quiesce-before-bump defers the bump (§3) |
| derived support counts (E) | MGraphScheduler | volatile; checkpoint-consistent rebuild target |
| retraction queue (E), pass state (S) | MGraphScheduler | volatile; self-healing at-least-once (re-publish / re-observation on resume) |
| mid-attempt bump table delta | MStore (via forced eCheckpoint) | durable BEFORE the bumped generation dispatches (§3 MID-BUMP FENCE, R3-F2) |
| causal stamps (S) | ride outputs | durable where the output is; `eAdopt` rewrites atomically |
| generation table | MGraphScheduler | latest-per-pending in checkpoints; death announce-visible (`eAnnGenBump` at carrier commits) |

Walker→graph mapping: unchanged from v2 (review-verified accurate).

## 6. Variant axes and mutation toggles

Axes and defaults unchanged from v2 (lineage per-leg; session A
default, vacuous outside G2/G9; compression off outside G9).

| toggle | what it removes/injects | kill cell | expected flip |
|---|---|---|---|
| `suppressionOff` | G-RULE-2 admission suppression | G1 mutant leg / G4 | P1-LEGALITY first-find (racing schedules; sequential adopts — live-fromGen deviation per R2-N1) |
| `sweepOff` | seal-time sweep | G5b | P5-UNDER (+ P6-S under S) |
| `sweepOverreach` | sweep drops in-closure partition | G5c | P5-OVER + P1-CONTENT |
| `purgeOff` (E) | pending-purge on death | G5e | execution-count oracle (redo machinery; seal stays green) |
| `stampMergeOff` (S) | read-side stamp merge | G2-S legs | P6-G RED (oracle, mechanism-independent) |
| `retractionOff` (E+B) | re-publish reader retraction | G2 E+B leg | P6-G RED |
| `overlayComposeDead` | §4b base-liveness precondition | G8b mutant leg | P1-CONTENT + P6-S (S) |
| `resumeCkptOff` | forced resume checkpoint | G1b | P-GEN RED |
| `demandDropOff` | drops one derived admission | G5f | closure oracle RED (SealExpect) |
| `adoptOnFail` | removes MATCH-only eligibility | G1c mutant leg | P-ADOPT RED (GS-CO-002: the smuggled rows(e2) sits INSIDE the sync-scoped freshness envelope, so no artifact-level oracle can see it; SealExpect stays GREEN by design — a declared control) |
| `writerAdopt` | removes writer-ineligibility | G2 announce-window legs | S: P6-S RED at seal (pass exhausts its budget against the stranded dead publish — R2-F2's loop, now a kill); E+B: P6-E RED (no re-publish, readers never retracted) |
| `quiesceOff` | quiesce-before-bump | G1d | P6-G RED (dead in-flight unit's late clear wipes the live re-derivation — R2-F1's walk, now a kill) |
| `midBumpFenceOff` | §3 mid-bump fence (checkpoint-at-bump) | G1e | P-GEN RED (two-crash id reuse — R3-F2's walk, now a kill; mirrors G1b/`resumeCkptOff`) |
| `markerCleanupOff` | REPLACES clear deletes marker | G8d | P-MARK RED (stale marker survives a record supersession; the flap-back then adopts mismatched content — P3′ RED under E) |

## 7. Properties (checkable forms)

[Post-freeze editorial, two-track seam: the composition algebra the
fold pins assume (L1–L6) and the stamp-lattice laws the S variant
rests on — merge as a join-semilattice, dead-membership
homomorphism, floor-compression staleness soundness (L7–L9) — are
mechanically proved in `formal/occult/LAWS.md`; the model consumes
them as assumptions per the brief's division of labor. No semantic
change.]

P1/P2/P4 as in v2 (P1 with §4d grounding + poison exemption; P2's
qualifying consult for adopted scopes is the adopting MATCH — true
under MATCH-only eligibility; P4 with the generation-blind
fingerprint). P3′ is the walker form, honestly named (v2 pin).

- P5 + CLOSURE ORACLE: unchanged from v2 (ghost closure from live
  announces; env-side counterfactual SealExpect as the independent
  base; round 2 confirmed the independence is real — the oracle is
  what catches R2-F5's starvation, now repaired). (GS-CO-002)
  SealExpect content is SYNC-SCOPED: the env announces the live
  per-key world at EVERY attempt start and the monitor accumulates
  the sync's acceptable epoch SET; sealed content must match SOME
  attempt-start world. Calibration find G1-CAL-1: a key whose
  derivation completed and checkpointed before a crash legitimately
  seals the earlier attempt's world (completed-across-crash,
  G-RULE-2), so the single-epoch expectation reds an honest history.
  Closure (which keys) stays exact both directions.
- P-ADOPT (adopt legality, GS-CO-002): every adoption is justified
  by a validated MATCH consult announced by the adopting
  (node, generation) BEFORE the adopt commits — the checkable form
  of R2-F3's MATCH-only eligibility. This monitor, not SealExpect,
  is the `adoptOnFail` kill: FAIL-adopt laundering is
  mechanism-visible only (see §6).
- P6-G (laundering oracle, all legs): unchanged from v2; round 2
  confirmed mechanism-independence (both kills flip against it).
- P6-E (mechanism conformance, E+B): retraction liveness — every
  reader execution of a dead value re-runs before seal (keying per
  §3; adopting re-readers included per R2-M5's pin).
- P6-S (mechanism conformance, S): AT-SEAL form only (round-2 R2-F6):
  no sealed output carries a dead-generation stamp; no partition
  mixes causally incomparable generations. P6-S red MEANS the
  mechanism failed (a dead stamp survived the pass). The
  VALUE-BLINDNESS pin is recast (R2-F6's vocabulary fix):
  dead-stamp-forced redo on value-identical re-derivation is
  intended MECHANISM BEHAVIOR whose signal is the FORCED-REDO COUNT
  (derived announces: observation-pass re-runs, dead-read events,
  retraction re-admissions) — a bake-off metric, never a property
  verdict. A cell expecting mechanism overhead declares an expected
  count, not a red.
- P-GEN: no two attempts contain store-commit announces attributed
  to the same (node, generation); adoption re-announces attribute to
  the adopter (R2-N3's rule, recorded).
- P-MARK (round-2 R2-F4): marker ⟹ partition equals the marked
  round's outputs; announce-evidenced (markers, clears, adopts, and
  row commits are all announced).
- Compression admissibility: unchanged (G9; safety verdicts
  invariant, redo may grow).

### 7.5. The frozen mechanism tally (amended v3, BEFORE any bake-off run — the round-2 findings changed mechanism counts and the tally must reflect them pre-run; further amendment requires a change order)

| column | mechanisms |
|---|---|
| shared | frontier checkpoint (+ admitted-derivation set with DEATH SEMANTICS, admitted-by edges, cursors); forced resume checkpoint; generation table + bump rule + QUIESCE-BEFORE-BUMP; markers with premise digests + publishBearing + adopt-or-re-derive (MATCH-only, writer-ineligible) + marker lifecycle column; supersession matrix + poison rule; sweep + closure oracle; unit ops |
| E adds | durable admitted-by edges (checkpoint row); ∀-pending-purge (redo machinery); derived-support rebuild + agreement check; retraction rule + queue with pinned enqueue/drain; REQUIRES session variant B |
| S adds | stamp merge on read; stamp field + `eAdopt` rewrite; three observation points (dispatch-time refusal, session-read read-through + dead-read count, pre-seal pass with iteration budget); optional bucketed compression |

Counting rule unchanged (durable state classes > scheduler rules >
per-output overhead).

## 8. Small-scope configuration budgets

As v2 (safety envelope; widened bake-off envelope: depth 2, one
fan-in edge), plus: PASS-ITERATION BUDGET (round-2 R2-M7): ≤ 3
pre-seal pass iterations per seal; honest cells must converge within
it (checked — a budget-exhausted seal with dead stamps is P6-S red
and means a mechanism bug). ATTEMPT BUDGET ≤ 3 (unchanged).
INDUCTIVE-BET RESTATEMENT: the envelope excludes session-transitive
(reader-writer) chains, publish-derived demand, and SESSIONS ×
DEMAND SHRINK (round-3 R3-M4: a de-demanded writer × a
still-demanded reader of its sync-scoped value — the one shape
where §4a's convergence mechanism has no lever) by declaration
(§1); first-order retraction and row-transitive support are
exercised (G6b/G6c); the bet is that no graph-runtime bug class
requires deeper session topology — stated, not assumed silently.

## 9. Scenarios and cells (expected verdicts declared before first run)

- **G1 phantom union**: legs (i) crash-before-commit → fetch-fresh,
  GREEN (round-2 walked, derives); (ii) crash-after-commit +
  mutation → marker found, revalidation FAILS → RE-DERIVE (MATCH-only
  pin; NOT adoption) → fetch-fresh RECORD round — §4b's record
  REPLACES row removes the dead rows AND deletes the marker (round-2
  text correction applied) → seal rows(e2)@V2, GREEN both variants;
  (iii) crash-after-commit, no mutation → MATCH, digest equal,
  writer bit clear → ADOPT → GREEN both variants (round-2: "the
  repair working"). Mutant `suppressionOff`: P1-LEGALITY first-find
  via the racing schedule (sequential adopts; live-fromGen deviation
  noted).
- **G1b generation-reuse probe**: unchanged (round-2 verified
  well-formed). `resumeCkptOff` → P-GEN RED.
- **G1c FAIL-adopt probe (round-2 R2-F3)**: e1→e2→e3, crash in the
  announce window after attempt 1's CHANGED-WITH-DIFF unit (marker
  digest = (V1, FAIL)); attempt 2's re-consult FAILS vs e3. Honest:
  MATCH-only pin forces re-derive → fetch reflects e3 → GREEN.
  Mutant `adoptOnFail`: adopts rows(e2) after the FAIL consult →
  P-ADOPT RED (GS-CO-002; kill reassigned off SealExpect — the
  sync-scoped expectation set accepts rows(e2) because e2 is a
  legitimate attempt-start world of the sync, and the
  completed-across-crash schedule seals it honestly; SealExpect and
  P3′ are declared GREEN controls on the mutant cell).
- **G1d dead-in-flight probe (round-2 R2-F1)**: G2's E+B chassis,
  schedule forcing the retraction bump while the dying reader
  executes. Honest: quiesce defers the bump; the re-derivation is
  the last writer; P6-G GREEN. Mutant `quiesceOff`: the dead unit's
  late clear+commit wipes the live rows → P6-G RED.
- **G1e mid-bump reuse probe (round-3 R3-F2)**: the G-pending
  chassis, two crashes; attempt 2's retraction-forced bump mints
  (G, g3), G@g3 commits a durable unit, crash lands with the
  elective checkpoint skipped. Honest: the §3 MID-BUMP FENCE forces
  the checkpoint at the bump, so resume 2 restores a table already
  carrying g3 and mints g4 — no collision; P-GEN GREEN; the reuse
  history is unreachable-after-pin. Mutant `midBumpFenceOff`: resume
  2 re-mints (G, g3) from the stale table → attempts 2 and 3 both
  carry (G, g3)-attributed store commits → P-GEN RED.
- **G2 session laundering**: legs as v2 with round-2 corrections:
  E+A → P6-G RED (derives; round-2 walked). E+B → P6-G GREEN under
  the quiesce pin (round-2: conditionally derivable, now
  unconditional); `retractionOff` → P6-G RED. S+A and S+B → P6-G
  GREEN, P6-S green (derives; the writer re-derives live and the
  pass converges); `stampMergeOff` → P6-G RED. SAME-VALUE CONTROL
  LEG (round-2 R2-F6 correction): H@g2 re-derives d1 exactly →
  P6-G GREEN, P6-S GREEN at seal, FORCED-REDO COUNT ≥ 1 (declared
  expected count, not a red — G's re-derivation is forced by the
  writer-stamp delta). ANNOUNCE-WINDOW LEG (round-2 R2-F2's
  placement, now scripted): checkpoint captures H pending with its
  publish-bearing unit committed; crash; resume bumps H → H is
  adoption-INELIGIBLE (publishBearing) → re-derives → re-publishes
  → readers cleared (retraction under E+B / stamp delta under S) →
  P6-G GREEN, P6-S green, pass converges within budget. CHASSIS
  REGISTRATION (round-3 R3-F1): the `writerAdopt` kill needs an
  adoption-ELIGIBLE publish-bearing marker, which under the §3
  body-op pin is constructible by script — a REPLAY-verdict
  publish-bearing unit (MATCH consult, body publish) with a
  premise-stable resume (no between-attempt mutation), so the
  re-consult MATCHes and the digest is equal; the kill leg schedules
  exactly this chassis. Mutant
  `writerAdopt` on this leg: S → P6-S RED (stranded dead publish;
  pass budget-exhausts — R2-F2's loop as a kill); E+B → P6-E RED (no
  re-publish, no retraction). WRITER FLAP-BACK PROBE (round-3
  R3-F1's second registration): H's diff-verdict publish-bearing
  unit commits; checkpoint in the announce window; crash; upstream
  flaps back (content(e3) = content(e1)). Resume: H ineligible →
  re-derives → re-consult MATCHes → REPLAY verdict → the body-op
  pin re-publishes d1@g2 anyway → writer-stamp delta re-derives G →
  pass converges. Expected: GREEN, forced-redo count ≥ 1 (under the
  elision reading this history would strand d1@g1 and red P6-S
  honestly — the probe is the body-op pin's load-bearing witness).
  G-PENDING LEG (round-2 R2-M6(i)):
  checkpoint captures G pending mid-read-window; G's pre-re-publish
  re-run reads stale d1, then H re-publishes d2 → the re-retraction
  clause fires (G re-runs again) → P6-G GREEN; witnesses the keying
  pin's re-retraction clause.
- **G3 artifact swap**: unchanged, GREEN (round-1 walked clean).
- **G4 duplicate admission**: unchanged (suppression GREEN;
  `suppressionOff` RED; distinct-derivation shape lives in G8c).
- **G5 sweep family**: (a)–(d) unchanged from v2 (round-1/round-2
  walked); (e) purge-domain cell under the ∀-purge pin — the
  paginated parent's child C has ONE admitted-by edge (dead) → purged
  → hash removed → count oracle asserts no post-resume C execution;
  `purgeOff` → count-oracle RED. (f) `demandDropOff` → closure
  oracle RED; HONEST BASELINE re-derived under the R2-F5 pins
  (no-shrink epoch: C re-admitted after hash removal via the live
  re-derivation — starvation unreachable; round-2's composition
  defect repaired and the cell asserts it).
- **G6 redo-work bake-off**: scripts unchanged from v2 [GS-CO-005:
  the v2 text is unrecoverable; the control legs are declared in
  §12]; G6c's E leg
  now derives under the ∀-purge pin (C survives on S2's live edge —
  round-2's ∃/∀ divergence resolved); expected counts unchanged;
  writers' forced re-derivations (writer-ineligibility) appear in
  the counts wherever a session writer dies — declared expected,
  charged to BOTH variants equally (the pin is shared machinery).
- **G7 progress under churn**: unchanged (generation-blind
  fingerprint; ladder; budget 3).
- **G8 supersession family**: (a) record REPLACES over dead debris —
  GREEN (clear wipes rows AND marker); (b) OVERLAY-intent dead base —
  honest GREEN by scripted policy, `overlayComposeDead` RED;
  (c) same-key distinct-derivation race — poison + SealExpect
  exclusion + legality exemption + MARKER VOIDED (R2-M8; a scripted
  forced re-run of the first node post-poison must NOT adopt — the
  leg asserts re-derive-or-refuse on a poisoned key); (d) MARKER
  FLAP-BACK PROBE (round-2 R2-F4): e1→e2→e3 with content(e3) =
  content(e1), two crashes; attempt 2 re-derives via record REPLACES
  (marker deleted); attempt 3's consult finds NO marker → ordinary
  consult → V1 MATCHes vs e3 (truthful validator; content reverted)
  → REPLAY verdict (round-3 R3-M1's correction: not fetch-fresh —
  replay of the e1 base = rows(e3), SealExpect verdict-equal either
  way) → SealExpect rows(e3) GREEN. Mutant `markerCleanupOff`:
  the stale marker survives, attempt 3 premise-matches it (V1 MATCH
  vs e3) and adopts content the marker no longer describes → P-MARK
  RED (+ P3′ RED under E per the round-2 walk).
- **G9 compression admissibility**: unchanged from v2.

## 10. Adequacy obligations

1–6 as v2 (kills incl. count-oracle kills; reachability probes;
independent oracle bases; rebuild-agreement active in E resumes;
bake-off table from checker output + frozen tally; G5d meta-analysis
with a which-is-right determination).
7. The decision rule (§10.7, v2) unchanged [GS-CO-005: the v2 text
   is unrecoverable; the rule as declared in §12 is the citable
   version]; the §7.5 tally as amended
   v3 is final for the bake-off (further amendment = change order
   BEFORE any bake-off run).
8. NEW (round-2): P-MARK active in every cell with a marker;
   convergence of the pre-seal pass within the §8 iteration budget is
   asserted in every honest S cell (a budget-exhausted honest seal is
   a finding, not noise).

## 11. Adversarial review record

Three rounds complete; the spec is FROZEN at v4. Round 1 (full):
REJECT, 11 majors — dispositioned in v2. Round 2 (targeted,
adoption): REJECT, 6 majors — adoption core verified sound;
seams dispositioned in v3. Round 3 (targeted, the v3 repairs;
`formal/reviews/graph-spec-round3-repairs.md`): REJECT, 2 majors +
4 minors + 3 notes — ALL six round-2 repairs verified soundly
applied and composing (writer-ineligibility convergence derives
mechanically on every scripted honest history; quiesce cannot starve
or deadlock; the marker matrix is total; admitted-set death
semantics close the starve/double-admission space; MATCH-only is
classifiable everywhere with SealExpect the right oracle; the G2
table derives leg by leg), and every round-3 finding was
fix-without-re-review — per the review's own verdict, the fixed
spec needed a disposition registration check, not a fourth round.
v4 is that registration (§12). Checker verdicts against v4 are
citable.

Standing questions (future change orders): MODEL_SPEC §10's list;
the elision-vocabulary boundary (§1 — if the runtime design elides
publish-bearing replay, re-open R3-F1's product); sessions × demand
shrink (§1/§8) if session scoping ever outlives a sync.

## 12. Change-order log

- v1: initial draft. Round-1 review: REJECT (11 majors + 7 minors +
  3 notes).
- v2: round-1 dispositions (see the v2 entry in the git history of
  this file and `formal/reviews/graph-spec-round1.md`). Headline:
  premise-validated adoption replacing marker suppression;
  P6-G laundering oracle; generation-grounded fold pins; forced
  resume checkpoint; durable admitted-by edges; total supersession
  matrix; closure oracle; G5d meta-analysis reformulation;
  divergence bake-off scripts + widened envelope; compression cells;
  generation-blind P4 fingerprint; 10 minor/note registrations.
  Round-2 targeted review of the adoption mechanism: REJECT (6
  majors + 8 minors + 3 notes; core verified sound, seams found).
- v3: round-2 dispositions
  (`formal/reviews/graph-spec-round2-adoption.md`). R2-F1 →
  QUIESCE-BEFORE-BUMP pin + retraction-queue semantics + G1d probe +
  `quiesceOff` kill. R2-F2 (re-review target) → WRITER-ADOPTION
  INELIGIBILITY: publish-bearing markers never adopt; dead publishes
  are transient by construction and the pre-seal pass's convergence
  is argued and checked (§4a, §8 iteration budget); `writerAdopt`
  kill on the new announce-window leg; publish-derived demand
  excluded from G-RULE-1 by declaration (§1). R2-F3 → MATCH-ONLY
  adoption eligibility + ADOPT registered as a verdict class + G1c
  probe + `adoptOnFail` kill + freshness prose corrected. R2-F4 →
  marker lifecycle column in §4b (REPLACES clear deletes; unit put
  overwrites; poison voids), P-MARK invariant monitor, §4a/G1(ii)
  text corrections, G8d flap-back probe + `markerCleanupOff` kill,
  per-sync marker scoping (R2-M3). R2-F5 → ∀-purge predicate;
  purge/refusal remove the derivation hash; G-RULE-2 MUST-suppress;
  G5f baseline and G6c E-leg re-derived. R2-F6 → P6-S at-seal form
  only; forced-redo COUNT as the mechanism-overhead signal; G2
  same-value control leg corrected. Minors: session-read observation
  registered as read-through + dead-read count with the
  writer-pending invariant (R2-M1); derived-announce carrier pin
  (R2-M2); marker scoping (R2-M3); dead-base detection wording
  (R2-M4); adopting re-reads register normally (R2-M5); G-pending
  re-retraction leg added, session-transitive chains and
  publish-derived demand excluded with argument (R2-M6);
  pass-iteration budget row (R2-M7); poison-voids-marker + G8c
  post-poison leg (R2-M8). Notes recorded: eAdopt fromGen-dead
  precondition (R2-N1), digest-closure vacuity sentence (R2-N2),
  P-GEN monitor rule (R2-N3). Tally amended pre-run (§7.5).
  Round-3 targeted review of the v3 repairs: REJECT (2 majors +
  4 minors + 3 notes; all six round-2 repairs verified sound; every
  finding fix-without-re-review).
- v4 (FROZEN): round-3 dispositions
  (`formal/reviews/graph-spec-round3-repairs.md`). R3-F1 →
  SESSION-PUBLISH BODY-OP PIN (§3): session publishes are node-body
  store ops executed by every non-adopted execution regardless of
  verdict class (the reading round 2's accepted walks used,
  registered); declared within-sync deviation from the walker's
  elision vocabulary with the §1 F17 boundary sentence extended;
  announce-window kill chassis registered (replay-verdict
  publish-bearing unit, premise-stable resume); writer flap-back
  probe leg added to G2 (expected GREEN, forced-redo ≥ 1). R3-F2 →
  MID-BUMP FENCE (§3): a mid-attempt bump's generation-table delta
  is durable (forced eCheckpoint) before the bumped generation
  dispatches — the F4 discipline extended to the second minting
  path; §5's two rows amended ("self-healing" scoped to the
  scheduling effect, never the minted id); G1e reuse probe +
  `midBumpFenceOff` kill (P-GEN RED). R3-M1 → G8d honest-leg
  verdict corrected to REPLAY (SealExpect verdict-equal). R3-M2 →
  store-side `eAdopt` poison precondition (closes the
  check-then-act window). R3-M3 → pass ITERATION BOUNDARY pin (one
  iteration = one scan over a drained frontier). R3-M4 →
  convergence claim scoped to the scripted envelope; SESSIONS ×
  DEMAND SHRINK exclusion declared in §1 and §8's inductive bet.
  Notes recorded: at-least-once cost claim verified honest (R3-N1,
  into §4a); deferral non-starvation/non-deadlock arguments (R3-N2,
  into §3); round-2 registrations sweep-verified applied (R3-N3).
  FROZEN — subsequent changes are GS-CO-NNN change orders.
- GS-CO-001 (calibration, G1 build-out): G-RULE-3's durable fence
  made TOTAL over all four minting paths (attempt-start root mint,
  first-admission mid-attempt mint, resume bump, mid-attempt bump).
  Found by P-GEN redding an HONEST single-crash G1 history: attempt
  1 crashed before any checkpoint, attempt 2 cold-restarted and
  re-minted (P, 1). Encoding: path (a) rides `resumeCkptOff`
  (making the G1b kill fireable with one crash — acceptable, the
  kill statement is unchanged), path (b) rides `midBumpFenceOff`.
  §2 amended in place, tagged.
- GS-CO-002 (calibration find G1-CAL-1): SealExpect content check
  made sync-scoped (acceptable epoch SET accumulated across attempt
  starts) — the single-epoch form reds the honest
  completed-across-crash schedule, which G-RULE-2 licenses. The
  `adoptOnFail` kill is consequently invisible to every
  artifact-level oracle and moves to the new P-ADOPT mechanism
  monitor (adoption requires a prior validated MATCH consult by the
  adopting generation, announced before the adopt commits — the
  announce-order pin is part of the monitor's soundness). §6, §7,
  §9 amended in place, tagged. Calibrated: G1 sweep 10/10 at 5000
  schedules (greens hold incl. SealExpect+P3′ controls on the
  mutant; kills red with first-finds P1-LEGALITY / P-GEN /
  P-ADOPT).
- GS-CO-003 (calibration find, G2 build-out): CARRIER-DURABILITY
  ATOMICITY. The R2-M2 carrier pin extends to durability: a
  completion carrier's derived effects — every admission, mint, and
  admitted-by edge — commit as ONE durable delta, and no checkpoint
  may separate the carrier's completion from its admissions. The
  per-mint fencing GS-CO-001 first encoded leaves a lost-demand
  window: a crash between two children's fences restores the parent
  COMPLETED with the second child's admission gone; a completed
  parent never re-derives, the demand starves, and the closure
  oracle reds an honest session-topology history (found on the
  first honest G2 run). Edge-only registrations are covered too: a
  lost live admitted-by edge mis-arms the ∀-purge predicate
  (R2-F5) on resume. Encoding: one forced checkpoint at the end of
  the carrier's demand loop, riding `midBumpFenceOff`. §2's
  GS-CO-001 sentence is superseded on the (b) path by this rule.
- GS-CO-004 (calibration find, G2 build-out): RETRACTION CATCH-UP
  BOUND. The retraction rule is RE-PUBLISH-DRIVEN (R2-M6(i)'s
  wording holds as written); the registration side (a read that
  registers when its value's death is already knowable, i.e. the
  read-note lost the race to the re-publish carrier) is a CATCH-UP
  retraction spent ONCE per (reader, dead writer-generation). An
  unbounded registration-side rule livelocks the `writerAdopt`
  strand — the adopted writer never re-publishes, the reader
  re-runs forever, the frontier never drains, and the at-seal
  oracles (P6-E/P6-S) are structurally unevaluable, turning the
  kill cell GREEN-by-divergence (found: the kill would not fire).
  With the bound, the strand costs one re-run, seals, and P6-E
  reds the still-dead final read. Session reads register at READ
  time (read-through, R2-M1) via a scheduler note; carrier-time
  registration makes the R2-F1 dying-reader race structurally
  unreachable (also found in this build-out).
- GS-CO-005 (bake-off phase): BAKE-OFF PROTOCOL REGISTRATION —
  provenance repair plus pre-run declarations.
  (a) REPAIR: §10.6's meta-analysis, §10.7's decision rule, and §9
  G6's "scripts unchanged from v2" cite v2 subsection text
  recoverable only from this file's git history, which was never
  created (the formal tree is untracked as of this registration).
  The rule and both deferred procedures are DECLARED here, BEFORE
  any bake-off run; this text is the citable version and supersedes
  the §9/§10 references.
  (b) DECISION RULE (§10.7 restated): lexicographic, first non-tie
  decides. Axis 1 — property satisfaction on the frozen 66-cell
  matrix: an honest-cell red or an unfired declared kill eliminates
  a variant; a correctness dependency (E REQUIRES session variant
  B, evidenced by tcG2ea_P6G) travels with the variant as part of
  its Axis-2 mechanism bill, not as an elimination. Axis 2 —
  mechanism count per the §7.5 frozen tally under its counting rule
  (durable state classes > scheduler rules > per-output overhead).
  Axis 3 — redo work under the divergence scripts: minimal green
  exec bounds per chassis, the declared-expected redo probes, and
  pass-budget consumption. A hybrid recommendation is admissible
  only if expressible as the shared column plus exactly ONE
  variant's adds.
  (c) G6 v1 CONTROLS (metric floor, declared): no-crash G6a/G6c
  chassis at bound 1 — tcG6aE_Ctl / tcG6aS_Ctl / tcG6cE_Ctl /
  tcG6cS_Ctl declared GREEN (zero-crash redo is zero under BOTH
  variants; a red is a variant-overhead find, Axis-3 data). G6b
  bound-1 probes — tcG6bE_Redo / tcG6bS_Redo declared RED (the
  mutation chassis's redo exists under both variants; a green leg
  is a divergence datum).
  (d) G5d META-ANALYSIS (reachable-seal-world comparison,
  declared): GSEALWORLD existence probe — RED means the announced
  target world is sealed for the interrupted sync, where a world is
  the manifest restricted to keys sealing non-empty partitions. On
  the shrink chassis (cell 24, honest config), both variants:
  W1 = {0→2} REACHABLE (RED probe: P re-ran at e2, C swept);
  W2 = {0→1, 1→1} REACHABLE (RED probe: P completed-across-crash
  at e1, C live — G-RULE-2); W3 = {0→2, 1→1} UNREACHABLE (GREEN
  probe: the sweep-failure world; honest reachability would be a
  P5-shaped finding). The W1/W2 REDs on the same chassis are the
  probe mechanism's positive controls against a vacuous W3 GREEN.
  WHICH-IS-RIGHT RULE: every reachable world must lie inside the
  sync-scoped SealExpect envelope (asserted in-cell by the honest
  G5a greens); a world reachable under exactly ONE variant is a
  divergence finding that blocks Axis-3 citation until
  dispositioned.
