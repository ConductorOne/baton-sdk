# Sync formal verification — synthesis report

This is the executive summary of the formal verification effort for
baton-sdk's sync scheduling semantics (charter:
`docs/tasks/sync-formal-model-brief.md`). It states what was modeled,
what was found, the verdict, and — as precisely as possible — what is
and is not guaranteed. Detail lives in the documents each section
cites. Public-repo content rules apply throughout.

## The verdict in three sentences

The current tiered-walker + source-cache-replay design mechanically
reproduces every calibration bug it was suspected of, including the
phantom union, with each failure caught by a dedicated monitor. The
proposed demand-graph runtime, after three adversarial spec reviews
and a calibration program that found and fixed five design bugs of
its own, is verified at small scope in both lineage variants, and the
registered decision rule selects **variant S (observable-causal
stamps)** — see `graph/BAKEOFF.md`. The shipped syncer's real
commit-order behavior has been witnessed conformant to the ordering
and durability policies on every execution we exported (cold, warm,
crash/resume, tombstone-delta), and the instrument is sharp enough
that it falsified one piece of documented resume behavior along the
way.

## Track A — calibration model of the current design (P)

`MODEL_SPEC.md` (v11, frozen after seven review rounds plus
MS-CO-001) and the `walker/` P project. Authority is earned by
rediscovery: with mitigations toggled off, the checker finds the
known bugs; with the shipped/staged fixes toggled on, the same cells
go green. The current sweep is 55 cells, 0 mismatches, 10k schedules
per cell (`walker/CALIBRATION.md`).

Reproduced reds include: the phantom union in content, staleness, and
epoch-coherence form (scenario 1); session laundering under the
shipped free-form sessions (scenario 2 — an EXPECTED finding, fixed
only by the demand-graph work); session-checkpoint consistency in
both directions (scenario 2's P6-C cells: the shipped zombie
direction and the rejected resume-clear's amnesia direction, with
checkpoint-consistent sessions green — see finding 0); the
artifact-swap hit-rebind residual hole and the CO-6b-004 binding kill
that closes most of it (scenario 3); warm-drift, overlay placement,
and progress cells (scenarios 5–7); and external principals
(scenario 8's P8 monitor — the `deleteStaleExternalPrincipals`
contract: the non-deleting engine's stale-survivor degrade and the
stale-list recency mutant red, the capable-engine crash/stop/carry
schedules green). Every red archives its
counterexample schedule under `walker/traces/`.

## Track B — demand-graph runtime model and the bake-off (P)

`GRAPH_MODEL_SPEC.md` (v4, frozen after three adversarial rounds) and
the `graph/` P project: frontier scheduler, generations with
quiesce-before-bump and the total mint fence, premise-validated
markers, seal-time sweep with the closure oracle, atomic units, and
the two lineage candidates — eager edges (E) and observable-causal
stamps (S).

The calibration program found five design bugs before any bake-off
run (GS-CO-001..005 in `graph/CALIBRATION.md`), among them generation
identity reuse across crash re-mints, the carrier-durability fence,
and the retraction catch-up bound; plus two mechanism repairs
(G8B-CAL-1, G9-CAL-1 — stamp compression as first drafted livelocked
honest histories). The frozen matrix is 66 cells clean at 10k
schedules, and the 12-cell bake-off phase matched its declared
verdicts 12/12.

The bake-off (`graph/BAKEOFF.md`, assembled under the GS-CO-005
registered decision rule) ties on property satisfaction, and S wins
on mechanism count: no new durable state class, versus E's edge
checkpoint rows, retraction queue, two recovery machineries, and —
decisive in kind, not just count — E's correctness DEPENDENCE on the
constrained session primitives. The calibrated laundering cell
(`tcG2ea_P6G`) shows E sealing a dead writer's value under free-form
sessions with every artifact-level oracle green; S is green under
free-form sessions. Redo work corroborates and does not decide: the
reachable seal-world sets are identical under both variants.

## Track C — the deductive track (Occult)

`occult/` (deliverables 6–9), proved in the Occult engine (sibling
repo `../occult`) through the Go host in `occult/host/`:

- **Laws (D9)**: 15 equational laws of the composition algebra and
  stamp lattice proved, 5 negative controls correctly refused
  (`occult/LAWS.md`). One law (L1, REPLACES absorption) closes in
  bounded form pending an induction tactic.
- **Phantom union, derived**: the broken composition rule
  (`occult/src/sync_phantom.occult`) DERIVES the phantom union
  deductively — the same bug Track A reaches by search, reached by
  proof, from an axiomatization of what the broken algorithm does.
- **Protocol (D8)**: the syncer↔connector source-cache lookup
  continuation as a global session term with per-role projections —
  6 projection derivations plus a structural expansion, 4 polarity/
  cap/leg controls refused. The bounce cap is structural (no
  five-bounce term exists), mirroring
  `sourcecache.MaxLookupBouncesPerRequest`.
- **Trace policies (D7)**: seven ordering/durability policies
  (consult-before-replay, clear-before-write, once-per-scope,
  publish-before-checkpoint quiescence, seal obligations,
  session-checkpoint consistency — finding 0's root cause as a
  policy — and external-principal grounding, the
  `deleteStaleExternalPrincipals` contract) over a canonical event
  vocabulary including attempt
  boundaries (`ev_resume`), the delta protocol's delete leg
  (`ev_delete`), session operations (`ev_swrite`,
  `ev_sread_hit`, `ev_sread_miss`), and the external-principal
  phase (`ep_list`, `ep_live`, `ep_recon`, `ep_copy`). Verdict
  matrix: 140 cells (20 fixtures × 7 policies), each green fixture
  satisfies all, each red violates exactly its own.
- **Reference implementation**: an executable Go prototype of the
  demand-graph runtime whose traces are judged by the same oracle;
  its legacy mode reproduces the broken behavior and is caught.
- **The bridge to shipped code (D6)**: `pkg/sync` carries a
  test-only commit-order recorder (`pkg/sync/sync_trace_audit.go`,
  nil in production — the field is only ever assigned from test
  files, which are not compiled into non-test binaries). Chaos tests
  run the real syncer and export JSONL fixtures; the oracle judges
  them: 56 policy cells across cold, warm, crash/resume,
  tombstone-delta, record-flip, session-zombie, and
  external-principal executions — 54 green plus TWO deliberately red
  cells: the session-zombie fixture's `session_ckpt_consistency`
  verdict, the standing known-defect pin on the shipped session
  semantics (finding 0), and the SQLite external-principal fixture's
  `external_principal_grounding` verdict, the standing known-degrade
  pin on the non-deleting engine's warn-and-continue resume.
  Planted-violation tests prove the bridge detects what it claims
  to (`occult/TRACE_BRIDGE.md`).

## Findings register

Findings that matter beyond the models themselves:

0. **Phantom union live in shipped code (real defect, FIXED).** The
   walker model's scenario-1 red (the phantom union, tc1c flavor)
   was reachable in the shipped 6b syncer via the verdict-flip path:
   a warm round cut after its replay copy committed but before its
   validator published, upstream moving between attempts, and the
   resume's consult missing — the connector's fresh RECORD round
   composed with the crashed attempt's copied debris and sealed the
   union under the fresh validator, which the next sync validates
   clean and replays forward (permanently stale "live" data).
   Witnessed by `TestChaosSourceCacheRecordFlipOverReplayDebris`,
   fixed by record-round grounding (`groundRecordScope` +
   `ClearSourceCacheScope`): a record round is a replacement
   listing, so a partition holding rows no completed round published
   is cleared before the round's first write. The grounding is
   trace-visible ("replacement rounds clear first" — the policy
   doctrine, previously structural, now witnessed), and the fix's
   mechanism and outcome are both pinned by the witness test. Note
   the model's verified V-ATOMIC/V-OVERLAY-UNIT fix family maps onto
   a code base with durable marker suppression; the shipped code
   heals by re-execution (restart-from-root + idempotent re-copy),
   so grounding was the missing piece, not unit-mode commit.

   The adjacent flank is the SESSION STORE — with a CORRECTED
   provenance note: an earlier revision of this paragraph claimed
   the hazard was found by code reading alone and that "none of the
   models contain a session store." That second claim was WRONG. The
   walker model has always carried the shipped session semantics
   (MStore's `sessionKV`: durable at op commit, survives attempts
   and crashes, while the checkpoint token is separate state), and
   scenario 2's crash cell (`tc2crash_P6A`, an EXPECTED red since
   calibration) contains exactly the zombie mechanism — the re-run
   reads the dead attempt's durable stale value. The failure was
   dispositional, not model-side: that red was filed as a
   future-runtime obligation (sessions variant B, the graph
   addendum) instead of being routed as a shipped-code change order,
   and the checkpoint-relative ROOT CAUSE — post-crash session state
   must equal the restored checkpoint's — was never stated as a
   property in any system. Both gaps are now closed mechanically:

   - **P**: the P6-C monitor states the constraint in BOTH
     directions. `tc2crash_P6C` reds `P6-C-ZOMBIE` under shipped
     semantics (a dead attempt's beyond-checkpoint write observed by
     the re-run); `tc2clear_P6C` reds `P6-C-AMNESIA` under the
     rejected wholesale resume-clear (a checkpoint-committed value
     destroyed — data whose producing work will not re-run);
     `tc2consistent_P6C` is green under checkpoint-consistent
     sessions, the registered fix. The amnesia premise needed a
     reversed root order (cell 21) to be reachable — the writer must
     pop before the reader for a committed value to precede a
     re-run read.
   - **Occult**: trace policy 6 (`session_ckpt_consistency`) states
     the same constraint over the canonical vocabulary, with four
     fixtures — including the correct-rollback green, where a crash
     legally erases an UN-checkpointed write and the re-run's miss
     is the fix behaving properly.
   - **Witnessed**: `warm_replay_sync_session_zombie.jsonl`,
     recorded from a REAL syncer crash/resume execution
     (`TestChaosSourceCacheSessionPersistsAcrossResume`, which acts
     as the session actor), is judged RED by the oracle — a standing
     known-defect pin that flips to green when checkpoint-consistent
     sessions land.

   On the code side: a mechanical fix (clearing the namespace on a
   participating resume) was shipped and then REVERTED as unsound —
   resume never re-runs completed actions, so a wholesale clear
   destroys session caches whose producing work will not execute
   again (now also a model verdict: `P6-C-AMNESIA`). Current stance
   is contractual (CO-6b-009 in the 6b plan): session use must
   survive at-least-once re-execution with prior state present,
   consults must never be answered from session caches, and session
   state is silently partial for replayed scopes — pinned in
   `pkg/sourcecache` and `pkg/session/README.md`, with
   persistence-across-resume pinned by
   `TestChaosSourceCacheSessionPersistsAcrossResume`. The correct
   mechanical fence (checkpoint-consistent sessions: a volatile
   overlay flushed atomically with the checkpoint) is registered as
   future work — and is now the variant the models verify green;
   the lineage-bearing fix (session reads as stamped observation
   points) is variant-S scope.
1. **Resume re-copy (real code, documentation falsified).** The
   resume suite documented that a restored replayed-set skips the
   replay copy across a mid-batch cut. The real-trace instrument
   shows the skip cannot occur: checkpoints commit at batch
   boundaries, so a mid-batch crash always forces a re-copy, and the
   actual guard is the copy's replacement idempotence. Comments
   corrected in `pkg/sync/chaos_source_cache_resume_test.go`; pinned
   in `occult/TRACE_BRIDGE.md`.
2. **E-only laundering (design).** Eager-edge lineage is correct
   only with constrained session primitives; free-form reads seal a
   dead writer's value undetected. This moved the session-primitive
   audit from a correctness gate to an optimization under the
   adopted variant S.
3. **Generation identity reuse (design, fixed in spec).** Crash
   before checkpoint allowed a re-minted generation to collide with
   its predecessor; fixed by the total mint fence (GS-CO-002/003
   family), verified by kill cells.
4. **Stamp compression livelock (design, fixed in spec).** The
   first-draft compression rules livelocked honest histories into
   the pre-seal pass budget; admissible only with the G9-CAL-1
   minting rules.
5. **Scheduler sensitivity (verification methodology).** The
   dying-reader race (`tcG1d_P6G`) needed feedback-PCT and a third
   worker to exhibit — implementation tests for that race must not
   rely on uniform random schedules.
6. **Engine findings (tooling).** Ground-term evaluation cost grows
   ~2x per trace event (the gate for longer real traces), and
   parameterized protocol definitions do not project. Recorded with
   six other asks in `docs/tasks/occult-engine-changes-brief.md`
   (local working doc).

## What is guaranteed, and what is not

Three grades of code linkage, in decreasing mechanical strength:

- **Witnessed** (trace bridge): the policy verdicts hold for the
  real executions we exported. This catches what happens; it does
  not prove what cannot happen.
- **Calibrated** (both P models, the phantom derivation): the models
  reproduce the known bugs, which is evidence of fidelity, not proof
  of it.
- **Asserted** (laws ↔ named function contracts, protocol ↔ proto
  shapes and Go constants): documented correspondence, held by
  review. If the Go side drifts, nothing goes red automatically.

Standing limits: P verdicts are exhaustive only within the stated
schedule budgets and small-scope configurations (each spec's §8
declares its exclusions — notably sessions × demand shrink and
session-transitive chains in the graph model). Redo figures are
counts at small scope, not throughput. The models arbitrate designs,
not Go code; the trace bridge is the standing instrument that must
stay current as the design becomes an implementation.

## What's next

1. **The variant-S RFC and production implementation** in
   `pkg/sync`, carrying the four design consequences listed in
   `graph/BAKEOFF.md` (primitives as optimization, observation-point
   discipline, compression's minting rules, non-uniform-schedule
   race tests).
2. **Keep the oracle current**: as demand-graph code lands, extend
   the trace vocabulary with generation stamps and grow fixtures
   alongside the implementation — the bridge is only as good as its
   coverage.
3. **Occult Ask 8** (tractable ground evaluation) when engine time
   exists: it unblocks realistic trace lengths and is the wedge for
   the longer-term single-engine unification path.
