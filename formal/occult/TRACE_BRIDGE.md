# Trace bridge — P traces and real syncs onto the Occult policy oracle (deliverable 6)

The policy oracle set (`src/sync_trace_policies.occult`, deliverable 7)
consumes a canonical event list. This note pins the canonical
vocabulary, the two mappings onto it (P counterexample traces; real
sync executions), and the evaluation of the engine's built-in runtime
trace checker.

## Canonical vocabulary

One trace = one sync attempt's events, in commit order. Constructors
(two-scope envelope, matching the P models' small scopes):

| Event | Meaning |
|-------|---------|
| `ev_consult(s)` | source-cache consult for scope s (verdict arrives with it) |
| `ev_clear(s)` | partition clear for scope s |
| `ev_replay(s)` | committed replacement copy for scope s (replay-unit commit) |
| `ev_upsert(s)` | row upsert into scope s's partition |
| `ev_publish(s)` | scope s's rows published to the artifact |
| `ev_checkpoint` | durable watermark commit |
| `ev_seal` | artifact seal |

| `ev_delete(s)` | committed tombstone application (the delta protocol's delete leg) for scope s |
| `ev_resume` | crash/resume attempt boundary inside one sync's trace |

`ev_delete` is a WRITE with upsert's obligations: it needs grounding
(clear-before-write — a tombstone against a base this sync never
copied is the un-regrounded class, delete flavor), dirties the
quiescent-checkpoint flag, and marks the scope active for seal
obligations. Naming note: these "tombstones" are deletion entries in
the CONNECTOR RESPONSE (`DeletedIds`/`DeletedPrincipalIds` on the
replay/record annotations), applied synchronously as plain row
deletes — nothing deletion-shaped is durably stored.

The multi-attempt extension: a trace is ONE SYNC's events, with
`ev_resume` marking attempt boundaries. Durable facts persist across
the marker — consult flags (the checkpoint-durable hit-set), clear
grounding (committed rows), scope activity and publishes — and only
once-per-scope RESETS: an interrupted action restarts from its root
token, so the across-attempt replay re-copy is B5-legal at-least-once
idempotence, while a within-attempt duplicate remains the bug.
Single-attempt traces carry no marker and mean what they always did.

The external-principal extension (policy 7, the
`deleteStaleExternalPrincipals` contract): `ep_list` marks a phase run
listing the external source's CURRENT answer; `ep_live(p)` declares p
a member of that answer; `ep_recon` marks reconciliation COMPLETED
(stale copies deleted, or nothing stale); `ep_copy(p)` is p's
committed principal write. These live in their own keyspace —
invisible to the artifact policies (1–5) and the session policy (6),
like session events are to both. Committed copies are DURABLE across
`ev_resume` (checkpoint resume deliberately retains completed writes),
which is the debris premise: a between-attempt shrink strands a dead
attempt's copies unless reconciliation deletes them. The
warn-and-continue degrade of a non-deleting engine emits NO `ep_recon`
— the pass ran but did not reconcile — so its first copy is the
recon-before-copy violation.

Pending extension, tracked not modeled: generation stamps (the graph
model's variant-S lineage).

## Mapping 1: P model announce events — PROSE ONLY (no renderer)

STATUS: this mapping is a documented convention, not an implemented
instrument — `host/` carries no P-trace renderer (only
`renderRealTrace` for Mapping 2 and `refimpl.RenderOccult` for the
reference implementation). A P counterexample is judged today by
reading it against this table by hand; anyone automating it must
build the renderer from the ACTUAL announce vocabularies below and
add a planted-seal validation test (a renderer that fails to emit
`ev_seal` silently vacates the seal-anchored policies 4, 5, and 7,
which are green-by-prefix on seal-less traces).

The walker and graph models announce through the monitor event
vocabulary (`formal/walker/PSrc/Events.p`, `formal/graph/PSrc/Events.p`).
A P counterexample trace renders onto the canonical list by keeping
announce events in schedule order and dropping everything else:

| P announce (walker / graph where they differ) | Canonical |
|------------|-----------|
| `eAnnConsult` | `ev_consult(s)` |
| `eAnnClear` (incl. the unit's clear constituent) | `ev_clear(s)` |
| `eAnnReplay` / `eAnnReplayCopy` (copy commit) | `ev_replay(s)` |
| `eAnnUpsert` | `ev_upsert(s)` |
| `eAnnTombstones` (one per removed id) | `ev_delete(s)` |
| `eAnnPublish` (artifact-facing publish only) | `ev_publish(s)` |
| `eAnnCheckpoint` | `ev_checkpoint` |
| `eAnnSessionSet` | `ev_swrite(k)` |
| `eAnnSessionGet` / `eAnnSessionRead` (found: hit, else miss) | `ev_sread_hit(k)` / `ev_sread_miss(k)` |
| `eAnnSeal` / `eAnnGSeal` | `ev_seal` |

Session-write REQUESTS (`eGSessionPub`) do not render: they are the
wire message to the store, and the canonical event is the store's
committed announce (`eAnnSessionSet`), same as every other write.
Session events map to the session keyspace (`ev_swrite`), never to
`ev_publish` — a session write is not an artifact publish.

The atomic units expand to their ANNOUNCE order, which differs by
model: the walker's `eReplayUnit`/`eOverlayUnit` announce clear, copy
(, upserts, tombstones), publish; the graph's `eGReplayUnit` announces
the marker put FIRST (the P-MARK convention — see
`graph/PSrc/Store.p`'s unit handlers), then clear, copy, publish; and
the graph's `eGOverlayUnit` announces marker, clear, copy, upserts,
tombstones, publish. Marker puts have no canonical event (generation
stamps are the tracked pending extension). ONE deliberate exception to
"every unit contributes a clear": under the G8b `composeDead` INJECT
(`Store.p`'s overlay handler, the `tcG8bMut_P1` kill), the overlay
unit composes onto existing debris and announces the copy with NO
clear at all — that missing clear IS the kill, so a hand-renderer
following this table must not supply a clear the model never
announced, or the injected counterexample's clear-before-write red is
masked into a green. The policies check the leg ordering the honest
units guarantee by construction; running unit-built traces through
the oracle is a consistency check of that guarantee, not new
information. Crash scenarios cut the list at the crash point:
a cut trace must still satisfy the prefix-closed policies (1–3),
while the seal-anchored policies (4–5) are vacuous without `ev_seal`
— exactly the sync-scoped-freshness stance the models take.

## Mapping 2: real sync executions — IMPLEMENTED

The runtime emits the same shapes from the syncer:
`ev_consult` = the source-cache lookup round-trip;
`ev_clear`/`ev_upsert` = c1z store writes; `ev_replay` = the completed
replacement copy of an attested base; `ev_publish` = the scope's rows
becoming artifact-visible; `ev_checkpoint` = the sync-state watermark
write; `ev_seal` = `.c1z` finalization. A chaos harness (kill points
between any two events) yields cut traces checked as above. The
chaos-scenario map is: each red fixture in the policy module is the
minimal chaos outcome for its policy — e.g. `trace_red_cbp` is
"killed the watermark write, sealed anyway", `trace_red_seal` is
"sealed with an active unpublished scope" (the phantom-union family's
observable footprint at the artifact boundary).

This leg now runs against the SHIPPED syncer. `pkg/sync` carries a
test-only commit-order recorder (`sync_trace_audit.go`,
`testSyncTraceAudit` — nil-checked, one pointer test per event, the
`testQueueAudit` pattern) fired at the orchestration seams: lookup
resolution (consult), the replay unit's clear+copy legs in the store's
contractual order, scoped page-row commits (upsert, page granularity),
manifest-entry writes (publish), durable checkpoint tokens, and
EndSync (seal). The chaos test `chaos_trace_oracle_test.go` records
the reference source-cache scenario cold and warm and exports JSONL
fixtures (`host/testdata/realtraces/`); `host/real_trace_oracle_test.go`
renders them onto the canonical vocabulary and checks all seven
policies.

Two conventions live in the RENDERER, never the recorder (the recorder
is purely observational): scope names map onto s1/s2 in first-seen
order (two-scope envelope), and a NON-resumed attempt's upsert with no
earlier explicit clear gets a structural `ev_clear` inserted — the
partition was born empty at StartNewSync. Resumed attempts get no such
insertion, which is exactly how an un-regrounded resume reds
clear-before-upsert. The bridge itself is validated by planted
violations (`TestRealTraceBridgeCatchesPlantedViolation`): dropping the
warm fixture's consult reds policy 1, and replaying its writes as a
resumed attempt with the replay downgraded to a bare upsert reds
policy 2.

Multi-attempt traces are in-domain too. The chaos harness cuts a warm
two-page delta round with an `EffectCrash` after the replay unit and
overlay upsert committed, resumes with a new syncer, and exports the
two attempts as one fixture with a `{"kind":"resume"}` marker line
(`warm_replay_sync_interrupted.jsonl`). All seven policies are green
on it, and `TestRealTraceBridgeResumeMarkerLoadBearing` proves the marker
is load-bearing: stripping it turns the two legal across-attempt
replays into a within-attempt duplicate and reds once-per-scope. Two
rendering notes: consecutive checkpoints coalesce to one (verdict
preserving; the engine's evaluation cost grows steeply with event
count), and the structural clear is once per scope per SYNC, attempts
included, granted to the scope's first WRITE (upsert or delete).

Division-of-labor note for the one-term rendering: clear grounding
persists across `ev_resume` BY DESIGN (committed rows survive the
crash), so in a whole-sync term an attempt-1 clear legitimately
grounds an attempt-2 write and policy 2 cannot red the
resume-without-regrounding class here. That class is covered on the
refimpl leg, which renders each attempt as its own term — a resumed
attempt writing without its own grounding reds clear-before-upsert
(`TestRefImplLegacyCrashResume`) — and its record-flavor incarnation
(the verdict-flip union) is a CONTENT violation owned by the
exporting test's content oracle, per the scope note below. The
policy module carries the same statement at its `ev_resume`
declaration.

The delete leg is fixtured too: `warm_replay_sync_tombstone.jsonl`
records a warm delta round that replays the base, overlay-upserts one
row, tombstones a departed row, and publishes — B3's within-page
commit order (rows, then tombstones, then the validator) appears
directly in the trace, and the exporting chaos test's content oracle
proves the tombstoned row is absent from the sealed artifact.
`TestRealTraceBridgeCatchesUngroundedDelete` plants the violation:
the same real delete with its grounding stripped reds
clear-before-upsert.

The instrument also produced a finding about the shipped resume path:
for a mid-chain cut, the resume RE-RUNS the replay copy regardless of
checkpoint cadence — checkpoints commit at batch boundaries and a page
chain runs inside one batch, so `MarkSourceCacheReplayed` from a cut
chain never reaches a checkpoint. The resume suite's prior comment
claimed the restored replayed-set skips the copy; the trace recorder
is the first instrument able to distinguish that skip from an
idempotent re-copy, and it falsified the claimed mechanism (the
corrected comments live in `chaos_source_cache_resume_test.go`; the
convergence conclusion was always right, via B5 idempotence). The
replayed-set's real skip role is within-attempt: a later replay
annotation for an already-copied scope skips.

The instrument's second finding was a live defect, model-predicted:
the walker calibration's scenario-1 family (the phantom union, tc1c
flavor — `formal/walker/CALIBRATION.md`) was REACHABLE in the shipped
syncer via the verdict-flip path. A warm round cut after its replay
copy committed but before its validator published, upstream moving
between attempts, and the resume's consult missing meant the connector
served a fresh RECORD round — which composed with the crashed
attempt's copied debris and sealed the union under the fresh validator
(the non-self-healing direction: the next sync's consult validates the
entry clean and replays the mosaic forward). Witnessed by
`TestChaosSourceCacheRecordFlipOverReplayDebris`
(`pkg/sync/chaos_source_cache_resume_test.go`); fixed by RECORD-ROUND
GROUNDING: a record round is a replacement listing, so before its
first write to a scope this attempt, a partition holding rows that no
completed round published is cleared (`ClearSourceCacheScope` — the
replay unit's clear leg exposed standalone; `groundRecordScope` in
`source_cache_orchestration.go`). The fix is trace-visible: record
rounds now emit a REAL `ev_clear` before their first write —
"replacement rounds clear first", previously granted structurally by
the renderer, now witnessed in `cold_record_sync.jsonl` — and the flip
scenario is fixtured as `warm_replay_sync_record_flip.jsonl`, where
attempt 2's clear with no replay after it IS the grounding. Scope
note, pinned honestly: the ordering policies do NOT red the un-fixed
flip — attempt 1's real clear grounds the scope durably across resume,
so the union was ordering-legal; it is a CONTENT violation (the
walker model's `P1-CONTENT`), owned by the exporting test's content
oracle. The policies' role in the fix is the positive direction: the
grounded trace joins the green suite. (The grounding rule is now also
a modeled mitigation on the P side — walker MS-CO-003 — whose ladder
both kills the toggle and REGISTERS a residual the shipped skip
leaves open: a replay round that completed and PUBLISHED inside the
crash window is exempt from grounding, and a verdict-flipped re-run
then unions over its rows. See `walker/CALIBRATION.md`, MS-CO-003.)

The instrument's third finding is a STANDING known-defect pin — the
first fixture committed with a deliberately red expectation. Sessions
commit durably at op time, OUTSIDE the checkpoint mechanism
(`SessionSet` batches commit immediately in the pebble engine), so a
crash rolls the cursor back but not the session namespace: writes from
beyond the restored checkpoint survive into the re-run window, which
can then consume its own dead attempt's "future" (CO-6b-009). The
session vocabulary (`ev_swrite`, `ev_sread_hit`, `ev_sread_miss`, keys
on the one-key `k1` envelope) and policy 6
(`session_ckpt_consistency`) state the root cause: post-crash session
state must equal the restored checkpoint's, in both directions —
ZOMBIE (a dead attempt's beyond-checkpoint write observed by the
re-run) and AMNESIA (a checkpoint-committed value silently deleted;
its producing work never re-runs — the shape of the reverted
resume-clear fix). The fixture `warm_replay_sync_session_zombie.jsonl`
is recorded from a real crash/resume execution by
`TestChaosSourceCacheSessionPersistsAcrossResume`, which acts as the
session actor (the chaos connector has no session plumbing): the
probe write and the re-run read fire through the same recorder at the
moment of their real store operations. The oracle judges it
`violation: session-zombie-read` — asserted as the EXPECTED verdict in
`realTraceExpected`. When checkpoint-consistent sessions land (the
registered future work), the recorded read becomes a miss and the
expectation flips to "ok". The same constraint is model-side in the
walker as the P6-C monitor, whose three cells red the shipped
semantics (zombie), red the rejected wholesale clear (amnesia), and
green the checkpoint-consistent fix — `formal/walker/CALIBRATION.md`,
decision 25.

The instrument's fourth leg is the external-principal phase
(`SyncExternalResources` — an outside source's principals copied into
the sync store beside connector rows). The syncer records `ep_list`
after the source's answer is listed, `ep_live` per member, `ep_recon`
when `deleteStaleExternalPrincipals` COMPLETES (deletes applied or
none needed — the warn-and-continue degrade branch of a non-deleting
engine records nothing), and `ep_copy` per committed principal write.
Two fixtures are exported by the existing external-principal chaos
tests: `external_resume_current_answer.jsonl` (five attempts,
capable engine, shrink mid-sync — green on all seven policies: every
attempt that COPIES does so only after a completed `ep_recon`;
mid-phase crash attempts re-list and are cut before reconciling,
which is legal — the recon gate binds copies, not lists) and
`external_resume_sqlite_degrade.jsonl` — a STANDING KNOWN-DEGRADE PIN,
expected `violation: ext-recon-before-copy`: SQLite cannot delete, the
resume warns and copies the current answer over the dead attempt's
unreconciled debris. That is the ACCEPTED degradation (one-artifact
staleness, self-healing at the next cold sync, no replay channel to
launder it further), documented mechanically as an expected red
exactly like the session pin. The renderer maps distinct principal
ids onto the two-principal envelope (p1/p2) in first-seen order and
projects further principals out — sound for the kept ones, since the
policy tracks principals independently and the recon gate is
principal-agnostic. Bridge validation for the other direction:
`TestRealTraceBridgeCatchesStaleExternalSurvivor` strips the final
attempt's reconciliation and copies from the capable-engine fixture's
REAL events and the oracle reds `ext-stale-survivor`. The same
contract is model-side in the walker as the P8 monitor (scenario 8,
`formal/walker/CALIBRATION.md`): two calibrated reds — the
non-deleting engine's stale survivor, the stale-list recency mutant —
and three greens including the completed-then-crash carry
(sync-scoped freshness).

This leg has an executable instance: `host/refimpl/` (the demand-graph
reference implementation) emits canonical traces from real executions
of the phantom-union scenario, rendered by `RenderOccult` and checked
through the oracle in `host/refimpl_oracle_test.go`. The run also
documents the oracle's DIVISION OF LABOR empirically: the legacy
composition bug (misgrounded diff) seals a phantom artifact with a
policy-clean trace — ordering policies cannot see it, the algebra owns
it (`phantom_test.go`) — while the legacy resume-without-regrounding
habit is an ordering bug and fires clear-before-upsert on the resumed
attempt.

## Engine runtime trace checker: evaluated, not adopted

The engine ships a runtime trace checker
(`../occult/runtime_trace_checker.go`) with policies over a HARDCODED
vocabulary (`consume/emit/commit/checkpoint/ack` —
`runtime_trace_policies.go`). Two of its obligations rhyme with ours
(checkpoint-before-progress ~ its commit/checkpoint ordering), but the
vocabulary cannot express scopes, consults, or seals without a mapping
layer that would erase exactly the distinctions our policies check.
Decision: the oracle is the pure-Occult policy set of deliverable 7,
run through the engine host (`host/trace_policies_test.go`); the
runtime checker stays unused for this track. If the engine grows
user-defined trace vocabularies, revisit.
