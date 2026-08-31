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

Pending extension, tracked not modeled: generation stamps (the graph
model's variant-S lineage).

## Mapping 1: P model announce events

The walker and graph models announce through the monitor event
vocabulary (`formal/walker/PSrc/Events.p`, `formal/graph/PSrc/Events.p`).
A P counterexample trace renders onto the canonical list by keeping
announce events in schedule order and dropping everything else:

| P announce | Canonical |
|------------|-----------|
| `eAnnConsult` | `ev_consult(s)` |
| `eAnnClear` / the clear leg of `eReplayUnit`/`eGReplayUnit` | `ev_clear(s)` |
| `eAnnReplay` (copy commit) | `ev_replay(s)` |
| `eAnnUpsert` | `ev_upsert(s)` |
| `eAnnPublish` / `eGSessionPub` (artifact-facing) | `ev_publish(s)` |
| `eAnnCheckpoint` | `ev_checkpoint` |
| `eAnnSealed` | `ev_seal` |

The atomic units (`eReplayUnit`, `eOverlayUnit`, `eGOverlayUnit`)
expand to their leg order — clear, copy, marker, publish — because the
policies check the leg ordering that the units guarantee by
construction; running unit-built traces through the oracle is a
consistency check of that guarantee, not new information. Crash
scenarios cut the list at the crash point: a cut trace must still
satisfy the prefix-closed policies (1–3), while the seal-anchored
policies (4–5) are vacuous without `ev_seal` — exactly the
sync-scoped-freshness stance the models take.

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
renders them onto the canonical vocabulary and checks all five
policies — 10/10 cells green.

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
(`warm_replay_sync_interrupted.jsonl`). All five policies are green on
it, and `TestRealTraceBridgeResumeMarkerLoadBearing` proves the marker
is load-bearing: stripping it turns the two legal across-attempt
replays into a within-attempt duplicate and reds once-per-scope. Two
rendering notes: consecutive checkpoints coalesce to one (verdict
preserving; the engine's evaluation cost grows steeply with event
count), and the structural clear is once per scope per SYNC, attempts
included, granted to the scope's first WRITE (upsert or delete).

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
grounded trace joins the green suite.

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
