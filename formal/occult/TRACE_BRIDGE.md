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

Pending extensions, tracked not modeled: attempt/resume markers
(multi-attempt traces), tombstone events (no policy gates deletes),
generation stamps (the graph model's variant-S lineage).

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

## Mapping 2: real sync executions

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
