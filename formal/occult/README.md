# Occult track — deductive verification of sync scheduling semantics

The parallel track to the P models (brief:
`docs/tasks/sync-formal-model-brief.md`, "Division of labor with
Occult"). P explores reachability; this track proves stated laws and
checks traces deductively with the Occult engine (sibling repo,
`../occult`). The same semantics are never modeled twice — the seams
are (a) equational proofs consumed by the P models as assumptions and
(b) one trace-policy set intended to check both P counterexample
traces and real sync executions.

## Contents

- `LAWS.md` — deliverable 9: the equational law inventory
  (composition algebra, stamp lattice) with per-law status. The P
  model specs cite this document.
- `TRACE_BRIDGE.md` — deliverable 6: the canonical trace vocabulary,
  the mappings from P announce traces and real sync executions onto
  it, and the runtime-trace-checker evaluation.
- `src/` — pure `.occult` sources:
  - `sync_laws.occult` — defining equations of the composition
    algebra, row-map observations, and stamp dead-membership.
  - `sync_fixtures.occult` — free Skolem constants for law checks.
  - `sync_phantom.occult` — the KNOWN-BROKEN composition vs the
    premise-validated one: the phantom union stated as an algebra
    (stale-attested replay base + last-sync-grounded diff).
  - `sync_protocol.occult` — deliverable 8: the syncer↔connector
    source-cache session terms (offer→ask→answers, bounce cap 4
    structural) with per-role projections via the engine's
    protocol/projection stdlib.
  - `sync_trace_policies.occult` — deliverable 7: the five
    ordering/durability policies as recursive verdict functions, with
    green/red fixtures.
- `host/` — the verification harness: a standalone Go module (local
  `replace` onto `../occult`; NOT part of the baton-sdk public module)
  that loads the sources axiomatically and drives the engine's
  equality-saturation and evaluation pipelines. Test files:
  `laws_test.go`, `compression_test.go`, `phantom_test.go`,
  `protocol_test.go`, `trace_policies_test.go`,
  `refimpl_oracle_test.go`, `real_trace_oracle_test.go` (real syncer
  traces — see below), `raft_probe_test.go` (harness positive
  control), `pipeline_test.go` (the shared load/bridge/saturate
  sequence).
- `host/testdata/realtraces/` — JSONL trace fixtures recorded from
  REAL `pkg/sync` executions by the chaos harness
  (`pkg/sync/chaos_trace_oracle_test.go`); regeneration instructions
  are in that file's header comment.
- `host/refimpl/` — an executable REFERENCE implementation of the
  demand-graph runtime's per-scope loop (the known-good algorithm,
  which has a frozen P model but no production implementation), with a
  LEGACY mode reproducing the known-broken algorithm's habits. A
  modeling artifact, not production code.
- `tests/` — CLI probes (`probe_axiom_fire.occult` documents why plain
  CLI file loading is definitional, which is what forced the host).

## Running

```bash
cd formal/occult/host && go test -timeout 30m ./...
```

(The saturation suites exceed go test's default 10-minute timeout
when run together.)

Requires the sibling engine checkout at `../occult` (the go.mod
`replace` points there). The pure `.occult` sources carry no Go
dependency; baton-sdk's public module does not require the engine
repo.

## Deliverable status

- 9 (equational proofs): DONE — 15 laws closed by equality saturation
  (L1–L6, L8 over free Skolem constants), L7 loaded as the stdlib
  semilattice assumption, L9a/L9b ground-exhaustive over generations
  0..7; 5 saturation negative controls + 1 ground false-live control
  refused (the false-live control reproduces G9-CAL-1's parity
  ambiguity deductively). See `LAWS.md`.
- 7 (trace-policy oracle set): DONE — five policies, 30-cell verdict
  matrix (6 fixtures × 5 policies): green satisfies all, each red
  violates exactly its own policy. Pending extensions: multi-attempt
  traces, tombstone events.
- 8 (MPST protocol contract): DONE — 7 projection derivations (syncer
  = P_leader, connector = P_follower; direct, one-bounce, and maximal
  four-bounce sessions; record and replay legs) + 4 polarity/shape
  controls. Bounce cap is structural (no five-bounce term exists);
  stuck-freedom and cap-violation checking are open engine work.
- 6 (trace bridge): DONE — see `TRACE_BRIDGE.md`. The engine's
  runtime trace checker was evaluated and NOT adopted (hardcoded
  vocabulary); the deliverable-7 policy set is the oracle. The "real
  executions" leg is IMPLEMENTED end to end: the shipped syncer
  carries a test-only commit-order recorder
  (`pkg/sync/sync_trace_audit.go`), the chaos harness exports cold-
  and warm-sync JSONL fixtures, and `real_trace_oracle_test.go`
  checks them against all five policies (10/10 green) with planted-
  violation validation of the bridge itself. The refimpl leg remains
  as the demand-graph instance of the same oracle.

## Broken vs good, both ways

Two demonstrations pair the known-broken algorithm against the
known-good one:

- DEDUCTIVE (`phantom_test.go` + `src/sync_phantom.occult`): the
  engine PROVES the broken composition manufactures the phantom union
  — a row deleted upstream survives in the sealed artifact — while
  every ingredient response is individually truthful, and proves the
  premise-validated grounding yields exactly upstream truth from the
  same stale-cache ingredients. The controls prove the broken artifact
  is NOT the epoch it claims to be.
- EXECUTABLE (`refimpl_oracle_test.go` + `host/refimpl/`): the
  demand-graph reference implementation runs the same scenario to seal
  — content matches upstream and every attempt trace passes all five
  policies, with and without a crash. In legacy mode the sealed
  artifact carries the phantom row: on a clean run the ordering
  policies are (correctly) blind to it — the composition class belongs
  to the algebra — and on a crash/resume run the legacy
  resume-without-regrounding habit is caught by the trace oracle,
  firing exactly clear-before-upsert on the resumed attempt.

## Engine findings (for upstream)

- Plain CLI file evaluation loads `=` definitionally; axiomatic
  loading needs a host (`LoadStdlib`) — no CLI flag exists for it.
- Same-line trailing `#` comments after statements fail to parse in
  stdlib-loaded modules; full-line comments only.
- Parameterized protocol definitions (`f(r) = ... send(r) ...` with
  `r : Protocol`) gate their rewrite on classifier membership that
  free message tags don't carry; closed session terms (the raft shape)
  derive fine, and projection equivalences close as CONDITIONAL
  equivalences under `Domain:Protocol` membership premises (same as
  the engine's own raft test on the egraph backend).
- `pipe` has no associativity axiom: session terms and expected
  projections must share the same grouping.
