# Formal model of sync scheduling semantics

This directory holds the P model of baton-sdk's sync scheduling semantics
and its supporting documents, per
`docs/tasks/sync-formal-model-brief.md`. This is a public repo: no customer
names, tenant IDs, or internal infra in any artifact here, including model
comments and trace renderings.

## Contents

- `GLOSSARY.md` — deliverable 0: pinned vocabulary. Read this first; every
  other document and every P identifier uses these meanings.
- `MODEL_SPEC.md` — the frozen model specification (machines, abstraction
  decisions, crash semantics, properties, calibration configurations).
  Frozen before P code is written; deviations discovered during modeling
  are appended as change orders, never silently absorbed. The spec receives
  an adversarial review before any green checker run is trusted.
- `GRAPH_MODEL_SPEC.md` — deliverable 4: the demand-graph runtime model
  spec (frontier scheduler, generations, sweep, lineage bake-off E vs S).
  v4, FROZEN after three adversarial review rounds; inherits MODEL_SPEC's
  conventions and adopts its design-variant verdicts (unit-mode
  materialization) as settled hand-offs.
- `reviews/` — adversarial review rounds for the specs, one file per
  round; dispositions are logged in the specs' change-order sections.
- `walker/` — P project: the tiered walker + source-cache replay (6b)
  calibration model (deliverables 2–3). Calibrated and frozen; see
  `walker/CALIBRATION.md`.
- `graph/` — P project: the demand-graph runtime model (deliverable 4).
  Built after `GRAPH_MODEL_SPEC.md` cleared review. Calibrated and
  frozen: the full 66-cell matrix (G1-G9) sweeps clean at 10k
  schedules, plus the 12-cell bake-off phase (12/12 on declared
  verdicts); see `graph/CALIBRATION.md` for the run log and the
  calibration finds (GS-CO-001..005, G8B-CAL-1, G9-CAL-1).
- `graph/BAKEOFF.md` — deliverable 4's written recommendation: the
  lineage bake-off verdict (variant S — observable-causal stamps),
  assembled under GS-CO-005's registered decision rule. This is the
  artifact the demand-graph RFC cites.
- `occult/` — the deductive track (deliverables 6–9): equational laws
  of the composition algebra and stamp lattice proved by equality
  saturation in the Occult engine (sibling repo `../occult`), the
  trace-policy oracle set, the session-typed protocol contract with
  per-role projections, and the trace-bridge note. Includes the
  broken-vs-good pairing: the phantom union DERIVED deductively from
  the broken composition rule, and an executable reference
  implementation of the demand-graph runtime validated against the
  trace oracle (legacy mode reproduces the broken behavior and is
  caught). The trace bridge is closed against the SHIPPED syncer too:
  `pkg/sync` chaos tests record real commit-order traces
  (`pkg/sync/sync_trace_audit.go`) exported as fixtures and checked by
  the same oracle. See `occult/README.md` for status and
  `occult/LAWS.md` for the law inventory.

## Toolchain

The P checker (https://p-org.github.io/P/) installs as a dotnet global
tool:

```bash
dotnet tool install --global P   # p --version → 3.1.0 at time of writing
```

Build and check (from a project directory such as `walker/`):

```bash
p compile
p check -tc <TestCaseName> -i 10000
```

## Standing rules

- Calibration before trust: the model earns authority by mechanically
  rediscovering the known bugs (brief, "Calibration cases") with the
  corresponding mitigations toggled off, and verifying the shipped/staged
  fixes close them when toggled on. A green checker run means nothing
  until the model has found the bugs we already know about.
- The model validates designs, not Go code. The bridge to the
  implementation is trace-driven (deliverable 6).
- Candidate invariants are inputs to be checked, not assumptions;
  refutation is a success.
