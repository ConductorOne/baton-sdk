# baton-sdk PR Review Criteria

Use these criteria as an additive layer on top of the base PR review prompt. Do not repeat
generic security and correctness checks; focus on the SDK contracts that downstream
connectors and platform code depend on.

baton-sdk is a shared library for hundreds of connectors. A local-looking change can affect
every repo that imports the SDK, every serialized sync state record that gets resumed later,
and every generated wire type. Review exported APIs, proto fields, serialized state,
defaults, and dependency changes as compatibility surfaces.

## Risk Triage And Escalation (do this first)

Before the detailed review, score the change per `docs/BUG_CATCHING.md` §2:
risk is escape times consequence. Score the failure mode, not the subsystem.

Escape axes — how likely a defect ships and persists:

1. **Silence** — if this change is wrong, what makes noise? A panic or a failed
   request is low; a well-formed wrong row, a silently skipped step, or an absent
   record is high.
2. **Durability** — does any effect outlive the process (c1z contents, sync tokens,
   session values, proto wire types, exported config schemas)? Durable output will
   be read by SDK versions that do not exist yet.
3. **Uncontrolled dimensions** — does correctness or liveness depend on goroutine
   schedule, crash or checkpoint timing, environment faults (network, disk, Lambda
   freeze/thaw), which SDK version wrote the data being read, or data volume?
   Scale is the dimension local testing undersamples: O(n^2) is correct at every
   fixture size and dead at whale scale.
4. **Consumer distance** — who interprets the output: the same package, another
   process, the c1 platform, downstream connectors, or a future SDK version?

Consequence — place the worst credible failure on the remediation ladder:
(1) redeploy, (2) re-sync, (3) migrate every artifact in the fleet, (4) coordinate
a contract change across repos and deployments we do not control, (5) irrecoverable
external side effects (provisioning writes, credential operations, deletions).

Verdict rules: silent + durable is HIGH (and compounds — detection latency grows
the migration bill). Any version-pair dependence is HIGH. Remediation rung 4 or 5
is HIGH regardless of escape score, and the ask shifts to prevention: golden
artifact corpus for format changes, dry-run/idempotency for external effects.
Otherwise, two or more escape yes answers is HIGH. Default-path changes outrank
option-gated ones.

Cost contracts: if the PR touches grant expansion, compaction, the per-checkpoint
loop, or anything that runs at artifact-open time (including migrations), require
a stated cost-curve delta (big-O in grants/entitlements/graph size) and a
benchmark that enforces it. A missing benchmark on these paths is a finding.
Open-time migrations must be chunked and resumable; an over-budget migration
retries the same artifact into a permanent every-sync failure.

Lead your review with a short triage block — each axis yes/no with a one-line
reason, then the verdict. For HIGH verdicts, recommend escalation beyond this
review; a single-shot CI review is advisory sampling, not coverage. Specifically:

- Name the review-blind class the risk falls in (multi-artifact / schedule /
  absence / error-path, per `docs/BUG_CATCHING.md` §2).
- Name the instrument that would give real coverage — differential oracle against
  full recomputation, two-artifact cross-version harness, `-race` plus seeded soak,
  fault injection at the seam, or a permutation table as a table-driven test — and
  state whether the PR already contains it.
- A HIGH-risk PR that names no instrument and no permutation table is itself a
  finding: request the full pass-set review per `docs/BUG_CATCHING.md` §6 before
  merge.

Do not attempt the full seven-pass review yourself; your job on HIGH is to flag,
route, and check that the claimed instruments actually exist in the diff.

## Exported Go API Stability

Treat exported APIs in `pkg/connectorbuilder` as connector contracts, especially:

- `ConnectorBuilder`
- `ResourceSyncer` and `ResourceSyncerV2`
- `ResourceProvisioner` and `ResourceProvisionerV2`
- action and event provider interfaces

Adding a method to a widely implemented interface breaks implementations that do not define
it. Changing or removing a method signature breaks all callers. New capability surfaces
should use a new versioned interface with an adapter from the old shape, following existing
patterns such as `resourceSyncerV1toV2` and `oldEventFeedWrapper`.

When a PR adds a new versioned interface, check that old-path behavior remains covered by
tests. Tests that only exercise the new path do not prove compatibility for existing
connectors.

Exported structs are also contracts. Removing fields, changing field types, or changing
zero-value behavior can break callers. Additive optional fields are the safe shape. Existing
functional options and constructors must keep their signatures and defaults; widen behavior
with new options or new helpers rather than new required parameters.

## Proto And Wire Compatibility

Proto files under `proto/c1/connector/v2/` and generated files under
`pb/c1/connector/v2/` are wire contracts, not ordinary implementation details.

- Do not renumber fields.
- Do not reuse field numbers.
- Do not change active field types.
- Do not remove active fields in place.
- Deprecate first, keep old numbers reserved, and regenerate checked-in Go output.

Any proto source change must include matching generated `pb/.../*.pb.go` changes. Stale or
hand-edited generated code is a correctness finding.

## Serialized State

Anything marshaled by one SDK version and unmarshaled by another must round-trip safely.
Pay close attention to pagination state and annotations:

- `pagination.Bag`
- `pagination.Token`
- annotation helpers that pack proto annotations through `anypb`

A change that alters serialized pagination tokens can strand in-progress syncs after a
deployment. Require compatibility tests for changes to these types or their marshal paths.

## Defaults

Default behavior must stay stable unless the PR clearly calls out a deliberate compatibility
break. Watch changes to retry behavior, timeout values, rate-limit handling, pagination
sizes, cache behavior, TLS behavior, annotations, and generated resource defaults. New
behavior should default to the old behavior and be opt-in where possible.

## Deprecation

Deprecation marks a migration path; it is not removal. Go symbols need `// Deprecated:`
comments and proto fields need `[deprecated = true]`. Do not remove a deprecated symbol in
the same change that introduces the deprecation, and do not remove a still-used deprecated
surface without a coordinated major-version plan.

## Versioning And Communication

Breaking SDK changes must be reflected in `pkg/sdk/version.go` and described in the PR. This
repo is still pre-1.0, so a 0.x minor bump can be the compatibility signal for a break; do
not assume a patch bump is safe for signature, wire, serialized-state, or default-behavior
changes.

If a change affects downstream connector behavior, the PR should include a migration note or
rollout explanation. There is no separate changelog gate for the reviewer to rely on.

## Dependency Review

Review `go.mod`, `go.sum`, and vendored dependency changes as part of the SDK contract.
Dependency bumps here propagate to downstream connectors. Confirm new or updated modules are
required by the code change, do not unexpectedly raise the minimum supported version surface,
and do not widen transitive behavior without a reason visible in the PR.

## Scope Control

Shared SDK convenience APIs become long-term commitments. If a helper is only needed by one
caller, check whether it belongs in that caller instead of the SDK. Prefer narrow additions
that preserve existing contracts over broad abstractions that create new obligations for all
downstreams.
