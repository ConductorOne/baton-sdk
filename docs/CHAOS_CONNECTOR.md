# Internal chaos connector

The chaos connector is a deterministic adversarial environment for verifying
SDK behavior in the face of poorly constructed connectors, buggy upstream
APIs, and unreliable execution environments. It is test infrastructure, not a
customer-facing example and not a source of unstructured randomness.

Its purpose is to make connector behavior an explicit environment input:
every injected behavior is reproducible, every required injection is observed,
and every verdict is judged by a contract-specific oracle.

Connector-visible behavior should be expressible through this harness; every
new or changed correctness obligation must identify executable evidence,
whether owned here or by a more appropriate specialized instrument.

It does not attempt to emulate arbitrary third-party APIs directly. Instead,
it represents their SDK-visible consequences: malformed and inconsistent data,
pagination defects, answer drift, transient and fatal errors, lost responses,
concurrency, interruption, and replay. Environment adapters extend those
deterministic scenarios to transport boundaries, persisted stores, and real
process death.

## Boundaries

The subsystem has five independent inputs and outputs:

1. A **scenario** describes deterministic connector data, capabilities,
   pagination topology, mutable state, logical time, and temporal epochs.
2. An **operation** identifies one connector call by domain, service, method,
   logical subject, page token, attempt, and injection phase.
3. A **schedule** matches logical operations and applies replayable effects.
4. A **trace** records calls, matches, effects, timing, and outcomes.
5. A **manifest** derives expected logical outcomes directly from the scenario.

The scenario and manifest must not inspect syncer state, C1Z contents, Pebble
keys, or results produced by the connector under test. Store and trace auditors
compare those independent expectations with observed behavior.

The initial implementation is internal to this module. It exercises a real
`connectorbuilder.ConnectorBuilderV2` through three adapter modes:

- a direct in-process client for fast exhaustive sweeps;
- an in-process gRPC client for clean-path protobuf serialization and `Any`
  round trips;
- an in-process gRPC server-fault mode where injected statuses and mutated
  responses cross serialization before the SDK observes them.

Subprocess execution is a later adapter over the same scenario and schedule
formats.

## Logical operation identity

Schedules match stable protocol identity, never incidental goroutine ordering:

- fault domain;
- gRPC service and method;
- resource type and logical resource or operation ID;
- page or stream token;
- attempt number;
- phase: before call, after delegate, or before response.

A schedule may use a global call ordinal only when it declares the scenario
serialized. Concurrent scenarios use logical keys and deterministic barriers.

## Initial effects

The schedule format is versioned. Its first version supports:

- classified gRPC errors;
- deterministic delay, blocking, and barriers;
- cancellation and deadline expiry;
- loss of a response after the delegated operation completed;
- response and annotation mutation;
- deterministic scenario epoch transitions.

The serialized `"crash"` effect is a cooperative in-process interruption. It
returns `ErrInterruptRequested`; it is process-death evidence only when a
process-capable harness translates it into termination. In-process tests that
close the store afterward claim persisted interruption/resume, not crash
durability.

Process death, transport resets, and filesystem failures have reserved fault
domains but are not claimed by the first implementation.

## Connector surface

The connector provides deterministic clean behavior and operation identities
for:

- metadata, validation, and cleanup;
- classic, static, targeted, and type-scoped collection;
- resource, entitlement, and grant pagination, including spawned page tokens;
- grant and revoke;
- resource and account lifecycle;
- credential rotation and issuance;
- event feeds;
- tickets;
- synchronous and asynchronous actions;
- direct resource lookup.

Asset streaming is explicitly excluded while
`pkg/connectorbuilder/assets.go` remains a disabled production stub. The
coverage registry must keep this exclusion visible.

Every new connector RPC, response type, capability, and annotation-bearing
field must be registered as supported or explicitly excluded with a reason.

### Event feed pagination

`EventFeedSpec` declares one feed as a flat, deterministic event log served
page by page from `Dataset.EventFeeds`. Cursors are offsets, independently
derived from `has_more` rather than conflated the way `Page[T].Next` is: a
stable cursor with `has_more=false` is a legitimate caught-up response, and
only `has_more=true` with a stuck cursor is a defect. `Builder.EventFeeds`
enumerates every feed declared in the active dataset, sorted by id, so
`ListEventFeeds` output is deterministic once a scenario declares more than
one feed.

A bridge test drives the real `pkg/tasks/local` event feed task manager
against this connector over the direct and in-process gRPC adapters, using
the harness's own trace as the cursor-chaining oracle rather than a
hand-rolled fake. `tasks.Manager.Process` takes the connector client as a
parameter, so no new harness machinery was needed to reach it.

This covers page-size-honoring pagination, cursor chaining, `start_at`
filtering, and rejection of an unknown or out-of-range cursor, for a single
declared feed with no fault injection. See "Coverage claims and exclusions"
for what remains open.

## Response and annotation model

The complete protobuf response is connector-controlled input. Mutation support
covers ordinary fields, repeated and nested messages, unknown protobuf fields,
and `google.protobuf.Any`.

Known annotations are classified by:

- legal attachment scope;
- cardinality and ordering;
- conflicts;
- validation requirements;
- whether they control collection, ingestion, provisioning, telemetry,
  compatibility, or carry domain data;
- the SDK obligation to consume, preserve, ignore, report, skip, or reject.

Unknown, malformed, duplicated, conflicting, misplaced, deprecated, and
oversized annotations are separate coverage cells. Reflective mutation does not
itself establish correctness: every mutation family needs a stated policy
oracle.
The mutation registry rejects any application whose before/after protobufs are
equal, so a fired mutation rule is not accepted as evidence when the selected
response made it a no-op.

## Oracle planes

No single final-store equation is sufficient. Scenarios declare the applicable
oracles:

- **data:** canonical records, values, provenance, and referential closure;
- **lifecycle:** sealed, resumable-unfinished, or cleanly failed;
- **control:** classification, retries, fallback, attempts, and cancellation;
- **liveness:** completion or classified failure within a stated budget;
- **side effects:** the declared idempotency or exactly-once obligation;
- **evidence:** warnings, errors, metrics, traces, and fired-rule counts;
- **resources:** workers, streams, handles, and processes terminate.

Changing connector answers may weaken completeness legitimately. Such scenarios
state their weaker oracle in advance; for example, termination, deduplication,
and referential closure without grant completeness.

## Instrument validation

Every required schedule rule has a minimum fire count. A run fails when a
required rule did not fire, even if all product assertions passed.

Each distinct oracle or failure mechanism gets a representative planted
violation demonstrating that the harness reports failure. A red result is
classified as product defect, invalid premise, injector defect, oracle defect,
or unresolved contract before production code changes.

Identity, trace, semantic-content, and lifecycle oracles have explicit
negative controls. The semantic and lifecycle integration tests call the same
error-returning comparators calibrated by those controls rather than duplicating
assertion logic. Lifecycle policy representatives run through both the direct
and in-memory gRPC adapters. The tag-gated real-binary instrument also has a
Pebble chaos mode that persists a retained dangling row, expires one process,
SIGKILLs the next, and verifies a final process converges to the uninterrupted
baseline.

Sync tests use one typed harness owner for builder, transport, syncer, and
cleanup wiring. The process instrument resolves the same named lifecycle-case
registry rather than independently selecting or rebuilding policy fixtures.
Execution remains deliberately separate: in-process tests provide precise cut
control, while the real-binary driver provides OS-death semantics.

Semantic observations exhaustively page the public store interface and reject
unknown entity kinds. Optional expected fields use explicit pointers, allowing
the oracle to distinguish “must be empty” from “not part of this assertion.”
Identity snapshots key entitlements by resource type, resource ID, and public
entitlement ID; grant identities include that structured entitlement identity
plus the structured principal identity. Bare public IDs are never treated as
globally unique by the oracle.

Seeds, schedules, and traces are emitted as replay artifacts. Seeded schedules
are measured sampling, never closure.

Matcher string fields are explicit optional values: nil is a wildcard and a
pointer to `""` matches an actual empty value such as the root page token.
Effects execute in their declared order, and after-delegate effects do not
replace a real delegate error. Runs clone their scenarios on construction and
return cloned public views so a corpus fixture cannot race or rewrite the
active connector world.

## First verification stages

### Stage 1: framework and surface

- replay is stable across repeated runs;
- concurrent call order does not change logical matching;
- required-rule accounting catches vacuous schedules;
- every registered connector RPC has clean deterministic behavior or an
  explicit exclusion;
- advertised capabilities equal implemented capabilities.

### Stage 2: clean collection

One scenario combines classic, static, targeted, and type-scoped resource
types, sequential pages, and spawned cursors. Worker counts one and many must
produce the exact canonical manifest.

### Stage 3: error obligations

Resource, entitlement, and grant calls inject retryable, lost-response,
warn-and-drop, and fatal outcomes through direct, client-fault gRPC, and
server-fault gRPC modes. Tests assert attempts and exact call budgets, exact
omissions, error identity, sealing behavior, same-sync cold-resume convergence,
and whole-store equivalence to an uninterrupted run where recovery is
promised.

### Stage 4: pagination and liveness

Repeated and cyclic tokens, empty pages with continuation, overlapping pages,
duplicate spawned cursors, endless unique tokens, retry drift, and topology
changes must terminate correctly or fail classified within budget.
Blocked connector calls are also expired by a real context deadline, while an
explicit connector cancellation covers the second cancellation source. Both
must return within a wall-clock bound, leave no active call in the fault
wrapper, leave one unfinished sync, and cold resume that same sync to manifest
identity and whole-store equivalence. This is connector-call accounting, not a
general proof that every process goroutine terminates.

### Stage 5: response policy

Annotation and general response mutations exercise known, unknown, malformed,
duplicated, conflicting, misplaced, and oversized inputs. Invalid and
relationally inconsistent records use a written accept, normalize, skip,
reject, or fail policy. Missing policy is a blocking finding, not an invitation
for the test to invent one.

The sync-level mutation representatives require malformed known control
annotations to fail without sealing, a cleared continuation token to seal only
the connector-visible prefix without requesting the hidden page, and list
reordering to preserve complete logical content. Each mutation is targeted at
a non-empty response where it can change behavior and runs through direct,
client-fault gRPC, and server-fault gRPC modes.
`BatonID` is an SDK-reserved ownership marker: primary connector resources that
carry it are rejected before ingestion, preventing connector-controlled bytes
from being mistaken for externally copied principals during reconciliation.

`ReferentialCorpus` generates the closed resource-identity,
entitlement-to-resource, and grant-entitlement-by-principal matrix. Its 77
named cells each carry a policy and scenario mutator and run through the full
sync lifecycle over both transports, drop counters, sealing check, and
store-presence oracle. New
reference shapes are added to the applicable path vocabulary and therefore
expand the grant cross-product automatically. `InitialDataCorpus` remains the
registry for non-referential representation, temporal, and legal-hostility
cases; a case cannot become gating while its policy is `unresolved`.

`SemanticCorpus` adds same-page and cross-page duplicate identities plus
missing, unknown, self-cyclic, and mutually cyclic parent references. Duplicate
tests assert canonical multiplicity and final content, making overwrite order
independent of page boundaries; every case runs through both transports.
`TemporalCorpus` loses the first response,
changes the scenario epoch, and verifies that resource, entitlement, and grant
retries converge to the retry answer without retaining the unseen answer.

`ConcurrentDuplicateCorpus` uses spawned entitlement/grant cursors and
independent parent-scoped requests for the same child resource type, with
barriers to force both conflicting-response completion orders for resources,
entitlements, and grants. A live run retains the last completed write. Its
interruption/resume variant verifies that the complete pending frontier is replayed
inside the same sync run; with one resume worker, stable action order
determines the last write regardless of which sibling was interrupted before
the persisted interruption. The complete connector-visible store is compared with an
uninterrupted one-worker reference run, not only with the contested row.
The harness proves that both conflicting values were observed, but the SDK
does not yet emit exact entitlement/grant conflict counters: doing that would
require either a read before every put or sync-wide identity state. Neither
cost is introduced implicitly by these tests.

`LifecycleCorpus` crosses one representative of each data policy with an
interrupted entitlement page and persisted resume. It verifies that dropped
rows remain absent, hard-invalid rows cannot seal, retained dangling rows
survive, and an interrupted response is replaced by the resume-time answer.
Page-chain replay is at-least-once: a dropped row before the cut is observed
and counted once in each attempt, while remaining absent from both artifacts.
Every successful resume must finish the original sync ID and match the complete
logical content of an uninterrupted run against the resume-time scenario.

`ExternalPrincipalCorpus` composes two connector worlds: a sealed external
user/group sync and an internal sync containing external-match grant carriers.
It covers match-all, ID, email, user-profile, group-profile, misses, and
expandable-entitlement remapping through both transports. Each case is also
cut after rewritten grants are put but before its carrier is deleted, then
resumed against the persisted checkpoint. A changed-external-answer case
verifies replay converges to the principals visible at resume time. Its oracle
checks multiplicity, unresolved carriers, expansion targets, sealing, original
sync identity, and whole-store equivalence to a clean run against the current
external source.
Cleanup walks each entity keyspace once, deletes dependent grants and
entitlements by structured identity before the stale principal, and has
separate durable cuts for every delete loop. A scale-contract test holds scan
passes constant while fixture size grows from one to one thousand rows.
SQLite does not expose the exact structured delete capabilities required by
that cleanup. On the deprecated engine, resume logs a warning, skips stale-row
reconciliation, continues ingesting the current external answer, and seals
instead of hard-failing the sync; a dedicated regression test pins that
degradation contract.

One bounded combined-fault case loses the first entitlement response, proves
that loss was observed, pauses the retry at a deterministic connector barrier,
then fails the first subsequent write-class Pebble filesystem operation. It
cuts a strict crash image before close, proves both fault domains fired,
reopens the image as
resumable-unfinished, and requires the same sync run to seal with exact
manifest identities and connector-visible content equivalent to an
uninterrupted run. This is coverage of that named ordering only, not closure
over the connector-by-filesystem schedule product.

### Stage 6: checkpoint and resume

The shared scenario model replaces duplicated topology fixtures only after the
new clean and faulty oracles are validated. Existing checkpoint, response-cut,
double-cut, changed-worker, queue-audit, real-process, compatibility, and
errorfs instruments retain their distinct coverage claims.

`CursorGraph` is the shared transport-independent pagination topology. The
randomized scheduler soak uses its generator directly; hand-built checkpoint
and changed-answer fixtures use the same type directly while retaining their
specialized cut connector and queue-audit instrumentation.

## Coverage claims and exclusions

Bounded deterministic cases run in ordinary CI. Exhaustive cut and race-heavy
checks belong to `test-extra`; longer seeded combinations belong to nightly.
`make chaos-check` runs the focused bounded race suite.
`make chaos-soak CHAOS_ITERATIONS=N` replays generated fan-out scenarios with
deterministic retry schedules; `BATON_CHAOS_SEED` selects the first replay seed.

The first implementation does not claim:

- asset streaming;
- real subprocess or Lambda behavior;
- malformed wire bytes, trailers, or connection resets;
- combined connector and filesystem schedules other than the named
  lost-entitlement-response/first-subsequent-write case;
- raw `os.*` calls outside existing injectable seams;
- closure over generated or seeded schedules;
- deletion of disappeared external resource-type rows: resource types do not
  yet carry store-owned provenance, and deleting by a shared public type ID
  could orphan primary-connector resources;
- capability-specific mutation semantics before an independent oracle exists;
- event feed fault injection: retryable and fatal errors, deadline expiry,
  and lost responses mid-pagination (the class of defect that motivated
  CE-1027) are not yet exercised for event feeds;
- a multi-feed scenario: `Builder.EventFeeds` already enumerates every feed
  a dataset declares, sorted by id, but no scenario or test yet declares
  more than one, so that path and the `ListEventFeeds` ordering guarantee it
  exists for are unexercised;
- annotation-bearing event responses and event-specific response mutation
  coverage: `EventFeedSpec` never populates `Event.annotations` or
  `ListEventsResponse.annotations`, and the generic `next_page_token`
  mutation representative does not generalize to the `cursor` field name
  event feeds use;
- an oracle for `pkg/tasks/local`'s per-page stderr log fields (`page`,
  `events`, `duration_ms`, `cursor`): nothing in the harness observes log
  output today.

An exclusion remains a coverage entry and must not silently disappear from the
registry.
