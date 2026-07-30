# Internal chaos connector

This document freezes the initial contract and verification plan for the
SDK-owned adversarial connector. The connector is test infrastructure, not a
customer-facing example and not a source of unstructured randomness.

Its purpose is to make connector behavior an explicit environment input:
every injected behavior is reproducible, every required injection is observed,
and every verdict is judged by a contract-specific oracle.

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
`connectorbuilder.ConnectorBuilderV2` through two adapters:

- a direct in-process client for fast exhaustive sweeps;
- an in-process gRPC client for protobuf serialization and `Any` round trips.

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

Seeds, schedules, and traces are emitted as replay artifacts. Seeded schedules
are measured sampling, never closure.

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

Resource, entitlement, and grant calls inject retryable, warn-and-drop, and
fatal outcomes. Tests assert attempts and budgets, exact tagged omissions,
error identity, sealing behavior, and cold-resume convergence where promised.

### Stage 4: pagination and liveness

Repeated and cyclic tokens, empty pages with continuation, overlapping pages,
duplicate spawned cursors, endless unique tokens, retry drift, and topology
changes must terminate correctly or fail classified within budget.

### Stage 5: response policy

Annotation and general response mutations exercise known, unknown, malformed,
duplicated, conflicting, misplaced, and oversized inputs. Invalid and
relationally inconsistent records use a written accept, normalize, skip,
reject, or fail policy. Missing policy is a blocking finding, not an invitation
for the test to invent one.

The initial named corpus contains one executable malformed-entitlement policy
and explicitly unresolved relational and temporal cases. `InitialDataCorpus`
is the policy registry: a case cannot become a gating test until its expected
treatment is no longer `unresolved`.

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
- combined connector and filesystem schedules;
- raw `os.*` calls outside existing injectable seams;
- closure over generated or seeded schedules;
- capability-specific mutation semantics before an independent oracle exists.

An exclusion remains a coverage entry and must not silently disappear from the
registry.
