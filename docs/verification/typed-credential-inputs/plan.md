# Typed Credential Inputs Verification Plan

Status: frozen pre-implementation baseline.

The public credential-issuance surfaces were inspected while establishing the
cross-repository design before this plan was written. No implementation edits
had been made. This records that deviation from the implementation-blind ideal
instead of overstating provenance. Changes to these criteria require an
append-only change order.

## Risk verdict and boundary

This is HIGH risk. Credential issuance has an external side effect, request
data crosses repository and process boundaries, and a contract mistake requires
coordinated SDK and host changes. The change is additive protobuf evolution:
connector-owned typed input schemas and request values are added while existing
credential options remain wire-compatible.

The SDK owns schema validity, descriptor selection, request-value validation,
and delivery of validated values to the connector implementation. The host owns
offering policy and must narrow, never expand, the connector schema. Host policy
is verified in the host repository and is not an SDK closure claim.

No hot-path cost curve changes: validation is linear in the descriptor's field,
rule, constraint, and submitted-value counts and runs once before issuance.

## Contract claims

- C1 — Each credential issue option may declare a typed request schema composed
  only of supported input field kinds, rules, and cross-field constraints.
- C2 — Capability publication rejects malformed schemas, including empty or
  duplicate field names, unsupported output-only/resource field kinds, invalid
  defaults/rules, and constraints that refer to undeclared fields.
- C3 — Descriptor resolution remains keyed by credential shape and output
  resource type; the selected descriptor alone defines the accepted request.
- C4 — Unknown request keys, missing required values, wrong protobuf value kinds,
  invalid rule values, and failed cross-field constraints are rejected before
  the connector implementation is invoked.
- C5 — Nil and empty request data remain valid for descriptors with no required
  typed fields, preserving existing callers and connectors.
- C6 — Valid scalar and collection values arrive unchanged in
  `CredentialIssueInput`; validation does not synthesize connector-owned values.
- C7 — Existing scopes, audiences, expiry, key-generation, and output-type
  validation continue to apply and coexist with typed request data.
- C8 — New protobuf fields are additive, use new field numbers, survive wire
  round trips, and pass repository compatibility and generation checks.
- C9 — Validation failure is side-effect free at the SDK seam: the issuer call
  count remains zero and a later valid request succeeds.

## Coverage model and oracles

The table-driven validator instrument covers these dimensions:

- schema: absent, empty, one field, multiple fields, duplicate/empty names,
  unsupported kind, required/optional, default present/absent;
- value kind: null, string, number/integer, boolean, list, object, and nested
  incompatible values;
- rules: accepted value plus every boundary and one violation for each supported
  string, integer, boolean, string-list, and string-map rule;
- constraints: satisfied and failed `required_together`, `mutually_exclusive`,
  `exactly_one`, and dependency relations, including an unknown field reference;
- request key set: omitted, exact, extra, and mixed valid/invalid;
- descriptor choice: one shape, same shape with distinct output types, and no
  matching descriptor;
- compatibility: legacy-only request, typed-only request, and both legacy and
  typed values.

Oracles:

- O1 — validation result and stable error code/message identify the rejected
  field or constraint;
- O2 — a recording issuer proves exact `CredentialIssueInput` equality and zero
  invocations on every invalid case;
- O3 — protobuf marshal/unmarshal equality for old and new message shapes;
- O4 — existing credential issue suites remain green without fixture changes
  that weaken their assertions;
- O5 — a planted validator bypass or inverted table expectation makes the
  focused test fail.

## Required evidence and closure

Each criterion is `not assessed` until `evidence.md` maps it to an executable
artifact and a passing command at a named commit. Required gates are focused
unit tests, race-enabled connectorbuilder tests, protobuf formatting/lint and
breaking checks, generated-code cleanliness, `make lint`, and `go test ./...`.
Structural coverage must disposition every uncovered changed validation branch.
A final-code review must produce no new correctness finding before closure.

## Change orders

### CO-1 — Defaults are host policy, not connector schema

Recorded after implementation inspection. `c1.config.v1.Field` has no default
value member. Connector schemas therefore define accepted and required values;
offering defaults belong to the host policy layered over that schema. In C2,
"invalid defaults" is not an SDK schema case. In the coverage model, schema
default-present/default-absent is replaced by host verification of offering
defaults. This does not weaken an SDK-side behavior because the underlying SDK
field contract cannot express a default.

### CO-2 — Correction: typed field variants do carry defaults

CO-1 was based on the outer `Field` message and missed `default_value` and
`suggested_value` on each accepted typed field variant. It is superseded by
this correction. Schema validation covers every non-zero/non-empty advertised
default and suggestion using the same rules as request data; proto3 scalar
fields do not preserve presence for zero values. C2 and the schema
default-present/default-absent coverage dimension remain SDK closure claims.

Credential issuance intentionally treats correctly typed empty strings, lists,
and maps as omitted when evaluating cross-field constraints. The generic action
argument validator treats every non-null value as present. These are separate
contracts: credential forms use config-field empty semantics, while action
arguments retain their existing wire-presence behavior.

### CO-3 — Secondary fields are DEPENDENT_ON-only

Recorded after implementation inspection. The schema validator checked duplicate
and unknown secondary field names for every constraint kind, but the request
evaluator reads `secondary_field_names` only for DEPENDENT_ON. A non-DEPENDENT_ON
constraint (e.g. MUTUALLY_EXCLUSIVE) with secondary fields therefore accepted a
schema whose secondary names were silently ignored at evaluation. Schema
publication now rejects nonempty `secondary_field_names` unless the kind is
DEPENDENT_ON, and the public `CredentialIssueRequestSchema` documentation states
the restriction. This tightens C2 without changing the shared `config.Constraint`
contract, which has no per-kind semantics annotation.

### CO-4 — DEPENDENT_ON lists must be disjoint

Recorded after final-code review. The schema validator rejected duplicates
within each constraint list and references to unknown fields, but accepted a
name appearing in both `field_names` and `secondary_field_names`. The public
`FieldsDependentOn` DSL rejects that cross-list overlap because the
dependency would be self-satisfied; the request evaluator likewise counts
one submitted field on both sides. Schema publication now rejects the first
secondary name also contained in the primary names, with an error naming
the field, preserving declaration-order errors, unknown-name checks,
missing-secondary checks, and the non-DEPENDENT_ON prohibition. This
tightens C2 without changing the shared `config.Constraint` contract.

### CO-5 — Required-field aggregate feasibility at publication

Recorded after final-code review. Individual string bounds and list
MinItems/MaxItems were each capped, but nothing aggregated them: a required
list with 64 items of 2000 bytes minimum, or two required strings of 40000
bytes each, published successfully while every satisfying request exceeded
the 65536-byte request-data cap and failed request validation. Publication
now computes a conservative protobuf lower bound over unconditionally
required fields and rejects when a proven lower bound exceeds the cap. The
bound reuses `credentialIssueRequestFieldIsRequired` for requiredness,
saturates every intermediate at cap+1, clamps uint64 rule values before
conversion, sizes each list element as a complete Value, and floors
required lists at max(1, min_items) so an explicit MinItems=0 cannot
collapse it. Optional fields contribute zero and cross-constraint branches
are not summed: this is a publication-quality correction, not a proof that
every accepted schema is satisfiable, and no constraint solver is
introduced. This tightens C2; the runtime cap remains the enforcement
seam.
