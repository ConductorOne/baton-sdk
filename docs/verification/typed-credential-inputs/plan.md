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
