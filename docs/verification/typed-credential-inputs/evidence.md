# Typed Credential Inputs Verification Evidence

Plan: [`plan.md`](plan.md)

Implementation revision: `82323321`.

| Criterion | Status | Instrument | Evidence |
| --- | --- | --- | --- |
| C1 | pass | `TestValidateCredentialIssueRequestSchema`, `TestValidateCredentialIssueRequestDataTypes` | Supported scalar/collection kinds and structural schema checks pass. |
| C2 | pass | `TestValidateCredentialIssueRequestSchema`; CO-2, CO-3, CO-4, CO-5 | Duplicate fields, unsupported kinds, invalid patterns/defaults/suggestions, oversized collections, invalid constraint references, secondary fields on non-DEPENDENT_ON constraints, overlapping DEPENDENT_ON lists, and required-field aggregates that cannot fit the request-data cap are rejected at capability construction. |
| C3 | pass | existing credential issue suite plus `TestIssueCredentialValidatesAndForwardsRequestData` | Validation uses the schema on the descriptor selected by shape and output resource type. |
| C4 | pass | `TestValidateCredentialIssueRequestData`, `TestValidateCredentialIssueRequestConstraints` | Unknown/missing/wrong-kind/rule/constraint failures are covered. |
| C5 | pass | `TestValidateCredentialIssueRequestData` | Nil schema plus nil data remains valid. |
| C6 | pass | `TestIssueCredentialValidatesAndForwardsRequestData` | A valid `Struct` reaches `CredentialIssueInput.RequestData` unchanged. |
| C7 | pass | complete `pkg/connectorbuilder` test suite | Existing option, scope, audience, expiry, and key-profile tests pass with typed inputs enabled. |
| C8 | pass | `TestCredentialIssueTypedInputsWireRoundTrip`, `buf lint`, `buf breaking --against '.git#tag=v0.26.0'`, `make protogen` | Additive fields round-trip, compatibility checks pass, and regeneration leaves the tree clean. |
| C9 | pass | `TestIssueCredentialValidatesAndForwardsRequestData` | Invalid data leaves the recording issuer untouched; the corrected request then succeeds. |

## Commands

- `GOTOOLCHAIN=go1.25.2 go test -race ./pkg/field ./pkg/connectorbuilder ./pkg/tasks/c1api` — pass.
- `buf lint` — pass.
- `buf breaking --against '.git#tag=v0.26.0'` — pass.
- `make protogen` followed by `git status --short` — pass; clean tree, no generated change needed.
- `GOTOOLCHAIN=go1.25.2 go test ./...` — pass; exit 0, no failing packages.
- `GOTOOLCHAIN=go1.25.2 golangci-lint run --timeout=10m` — pass; 0 issues.
- `make lint` — unavailable under the default Go 1.27 toolchain: the installed
  golangci-lint 2.9.0 panics on Go 1.27 export data (binary built with
  go1.26). The panic reproduces at the pre-change revision without the
  feature code. The full-repo lint above passes under the matching
  GOTOOLCHAIN, and `go test ./...` under default Go 1.27 hits the same
  `cockroachdb/swiss` `!go1.27` build constraint in the compat/crash
  harnesses as before; failure sets are identical with and without these
  changes.

The limited gates reproduce without the feature code and are toolchain
compatibility constraints, not typed-input validation failures.
