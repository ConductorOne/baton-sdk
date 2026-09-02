# Typed Credential Inputs Verification Evidence

Plan: [`plan.md`](plan.md)

Implementation revision: `39caa2e4`.

| Criterion | Status | Instrument | Evidence |
| --- | --- | --- | --- |
| C1 | pass | `TestValidateCredentialIssueRequestSchema`, `TestValidateCredentialIssueRequestDataTypes` | Supported scalar/collection kinds and structural schema checks pass. |
| C2 | pass | `TestValidateCredentialIssueRequestSchema`; CO-1 | Duplicate fields, unsupported kinds, invalid patterns, and invalid constraint references are rejected at capability construction. |
| C3 | pass | existing credential issue suite plus `TestIssueCredentialValidatesAndForwardsRequestData` | Validation uses the schema on the descriptor selected by shape and output resource type. |
| C4 | pass | `TestValidateCredentialIssueRequestData`, `TestValidateCredentialIssueRequestConstraints` | Unknown/missing/wrong-kind/rule/constraint failures are covered. |
| C5 | pass | `TestValidateCredentialIssueRequestData` | Nil schema plus nil data remains valid. |
| C6 | pass | `TestIssueCredentialValidatesAndForwardsRequestData` | A valid `Struct` reaches `CredentialIssueInput.RequestData` unchanged. |
| C7 | pass | complete `pkg/connectorbuilder` test suite | Existing option, scope, audience, expiry, and key-profile tests pass with typed inputs enabled. |
| C8 | pass | `TestCredentialIssueTypedInputsWireRoundTrip`, `buf lint`, `buf breaking --against '.git#tag=v0.26.0'`, `make protogen` | Additive fields round-trip, compatibility checks pass, and regeneration leaves the tree clean. |
| C9 | pass | `TestIssueCredentialValidatesAndForwardsRequestData` | Invalid data leaves the recording issuer untouched; the corrected request then succeeds. |

## Commands

- `go test -race ./pkg/connectorbuilder -run 'Test(ValidateCredentialIssueRequest|CredentialIssueTypedInputsWireRoundTrip|IssueCredentialValidatesAndForwardsRequestData)' -count=1` — pass.
- `buf lint` — pass.
- `buf breaking --against '.git#tag=v0.26.0'` — pass.
- `make protogen` followed by `git status --short` — pass; clean tree.
- `go test ./...` under the available Go 1.27 toolchain — all packages except
  `cmd/baton-compat-harness` and `cmd/baton-crash-harness` pass. Those harnesses
  launch nested builds without the `untested_go_version` compatibility tag and
  hit the v0.26 baseline's `cockroachdb/swiss` `!go1.27` build constraint.
- `make lint` — unavailable in this environment. The installed golangci-lint
  2.9.0 type checker cannot decode Go 1.27 export data; a locally rebuilt 2.9.0
  binary has the same analyzer limitation.

The two limited gates reproduce without the feature code and are toolchain
compatibility constraints, not typed-input validation failures.
