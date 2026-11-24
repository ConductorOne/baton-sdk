# Strict config version verification evidence

## Source and scope

Base: `c57f8acfdfe61e4607d787e11570c9a91493df8f` (`main`, SDK version v0.47.0). Frozen plan: `ae54930a`. Environment: Linux arm64, Go 1.27.4. All tests below execute local code and fixtures, not deployed Lambda functions or a vendor. No consumer retry or rollout claim is made.

Implementation and tests: `f241ddbc55027ae14ed573840661a1a69848c48e`. The final gates and seven mutation checks use this source/test tree. Later documentation-only commits do not change the tested files.

The generated Grant RPC handler records which connector generation received each dispatch. Requests and responses pass through JSON encoding/decoding and the actual Lambda reloader `Handler`; replacement uses the real service registry. No AWS or credential fixture is needed.

## Current behavioral evidence

| Criterion | Instrument and assertions | Status |
| --- | --- | --- |
| V1 | `TestLambdaStrictConfigVersionCapsAndConvergence/global_floor`: an actual rebuild warms A, then strict B refuses inside the global floor; build and dispatch counts stay unchanged. | verified to stated coverage |
| V2 | `TestLambdaStrictConfigVersionCapsAndConvergence/repeated_version`: a build requested for B serves A; after backdating beyond the floor but inside the repeat cap, strict B refuses without build or dispatch. | verified to stated coverage |
| V3 | Both cap cases backdate past both caps, serve B, and assert one additional build and one dispatch on B. | verified to stated coverage |
| V4 | `TestLambdaStrictConfigVersionRejectsServedSkew`: a completed rebuild requested for B still serves A; received status is `FailedPrecondition`, build list is B, dispatch list is empty. | verified to stated coverage |
| V5 (CO1) | `TestLambdaStrictConfigVersionHeaderPresence`: matching stamps, non-boolean values, absent/empty stamps, empty generations, and empty strict values treated as absent by the existing transport. | verified to stated coverage |
| V6 | Both cap cases send non-strict B and observe A, with no new build. Header tests also cover unstamped non-strict calls. | verified to stated coverage |
| V7 | `TestLambdaStrictConfigVersionReloadError`: legacy/identity reloader modes × strict/non-strict; `Unavailable`, attempted build B, current A, no dispatch. | verified to stated coverage |
| V8 | All instruments above use the generated Grant registration, reloader Handler, request JSON round-trip, response JSON round-trip and received gRPC status. | verified to stated coverage |
| I1 | Cap convergence dispatches B after replacement; a successful rebuild still serving A refuses. | verified to stated coverage |
| I2 (CO1) | HeaderPresence asserts that `false` opts in and the transport-dropped empty value does not. A boolean-parser mutation is rejected. | verified to stated coverage |
| I3 | Generated Grant dispatch records stay unchanged on every refusal. Dispatch-before-refusal mutation is rejected. | verified to stated coverage |
| I4 | Request/response JSON round-trips preserve the received application status, including both reload modes. Wrong-status mutation is rejected. | verified to stated coverage |

Coverage is bounded to the named cells, not arbitrary schedules. Lambda's serial invocation model is assumed; independently concurrent generation replacement between comparison and dispatch is not supported by this check.

## Commands completed

- PASS: `go test -race -count=1 ./pkg/cli ./pkg/lambda/grpc/...` (all four packages).
- PASS: `go test -race -tags baton_lambda_support -count=1 ./pkg/cli ./pkg/lambda/grpc/...` (all four packages).
- PASS: `go build ./...`.
- PASS: `go build -tags baton_lambda_support ./...`.
- PASS: `go test -race -tags baton_lambda_support -run '^TestLambdaStrictConfigVersion' -count=1 -coverprofile=<temporary-profile> ./pkg/cli`.
- PASS: `git diff --check`.
- BLOCKED: `golangci-lint run --build-tags baton_lambda_support --new-from-rev origin/main --timeout 5m ./pkg/cli/...`. Installed linter was built with Go 1.26 and refuses a module targeting Go 1.27.4 before analysis. This is not a passing lint result.

The focused coverage profile records 18 executions of the new strict condition, 6 of the refusal body, and 12 of the unchanged dispatch continuation. Conditions absent-header, matching-generation and mismatching-generation are exercised. No newly added branch is uncovered. The whole CLI package percentage from this focused run (6.8%) is not a whole-package coverage claim.

## Planted violations

Seven isolated Go source overlays were tested with `go test -overlay <overlay.json> -tags baton_lambda_support -run '^<test>$' -count=1 ./pkg/cli`. No production source was modified; temporary overlays were removed. Each failure was a behavioral assertion, not a compile/setup error.

| Violation | Detecting instrument | Observed failure |
| --- | --- | --- |
| Remove strict check | CapsAndConvergence | strict refusal received OK instead of `FailedPrecondition` |
| Compare before reload | CapsAndConvergence | after cap expiry received `FailedPrecondition` instead of OK |
| Apply check without opt-in | CapsAndConvergence | non-strict capped call received `FailedPrecondition` instead of OK |
| Dispatch before refusing | CapsAndConvergence | dispatch list grew despite refusal |
| Return `Unavailable` for skew | RejectsServedSkew | received `Unavailable` instead of `FailedPrecondition` |
| Compare last requested version instead of current served version | RejectsServedSkew | received OK despite a successful build still serving A |
| Parse strict opt-in as boolean `true` | HeaderPresence/false_strict_value | received OK rather than `FailedPrecondition` for the present non-empty `false` header |

All seven checks were repeated or added after the cap fixture was strengthened to warm A with an actual rebuild. All seven representative violations were rejected against the final fixture.

## Initial failure and boundary resolution

The first complete focused run failed only `HeaderPresence/empty_strict_value`: it expected an empty-valued strict header to opt in but received OK. `pkg/lambda/grpc/util.go:UnmarshalMetadata` explicitly discards empty strings, so the decoded `Request.Headers()` omits this header. This was a transport premise mismatch, not evidence that a non-empty strict header bypasses the check. CO1 resolves the contract at the decoded metadata boundary: the known-answer test pins empty strict values as non-strict; non-empty values opt in without boolean parsing. No transport behavior or API has changed.

Independent final-code and evidence review remains pending. Full signoff is not claimed.
