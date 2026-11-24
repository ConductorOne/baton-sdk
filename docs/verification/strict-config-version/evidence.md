# Strict config version verification evidence

## Source and scope

Base: `c57f8acfdfe61e4607d787e11570c9a91493df8f` (`main`, SDK version v0.47.0). Frozen plan: `ae54930a`. Environment: Linux arm64, Go 1.27.4. All tests below execute local code and fixtures, not deployed Lambda functions or a vendor. No consumer retry or rollout claim is made.

Initial implementation and tests: `f241ddbc55027ae14ed573840661a1a69848c48e`. The initial gates and seven mutation checks below used that source/test tree. Independent review found a concurrent-dispatch gap; these serial-case results were not whole-contract closure. The corrected implementation and retained rerun evidence are recorded under CO2 below.

The generated Grant RPC handler records which connector generation received each dispatch. Requests and responses pass through JSON encoding/decoding and the actual Lambda reloader `Handler`; replacement uses the real service registry. No AWS or credential fixture is needed.

## Initial behavioral evidence (serial cases; rerun under CO2)

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

The initial report assumed serial invocation and excluded concurrent replacement. Independent review refuted that assumption: the vendored Lambda runtime supports multiple invocation loops. The exclusion is withdrawn; CO2 below covers the supported concurrent dispatch boundary.

## Initial commands completed

- PASS: `go test -race -count=1 ./pkg/cli ./pkg/lambda/grpc/...` (all four packages).
- PASS: `go test -race -tags baton_lambda_support -count=1 ./pkg/cli ./pkg/lambda/grpc/...` (all four packages).
- PASS: `go build ./...`.
- PASS: `go build -tags baton_lambda_support ./...`.
- PASS: `go test -race -tags baton_lambda_support -run '^TestLambdaStrictConfigVersion' -count=1 -coverprofile=<temporary-profile> ./pkg/cli`.
- PASS: `git diff --check`.
- BLOCKED: `golangci-lint run --build-tags baton_lambda_support --new-from-rev origin/main --timeout 5m ./pkg/cli/...`. Installed linter was built with Go 1.26 and refuses a module targeting Go 1.27.4 before analysis. This is not a passing lint result.

The initial focused coverage profile reported 18 executions of the strict condition, 6 of the refusal body, and 12 of the dispatch continuation. It exercised the serial branches, not concurrent replacement. The latest changed-range coverage excerpt is retained in `coverage.md`; duplicate whole-package profiles were removed.

## Initial planted violations

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

All seven checks were repeated or added after the initial cap fixture was strengthened to warm A with an actual rebuild. All seven were rejected against that serial fixture. CO2 reruns these mechanisms against the corrected implementation and adds four acquisition/ownership mutations, retaining the exact diffs and output.

## Initial failure and boundary resolution

The first complete focused run failed only `HeaderPresence/empty_strict_value`: it expected an empty-valued strict header to opt in but received OK. `pkg/lambda/grpc/util.go:UnmarshalMetadata` explicitly discards empty strings, so the decoded `Request.Headers()` omits this header. This was a transport premise mismatch, not evidence that a non-empty strict header bypasses the check. CO1 resolves the contract at the decoded metadata boundary: the known-answer test pins empty strict values as non-strict; non-empty values opt in without boolean parsing. CO1 changed no metadata serialization behavior and added no transport API.

Independent review requested a correction for concurrent dispatch. That correction and its verification follow; independent re-review of the corrected head remains pending. Full signoff is not claimed.

## CO2: atomic concurrent-generation acquisition

Corrected source/tests: `6f59c9a904bb6d96c2f405f4d1290113570c0599`. Later evidence-only commits do not change these files.

The strict reloader retains its early version refusal, then rechecks the version under `r.mu` through `Server.HandlerWithDispatchGuard`. The server acquires its registry mutex and the selected implementation's in-flight reference before releasing `r.mu`. Reload already takes `r.mu → server.mu`; both paths use this order. Both locks are released before connector execution, so an acquired strict A request can finish on A while a peer rebuilds and dispatches B. The comparison uses the current version, not pointer identity; a reused implementation pointer does not bypass it. Non-strict calls still use the existing unguarded Handler.

### Executed failure before correction

`co2-failed-before.json` retains the exact instrumentation patch against its full `base_sha`, command, output and expected assertion. Applying that patch to a clean checkout of the named base recreates the test and barrier without the fix. The race-enabled run failed because the recorded dispatch list was `["B", "B"]` instead of `["B"]`: strict A dispatched on B after the non-strict B invocation replaced it. This was an executed product regression, not a setup or build failure.

### Current criteria

| Criterion | Instrument and oracle | Status |
| --- | --- | --- |
| V1–V8, I1–I4 | All original serial cap, convergence, header, status, no-dispatch and error fixtures rerun against the corrected source; see `co2-focused-after.txt` and `co2-gates.txt`. | verified to the individual coverage stated above |
| V9 | `ConcurrentReplacementBeforeAcquire`: barrier after early strict A comparison; non-strict B rebuilds and dispatches; resumed A returns `FailedPrecondition` and dispatch list stays `["B"]`. Both distinct and reused implementation pointers. | verified to stated interleaving |
| V10 | `ConcurrentReplacementAfterAcquire`: strict A owns a reference and pauses; B rebuilds and dispatches; A's drain channel remains open and A is not closed; after resumption A dispatches on A, then drains and closes. | verified to stated interleaving |
| I5 | Same acquisition tests and release tests exercise version validation, registry selection and ownership transfer; non-strict peer reload succeeds while A is already acquired. | verified to stated coverage |
| I6 | `TestDispatchGuardHeldUntilImplementationAcquired` observes guarded dispatch blocked on the registry mutex and verifies the external lock cannot be acquired until service acquisition completes. Its early-release mutation fails this assertion. | verified to stated coverage |
| I7 | `TestHandlerWithDispatchGuard`: seven success/error paths × two wire encodings, external release exactly once, release before handler, no leaked in-flight reference. Missing-release and missing-reference mutations fail. | verified to stated coverage |
| I8 | V9's reused-pointer cell refuses despite the same connector pointer being returned under B. | verified to stated coverage |

The two CLI interleavings and the blocked-acquisition instrument also pass 100 repeated race-enabled runs each. These are deterministic barrier schedules with repeated execution, not a claim that arbitrary schedules were exhaustively enumerated.

### Retained gates and artifacts

- `co2-gates.txt`: exact source SHA, both-mode affected-package race results, both builds, focused/full coverage commands, 100-repeat concurrency checks and diff check. Every listed command exits zero.
- `co2-focused-after.txt`: verbose corrected-fixture output at the exact source SHA.
- `coverage.md`: the current changed-range excerpt replaces the two historical whole-package profile dumps. Full package tests still exercise the guarded and unguarded paths; the excerpt is not a whole-package coverage claim.
- `co2-mutations.json`: eleven exact source diffs, source SHA and SHA-256 hashes, normalized command/output paths, exit codes and distinguishing expected assertions. All eleven fail by their intended behavioral assertion, with no compile/setup failure.
- `co2-failed-before.json`: the pre-fix reproduction described above, with exact source patch and normalized workspace paths.

The eleven mutations cover the original seven mechanisms plus bypassing guarded acquisition, releasing the external guard before acquisition, omitting its release, and omitting the in-flight reference. Temporary overlay files were removed; original source bytes were checked unchanged after execution. No detached provider or deployed behavior is claimed.

The local lint toolchain blocker remains as recorded above; the correction does not claim a passing local lint run. Independent final-code/evidence re-review and normal PR checks remain separate gates.

## CO3: typed configuration-version refusal

Source/tests: `22c29287cbccbccb088dec636b642567153e9b01`. Both strict-refusal exits now call `lambdaConnectorConfigVersionNotReadyError`. It retains code `FailedPrecondition` and message `lambda-run: connector config version is not ready`, adding one `google.rpc.ErrorInfo` with:

- reason: `CONNECTOR_CONFIG_VERSION_NOT_READY`
- domain: `github.com/conductorone/baton-sdk`
- no metadata

The domain follows `GrantCancelledErrorDomain` in `pkg/types/grant/cancelled.go`; the provisional shorter domain is not used. Ordinary vendor and execution-scope preconditions are unchanged. No caller decoder or retry policy is implemented here.

| Criterion | Instrument and oracle | Status |
| --- | --- | --- |
| V11 | Cap/skew tests and `ConcurrentReplacementBeforeAcquire` assert exact code, fixed message, one typed ErrorInfo, exact reason/domain and no metadata after JSON response and gRPC status decoding. Concurrent requests now also round-trip request JSON, exercising guarded response framing. | verified to stated local serialization coverage |
| V12 | `OrdinaryPreconditions`, strict/non-strict × bare same-message vendor status, foreign-domain same-reason detail, same-domain foreign-reason detail, and execution-scope mismatch. Full vendor status protos are preserved; bare/vendor-scope statuses gain no SDK marker. | verified to stated eight cells |
| I9 | One helper constructs both refusal statuses. Successful static-detail encoding is exercised; failure returns a fixed `Internal` status without dispatch rather than an untyped generation refusal. | verified for fixed valid detail; defensive encoding-failure branch explicitly excluded from fault injection |

The excluded branch is not reachable with the helper's non-OK status and fixed valid generated `ErrorInfo` containing only ASCII strings and no metadata. Vendored `WithDetails` can fail for an OK status or failed `anypb.New` encoding. No new injection API was added solely to corrupt static protobuf encoding. This is source-based branch disposition, not a claim that the fallback was executed.

### Retained CO3 evidence

- `co3-failed-before.json`: exact test patch and source base, plus workspace-normalized command/output before the detail was added. Both early/capped and concurrent refusals failed the one-typed-detail assertion; ordinary preconditions passed.
- `co3-focused-after.txt`: all strict-version cases, including the typed detail and ordinary-error cells, pass with race detection.
- `co3-gates.txt`: exact source SHA and outputs for affected-package race tests with/without Lambda support, both builds, complete affected-package coverage, 100 repeated concurrent-generation cases and diff check. All commands exit zero.
- `coverage.md`: the current changed-range excerpt includes successful ErrorInfo encoding and the explicitly excluded defensive fallback, without retaining duplicate whole-package profiles.
- `co3-mutations.json`: exact diffs and source hashes, with workspace-normalized command/output for six rejected violations: stripping ErrorInfo, wrong reason, wrong domain, bypassing the helper on the early exit, bypassing it on the acquisition exit, and incorrectly marking an ordinary execution-scope precondition.

The prior eleven mutation executions remain retained at their recorded CO2 revision; they are not relabelled as executions against CO3. This delta reruns the full package gates and concurrency tests, and adds the six typed-detail mutations. All mutation failures were intended behavioral assertions, not build/setup failures. Temporary overlays were removed and original source bytes were checked unchanged.

Local lint remains blocked by the previously recorded toolchain mismatch. Independent light-delta review and PR CI are pending; no deployed caller-classification or vendor evidence is claimed.

## Final review cleanups

Source/tests: `88892d584497b0777f6079c7d2ec2396ad00f13d`. The strict opt-in now checks the first decoded header value locally for non-emptiness. This is equivalent with the current decoder, while avoiding a dependency on that decoder dropping empty values. The tested dispatch guard is unchanged. Header semantics, the early-refusal rationale and the guard's method-parsing/order/release contract are documented in source; V5 states the same empty-value rule.

`header-value-independence.json` records a bounded dependency perturbation: retaining empty values in metadata decoding still passes the header matrix with the local check. Replacing only that local check with the previous key-presence check makes the empty-value cell fail under the same perturbation. These are fixture overlays, not a transport change or a claim of a bug under the current decoder.

`hygiene-gates.txt` records the final-source affected-package race tests in both modes, both builds, full affected-package coverage generation and 100 concurrent-generation repetitions; all exit zero. `coverage.md` retains only 46 relevant coverage blocks (about 4.6 KB), replacing the two approximately 123 KB whole-package dumps. Full temporary profiles were removed after extracting the reported ranges.

Public evidence paths are repository-relative; temporary overlay paths use `overlay/<name>.json`. Source patches, source/mutant hashes, assertion content and exit codes are unchanged. Output paths are normalized, so retained output is not claimed to be byte-identical to the original machine-specific text. Historical runs retain their original source SHAs rather than being relabelled as final-head executions.

The local lint limitation remains as recorded. All evidence remains local source/fixture proof; CI and human merge approval are separate.
