# Lambda invoke identity evidence

## Source, scope and claim

Base: public baton-sdk `b00aa1a5`, the version-file commit immediately after the `v0.46.0` tag (`cb1cfe9d`). Implementation: `2dfdb34a`; construction/HTTP-smoke follow-up: `f2f6130f`; final source: `c9a24ac674eb9d86492d59f6b6f91214f3c15b6d`. The final source differs from `f2f6130f` only by restoring the legacy connector-close warning's `config_version` field.

This stage adds the opt-in SDK mode, transport option/field and config response field. No dependency upgrade, deployment, AWS resource change or credential provisioning is included. Results below are reported local unit/fixture observations, not deployed AWS isolation or prepared-provider evidence. The local HTTP smoke uses the real SDK client transport against a test Lambda endpoint, not the AWS runtime.

The frozen plan remains unchanged. Criterion states below mean **verified to the explicitly stated fixture coverage**, with the reductions and missing instruments listed here. They do not assert exhaustive branch coverage or whole-plan closure. No sync/storage hot-path cost curve is changed; cold-start and real platform costs were not measured.

## Implementation-obligation addendum

- `lambdaInvokeIdentityHandler.prepare` serializes authentication and first binding. A provisional validator is retained only after generation construction and registration succeed; failures close it. Registration happens on a new, unpublished server. Failed first builds leave no published server or binding.
- `lambdaIdentityRuntimeFactory` owns the C1 connection, parsed key and process-derived identity context. Failure closes/cancels acquired resources; success transfers ownership to the handler. Token/session clients use the identity context, not the building invocation.
- `buildIdentityGeneration` owns the process-derived generation context and construction timer. Failure cancels it; success stops the timer while preserving the generation context until close. Context cancellation bounds cooperative operations; arbitrary connector code that ignores cancellation cannot be forcibly interrupted by a Go context.
- Scope validation is in the shared generation builder, before decryption and construction. Only the dedicated scope error becomes FailedPrecondition; exchange/fetch/constructor failures become Unavailable regardless of a nested authentication status.
- The existing reloader owns replacement and draining. In identity mode a failed replacement closes the new generation. Cleanup warnings contain fixed text, not arbitrary error or panic values.
- Identity-mode handler recovery and pre-dispatch errors contain fixed text. Request **and response** decode diagnostics and the frame-only legacy-encoding warning are sanitized in **all modes**, as exception E1 below records.
- Bound logger fields are attached after telemetry initialization, so both local and export cores receive them. Construction and dispatched RPCs receive the parsed bound key; the process context does not.

## Executed gates

All final-source test/build commands below ran at `c9a24ac6`, with Go `1.27.4` on Linux arm64. The later evidence-only commit does not alter their executable subject.

### G1: touched-package suites and structural coverage — PASS

```sh
env GOMAXPROCS=4 CGO_ENABLED=0 go test -p 4 -mod=vendor -buildvcs=false \
  -tags=baton_lambda_support -count=1 \
  -coverpkg=./pkg/auth,./pkg/cli,./pkg/field,./pkg/lambda/grpc/... \
  -coverprofile=/tmp/cxf63-sdk-coverage.out -timeout=10m \
  ./pkg/auth ./pkg/cli ./pkg/field ./pkg/lambda/grpc/... \
  ./pb/c1/transport/v1 ./pb/c1/connectorapi/baton/v1
```

Six package suites passed: auth, cli, field, Lambda transport, config and middleware. The two generated protobuf packages had no tests; descriptor/presence and serialization assertions execute in the transport suite. Coverage blocks were aggregated across package test binaries before identifying uncovered changed code; a zero in one binary is not a global zero.

### G2: tagged SDK build — PASS

```sh
env GOMAXPROCS=4 CGO_ENABLED=0 go build -p 4 -mod=vendor \
  -buildvcs=false -tags=baton_lambda_support ./...
```

### G3: touched-package race suites — PASS

```sh
env GOMAXPROCS=4 CGO_ENABLED=1 go test -p 4 -race -mod=vendor \
  -buildvcs=false -tags=baton_lambda_support -count=1 -timeout=10m \
  ./pkg/auth ./pkg/cli ./pkg/field ./pkg/lambda/grpc/...
```

All six suites passed with no race report, including eight concurrent first invocations. This is bounded schedule testing, not an exhaustive interleaving proof.

### G4: protocol generation and compatibility — PASS

- `make protogen` succeeded after the schema additions and again at `f2f6130f`.
- `git diff --exit-code -- pb` after the second generation was clean: generated output reproduced the committed files.
- `buf breaking --against '.git#branch=main'` passed against `b00aa1a5`. The schema is unchanged between that check and `c9a24ac6`.
- `git diff --check` passed.
- No lint was run in this stage, as requested. CI remains responsible for lint.

## Criterion-to-instrument results

All rows below reference G1 and G3 at `c9a24ac6` unless another revision is named. Each fixture asserts its setup before the postcondition; M1–M5 below validate representative assertion mechanisms with planted violations.

| ID / state | Instrument, premise and oracle | Covered space, outcome and reduction |
| --- | --- | --- |
| I1 — verified to stated coverage | `TestInvokeIdentityHostRejectionsMakeNoOutboundRequests` supplies malformed client ids and DNS/suffix cases to the real handler; fake validator and runtime-factory counters must both remain zero, status must be FailedPrecondition and runtime must remain nil. `TestInvokeIdentityMissingPlatformScopeAndPanicSanitization` checks absent platform context before either counter moves. | PASS: missing identity; empty/malformed client-id components; overlong labels/name; leading/trailing hyphens; trailing dot; wrong/look-alike suffix; extra label; uppercase; port; IP/brackets; percent encoding; URL delimiters and empty labels. Positive single-label and double-hyphen suffix examples succeed in the host parser. This enumerates named grammar classes, not every possible DNS string. |
| I2 — verified to stated coverage | `TestInvokeIdentityJWTBeforeBuildAndRejectedFirstHostDoesNotBind` uses actual signed tokens/static validators: wrong issuer, subject, audience, expiry, not-before and signature reject with zero factory calls and no binding. An invalid first host identity is followed by a correctly signed different host, which binds and builds once. `TestInvokeIdentityJWKSRedirectRefused` sends a real redirect through the production JWKS client's redirect policy and asserts zero target requests. `TestRemoteValidatorLoadsAuthenticatesAndCloses` asserts one initial HTTP JWKS request, valid token verification, cache reuse and calls Close. | PASS. The redirect test uses a local TLS server, and the remote validator test uses a local HTTP JWKS server. Host validation and default HTTPS URL construction are composed in production and source-reviewed; the default handler validator-factory closure itself is not executed against a routed HTTPS tenant host. |
| I3 — verified to stated coverage | `TestInvokeIdentityScopeCheckedBeforeConstructionEveryBuild` uses a real encrypted config response and actual generation builder for missing, wrong and matching scope, at versions v1 and v2. Two fetches are required; construction count is zero for missing/wrong scope and two for matching scope. `TestInvokeIdentityFailedBuildCleansUpAndCanRetry` and `TestInvokeIdentityBindReloadContextKeyAndLogs` assert FailedPrecondition for the dedicated scope error on first build and reload. | PASS. First/reload scope checks use the same production builder. The complete OAuth exchange and generation builder are not run together against a live config service. |
| I4 — verified to stated coverage | `TestInvokeIdentityBindReloadContextKeyAndLogs` asserts the first stamped invoke builds once, later same identity serves, a different client id is refused without rebuilding, and the original connector keeps serving. `TestInvokeIdentityConcurrentFirstInvokeBindsOnce` runs eight first calls and asserts one validator and one runtime factory. `TestInvokeIdentityNilConstructionFailsBeforeRegistration` rejects a nil constructor result. | PASS. The initial build occurs directly, without entering the existing reload cap; an immediate stamped dispatch does not build a second generation. The concurrency fixture covers eight same-identity callers, not all schedules or mixed-identity interleavings. |
| I5 — verified to stated coverage | `TestInvokeIdentityFailedBuildCleansUpAndCanRetry` injects ordinary, scope and nested Unauthenticated errors, asserts classification, one runtime close, no reloader/binding, successful retry, and final close. `TestIdentityGenerationBudgetAndPersistentLifetime` blocks on context cancellation to prove the construction timer and checks that successful context lifetime persists until close. `TestInvokeIdentityPartialConstructionClosesConnector` checks that a returned partial connector plus error is closed exactly once. `TestInvokeIdentityNilConstructionFailsBeforeRegistration` covers nil success. | PASS. Context survives the building invocation in the bind/reload fixture, and the process context has no key. Fake close counters prove handler ownership transfer; the real runtime factory/key path also runs in I8. Factory-initialization errors, cleanup-error warnings, late-success timeout and registration/replacement failure branches remain missing instruments below. Non-cooperative connector code is excluded from the context-timeout claim. |
| I6 — verified to stated coverage | `TestInvokeIdentityBindReloadContextKeyAndLogs` checks that an unauthenticated version change does not build, an authenticated change reloads within the identity, scope/build failure retains version v2, and v2 continues serving. Existing `TestReloadRateCapsPersistentMismatch`, `TestReloadGlobalFloorAndNewVersion` and served-version tests execute in G1/G3. | PASS. Fake runtime builds isolate handler/reloader ordering from the config-service fixtures in I3. The complete live session/token lifecycle through a real config reload is not exercised. |
| I7 — verified to stated coverage | `TestInvokeIdentityEncodings` round-trips the same identity through dual, frame-only and legacy payloads and checks metadata retains the bearer header but contains no identity/secret. `TestLambdaTransportInvokeIdentity` uses the real AWS client against a test HTTP endpoint, asserts the TenantId header and typed payload, checks the no-option path, and verifies the caller's request remains credential-free. | PASS. Legacy source-compatible constructor calls compile; typed function-value assignments in external repositories are not tested. |
| I8 — verified to stated coverage | `TestInvokeIdentityBindReloadContextKeyAndLogs` checks the same parsed key reaches dispatched context, process context has none, and local records have both identity fields. `TestIdentityRuntimeKeyDecryptsDispatchedCredentials` runs the actual runtime factory, then dispatches through a controlled generation and uses `crypto.ConvertCredentialOptions` to decrypt a password addressed to the parsed key. `TestInvokeIdentityExportedLogFields` captures actual telemetry-bridge export records and asserts both attributes. | PASS. The export is through the production bridge with an in-process exporter, not a deployed collector. The real factory uses a lazy C1 connection; the fixture replaces its build to avoid a provider call. |
| I9 — verified to stated coverage | `TestRequestDecodeErrorsDoNotExposePayload` and `TestResponseDecodeErrorsDoNotExposePayload` put the marker in parser-sensitive tokens/type URLs. Handler panic/error tests check returned status and captured logs. `TestInvokeIdentityHTTPRejectionSmoke` carries the marker through the real transport/client and test Lambda endpoint for decode, missing identity, host, token and build stages. | PASS. The decode stage creates a non-empty synthetic Lambda log tail and `LambdaInvokeFailure.LogSummary`; both, the error payload, caller error and captured records must omit the marker. Other stages return gRPC status responses rather than Lambda FunctionError, so there is no returned failure tail on those paths. This proves the SDK fixture boundary, not the real AWS runtime's logging. Arbitrary connector-authored logs are not constrained by the SDK. |
| I10 — verified to stated coverage, with E1 | `TestInvokeIdentityModeRequiresSuffixButNotLegacyCredentials` and `TestInvokeIdentityModeEnvironmentAndFlag` check flag/env activation, required suffix and absence of a legacy-credential requirement. I8's real factory fixture sets invalid env host overrides and still initializes. `TestLegacyReloaderIgnoresInvokeIdentity` dispatches with an invalid new identity and a valid legacy JWT; the scope-builder test ignores a wrong response scope when mode is off. | PASS for activation and legacy auth/config behavior. **E1 explicitly supersedes the frozen unchanged-diagnostics/test-assertion wording**, without changing the frozen baseline text. The legacy close warning's version field is restored at `c9a24ac6`; it is not a remaining exception. |
| I11 — verified to stated coverage | `TestExpectedExecutionScopePresenceAndFieldNumbers` checks optional presence survives binary serialization and numbers are 5 for response scope and 4 for request identity. Generated serializers are also used by I7. G1–G4 execute generation, compatibility, suites, build and races. | PASS. Generated packages contain no standalone tests. No lint, full-repository test sweep, external consumer compile or deployed platform exercise is claimed. |

### Follow-up test accounting

`f2f6130f` adds three top-level tests, with seven named cases when the five HTTP-smoke subcases are counted separately:

- `TestRemoteValidatorLoadsAuthenticatesAndCloses` → I2 and validator ownership in I5.
- `TestInvokeIdentityNilConstructionFailsBeforeRegistration` → I4/I5.
- `TestInvokeIdentityHTTPRejectionSmoke` → I1/I2/I5/I7/I9; subcases `decode`, `missing`, `host`, `token`, `build`.

`TestInvokeIdentityPartialConstructionClosesConnector` is a fourth relevant construction test, already present in `2dfdb34a`, and maps to I5. All run in final G1/G3. The nil-constructor test was first observed failing because the builder returned success for a nil server; the production guard was then added, and the final suites pass.

## E1: explicit I10 compatibility exception

This is a correction required by I9's secret-safety contract, owned by transport parsing/logging, not a change to the frozen criteria without a record.

1. **Request and response decode diagnostics change in every mode.** `Request.UnmarshalJSON` returns `invalid transport request`; `Response.UnmarshalJSON` returns `invalid transport response`. Decoding precedes handler mode inspection, and a client can attach an identity even to a mode-off function. Returning underlying protojson/frame diagnostics can expose payload tokens or type URLs through a runtime log tail and caller error. Wire acceptance, frame preference and error/no-error classification are unchanged.
2. **The frame-only warning is redacted in every mode.** `legacy_encoding_error` remains present but carries fixed text, not the underlying encoding error, for the same reason: arbitrary payload values must not enter logs. This reduces diagnostic detail without changing encoding selection.
3. **An existing test assertion changes.** `TestDecodeWireFrame_UnsupportedVersion` still rejects an unsupported version through the Request wrapper, but now expects the fixed safe error instead of `unsupported wire frame version 3`. It no longer independently asserts the decoder's version-specific diagnostic. Passing through payload-free wire errors and directly testing that diagnostic are deferred optional improvements, not claimed coverage.
4. **No legacy close-field exception remains.** The `config_version` field was inadvertently removed from the legacy stale-generation close warning in `f2f6130f`; `c9a24ac6` restores the original field. Identity-mode warnings continue to avoid arbitrary error/panic values.

## Instrument validation: planted violations

At final source `c9a24ac6`, five isolated Go compiler overlays modified temporary copies only. Production files remained unchanged. Each command used `CGO_ENABLED=0`, `GOMAXPROCS=2` and:

```sh
go test -p 2 -mod=vendor -buildvcs=false -tags=baton_lambda_support \
  -count=1 -timeout=2m -overlay=<temporary-overlay.json> -run '<test-expression>' <package>
```

| ID | Planted violation | Exact instrument / expected assertion failure | Outcome |
| --- | --- | --- | --- |
| M1 | Call the runtime factory before JWT validation | `TestInvokeIdentityJWTBeforeBuildAndRejectedFirstHostDoesNotBind`: factory counter expected zero, observed one | Rejected by assertion; exit 1 |
| M2 | Disable the expected-scope guard | `TestInvokeIdentityScopeCheckedBeforeConstructionEveryBuild`: expected scope error is absent / construction is no longer blocked | Rejected by assertion; exit 1 |
| M3 | Return the underlying request decode error | `TestRequestDecodeErrorsDoNotExposePayload`: parser-sensitive marker appears in error | Rejected by assertion; exit 1 |
| M4 | Rename the bound execution_scope log attribute | `TestInvokeIdentityExportedLogFields`: expected exported attribute is missing | Rejected by assertion; exit 1 |
| M5 | Drop ClientSecret when adding the wire identity | `TestLambdaTransportInvokeIdentity`: received secret does not equal the marker fixture | Rejected by assertion; exit 1 |

These validate five distinct representative oracles, not every cross-product cell. An earlier first overlay attempt exceeded a 180-second compile budget; it was retried successfully with a larger command budget, and all five were subsequently rerun at the final source. Earlier fixture-development runs also exposed incorrect RPC type names and a telemetry attribute type, which were corrected before passing gates. No failed run is counted as a passing gate.

## Uncovered changed-branch disposition

Locations below are at final source `c9a24ac6`. All are **missing instruments / explicit coverage limits**, not dead code and not evidence of execution. The independent source review supplies implementation/security review, not substitute runtime proof. No uncovered new branch was silently treated as verified.

| Changed source ranges | Uncovered behavior and disposition |
| --- | --- |
| `lambda_invoke_identity.go:66-72,154-155,164-165,178-179` | Default HTTPS validator-factory composition; key-fetch setup failure; missing/duplicate authorization headers; runtime-factory initialization failure. Source-reviewed; local validator, signed-token and generic build-error fixtures do not execute these exact guards. Missing instrument. |
| `lambda_invoke_identity.go:200-201,252-253,260-261` | Cleanup after registration panic, nil-generation close and connector-close error warning. Missing fault/cleanup instruments. |
| `lambda_invoke_identity.go:271-273,278-279,285-286,289-290,293-294,299-300,302-303,325-327` | DPoP initialization error, client-close error, factory rollback, invalid generated key, secret-key parse failure after client creation, concrete factory's timed builder, initialization timeout, and a builder that returns success only after its budget expires. The cooperative cancellation and persistent-context tests execute the shared budget helper but not every concrete factory/late-success exit. Missing instruments; no non-cooperative-code timeout claim. |
| `lambda_server__added.go:152-154,265-266,294-295,309-313` | Redacted reloader panic, failed replacement cleanup, cancellation after drain and stale-generation close warning branches. Existing reload tests prove replacement/caps; they do not inject these cleanup faults. The restored legacy warning is source-compared with the base. Missing instruments. |
| `lambda_server__added.go:372-373,440-442,459-460,473-475` | Flag binding error, actual AWS Lambda runtime start, legacy generated-key check and legacy initial build call. Startup refusal is tested, but no real Lambda runtime loop is started; extracted legacy startup is not a deployed smoke. Explicit integration exclusion / missing instrument. |
| `lambda_server__added.go:546-550,559-560,567-568,573-574,580-581,585-586,595-600,607-608,616-617,625-629,637-638` | Extracted existing session-store selection, config-fetch/decrypt/JSON/config-validation errors, optional auth/schema/egress/session sizing/OAuth branches and log-level failure. The shared scope guard, happy encrypted-config construction and constructor failures execute; these moved legacy branches are not newly instrumented. Missing instruments, not a claim of complete legacy configuration coverage. |
| `client.go:43-49` | Frame-only warning emission. Serialization fixtures prove frame-only identity round-trip, but do not drive this warning through RoundTrip. Missing instrument. |

After cross-binary aggregation, no other uncovered blocks intersect newly changed lines in auth, field defaults, config option selection or transport parsing. This statement concerns changed lines only, not entire-package coverage.

## Independent review and remaining boundary

The independent read-only implementation/security audit at `f2f6130f` found the mode conformant and no security defect. It requested this populated evidence record and the explicit I10 exception; it also identified the accidental legacy warning-field removal, now restored. The documentation/fix sign-off pass is pending on this evidence commit.

Not exercised: a deployed Lambda execution environment, real platform tenant isolation, live C1 exchange/config/session services together, external consumers, provider permissions, and CI lint. Those are not inferred from local tests or review. No merge, release or deployment is performed by this PR.
