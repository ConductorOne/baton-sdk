# Lambda invoke identity evidence

## Source, scope and claim

Base: public baton-sdk `b00aa1a5`, the version-file commit immediately after the `v0.46.0` tag (`cb1cfe9d`). Implementation: `2dfdb34a`; construction/HTTP-smoke follow-up: `f2f6130f`; initial verified source: `c9a24ac6`. CO2 records the CI-style correction at `ef5eef7b`; CO3 records simplified production source `d59953cf`. Production remains unchanged by CO4, which adds mode-off compatibility tests and records gate reruns at test revision `30a8ea39fad12da583bc8535497d068a4f062661`.

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

The initial G1–G3 test/build commands below ran at `c9a24ac6`, with Go `1.27.4` on Linux arm64. CO2 records reruns at `ef5eef7b`; CO3 at `d59953cf`; CO4 records the same gates and refreshed coverage at `30a8ea39`. The G1 command normalizes the temporary coverage destination to `coverage.out`; its other flags and package arguments are unchanged.

### G1: touched-package suites and structural coverage — PASS

```sh
env GOMAXPROCS=4 CGO_ENABLED=0 go test -p 4 -mod=vendor -buildvcs=false \
  -tags=baton_lambda_support -count=1 \
  -coverpkg=./pkg/auth,./pkg/cli,./pkg/field,./pkg/lambda/grpc/... \
  -coverprofile=coverage.out -timeout=10m \
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
- No local lint was run, as originally requested. The later CI lint failure and its bounded correction are recorded in CO2; local execution does not claim a lint verdict.

## Criterion-to-instrument results

All rows below were first verified by G1 and G3 at `c9a24ac6`; their executable instruments were rerun at `ef5eef7b`, `d59953cf` and `30a8ea39` as CO2–CO4 record. Each fixture asserts its setup before the postcondition. All prior mutation oracles were regenerated and rerun at every later revision; CO4 adds direct mode-off wire-acceptance and classification oracles.

| ID / state | Instrument, premise and oracle | Covered space, outcome and reduction |
| --- | --- | --- |
| I1 — verified to stated coverage | `TestInvokeIdentityHostRejectionsMakeNoOutboundRequests` supplies malformed client ids and DNS/suffix cases to the real handler; fake validator and runtime-factory counters must both remain zero, status must be FailedPrecondition and runtime must remain nil. `TestInvokeIdentityMissingPlatformScopeAndPanicSanitization` checks absent platform context before either counter moves. | PASS: missing identity; empty/malformed client-id components; overlong labels/name; leading/trailing hyphens; trailing dot; wrong/look-alike suffix; extra label; uppercase; port; IP/brackets; percent encoding; URL delimiters and empty labels. Positive single-label and double-hyphen suffix examples succeed in the host parser. This enumerates named grammar classes, not every possible DNS string. |
| I2 — verified to stated coverage | `TestInvokeIdentityJWTBeforeBuildAndRejectedFirstHostDoesNotBind` uses actual signed tokens/static validators: wrong issuer, subject, audience, expiry, not-before and signature reject with zero factory calls and no binding. An invalid first host identity is followed by a correctly signed different host, which binds and builds once. `TestInvokeIdentityJWKSRedirectRefused` sends a real redirect through the production JWKS client's redirect policy and asserts zero target requests. `TestRemoteValidatorLoadsAuthenticatesAndReusesKeys` asserts one initial HTTP JWKS request, valid token verification and cache reuse; Close is cleanup, not an asserted closure oracle. | PASS. The redirect test uses a local TLS server, and the remote validator test uses a local HTTP JWKS server. Host validation and default HTTPS URL construction are composed in production and source-reviewed; the default handler validator-factory closure itself is not executed against a routed HTTPS tenant host. |
| I3 — verified to stated coverage | `TestInvokeIdentityScopeCheckedBeforeConstructionEveryBuild` uses a real encrypted config response and actual generation builder for missing, wrong and matching scope, at versions v1 and v2. Two fetches are required; construction count is zero for missing/wrong scope and two for matching scope. `TestInvokeIdentityFailedBuildCleansUpAndCanRetry` and `TestInvokeIdentityBindReloadContextKeyAndLogs` assert FailedPrecondition for the dedicated scope error on first build and reload. | PASS. First/reload scope checks use the same production builder. The complete OAuth exchange and generation builder are not run together against a live config service. |
| I4 — verified to stated coverage | `TestInvokeIdentityBindReloadContextKeyAndLogs` asserts the first stamped invoke builds once, later same identity serves, a different client id is refused without rebuilding, and the original connector keeps serving. `TestInvokeIdentityConcurrentFirstInvokeBindsOnce` runs eight first calls and asserts one validator and one runtime factory. `TestInvokeIdentityNilConstructionFailsBeforeRegistration` rejects a nil constructor result. | PASS. The initial build occurs directly, without entering the existing reload cap; an immediate stamped dispatch does not build a second generation. The concurrency fixture covers eight same-identity callers, not all schedules or mixed-identity interleavings. |
| I5 — verified to stated coverage | `TestInvokeIdentityFailedBuildCleansUpAndCanRetry` injects ordinary, scope and nested Unauthenticated errors, asserts classification, one runtime close, no reloader/binding, successful retry, and final close. `TestIdentityGenerationBudgetAndPersistentLifetime` blocks on context cancellation to prove the construction timer and checks that successful context lifetime persists until close. `TestInvokeIdentityPartialConstructionClosesConnector` checks that a returned partial connector plus error is closed exactly once. `TestInvokeIdentityNilConstructionFailsBeforeRegistration` covers nil success. | PASS. Context survives the building invocation in the bind/reload fixture, and the process context has no key. Fake close counters prove handler ownership transfer; the real runtime factory/key path also runs in I8. Factory-initialization errors, cleanup-error warnings, late-success timeout and registration/replacement failure branches remain missing instruments below. Non-cooperative connector code is excluded from the context-timeout claim. |
| I6 — verified to stated coverage | `TestInvokeIdentityBindReloadContextKeyAndLogs` checks that an unauthenticated version change does not build, an authenticated change reloads within the identity, scope/build failure retains version v2, and v2 continues serving. Existing `TestReloadRateCapsPersistentMismatch`, `TestReloadGlobalFloorAndNewVersion` and served-version tests execute in G1/G3. | PASS. Fake runtime builds isolate handler/reloader ordering from the config-service fixtures in I3. The complete live session/token lifecycle through a real config reload is not exercised. |
| I7 — verified to stated coverage | `TestInvokeIdentityEncodings` round-trips the same identity through dual, frame-only and legacy payloads and checks metadata retains the bearer header but contains no identity/secret. `TestLambdaTransportInvokeIdentity` uses the real AWS client against a test HTTP endpoint, asserts the TenantId header and typed payload, checks the no-option path, and verifies the caller's request remains credential-free. | PASS. Legacy source-compatible constructor calls compile; typed function-value assignments in external repositories are not tested. |
| I8 — verified to stated coverage | `TestInvokeIdentityBindReloadContextKeyAndLogs` checks the same parsed key reaches dispatched context, process context has none, and local records have both identity fields. `TestIdentityRuntimeKeyDecryptsDispatchedCredentials` runs the actual runtime factory, then dispatches through a controlled generation and uses `crypto.ConvertCredentialOptions` to decrypt a password addressed to the parsed key. `TestInvokeIdentityExportedLogFields` captures actual telemetry-bridge export records and asserts both attributes. | PASS. The export is through the production bridge with an in-process exporter, not a deployed collector. The real factory uses a lazy C1 connection; the fixture replaces its build to avoid a provider call. |
| I9 — verified to stated coverage | `TestRequestDecodeErrorsDoNotExposePayload` and `TestResponseDecodeErrorsDoNotExposePayload` put the marker in parser-sensitive tokens/type URLs. Handler panic/error tests check returned status and captured logs. `TestInvokeIdentityHTTPRejectionSmoke` carries the marker through the real transport/client and test Lambda endpoint for decode, missing identity, host, token and build stages. | PASS. The decode stage creates a non-empty synthetic Lambda log tail and `LambdaInvokeFailure.LogSummary`; both, the error payload, caller error and captured records must omit the marker. Other stages return gRPC status responses rather than Lambda FunctionError, so there is no returned failure tail on those paths. This proves the SDK fixture boundary, not the real AWS runtime's logging. Arbitrary connector-authored logs are not constrained by the SDK. |
| I10 — verified to stated coverage, with E1 | `TestInvokeIdentityModeRequiresSuffixButNotLegacyCredentials` and `TestInvokeIdentityModeEnvironmentAndFlag` check flag/env activation, required suffix and absence of a legacy-credential requirement. I8's real factory fixture sets invalid env host overrides and still initializes. `TestLegacyReloaderIgnoresInvokeIdentity` dispatches with an invalid new identity and a valid legacy JWT; the scope-builder test ignores a wrong response scope when mode is off. `TestModeOffTransportPreservesWireAcceptanceAndErrorClassification` adds the explicit no-option transport matrix described in CO4. | PASS for activation, legacy auth/config behavior and the stated wire/classification matrix. **E1 explicitly supersedes the frozen unchanged-diagnostics/test-assertion wording**, without changing the frozen baseline text. The legacy close warning's version field is restored at `c9a24ac6`; it is not a remaining exception. |
| I11 — verified to stated coverage | `TestExpectedExecutionScopePresenceAndFieldNumbers` checks optional presence survives binary serialization and numbers are 5 for response scope and 4 for request identity. Generated serializers are also used by I7. G1–G4 execute generation, compatibility, suites, build and races. | PASS. Generated packages contain no standalone tests. No local lint, full-repository test sweep, external consumer compile or deployed platform exercise is claimed; CI lint status is recorded separately in CO2. |

### Follow-up test accounting

`f2f6130f` adds three top-level tests, with seven named cases when the five HTTP-smoke subcases are counted separately:

- `TestRemoteValidatorLoadsAuthenticatesAndReusesKeys` (renamed in CO3 from `TestRemoteValidatorLoadsAuthenticatesAndCloses`) → I2. The test invokes Close as cleanup but does not assert its effects.
- `TestInvokeIdentityNilConstructionFailsBeforeRegistration` → I4/I5.
- `TestInvokeIdentityHTTPRejectionSmoke` → I1/I2/I5/I7/I9; subcases `decode`, `missing`, `host`, `token`, `build`.

`TestInvokeIdentityPartialConstructionClosesConnector` is a fourth relevant construction test, already present in `2dfdb34a`, and maps to I5. All run in final G1/G3. The nil-constructor test was first observed failing because the builder returned success for a nil server; the production guard was then added, and the final suites pass.

## E1: explicit I10 compatibility exception

This is a correction required by I9's secret-safety contract, owned by transport parsing/logging, not a change to the frozen criteria without a record.

1. **Request and response decode diagnostics change in every mode.** `Request.UnmarshalJSON` returns `invalid transport request`; `Response.UnmarshalJSON` returns `invalid transport response`. Decoding precedes handler mode inspection, and a client can attach an identity even to a mode-off function. Returning underlying protojson/frame diagnostics can expose payload tokens or type URLs through a runtime log tail and caller error. Wire acceptance, frame preference and error/no-error classification are unchanged.
2. **The frame-only warning is redacted in every mode.** `legacy_encoding_error` remains present but carries fixed text, not the underlying encoding error, for the same reason: arbitrary payload values must not enter logs. This reduces diagnostic detail without changing encoding selection.
3. **An existing wrapper assertion changes, with its decoder oracle restored in CO4.** `TestDecodeWireFrame_UnsupportedVersion` keeps the fixed safe error assertion through the Request wrapper. CO4 also calls `decodeWireFrame` directly, asserts `unsupported wire frame version 3`, and checks that both errors remain classified Unknown. The mode-off transport matrix independently checks the explicit acceptance table and caller-visible classifications. Passing through payload-free wire errors remains unimplemented; the global diagnostic redaction is unchanged.
4. **No legacy close-field exception remains.** The `config_version` field was inadvertently removed from the legacy stale-generation close warning in `2dfdb34a` and remained absent at `f2f6130f`; `c9a24ac6` restores the original field. Identity-mode warnings continue to avoid arbitrary error/panic values.

## Instrument validation: planted violations

At initial verified source `c9a24ac6`, five isolated Go compiler overlays modified temporary copies only. Production files remained unchanged. Each command used `CGO_ENABLED=0`, `GOMAXPROCS=2` and:

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

Locations below are from the initial verified source `c9a24ac6`, not current line numbers after CO2/CO3. All are **missing instruments / explicit coverage limits**, not dead code and not evidence of execution. The independent source review supplies implementation/security review, not substitute runtime proof. No uncovered new branch was silently treated as verified. CO2 and CO3 record refreshed-profile dispositions; CO3 removes the redundant factory key assertion and tests the shared builder's key-rejection branches.

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

The independent read-only implementation/security audit at `f2f6130f` found the mode conformant and no security defect. Its correction pass **signed off `97123f39`**, confirming the populated I1–I11 record, E1, follow-up test mapping, uncovered-branch dispositions, frozen plan, public-safe PR body and one-line legacy warning restoration. The auditor checked the five mutation logs/results against M1–M5; it ran no tests and did not independently analyze the coverage profile. A documentation-only update recorded that sign-off and corrected the removal's commit attribution and local output-path presentation. The subsequent source delta at `ef5eef7b` received a separate read-only equivalence review, recorded in CO2.

Not exercised: a deployed Lambda execution environment, real platform tenant isolation, live C1 exchange/config/session services together, external consumers and provider permissions. No local lint was run; CI's result is separate from local test/build proof. These boundaries are not inferred from tests or review. No merge, release or deployment is performed by this PR.

## CO2: CI style correction and rerun evidence

**Classification:** correction in this PR, with no intended behavioral or protocol change. Source: [CI lint run 44431320714](https://github.com/ConductorOne/baton-sdk/actions/runs/44431320714), which failed on the pre-correction source with nine diagnostics. This does not weaken the frozen criteria or E1.

**Owning boundary and delta:** `pkg/cli/lambda_invoke_identity.go` and its three fixture files. Four named-return diagnostics are resolved by using local outputs in recovery closures and unnamed factory/builder results; the generation-builder signature is wrapped; the DNS character predicate is expressed with named letter/digit booleans; the smoke-stage assertions use a switch; test logger restoration uses deferred closures instead of deferring a function that itself returns a restoration function. No lint suppression, authentication/config logic, error text, log content or schema is added or changed.

**Affected criteria:** I1 (equivalent character predicate), I4/I5/I9 (recovery and return plumbing), I8/I9 (fixture logger restoration and stage assertions), I11 (CI gate). The read-only auditor compared every source/test hunk in `b0b912b9..ef5eef7b` and confirmed semantic equivalence; its source sign-off carries to `ef5eef7b`.

### Current-source reruns at ef5eef7b

- **G1 PASS:** all six touched-package suites and two generated-package compilations, first without coverage and then with the exact G1 coverage flags/package set above. The first coverage-refresh attempt terminated with exit 143 and no diagnostics; the same command was retried successfully. No result is claimed from the terminated attempt.
- **G2 PASS:** the exact tagged SDK build command above.
- **G3 PASS:** all six touched-package race suites with the exact G3 command above; no race report.
- **G4 carried forward:** no proto or generated file changed; prior deterministic generation and breaking-change results retain the same schema subject. `git diff --check` also passed on this correction.
- **M1–M5 rerun:** all five overlays were regenerated from `ef5eef7b`, not carried over from the old source, and all five were rejected by test assertions with exit 1. They use the same production violations/instruments recorded above.
- **M6 added for the changed recovery wrapper:** an isolated overlay returns the recovered panic value instead of the fixed initialization error. `TestInvokeIdentityMissingPlatformScopeAndPanicSanitization` rejects the marker-bearing error by assertion, exit 1. This validates the recovery-redaction oracle after restructuring.
- **Structural coverage refreshed:** coverage counts were aggregated across the package test binaries. No uncovered executable block intersects the changed production lines of the CI delta. Earlier missing-instrument categories remain coverage limits; this is not a claim of full-package or exhaustive branch closure.

Commands, environments and timeout flags are the same as G1–G3 and the overlay command above; only the source revision and local artifact destinations changed. These are reported local test/build observations. No local lint was run because the CI failure detail was available through the GitHub route. The post-push CI lint rerun is pending at this evidence commit; it must not be inferred from these local checks.

## CO3: centralized key validation and redundant-state cleanup

**Classification:** bounded correction and simplification in this PR. No authentication ordering, bind-once policy, scope enforcement, credential custody, error class or protocol changes. The executable subject is `d59953cf85900b44bcbfb00d4bd2cc0b48cd6bdc`.

**Folded changes:**

- The shared generation builder owns one checked private-key assertion and an explicit nil-key guard, before config fetch. Redundant checks at the legacy startup and identity-runtime call sites are removed. The legacy fixed cast-failure diagnostic is preserved; the mode still returns a fixed Unavailable response for build failure.
- Remove the redundant empty-suffix condition after constructor validation, retaining the required-suffix startup check and host grammar/suffix rule.
- Remove the redundant explicit false default from the mode field; `BoolField` already defaults to false.
- Remove the outbound request clone's unused `wireV2` copy. The reader audit, including tests, found that the flag controls server response encoding after request decoding; outbound payload marshaling reads the cloned message, not that flag.
- Rename the remote-validator test to `TestRemoteValidatorLoadsAuthenticatesAndReusesKeys`, matching its assertions. Its cleanup still calls Close, but it does not prove Close's effects.

**Deliberately unchanged:** the recovery closures were introduced by `ef5eef7b` because CI's `nonamedreturns` rule rejected the proposed named-return form. Restoring that form would reproduce the known lint failure, so recovery was not reshaped. Validator injection, test organization, startup timers, close-lock ownership and exported option APIs were not redesigned.

**Affected criteria and obligations:** I1/I10 retain the same host/default behavior; I2's test name is corrected; I5 gains direct key-rejection proof; I7 retains payload encoding without copying unused client state. `TestLambdaGenerationBuilderRejectsInvalidConfigurationKeys` covers missing, wrong-type and public-only keys in legacy and identity modes. It asserts no constructor-time panic, an error with no marker value, a nil generation, zero config calls and zero connector constructions. Before the fix all six cases failed on the unchecked assertion or nil dereference. The shared guard now returns an error without fetching config or constructing a connector.

**Current-source verification at d59953cf:**

- **G1 PASS:** all six touched-package suites, generated-package compilation and the same coverage flags/package set recorded above.
- **G2 PASS:** the tagged SDK build of `./...`.
- **G3 PASS:** all six touched-package race suites; no race report.
- **G4 carried forward:** schemas and generated output are unchanged; `git diff --check` passed.
- **M1–M6 regenerated and rerun:** all six prior violation oracles rejected their mutants by test assertions, exit 1; no old-source overlay was reused.
- **M7:** return success instead of rejecting an invalid private-key type in the shared guard. `TestLambdaGenerationBuilderRejectsInvalidConfigurationKeys` rejects the missing error by assertion, exit 1.
- **Structural coverage refreshed:** aggregate counts across test binaries show no uncovered executable blocks intersecting this production delta. Historical coverage limits remain explicit; the removed redundant factory assertion is no longer a current branch.

The new push's CI and the independent cleanup-delta audit are pending at this evidence commit. No local lint or deployed platform/provider check is claimed.

## CO4: mode-off wire acceptance and classification proof

**Classification:** test and release-note clarification in this PR; no production, protocol or dependency changes. The test subject is `30a8ea39fad12da583bc8535497d068a4f062661`; production remains byte-identical to `d59953cf`.

**Public release note:** the PR body's final `Release Notes` section now states that request and response decode errors no longer quote payload content, helping keep sensitive data out of logs and returned errors. It states that error classifications and accepted/rejected payloads are unchanged, including with per-invoke identity turned off. The published text was read back exactly after the update.

**I7/I10/I11 instrument:** `TestModeOffTransportPreservesWireAcceptanceAndErrorClassification` uses the real no-option Lambda client transport, a local HTTP Lambda endpoint and the ordinary transport server without the per-invoke identity handler. Every HTTP request asserts no TenantId header and no typed invoke identity, establishing the mode-off premise.

The matrix has eighteen cases:

- Legacy, dual and frame-only request encodings each run with application statuses OK, Unauthenticated, PermissionDenied and Unavailable. They must decode the same request, dispatch exactly once and preserve the caller-visible status; successful responses retain their config-version data.
- Unsupported versions, invalid binary frames and malformed JSON are rejected on the request side. They must not dispatch, and the client must retain the existing Unhandled `LambdaInvokeFailure` / Unknown classification.
- The same three rejected encoding families on the response side must remain plain transport errors classified Unknown, after exactly one dispatch, rather than becoming Lambda runtime failures.

For both directions, the helper compares the sanitizing wrapper with the unchanged `unmarshalTransportJSON` decoder **and** with explicit accepted/rejected expectations. The explicit table prevents a shared decoder change from satisfying a self-comparison. `TestDecodeWireFrame_UnsupportedVersion` additionally restores the direct version-diagnostic oracle while retaining the sanitized wrapper assertion. This is bounded SDK/HTTP-fixture proof, not a deployed Lambda or external-consumer compatibility run.

**Current-revision verification at 30a8ea39:**

- **G1 PASS:** all six touched-package suites, generated-package compilation and refreshed coverage with the same flags/package set recorded above.
- **G2 PASS:** the tagged SDK build of `./...`.
- **G3 PASS:** all six touched-package race suites; no race report.
- **G4 carried forward:** no schemas, generated code or production files changed; `git diff --check` passed.
- **M1–M7 regenerated and rerun:** all prior oracles rejected their mutants by test assertions, exit 1; no earlier-revision overlay was reused.
- **M8, acceptance:** disable unsupported-frame-version rejection. The new mode-off matrix rejects the changed acceptance decision by assertion, exit 1.
- **M9, decode classification:** change response decode errors from plain errors to InvalidArgument status errors. The mode-off matrix rejects the classification change by assertion, exit 1.
- **M10, application classification:** remap received application statuses to FailedPrecondition in the client connection. The mode-off matrix rejects the caller-visible status change by assertion, exit 1.

No new production branches or resource owners are introduced. Existing coverage limitations remain; the new matrix and three planted violations validate the acceptance/classification properties specifically. The new push's CI and independent delta review are pending at this evidence commit; no local lint or deployed provider check is claimed.

## CO5: review suggestions on the allowed-host suffix, panic logging, decode sentinels and fixture assertions

Four review suggestions on `988597ae` were checked against the source and all four were applied. Parent revision: `988597ae`.

1. **Allowed-host suffix needs its own delimiter.** `invokeIdentityHost` compares the suffix as a raw string, so a suffix of `example.com` also accepted `evilexample.com` and would have trusted that host's keys. `newLambdaInvokeIdentityHandler` now rejects a suffix that does not begin with `.` or `-`, and the flag help states the rule. A misconfigured function fails at startup instead of accepting a wider set of hosts. `TestInvokeIdentitySuffixRequiresLeadingDelimiter` covers four rejected and two accepted suffixes.
2. **A recovered panic in the identity handler is logged.** `Handler` now logs the fixed message `lambda-run: invoke identity handler panic` on the process logger. The panic value is not logged or attached as a field. `TestInvokeIdentityMissingPlatformScopeAndPanicSanitization` asserts one such record with no fields, none for an ordinary rejected invoke, and still asserts that the planted secret is absent from all records.
3. **Decode errors are exported sentinels.** `Request.UnmarshalJSON` returns `ErrInvalidTransportRequest` and `Response.UnmarshalJSON` returns `ErrInvalidTransportResponse`. The text is unchanged from E1 and the underlying decode error is still not wrapped, so no payload content is reachable through `Error` or `Unwrap`. Callers that matched the earlier diagnostic text can use `errors.Is`. `TestTransportDecodeErrorsAreClassifiableSentinels` checks both directions, that the two sentinels do not match each other, and that the sentinel survives `encoding/json`. This adds two exported variables to `pkg/lambda/grpc`; nothing is removed or renamed.
4. **Fixture handler assertions no longer stop the server goroutine.** The mode-off matrix handler and `assertModeOffDecodeCompatibility` use `assert` and return early with an HTTP 500, so a failed check is reported as that assertion rather than as a transport error. The checks themselves are unchanged.

**Verification at this revision:**

- `go vet` and `go build ./...`, with and without the `baton_lambda_support` tag: pass.
- `go test -race -count=1 -tags baton_lambda_support ./pkg/cli/ ./pkg/lambda/... ./pkg/field/...`: pass, no race report.
- `go test -count=1 ./pkg/cli/ ./pkg/lambda/... ./pkg/field/...` without the tag: pass.
- **Planted violations:** removing the panic log line fails the panic-sanitization test by assertion; disabling the delimiter check fails the suffix test by assertion. Both were reverted.

Not rerun here: the wider G1 to G4 gates and mutants M1 to M10, local lint, and any deployed check. The new push's CI and review are pending at this commit.

## CO6: redacted build and reload failures leave a log record

In identity mode the reloader and the first-build path returned a fixed status to the caller and logged nothing, so a failing reload left no record on the function side. Parent revision: `8fefa243`.

The underlying error stays out of both the response and the log, because it can quote configuration or request content. Each redacted exit now logs a fixed message at error level with only these fields:

- `requested_version` and, on reload, `current_version`. Both are already logged by the rate-cap warning.
- `cause_code`, the gRPC code of the underlying error. It is an enum, so it carries no payload, and it separates a failing configuration call (for example `PermissionDenied` or `Unavailable`) from a local decode, validation or construction failure (`Unknown`).
- `execution_scope` and `client_id`, which the bound logger already attaches to every identity-mode record.

Covered exits: reload failure, execution scope mismatch on reload, a recovered panic in the reloader (fixed message, no panic value), identity client creation failure, first-build failure and first-build scope mismatch. Authentication rejections are still not logged. The mode-off path is unchanged: `redactErrors` is false there and no new branch runs.

**Verification at this revision:**

- `TestInvokeIdentityBindReloadContextKeyAndLogs` asserts the exact field set of the reload, scope and panic records, and that no error-level record is written before the first failure. `TestInvokeIdentityFailedBuildCleansUpAndCanRetry` asserts the first-build records for a plain error, a scope mismatch and a status error. Both still assert that the planted secret is absent from all records.
- `go vet`, `go build ./...` with and without the `baton_lambda_support` tag, `go test -race -count=1 -tags baton_lambda_support ./pkg/cli/`, and `go test -count=1 ./pkg/cli/` without the tag: pass.
- **Planted violations:** attaching the error to the reload record fails the field-set assertion; deleting the scope and panic log lines fails the record-count assertions. Both were reverted.

Not rerun here: the other packages, the wider G1 to G4 gates and mutants M1 to M10, local lint, and any deployed check.

## CO7: adjudicated feedback corrections

Parent revision: `e0d8b0b2494a58f004aba9437e14a73903843565`. This entry
supersedes the earlier diagnostic-only exception and mode-off-unchanged claims
where stated below. The frozen baseline is retained; the approved correction
and compatibility amendments are recorded in the plan's CO7.

### Completion and instruments

| Item | Implementation and observed fixture coverage |
| --- | --- |
| 1 | `config.TLSConfig` exposes the existing system/custom-CA configuration. The identity handler creates its JWKS client once at startup. `TestInvokeIdentityJWKSClientTrustsConfiguredCA` rejects the fixture certificate without the bundle, accepts it with the bundle, and rejects an unreadable bundle at handler startup. Redirect refusal remains tested. |
| 2 | First-build cleanup restores the startup level unless registration succeeds, including panic. `TestInvokeIdentityFailedFirstBuildRestoresLogLevel` observes info → failed debug build → info, then a successful debug build that keeps debug, for error and panic. Registration failure uses the same deferred guard; it is not separately injected here. |
| 3–4 | Missing/malformed/multiple authorization headers and invalid tokens return Unauthenticated; host/identity/scope rejections retain FailedPrecondition. `TestInvokeIdentityBearerRejectedBeforeValidator` asserts zero validator builds for absent or malformed bearer headers. The signed-token tests and HTTP rejection smoke assert the new token code. The host check still precedes the header check. |
| 5 | Explicitly excluded pending a separate ruling. The prototype secret digest, change warning and rotation test were removed. Key-load failures also retain their prior FailedPrecondition status; changing them to Unavailable is excluded. |
| 6 | Startup validation checks the shortest admitted host's complete DNS syntax and requires a domain of at least two labels. The suffix table covers invalid delimiters, case, empty labels, trailing dots, ports and insufficient domain labels. Flag help and the validator document that public-suffix ownership is not checked; `.co.uk` is not rejected by a suffix list. |
| 7 | `identityMode` is explicit on the builder, reloader and generation. Scope is no longer a proxy for construction behavior, and cancel presence is no longer a proxy for cleanup redaction. `TestLambdaGenerationIdentityModeIsExplicit` supplies a nonempty expected scope in both modes and checks constructor context and generation marking. `TestGenerationCloseRedactionUsesIdentityModeNotCancel` tests both disagreeing flag/cancel combinations. Existing partial/nil-construction and legacy-scope tests still run. |
| 8 | Every unswapped new generation is closed, including mode off. `TestLambdaReloadClosesUnswappedGenerationInBothModes` covers both modes × replacement error/no matches, asserts the previous generation is retained, and checks exactly one close plus cancellation. **This is an approved mode-off lifecycle change fixing an existing leak**, beyond E1's diagnostics. |
| 9, 14 | All three identity recovery sites call `ugrpc.RecoveredPanicErrorRedacted`, which records the stack, recovery site and fixed status, never the panic value. Only the interceptor adds the resolved RPC method; pre-dispatch wrappers do not log the untrusted request method. Updated record assertions cover stack/method/redaction. `TestIdentityRecoveriesTreatNilPanicAsFailure` exercises all three sites, including under `GODEBUG=panicnil=1`. Caller codes remain Unavailable for build wrappers and Internal for dispatched RPC panics. |
| 10 | Builder errors retain their original text and unwrap chain while carrying fetch/scope/decrypt/unmarshal/validate/construct stages. First-build and reload logs add the stage without the cause text; untagged injected errors use `unknown`. `TestLambdaGenerationBuilderFailureStages` injects each stage through the real builder and checks scope/cause/status preservation and construction counts. Existing first/reload record assertions cover logged stages. |
| 11 | A typed unsupported-version error lets both wrappers retain the numeric version and wrap the appropriate sentinel: `invalid transport request: unsupported wire frame version N`, or the response equivalent. Exact-text and `errors.Is` assertions pass through direct and JSON decoding. **This supersedes E1's claim that every decode error is fixed text/unwrapped**; other parser details remain withheld. Acceptance and status classification are unchanged. |
| 12 | Removed the ineffective `defer handler.Close()` and documented that `StartWithOptions` exits the process. Explicit Close remains available to tests. No runtime cleanup-on-exit claim is made. |
| 13 | The build context remains process-derived. An invoke-cancellation callback cancels construction and is stopped before successful ownership transfer; it cannot cancel a successful generation later. `TestIdentityBuildHonorsInvokeCancellationOnlyDuringBuild` checks cancellation during construction and survival after success. The construction timeout/persistent-lifetime and dispatched-key tests also pass. |
| 15 | Kept the annotation removal with a comment explaining Cobra's post-PreRunE required-flag check, rather than changing legacy required-flag error text. Flag/environment and missing-suffix tests pass. |
| 16 | The transport creates a new request envelope sharing payload/headers, instead of deep-cloning the entire protobuf. `TestLambdaTransportInvokeIdentity` still checks the real encoded request and that the caller's request has no retained identity. The extra envelope work is constant-size; existing serialization costs are unchanged. No allocation benchmark is claimed. |
| 17 | `NewLambdaClientTransport` rejects an option with any empty identity field without including its values in the error. `TestNewLambdaClientTransportRejectsIncompleteInvokeIdentity` covers each field, all empty, a complete identity and no option. |

### Final-source gates

The following commands passed on the production/test source accompanying CO7.
Each race invocation ran seven package suites: auth, CLI, field, transport,
configuration, middleware and ugrpc. No race report occurred.

```sh
env GOMAXPROCS=4 CGO_ENABLED=1 go test -p 4 -race -mod=vendor \
  -buildvcs=false -tags=baton_lambda_support -count=1 -timeout=10m \
  ./pkg/auth ./pkg/cli ./pkg/field ./pkg/lambda/grpc/... ./pkg/ugrpc
env GOMAXPROCS=4 CGO_ENABLED=1 go test -p 4 -race -mod=vendor \
  -buildvcs=false -count=1 -timeout=10m \
  ./pkg/auth ./pkg/cli ./pkg/field ./pkg/lambda/grpc/... ./pkg/ugrpc
env GOMAXPROCS=4 CGO_ENABLED=0 go build -p 4 -mod=vendor \
  -buildvcs=false -tags=baton_lambda_support ./...
env GOMAXPROCS=4 CGO_ENABLED=0 go build -p 4 -mod=vendor \
  -buildvcs=false ./...
env GOMAXPROCS=4 CGO_ENABLED=0 go vet -p 4 -mod=vendor \
  -buildvcs=false -tags=baton_lambda_support \
  ./pkg/auth ./pkg/cli ./pkg/field ./pkg/lambda/grpc/... ./pkg/ugrpc
env GOMAXPROCS=4 CGO_ENABLED=0 go vet -p 4 -mod=vendor \
  -buildvcs=false ./pkg/auth ./pkg/cli ./pkg/field ./pkg/lambda/grpc/... ./pkg/ugrpc
env GOMAXPROCS=2 CGO_ENABLED=0 GODEBUG=panicnil=1 go test -p 2 \
  -mod=vendor -buildvcs=false -tags=baton_lambda_support -count=1 -timeout=2m \
  -run 'TestIdentityRecoveriesTreatNilPanicAsFailure|TestInvokeIdentityHTTPRejectionSmoke|TestRecoveryHandlerTreatsNilPanicValueAsAPanic' \
  ./pkg/cli ./pkg/ugrpc
```

The real client/HTTP-endpoint rejection smoke ran in the tagged race suite and
again in the explicit command above. This is SDK/local-fixture evidence, not a
deployed Lambda, AWS isolation or prepared-provider check.

Development failures were corrected before these gates: a fixture passed a
string to a byte field, a replacement-error assertion used the wrong phrase,
and a runtime-build override incorrectly expected its invoke argument to carry
the process-owned key. An intermediate whitespace check found a trailing blank
line in the new plan section; it was removed. These failed attempts are not
counted as passing checks.

### Instrument validation

Twenty-one isolated compiler overlays were regenerated from the final source.
Every planted violation exited 1 through a test assertion, not a compile error.
The originals were never modified by the overlays; temporary copies were removed.
Each used `GOMAXPROCS=2 CGO_ENABLED=0` and:

```sh
go test -p 2 -mod=vendor -buildvcs=false -tags=baton_lambda_support \
  -count=1 -timeout=2m -overlay=<isolated-overlay.json> \
  -run '^<instrument-name>$' <instrument-package>
```

| Violation | Rejecting instrument |
| --- | --- |
| Remove JWKS custom-CA TLS configuration | `TestInvokeIdentityJWKSClientTrustsConfiguredCA` |
| Disable startup-level restoration | `TestInvokeIdentityFailedFirstBuildRestoresLogLevel` |
| Change token failure status back to FailedPrecondition | `TestInvokeIdentityBearerRejectedBeforeValidator` |
| Build a validator before checking the header | `TestInvokeIdentityBearerRejectedBeforeValidator` |
| Disable whole-suffix syntax/domain validation | `TestInvokeIdentitySuffixRequiresLeadingDelimiter` |
| Use nonempty expected scope instead of the builder mode flag | `TestLambdaGenerationIdentityModeIsExplicit` |
| Use cancel presence instead of the generation mode flag for redaction | `TestGenerationCloseRedactionUsesIdentityModeNotCancel` |
| Disable identity-mode reloader handling | `TestInvokeIdentityBindReloadContextKeyAndLogs` |
| Skip unswapped cleanup in mode off | `TestLambdaReloadClosesUnswappedGenerationInBothModes` |
| Drop the interceptor's resolved method | `TestInvokeIdentityMissingPlatformScopeAndPanicSanitization` |
| Mislabel decrypt-stage errors as unknown | `TestLambdaGenerationBuilderFailureStages` |
| Drop the unsupported-version diagnostic from the request wrapper | `TestDecodeWireFrame_UnsupportedVersion` |
| Detach construction cancellation from the invoke | `TestIdentityBuildHonorsInvokeCancellationOnlyDuringBuild` |
| Drop stacks from the shared redacting panic helper | `TestInvokeIdentityMissingPlatformScopeAndPanicSanitization` |
| Retain Cobra-required annotations in identity mode | `TestInvokeIdentityModeRequiresSuffixButNotLegacyCredentials` and `TestInvokeIdentityModeEnvironmentAndFlag` |
| Mutate the caller's request when adding identity | `TestLambdaTransportInvokeIdentity` |
| Accept incomplete identity options | `TestNewLambdaClientTransportRejectsIncompleteInvokeIdentity` |
| Disable the scope guard | `TestInvokeIdentityScopeCheckedBeforeConstructionEveryBuild` |
| Omit the wire client secret | `TestLambdaTransportInvokeIdentity` |
| Rename the exported execution-scope attribute | `TestInvokeIdentityExportedLogFields` |
| Return underlying request decode errors | `TestRequestDecodeErrorsDoNotExposePayload` |

The first overlay batch correctly failed the invoke-budget test with `t.Fatal`,
but its output classifier recognized only structured assertion output. That
instrument was changed to `require.FailNow`, and the entire batch was regenerated
and rerun successfully. No invalid or surviving mutant is counted above.

### Limits and handoff

`git diff --check` passes after the documentation whitespace correction.
The suites rerun existing compatibility/security tests, but no refreshed
structural-coverage profile, complete historical M1–M10 overlay batch, protobuf
generation/breaking check, local lint, allocation benchmark or external-consumer
build is claimed here. Protocol/generated files are unchanged. New-source CI and
independent review remain separate gates; prior CI/review results are not carried
forward as approval. No deployed or prepared-provider test was run.

## CO8: architect rulings R12/R13 and delta-review corrections

Parent revision: `e263006412f5c010209dfc05070bd7906b0af9ec`. R12/R13 are
approved corrections in this stage, superseding CO7's two explicit exclusions.
The plan's CO8 records the contract and new key-refresh resource obligations.

### R12: one client key for the environment lifetime

`lambdaIdentityBinding` is published only with successful generation registration.
It holds the original raw secret for `subtle.ConstantTimeCompare` and the parsed
JWK thumbprint for equivalent-serialization checks. It does not hash the raw
secret. Repeated identical bytes bypass parsing; other serializations are parsed
and compared by key thumbprint, ignoring supplied key ids. A different key is
refused with FailedPrecondition and a distinct fixed message directing the
caller to a new execution scope. Unparseable secrets are refused before runtime
construction or dispatch. There is no rebind and no key-change log or warning
cache; neither secret values nor their digests enter messages/logs.

`TestIdentityBindingKeyLifetimeAndRedaction` uses real serialized private keys,
accepts the same key with changed JSON formatting and key-id metadata,
rejects a different key and an unparseable secret, observes no dispatch on those
refusals, and dispatches the original identity afterward through the same
generation. It checks raw secrets and their hex/base64 SHA-256 digests against
every captured message and log. `TestIdentityBindingRawEqualityFastPath` poisons
the slow-path thumbprint to observe identical-byte fast-path selection. This
does not claim timing measurements; the constant-time primitive is explicit in
the production comparison.

Existing handler fixtures now supply valid serialized test secrets rather than
unparseable placeholders. Their marker-bearing token, error, panic and parser
inputs remain in place, and the new ruling fixture checks actual secret values.

### R13: load failure versus missing token key

The identity validator opts into `auth.Config.RefreshOnUnknownKey`; legacy
validation retains its no-refresh behavior. A loaded set missing the token key
is refreshed once. A second missing-key result is `auth.ErrTokenKeyNotFound` and
becomes FailedPrecondition, not Unauthenticated/PermissionDenied. Network,
timeout, HTTP-status and document-decode failures are
`auth.ErrKeySetUnavailable` and become Unavailable. Typed load errors preserve
their prior text and unwrap chain.

Refresh replaces and closes the previous key set under the validator lock.
Fetches honor the invoke context, while refresh lifetime remains process-derived.
Failed initial loads cancel their allocated refresh context. Handler failure
exits close provisional validators, including a constructor returning both a
validator and an error; nothing is bound.

- `TestInvokeIdentityKeyLoadAndMissingKidClassification` covers network,
  timeout, non-success status, undecodable document, missing kid after exactly
  one refresh, failed refresh load and successful refresh authentication.
  Retries require fresh provisional validators. Close counters and nil
  validator/runtime/binding assertions cover disposal and lack of binding.
  The success case deliberately stops at a failing runtime factory; factory
  calls prove authentication progress, not a deployed credential exchange.
- `TestInvokeIdentityDiscardsValidatorReturnedWithLoadError` proves exactly
  one close when initialization returns a provisional validator plus an error.
- `TestRemoteValidatorRefreshIsOptInAndClosesReplacedKeys` covers refresh off/on,
  replaced-key closure, one process-owned refresh registration after the invoke
  ends, and zero registrations after Close.
- `TestFailedRemoteKeyLoadReleasesRefreshContext` observes no outstanding
  refresh-context registration after failed initialization.
- `TestKeySetLoadErrorsPreserveCauseAndClassification` checks `errors.Is`,
  the underlying cause and unchanged error text.

### Delta review

The three deliberate nil-panic sites carry justified `govet` suppressions; the
logger restore uses a deferred closure, and the overlong log statement is split.
The shared redacting recovery helper logs the exact fixed status it returns:
Unavailable for initialization/reload wrappers, Internal for dispatched RPC
panics. The nil-panic test compares those recorded codes with the returned codes.

Construction cancellation and budget expiry carry distinct `cancel`/`budget`
stages and preserve context causes. Invoke cancellation no longer says it
exceeded the time budget. `TestIdentityBuildCancellationAndBudgetStages` checks
stage, cause and message. `TestUnswappedCloseWarningsPreserveModeOffDetails`
checks legacy error/version fields for mode-off cleanup and their absence in
identity mode. The PR release notes describe `config.TLSConfig`,
`ugrpc.RecoveredPanicErrorRedacted` and the new auth classification/refresh API.

### Final-source verification

The following passed on the production/test source accompanying CO8:

- Seven affected-package suites under `-race`, both with and without
  `baton_lambda_support`, with no race report.
- Full `go build ./...` and affected-package `go vet`, both build modes.
- Explicit `GODEBUG=panicnil=1` recovery and HTTP rejection smoke.
- Compatible, repository-pinned golangci-lint v2.14.0 on all affected packages,
  with no issue filtering: **0 issues**.
- Eighteen freshly generated isolated planted violations, each rejected by a
  test assertion rather than a build failure.

The test/build/vet commands are the same seven-package commands recorded in
CO7. The explicit smoke command is:

```sh
env GOMAXPROCS=2 CGO_ENABLED=0 GODEBUG=panicnil=1 go test -p 2 \
  -mod=vendor -buildvcs=false -tags=baton_lambda_support -count=1 -timeout=2m \
  -run 'TestIdentityRecoveriesTreatNilPanicAsFailure|TestInvokeIdentityHTTPRejectionSmoke' \
  ./pkg/cli
```

The lint command uses the v2.14.0 binary built with Go 1.27.4:

```sh
env GOMAXPROCS=4 golangci-lint run --timeout=10m \
  ./pkg/auth/... ./pkg/cli/... ./pkg/lambda/grpc/... ./pkg/field/... ./pkg/ugrpc/...
```

An initial system linter built with Go 1.26 refused the module's Go 1.27.4
version; it supplied no lint verdict. A full-repository run of the compatible
linter found one pre-existing, untouched finding:
`pkg/uhttp/error_messages_test.go:892`, unchecked `resp.Body.Close()` (`revive`,
`unhandled-error`). It is explicitly excluded from this PR; the bounded fix
would be `require.NoError(t, resp.Body.Close())`. No full-repository lint pass is
claimed. CI lint is observed after pushing, separately from the local result.

Each mutation used `GOMAXPROCS=2 CGO_ENABLED=0` and the CO7 isolated-overlay
command (`-count=1 -timeout=2m`, tagged, with an exact instrument-name filter):

| Planted violation | Rejecting instrument |
| --- | --- |
| Allow a different bound key | `TestIdentityBindingKeyLifetimeAndRedaction` |
| Reject all nonidentical secret serializations | `TestIdentityBindingKeyLifetimeAndRedaction` |
| Skip malformed-secret validation | `TestIdentityBindingKeyLifetimeAndRedaction` |
| Disable the raw-equality fast path | `TestIdentityBindingRawEqualityFastPath` |
| Echo the presented secret in a key-refusal message | `TestIdentityBindingKeyLifetimeAndRedaction` |
| Classify key-load failure as Unauthenticated | `TestInvokeIdentityKeyLoadAndMissingKidClassification` |
| Classify missing kid as Unauthenticated | `TestInvokeIdentityKeyLoadAndMissingKidClassification` |
| Disable missing-key refresh | `TestInvokeIdentityKeyLoadAndMissingKidClassification` |
| Perform an extra refresh | `TestInvokeIdentityKeyLoadAndMissingKidClassification` |
| Skip provisional-validator Close | `TestInvokeIdentityDiscardsValidatorReturnedWithLoadError` and the key-load/missing-kid matrix |
| Retain the context allocated by a failed initial load | `TestFailedRemoteKeyLoadReleasesRefreshContext` |
| Omit closure of a replaced key set | `TestRemoteValidatorRefreshIsOptInAndClosesReplacedKeys` |
| Record Internal for an Unavailable panic response | `TestIdentityRecoveriesTreatNilPanicAsFailure` |
| Mislabel cancellation as unknown | `TestIdentityBuildCancellationAndBudgetStages` |
| Mislabel budget expiry as unknown | `TestIdentityBuildCancellationAndBudgetStages` |
| Describe invoke cancellation as time-budget expiry | `TestIdentityBuildCancellationAndBudgetStages` |
| Redact legacy cleanup error/version fields | `TestUnswappedCloseWarningsPreserveModeOffDetails` |
| Parent refreshed-key lifetime to the invoke | `TestRemoteValidatorRefreshIsOptInAndClosesReplacedKeys` |

All overlays were regenerated after the final lifetime-counter assertions, and
all eighteen were rejected. Original source files were not mutated.

These are SDK/unit and local HTTP fixture observations, not deployed Lambda,
AWS isolation or prepared-provider evidence. No schema/generated files changed;
protobuf generation/breaking checks, a refreshed coverage profile, timing or
allocation benchmarks, external-consumer builds, and the complete earlier
mutation batches were not rerun. Existing suites did rerun their compatibility
and redaction tests. Independent review remains a separate gate.
