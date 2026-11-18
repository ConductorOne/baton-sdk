# Lambda invoke identity evidence

## Source and scope

Base: public baton-sdk v0.46.0 (`b00aa1a5`). This stage adds the opt-in SDK mode, transport option/field and config response field. Existing functions remain on the legacy path. No dependency upgrade, deployment, AWS resource change or credential provisioning is included.

## Implementation-obligation addendum

- `lambdaInvokeIdentityHandler.prepare` serializes authentication and first binding. A provisional validator is retained only after generation construction and registration succeed. Failed authentication/build closes it. Service registration happens on a new, unpublished server; a failed first build leaves no registered server or binding.
- `lambdaIdentityRuntimeFactory` owns the C1 connection, parsed identity key and process-derived identity context. Failure closes/cancels acquired resources; success transfers ownership to the handler. Token/session clients use the process-derived identity context, not the building invocation.
- `buildIdentityGeneration` owns a process-derived generation context and construction timer. Failure cancels it; success stops the timer but preserves the context until the generation drains and closes. Cooperative operations must honor cancellation; arbitrary connector code cannot be forcibly interrupted by a Go context.
- Scope validation is inside the shared generation builder, before decryption and connector construction. Only the dedicated scope-mismatch error becomes FailedPrecondition; exchange/fetch/constructor errors become Unavailable regardless of nested gRPC status.
- The existing reloader owns replacement/draining. In identity mode a rejected replacement closes the new generation, and generation-close errors do not expose arbitrary error or panic values.
- In identity mode handler recovery and pre-dispatch errors contain fixed text. Transport request decoding returns a fixed error rather than parser diagnostics that can contain payload values. The SDK does not log the identity, client secret, bearer token or raw invoke payload. Connector-authored arbitrary logs are outside the SDK's ability to constrain.
- The bound logger adds `execution_scope` and `client_id` after telemetry initialization, so the same attached fields flow through both the local and export cores. Construction and dispatch receive the parsed bound key; the process context does not.

## Verification record

Current status: evidence incomplete; commands and independent audit results will be recorded after execution. Tests exercise SDK fixtures, not deployed AWS isolation. No lint is run in this stage, as requested.

| Criterion | Instrument | Current status |
| --- | --- | --- |
| I1 | `TestInvokeIdentityHostRejectionsMakeNoOutboundRequests`, `TestInvokeIdentityMissingPlatformScopeAndPanicSanitization` | evidence incomplete |
| I2 | `TestInvokeIdentityJWTBeforeBuildAndRejectedFirstHostDoesNotBind`, `TestInvokeIdentityJWKSRedirectRefused` | evidence incomplete |
| I3 | `TestInvokeIdentityScopeCheckedBeforeConstructionEveryBuild`, handler first/reload failure tests | evidence incomplete |
| I4 | `TestInvokeIdentityBindReloadContextKeyAndLogs`, `TestInvokeIdentityConcurrentFirstInvokeBindsOnce` | evidence incomplete |
| I5 | `TestInvokeIdentityFailedBuildCleansUpAndCanRetry`, `TestIdentityGenerationBudgetAndPersistentLifetime` | evidence incomplete |
| I6 | `TestInvokeIdentityBindReloadContextKeyAndLogs`, existing reload suites | evidence incomplete |
| I7 | `TestInvokeIdentityEncodings`, `TestLambdaTransportInvokeIdentity` | evidence incomplete |
| I8 | `TestInvokeIdentityBindReloadContextKeyAndLogs`, `TestIdentityRuntimeKeyDecryptsDispatchedCredentials` | evidence incomplete |
| I9 | `TestRequestDecodeErrorsDoNotExposePayload`, handler rejection/panic tests | evidence incomplete |
| I10 | `TestInvokeIdentityModeRequiresSuffixButNotLegacyCredentials`, real identity-client fixture, legacy scope test | evidence incomplete |
| I11 | descriptor/presence test, protocol generation/compatibility, tagged tests/build | evidence incomplete |

The fixture matrix is bounded; it is not an exhaustive model of all connector code or Lambda schedules. Independent final-code implementation/evidence audit, structural-coverage triage and representative planted-violation checks remain prerequisites to verification signoff.
