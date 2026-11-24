# Strict Lambda config version verification plan

## Scope and risk

A request carrying `x-baton-connector-config-version-strict` must not reach a connector whose current config version differs from the request's `x-baton-connector-config-version` stamp after the normal reload attempt. Return gRPC `FailedPrecondition` without dispatch. Header presence opts in; its value is not interpreted. Requests without the strict header retain existing behavior. Reload failures retain their existing status.

HIGH: a version-dependent dispatch error can produce irreversible external writes. The change is opt-in and adds constant-time header lookup and version comparison; no data-volume-dependent cost changes. This stage implements only SDK enforcement. Caller adoption, retry policy, runtime releases and deployed convergence belong to consumers, not this change.

## Frozen behavioral criteria

| ID | Required coverage and oracle |
| --- | --- |
| V1 | Warm generation A; a strict B request inside the one-second global floor returns `FailedPrecondition`, does not rebuild and does not dispatch. |
| V2 | Warm generation A after a successful rebuild requested for B that still served A; a repeated strict B request outside the global floor but inside the five-second repeat cap returns `FailedPrecondition`, does not rebuild and does not dispatch. |
| V3 | After either cap expires, strict B triggers a rebuild, serves B and dispatches exactly once. The connector observes B. |
| V4 | A successful rebuild that still serves A returns `FailedPrecondition` for strict B, with no dispatch. |
| V5 | Strict requests stamped with current A dispatch without rebuilding. Empty or non-boolean strict header values still opt in. A missing/empty version stamp follows the same current-versus-stamped comparison. |
| V6 | Non-strict B requests in both capped states continue dispatching A; unstamped non-strict requests remain unchanged. |
| V7 | A failed reload preserves its existing `Unavailable` response and dispatches nothing, with or without strict opt-in. |
| V8 | Exercise the actual Handler and registered RPC transport path, asserting response status and connector dispatch counts, not only helper return values. |

Fixtures establish cap timestamps directly, not with sleeps, and assert the active generation, requested rebuild version, build count and dispatch count. Tests run serially wherever process-global logging is involved.

## Instrument validation and gates

Plant representative violations, one at a time, and restore source after each: bypass strict enforcement; reject before reload (preventing convergence); ignore the strict opt-in (breaking legacy dispatch); and dispatch before refusing (violating the no-write guarantee). Each must make its relevant behavioral test fail for the intended assertion, not a build/setup failure.

Run affected packages with `-race`, with and without `baton_lambda_support`; run `go build ./...` both ways. Record exact commands, revisions, failures, exclusions and mutation results in `evidence.md`. Inspect changed-branch coverage and account for uncovered branches. Request independent final-code implementation and evidence review before full signoff; any unavailable review remains an explicit evidence limit.

## Initial evidence status

V1–V8: not assessed. No deployed behavior claimed.

## Implementation-obligation addendum

The implementation reads header presence after `reloadIfNeeded`, then obtains the generation label through the existing mutex-protected `currentVersion`. It adds no closer, counter, cache, persistent state, retry or ownership transfer. The status is returned as a transport `ErrorResponse`, before log-level application and server dispatch.

| ID | Invariant, premise and instrument |
| --- | --- |
| I1 | Compare the generation after reload, not before: V3 observes an actual service replacement and dispatches B; V4 observes a completed rebuild still serving A and refuses. |
| I2 | Header presence, not a parsed boolean, controls opt-in: V5 includes empty and `false` values through JSON request encoding/decoding. |
| I3 | A refusal performs no RPC side effect: the registered generated Grant handler records its connector's generation in a separate dispatch list; V1, V2, V4 and V5 compare this list. |
| I4 | Preserve the serialized gRPC status: every fixture invocation round-trips both request and response JSON and extracts the received status. Reload errors cover legacy and invoke-identity reloader modes. |

Lambda invokes a handler serially per execution environment. This change does not introduce serialization around independently concurrent direct calls to the reloader: a caller must not replace the generation concurrently between the version comparison and server dispatch. Supporting such a caller would require a separate generation-pinned dispatch contract. Existing generation replacement/drain and identity authentication are unchanged.

The implementation-blind plan was committed before further test/API inspection; the assignment required reopening the cited `handle` and reload lines before that plan. This ordering limitation is recorded rather than claiming the plan was written without any implementation context.

## CO1: decoded header presence

Clarification of V5 and I2 at the metadata boundary: `UnmarshalMetadata` drops empty string values, so an empty-valued strict header is absent in `Request.Headers()`. A strict call must send a non-empty header value (callers use `true`); any non-empty value, including `false`, opts in. Raw empty-header detection would require a transport API change and is outside this stage.

The initial empty-value expectation is replaced by a known-answer compatibility assertion that `strict: ""` remains non-strict. The original criterion and initial failed evidence remain recorded. This clarification changes no metadata serialization or dispatch behavior outside the new opt-in. Risk remains HIGH; all existing instruments and gates still apply.

## CO2: concurrent invocation dispatch pinning

Correction required by independent review: the vendored Lambda runtime supports concurrent invocation loops when `AWS_LAMBDA_MAX_CONCURRENCY` exceeds one. The serial-only assumption in the implementation-obligation addendum is false and is withdrawn. The original exact-generation contract includes these invocations.

Risk remains HIGH. A strict request must either acquire the checked connector generation for dispatch, retaining its existing in-flight ownership until completion, or refuse with `FailedPrecondition`. A concurrent non-strict invocation may rebuild and replace that generation between the version comparison and dispatch acquisition; this must never send the strict request to the replacement.

| ID | Required coverage and oracle |
| --- | --- |
| V9 | Pause strict A after its version check and before implementation acquisition; run a non-strict B invocation that rebuilds and serves B; resume A. Strict A refuses without dispatching on B. Observe both the forced barrier and the B rebuild. |
| V10 | Pause strict A after implementation acquisition; rebuild to B concurrently. Strict A completes on A, B dispatches on B, and A is not closed before its acquired request finishes. |
| I5 | Registry comparison and in-flight acquisition occur under the same mutex as replacement; release is owed exactly once on every dispatch exit. Non-strict Handler keeps its existing path. |

Before fixing, execute V9 against the reviewed implementation and retain its failing output. Plant a bypass of generation-pinned acquisition and require the same behavioral failure. Re-run both-mode package race/build gates and retain exact mutation patches, output and changed-branch coverage with this verification record. No deployed concurrent-runtime claim is made.

### CO2 implementation obligations

`HandlerWithDispatchGuard` runs the caller's guard before registry acquisition and releases it only after an in-flight service reference has been acquired, or on an acquisition error. The strict reloader guard takes `r.mu`, rechecks the current version, and returns its unlock operation. Reload already takes `r.mu` before replacing services under the server mutex. This preserves the lock order `r.mu → server.mu` and releases both locks before connector code runs; reload can proceed while an acquired request finishes on its original implementation.

| ID | Invariant and instrument |
| --- | --- |
| I6 | The external generation lock remains held while waiting for the registry mutex: `TestDispatchGuardHeldUntilImplementationAcquired` observes that blocked state and probes the external lock. A release-before-acquire mutation must fail this oracle. |
| I7 | The external guard release runs exactly once on refusal, malformed/unknown method, successful acquisition, timeout decode error and connector error. The in-flight reference is also released on every post-acquisition exit. `TestHandlerWithDispatchGuard` covers those cases and both wire encodings. |
| I8 | Version identity is not inferred from a connector pointer. V9 covers both distinct and reused implementation pointers under a new generation label. |

The initial version check remains a fast refusal before log-level application. The authoritative check runs inside the dispatch guard. The reloader's test hook pauses only between these two checks and is never populated in production. It owns no resource and transfers no lock.

## CO3: typed configuration-version refusal detail

Additive contract clarification: callers must distinguish SDK configuration-version refusals from connector/vendor `FailedPrecondition` statuses without inferring meaning from the message. Both strict-refusal exits retain `FailedPrecondition` and the fixed message `lambda-run: connector config version is not ready`, and add `google.rpc.ErrorInfo` with reason `CONNECTOR_CONFIG_VERSION_NOT_READY` and domain `baton-sdk` (subject to confirming an existing SDK domain convention).

Risk remains HIGH because a caller's retry classification crosses repositories. No caller retry policy or ordinary connector error changes belong in this stage.

| ID | Required coverage and oracle |
| --- | --- |
| V11 | Both the early capped/skew refusal and the authoritative concurrent-acquisition refusal carry exactly the agreed reason/domain pair after response JSON and gRPC status round-trip; code and message remain unchanged. |
| V12 | An ordinary connector/vendor `FailedPrecondition`, even with the same message, carries no SDK generation marker. Execution-scope mismatch retains its existing status with no generation marker. Existing foreign ErrorInfo is preserved, not rewritten. |
| I9 | A single helper owns the SDK refusal and its static detail. Error-detail attachment cannot return an untyped generation refusal on failure; any encoding failure must fail closed without dispatch. |

Plant removal of the detail and wrong reason/domain values, require the detail oracle to reject each, and retain exact mutation diffs/output. Re-run affected package race tests and both builds. This is local serialization/fixture proof, not evidence of a deployed caller decoding the marker.
