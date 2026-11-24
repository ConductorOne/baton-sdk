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
