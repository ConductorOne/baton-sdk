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
