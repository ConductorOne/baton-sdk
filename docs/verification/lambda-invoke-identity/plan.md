# Lambda per-invoke identity verification plan

Frozen behavioral baseline for CXF-63, SDK stage. Mode is opt-in and off by default.
Risk: HIGH (authentication, secret delivery, lifecycle and remote consumers). No change to sync/storage cost curves.

| ID | Required behavior and bounded verification space |
| --- | --- |
| I1 | Missing identity, invalid client-id/host grammar, wrong/look-alike suffix, extra label and absent platform scope reject with FailedPrecondition, before any outbound request. Cover DNS label/name limits, hyphens, uppercase, IPs, URL delimiters, ports, percent encoding and trailing dots. |
| I2 | JWT signature, issuer, subject, audience, expiry and not-before are validated before credential exchange/config fetch/construction. Rejected first identity leaves an unbound environment; a later valid host can bind. JWKS redirects are refused. |
| I3 | Each first/reload config response must state the non-empty platform execution scope; missing/mismatch constructs nothing and returns FailedPrecondition. |
| I4 | First successful build registers services and binds one client id and scope, without reload rate caps. Other identities/scopes reject without changing the active generation; same identity remains usable after the first invoke ends. |
| I5 | Exchange/fetch/construction failure returns Unavailable, closes acquired resources and leaves no first-build registration/binding. Builds have deadlines; persistent resources derive from process context. |
| I6 | Authenticated config-version changes reload only the bound identity and retain the existing reload caps. Failed reload retains the active generation. |
| I7 | Client option sets Lambda TenantId and typed request identity. Identity round-trips in dual, frame-only and legacy encodings, never into incoming metadata. Existing client calls remain source-compatible. |
| I8 | The parsed bound key reaches construction and every dispatched RPC; existing encrypted-password conversion works with it. Dispatched logs and exported log records include execution_scope and client_id. |
| I9 | Marker secrets and bearer tokens never appear in transport decode errors, pre-dispatch errors, panic records, Lambda failure summaries or client-returned errors. Exercise decode, host, token, build and dispatch failures. |
| I10 | Mode settings work through flags/env; suffix required only in mode. Legacy credential/auth/host overrides are ignored in mode. Mode-off behavior and existing tests remain unchanged. |
| I11 | Additive protocol fields have requested numbers and optional presence. Generated files match definitions; touched-package suites and tagged SDK build pass. No lint in this stage. |

## Instruments and limits

Use table-driven host/protocol tests, signed-token fixtures, fake exchange/config/constructor counters through the actual handler, resource-close counters, deadline/cancellation checks, captured log records, and an HTTP redirect fixture. Run touched-package suites with baton_lambda_support and race detection where supported, then the tagged SDK build. Validate representative rejection/counter and serialization oracles against planted violations. Record exact commands, outcomes, exclusions and uncovered changed branches in evidence.md. Tests prove SDK/fixture behavior, not deployed AWS isolation or a deployed provider; live integration belongs to the consuming release.

## Implementation obligations

Append resource ownership, registration/binding atomicity, validator lifetime, context and secret-error obligations after implementation, without weakening the baseline above. Independent evidence audit and focused implementation review remain required before signoff.
