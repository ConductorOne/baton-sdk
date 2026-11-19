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

## CO7: approved feedback corrections

Source: adjudicated review of `e0d8b0b2494a58f004aba9437e14a73903843565`.
The approved correction set belongs in this SDK stage; secret rotation and
changing key-load failures to Unavailable are excluded pending a separate ruling.

- I1/I2: validate the whole operator suffix at startup, share the configured CA
  roots for JWKS, reject absent/malformed bearer headers before fetching keys,
  and classify token failures as Unauthenticated. DNS syntax does not establish
  domain ownership or detect multi-label public suffixes.
- I3/I5/I6/I9: use an explicit identity-mode flag in the builder, reloader and
  generation; preserve error causes while tagging build stages; record redacted
  panic stacks and recovery sites, plus the resolved method in the interceptor;
  restore the startup log level on every failed first build, including panic.
  Invoke cancellation stops a build,
  but a successful generation remains process-derived and outlives the invoke.
- I7: reject empty identity-option fields and attach credentials using a new
  request envelope sharing the original payload and headers, without mutating
  the caller's request.
- I10 compatibility amendment: in both modes, close a newly built generation
  when replacement fails or matches no registered service. This fixes an
  existing mode-off resource leak and is an explicit exception to the baseline's
  mode-off-unchanged claim. Unsupported-version diagnostics retain the numeric
  version while still wrapping the transport sentinel; other parser details
  remain redacted. Wire acceptance and status classifications do not change.
- Keep Cobra's existing required-flag behavior in mode off; document why
  identity mode removes those annotations in PreRunE. Remove the ineffective
  cleanup defer around the process-exiting Lambda runtime entry point.

Risk remains HIGH: authentication, cross-invoke resource lifetime and remote
error contracts. No sync/storage hot-path cost curve changes. Required additional
instruments cover custom-CA trust/startup failure, startup-level restoration,
bearer ordering/classification, suffix syntax, stage/cause preservation,
mode-dependent context/redaction, unswapped cleanup in both modes, invoke-only
construction cancellation, redacted panic diagnostics and incomplete options.
Validate representative oracles with isolated planted violations and rerun
affected race suites, builds and vet in both build modes. Results and explicit
remaining limits belong in the CO7 evidence entry.

## CO8: architect rulings and delta-review corrections

Source: R12/R13 rulings and delta review of `e263006412f5c010209dfc05070bd7906b0af9ec`.
This supersedes CO7's exclusions for secret rotation and key-load classification.
Risk remains HIGH; the SDK owns the new binding and key-refresh lifecycle.

- R12 / I4/I5/I9: bind the client id, client key and platform execution scope
  once. Check repeated raw secrets with a constant-time equality fast path;
  otherwise parse and compare JWK thumbprints, not serialization or supplied
  key ids. Accept equivalent serializations, reject different or unparseable
  keys before dispatch, and retain the original generation. Different-key
  refusal is FailedPrecondition with a message distinct from the different-client
  refusal. No rebind is allowed. A key change requires a new execution scope
  and its new environment. Messages/logs never contain a secret or its digest;
  emit no key-change log rather than maintaining a warning cache.
- R13 / I2/I5/I9: a failed key load is Unavailable. A loaded key set missing the
  token's key gets exactly one refresh; a still-missing key is FailedPrecondition.
  Neither outcome is Unauthenticated or PermissionDenied. Failed provisional
  validators are discarded and nothing is bound. Refresh is opt-in for the
  identity handler; legacy validation does not gain a refresh. Replaced key sets
  and failed initialization contexts must be released.
- Review corrections: fix the five CI lint findings; record the same panic
  status returned to the caller; tag construction cancellation separately from
  budget expiry with accurate messages/causes; retain legacy error/version
  warning fields when closing an unswapped mode-off generation; document new
  exported helpers and auth classification options.

Instruments must cover R12's equivalent/different/unparseable keys, pre-dispatch
refusal and original-generation survival, fast-path selection and secret/digest
redaction; R13's network/timeout/status/document failures, one-refresh missing-key
refusal, refresh success and provisional cleanup; and the review's diagnostic
contracts. Validate representative oracles with planted violations, rerun
affected race suites in both build modes, builds, nil-panic smoke and compatible
lint. Record CI lint separately from local proof in CO8 evidence.
