# FK Inbox JWK Alternative — Evidence

Criterion status is recorded here as it is established. Nothing in this file is
claimed before it is run.

## Status at plan freeze

No implementation exists yet. No criterion is satisfied. This section is empty of
results on purpose: the plan is frozen implementation-blind, and filling this in
with a reconstruction of the finished diff would defeat that.

## Commands run so far

| Command | Result | Proves |
|---|---|---|
| `git show origin/main:proto/.../resource.proto \| grep -n VaultInbox` | no match | arm 102 is not in main |
| `git show v0.31.0:proto/.../resource.proto \| grep -c VaultInbox` | `0` | arm 102 is not in the latest release |
| `git worktree add` from the pushed branch | clean | isolated worktree; PR1145 checkout untouched |

## Criterion results

| Criterion | Result | Evidence |
|---|---|---|
| C1 byte-identical wire output | NOT RUN | — |
| C2 binding bytes unchanged | NOT RUN | — |
| C3 thumbprint unchanged, excludes extension | NOT RUN | — |
| C4 refusals occur before provider invocation | NOT RUN | — |
| C5 no downgrade on missing/wrong provider | NOT RUN | — |
| C6 capability/exclusivity/cardinality | NOT RUN | — |
| C7 conflicting kid refused | NOT RUN | — |
| C8 jwk/age unchanged | NOT RUN | — |
| C9 round trip / tamper | NOT RUN | — |
| C10 no arm-102 leftovers | NOT RUN | — |
| C11 measured delta | NOT RUN | — |

## Evidence gaps, stated up front

- **Every negative case must be mutation-checked.** A previous pass on this
  codebase produced a gate over a comment-only change that could not detect
  commentary, and a test that passed with its subject deleted. Until each C4/C5
  row has been shown to fail when the corresponding check is disabled, those rows
  are unproven.
- **Local lint is unavailable.** `golangci-lint` 2.9.0 in this environment was
  built with go1.26.0 and cannot load a go1.27.1 module. Lint coverage will come
  from CI's `go-lint` job at the matching toolchain, and this will be reported as
  CI-verified rather than locally verified.
- **C1 Rust ingestion and reveal are out of scope here.** They are a separate
  required integration lane. No result in this file will be presented as evidence
  for them; the strongest claim available here is that the emitted bytes are
  byte-identical to the path the Rust reader already consumes.
- **Attestation is not verified by this profile.** A JWK plus a matching
  thumbprint does not authenticate the destination. The SDK trusts the
  authenticated C1 action transport. This is a documented boundary, not a gap to
  be closed by this change.
