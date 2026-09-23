# FK Inbox JWK Alternative — Evidence

Criterion status is recorded here as it is established. Nothing in this file is
claimed before it is run.

## Branch and commits

Review follow-up: reject every `priv` member and require all extension members
to be present and non-null. An explicit empty `content_type` remains valid and
normalizes to `generic`. Added regression tests for each field's absence/null
and for private-member value types. These follow-up tests have not been run
locally; the earlier results below do not verify this revision. CI must run
tests and lint on the updated draft PR.

Worktree `/data/squire/src/baton-sdk-jwk-alt`, branch
`highb/IGA-4417/fk-inbox-jwk-alternative`.

| Commit | What |
|---|---|
| `21ab55cb` | source: typed JWK extension replaces the config arm |
| `d0e35c2d` | generated: protobuf output for the reserved arm |
| `7f0d1adf` | source: import the provider in the capability gate |
| `84cbb797` | source: migrate the tests to the typed JWK config |
| `6833e28e` | source: stop naming the removed message in the capability comment |
| `c3152f37` | generated: protobuf output for that comment |

The docs and this file land in the commit that carries them. Source always
precedes its generated output and the two are separate commits.

## Toolchain and where the checks ran

The worktree is the environment's own checkout; all commands below ran against
it with the environment toolchain: Go `go1.27.1 linux/arm64`, `buf` `1.64.0`,
`make` 4.4.1. `go` uses the environment's build cache.

`golangci-lint` 2.9.0 here is built with `go1.26.0` and refuses to load a
`go1.27.1` module:

```
Error: can't load config: the Go language version (go1.26) used to build
golangci-lint is lower than the targeted Go version (1.27.1)
```

Lint is therefore **not locally verified** and is deferred to CI's `go-lint`
job at the matching toolchain.

## Commands run so far

| Command | Result | Proves |
|---|---|---|
| `go build ./pkg/...` | exit 0 | production compiles |
| `go vet ./...` | exit 0 (no diagnostics) | every package and test file type-checks, so no caller of the removed config remains |
| `make protogen` | exit 0, no diff | checked-in generated output matches the proto source |
| `buf lint` | exit 0 | proto lint clean |
| `buf format --diff --exit-code` | exit 0 | proto formatting clean |
| `go test ./pkg/crypto/... ./pkg/actions/... ./pkg/connectorbuilder/... -count=1` | all `ok` | the migrated suite and the new cases pass |
| `golangci-lint run …` | config load error | lint deferred to CI (see above) |

## Coverage reduction (plan-mandated, confirmed by inspection)

The plan permits exercising the extension-shape and outer-JWK rows at a
representative gate path **only** if inspection confirms the paths share one
parser. They do. Every gate path reaches the same parser:

- `IssueCredential`, registered actions, `CreateAccount`, `RotateCredential`
  all call `crypto.ValidateEncryptionConfigs`
  (`pkg/connectorbuilder/credentials.go`, `pkg/connectorbuilder/accounts.go`,
  `pkg/actions/actions.go`);
- that calls `providers.GetEncryptorForConfig` and then the provider's
  `ValidateConfig`, which is `vaultinbox.Provider.ValidateConfig` →
  `recipientFromConfig` → `parseJWKDocument` / `parseExtension`;
- `Encrypt` calls the same `recipientFromConfig` before any sealing.

So the extension-shape and outer-JWK rows are exercised once at the parser
(`pkg/crypto/providers/vaultinbox`) and the per-path rows are exercised at the
gate (`pkg/connectorbuilder`, `pkg/actions`) and via
`crypto.ValidateEncryptionConfigs`. The reduction is by the gate paths, not the
parser.

## Criterion results

| Criterion | Result | Evidence |
|---|---|---|
| C1 byte-identical wire output | PARTIAL — binding, payload and fixture compatibility verified; same-randomness differential not run | The committed fixture `testdata/vault-inbox-submission-vector.json` was produced before this alternative (commit `57e5f9cc`) and the JWK-configured path binds the same coordinates (`binding_hex` equal) and opens its envelope to the exact expected payload (`TestCommittedInteropVectorMatchesProvider`). `TestBindingBytesMatchTheLatchkeyFraming` pins the AAD bytes against the Latchkey framing. Neither branch injects HPKE randomness, so ciphertext bytes cannot be pinned equal; the only remaining difference between branches is the random encapsulation. A literal `ae9de8d6`-vs-`HEAD` same-randomness ciphertext differential is therefore NOT RUN. |
| C2 binding bytes unchanged | VERIFIED | `TestBindingBytesMatchTheLatchkeyFraming` (pinned hex) and the committed `binding_hex` |
| C3 thumbprint unchanged, excludes extension | VERIFIED | `TestPublicKeyThumbprintMatchesLatchkeyVector` pins the Latchkey digest; `TestThumbprintIgnoresTheExtension` changes every extension coordinate and still validates under the same canonical `{alg,kty,pub}` thumbprint |
| C4 refusals occur before provider invocation | VERIFIED to the mutation-checked cases; pre-mint position pinned | `TestVaultInboxProviderRefusesAForeignProviderName`, `TestVaultInboxIssueCredentialRefusesBeforeMinting`, `TestVaultInboxRotateRefusesBeforeMinting` all assert `issuer/rotate calls == 0`; the malformed-shape table is refused inside `ValidateConfig`, before `Encrypt` |
| C5 no downgrade on missing/wrong provider | VERIFIED | wrong and empty provider refused (`TestValidateConfigRejectsUnsupportedProfiles`, `TestVaultInboxProviderRefusesAForeignProviderName`); an extended JWK with **no** provider is routed to the classical JWK provider and refused there rather than sealed (`TestExtendedJWKWithoutTheProviderDoesNotDowngrade`) |
| C6 capability/exclusivity/cardinality | VERIFIED | `TestVaultInboxProfileMustBeAdvertised`, `TestVaultInboxRecipientMustBeTheOnlyRecipient`, `TestVaultInboxIssuanceRequiresExactlyOnePlaintext`, `TestRegisteredActionVaultInboxCardinality`, `TestVaultInboxCreateAccountKeepsStructuredResults`, `TestVaultInboxRotateRefusesBeforeMinting`, `TestVaultInboxIssueCredentialMintsOnceAndSeals`, `TestVaultInboxIssueCredentialFailsAfterOneMint` |
| C7 conflicting kid refused | VERIFIED | `TestValidateConfigRefusesAConflictingKid`; guard isolated by mutation |
| C8 `baton/jwk/v1` and `age` unchanged | VERIFIED within the targeted run | `pkg/crypto/providers/jwk` and `pkg/crypto/providers/age` suites pass unchanged; no source change touches those packages |
| C9 round trip / two seals differ / tamper | VERIFIED | `TestEncryptProducesTheVaultInboxEnvelope` opens the envelope; `TestTwoSealsDiffer` shows the encapsulation is randomized; `TestEveryBindingFieldIsAuthenticated` fails to open after each single binding field is flipped |
| C10 no arm-102 leftovers | VERIFIED | `testdata` and `pb` are the only hits for the removed identifiers; the one production reference was a stale comment and is fixed in `6833e28e`/`c3152f37` |
| C11 measured delta | VERIFIED (measured, direction reported) | see below |

### C11 measurement

Production Go (excluding tests), `git diff --stat ae9de8d6 HEAD`:
`jwkconfig.go` +459, `vaultinbox.go` −164 (net −142), `registry.go` ±14,
`credentials.go` ±8 — net +305 lines, all in the vault-inbox package.

Proto source vs `ae9de8d6`: `resource.proto` −30 net (the
`VaultInboxRecipientConfig` message and `VaultInboxConfigVersion` enum removed,
field 102 and its name reserved); `connector.proto` one comment line changed.

Generated output vs `ae9de8d6`: `pb/` −825 net, including
`resource.pb.validate.go` −165 (the removed message's validator).

Against `origin/main` the alternative is purely additive (main has no vault-inbox
profile): production +305 and the whole feature +1727 lines including tests and
fixtures.

Direction: the alternative **relocates** the same number of protocol fields from
a dedicated protobuf message into a JWK extension and **removes** one message and
one enum, at the cost of ~460 lines of strict JSON parsing. It does not reduce the
schema to zero.

## Mutation matrix (the plan's instrument)

Every guard was disabled in turn and the package suite re-run; a guard is
"isolated" only when disabling it makes a test fail. 22 guards checked, 16
isolated.

| Guard disabled | Result | Failing case |
|---|---|---|
| `kid` vs `key_id` agreement | isolated | `TestValidateConfigRefusesAConflictingKid` |
| outer duplicate member | isolated | `…MalformedExtension/duplicate_outer_member` |
| extension duplicate member | isolated | `…MalformedExtension/duplicate_ext_member` |
| extension version | isolated | `…MalformedExtension/version_2`, `…UnsupportedProfiles/extension_version_unset` |
| extension suite | isolated | `…MalformedExtension/wrong_suite`, `…UnsupportedProfiles/suite_unset` |
| null member (`null kid`) | isolated | `…MalformedExtension/null_kid` |
| private material | isolated | `…MalformedJWK/private_present` |
| payload scheme | isolated | `…MalformedExtension/missing_payload_scheme`, `…UnsupportedProfiles/unsupported_scheme` |
| key generation non-zero | isolated | `…MalformedExtension/missing_generation`, `…UnsupportedProfiles/generation_zero` |
| identifier validation | isolated | eleven empty/oversized identifier cases across both tables |
| content-type bound | isolated | `…UnsupportedProfiles/content_type_too_long` |
| JWK size bound | isolated | `TestOversizedPublicJWKIsRejected` |
| provider mismatch | isolated | `…UnsupportedProfiles/{empty,wrong}_provider` |
| unknown config fields | isolated | `…UnsupportedProfiles/unknown_fields` |
| low-order X25519 probe | isolated | `…UnsupportedProfiles/low-order_recipient` |
| thumbprint agreement | isolated | `…UnsupportedProfiles/thumbprint_mismatch` |
| unknown extension member | **not isolated** | shadowed by `json.Decoder.DisallowUnknownFields` on the extension struct |
| trailing after the outer object | **not isolated** | shadowed by `json.Unmarshal` into the member map |
| trailing after the extension | **not isolated** | unreachable: `objectMemberNames` refuses trailing content before `parseExtension`'s own check |
| extension present | **not isolated** | shadowed by `parseExtension`'s "not a JSON object" check on an absent member |
| JWK config present | **not isolated** | shadowed by the parse of the empty `pub_key` |
| public key length | **not isolated** | shadowed by `hpke.MLKEM768X25519().NewPublicKey` |

The six non-isolated rows are recorded as gaps, not as coverage: each case is
still refused (its `require.Error` holds), but by a sibling guard, so the named
guard is redundant rather than proven. The trailing-after-the-extension guard in
particular is unreachable in the current call graph.

## Evidence gaps, stated

- **C1 same-randomness differential not run.** Neither branch exposes an
  injection point for the HPKE encapsulation, so byte-identical ciphertext for
  the same coordinates cannot be demonstrated across branches. The achievable
  claim — identical binding bytes, identical payload container, and an
  arm-102-era committed fixture that the new path opens — is what is recorded.
- **Six guards are shadowed** (table above). Their cases prove the refusal, not
  the specific guard.
- **Local lint unavailable.** Deferred to CI's `go-lint` job; reported as
  CI-verified, not locally verified.
- **C1 Rust ingestion and reveal are out of scope here.** They are a separate
  required integration lane. No result in this file is evidence for them; the
  strongest claim here is that the emitted bytes are byte-identical to the path
  the Rust reader already consumes.
- **Attestation is not verified by this profile.** A JWK plus a matching
  thumbprint does not authenticate the destination. The SDK trusts the
  authenticated C1 action transport. This is a documented boundary (§6.3 of
  `docs/vault-inbox-delivery.md`), not a gap this change closes.
- **`kid` is now coupled to `key_id`.** Per the plan's C7, a JWK whose `kid`
  disagrees with `key_id` is refused. A producer that emits an unrelated `kid`
  must match `key_id` or omit it. This is a producer-facing tradeoff, recorded in
  §5.1 of the delivery doc.
- **The full repository test suite was not run.** Only the four affected package
  groups were exercised; `go vet ./...` compiled every package and test file.
