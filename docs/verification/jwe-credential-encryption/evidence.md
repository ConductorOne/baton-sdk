# JWE SDK evidence

Stage: SDK implementation and verification. J10 (native Rust ingestion) is
deferred to the C1/Multipass integration stage.

Environment: remote, inside the SDK checkout. Go 1.27.1, buf 1.64.0, protobuf
33.5, golangci-lint v2.13.2 (installed to match the version CI pins; the
environment's preinstalled 2.9.0 refuses a go1.27.1 module).

All commands below were run remotely. Source changes were committed before any
generated artifact, and generated protobuf is its own commit.

## Status by criterion

| ID | State | Evidence |
| --- | --- | --- |
| J1 | verified to stated coverage | AAD proto round trip (binary, JSON, empty, invalid UTF-8); ignored JWK properties never reach headers |
| J2 | verified to stated coverage | Framing assertions, independent HPKE reader, committed fixture |
| J3 | verified to stated coverage | Parser matrix, three distinct public-key rejection mechanisms, bounded parser fuzz |
| J4 | verified to stated coverage | Fan-out and cardinality tests with connector-invocation counts |
| J5 | verified to stated coverage | Existing JWK/age suites plus direct-provider and resolver rejection tests |
| J6 | verified to stated coverage | Capability present only on issuer resource types and connector |
| J7 | verified to stated coverage | Tamper matrix over encapsulation, ciphertext, tag, header, context, and key |
| J8 | verified to stated coverage | Secret-marker assertions across parser and builder errors |
| J9 | verified to stated coverage | Remote generation, drift check, buf lint/format/breaking, lint, race, broad suite |
| J10 | deferred to C1/Multipass integration | Committed fixture is SDK-side evidence, not interop proof |

## Generator and tooling

- `make protogen` -> `buf generate`. buf 1.64.0, protobuf 33.5, Go 1.27.1.
  Five generated files changed: `connector.pb.go`, `connector_protoopaque.pb.go`,
  `resource.pb.go`, `resource.pb.validate.go`, `resource_protoopaque.pb.go`.
  No manual edits to generated files.
- **Drift check**: `make protogen` re-run against the committed tree produced no
  change, so the committed generated code is reproducible from the committed
  protos.
- `buf lint` clean. `buf format --diff --exit-code` clean.
- `buf breaking --against '.git#branch=origin/main'` clean against `3df6fad5`.
  The proto change is additive only: `CAPABILITY_CREDENTIAL_ENCRYPTION_JWE = 16`
  (no renumbering; 14 is untouched) and `additional_authenticated_data = 2` on
  `JWKPublicKeyConfig` (field 1 untouched).
- `filippo.io/hpke v0.4.0` was already pinned and vendored but marked `//
  indirect`. `go mod tidy` moved it to the direct block. `go mod vendor`
  produced no vendor or `go.sum` change and `go mod tidy -diff` is empty. No
  version change and no new crypto dependency.

Local/CI tool version differences, recorded rather than hidden: buf 1.64.0 here
vs 1.72.0 in CI; golangci-lint 2.13.2 matched by installing the pinned release.

## Declared configuration bounds

`EncryptionConfig.JWKPublicKeyConfig.pub_key` (tag 1) and
`additional_authenticated_data` (tag 2) now declare
`[(validate.rules).bytes = {max_len: 16384}]`. Maximum only: no minimums, no tag
changes, no provider or selection semantics change. The bound on `pub_key`
deliberately covers legacy JWK recipients as well as the JWE X-Wing recipient.

The generated validators enforce it, and the numbers match the provider's
runtime limits (`MaxJWKBytes` and `MaxAdditionalAuthenticatedDataBytes`, both
16 KiB), so the two layers agree on what is acceptable.

Red then green, in the committed history:

```
# at bddd0216's parent, before the generated update (tests and rule declared, generator not re-run)
--- FAIL: TestJWKPublicKeyConfigPubKeyBound/one_byte_above_the_bound
--- FAIL: TestJWKPublicKeyConfigAdditionalAuthenticatedDataBound/one_byte_above_the_bound
--- FAIL: TestJWKPublicKeyConfigBoundsAreIndependent/only_pub_key_is_over
--- FAIL: TestJWKPublicKeyConfigBoundsAreIndependent/only_the_context_is_over
--- FAIL: TestJWKPublicKeyConfigBoundsAreIndependent/both_are_over_and_both_are_reported

# after regenerating
ok  github.com/conductorone/baton-sdk/pb/c1/connector/v2
```

Coverage: each field is measured independently at 16383 / 16384 / 16385, absent
and empty values are unchanged, the nested `ValidateAll` reports both fields when
both are over, representative legacy RSA, EC and Ed25519 public JWKs plus the
X-Wing AKP JWK all fit inside the bound, and `TestRuntimeBoundsMatchDeclaredProtobufBounds`
pins the runtime constants against the declared bound.

## Command log

```
go test ./pkg/crypto/... ./pkg/connectorbuilder/ -count=1
  -> ok: pkg/crypto, pkg/crypto/providers, .../age, .../jwe, .../jwk, pkg/connectorbuilder

go test ./pkg/crypto/providers/jwe/ ./pkg/connectorbuilder/ -count=1 -v
  -> 100 top-level PASS, 214 subtest PASS, 0 FAIL, 1 SKIP
     (the skip is the fixture regeneration path, gated behind JWE_FIXTURE_REGENERATE=1)

go test -race ./pkg/crypto/... ./pkg/connectorbuilder/ -count=1
  -> ok, no data races

go test ./pkg/crypto/providers/jwe/ -coverprofile=...
  -> 93.9% of statements

golangci-lint run ./pkg/crypto/... ./pkg/connectorbuilder/   -> 0 issues
golangci-lint run (whole repo)                               -> 0 issues
gofmt -l pkg/crypto pkg/connectorbuilder                     -> no output

buf lint                        -> exit 0
buf format --diff --exit-code   -> exit 0
buf breaking --against '.git#branch=origin/main'
                                -> exit 0, no breaking changes
golangci-lint run (whole repo, pinned v2.13.2) -> 0 issues

go test ./pkg/crypto/providers/jwe/ -run '^$' -fuzz '^FuzzPublicJWKFields$' -fuzztime=60s -parallel=2
  -> PASS, 101,685 execs, 158 interesting corpus entries, 0 failures
go test ./pkg/crypto/providers/jwe/ -run '^$' -fuzz '^FuzzRecipientPreflight$' -fuzztime=60s -parallel=2
  -> PASS, 88,931 execs, 156 interesting corpus entries, 0 failures

go test -tags=baton_lambda_support -count=1 ./...
  -> exit 0; 90 of 90 packages in the module reported,
     65 ok and 25 with no test files, zero failures
```

## Coverage triage

Changed-package statement coverage is 93.9%; every function in the provider is
at or above 92% except `Encrypt` at 87%. Six blocks remain uncovered and are
unreachable by construction:

- `hpke.NewSender` failure and `Sender.Seal` failure: these can only fail on
  random-source failure or a nil AEAD, neither reachable with a validated key.
  The checks are kept because the library returns errors.
- `json.Marshal` of the six-string envelope: cannot fail.
- `ecdh.X25519().NewPrivateKey` on a 32-byte scalar: cannot fail.
- `ecdh.X25519().NewPublicKey` on the final 32 bytes: the slice length is fixed
  by the prior X-Wing length check.
- `token.(string)` failing inside an object: encoding/json always yields string
  object keys, or an error handled by the preceding branch.

No uncovered block was left as an unassessed obligation: two reachable blocks
were found and instrumented (cancelled context; structurally malformed JWK
shapes), which is what raised coverage from 91.8% to 93.9%.

## Instrument validation (planted violations)

Each plant was applied to a disposable copy of the checkout, run against the
targeted tests, and deleted. Nothing was pushed; the pristine tree was verified
clean afterwards (`git status --porcelain` empty, no leftover copies).

| Plant | Applied to | Targeted tests | Observed |
| --- | --- | --- | --- |
| Drop the HPKE AAD binding (`Seal(nil, …)`) | `jwe.go` | framing, payload/AAD matrix, freshness, tamper matrix | 4 tests FAIL, all with `chacha20poly1305: message authentication failed` |
| Bypass pre-mint validation (`ValidateEncryptionConfigs` returns nil) | `crypto.go` | create/rotate preflight, issuance rejections | 6 tests FAIL, including both zero-invocation create/rotate cases |
| Remove the low-order X25519 probe | `jwe.go` | validation matrix, mechanism test | matrix FAILS on exactly `X25519 low-order point zero` and `one` |
| Remove the protected-header bound | `jwe.go` | header limit tests, real issuance case | 4 tests FAIL, including `TestIssueCredentialRejectsOversizedProtectedHeaderBeforeIssuerCall` |

Two findings from the plant run worth recording:

- The tamper matrix passes trivially when sealing stops binding AAD, because
  every case expects a rejection. It is the *positive control* in each tamper
  test — and in the framing test — that catches the plant. The control is
  load-bearing, not decoration.
- `TestPublicKeyRejectionsUseDistinctMechanisms` still passes when the probe is
  removed, because it probes `hpke` and `crypto/ecdh` directly to prove the
  premise (the parser accepts low-order points; ECDH rejects them). The
  validation matrix is the test that fails on removal. The two instruments are
  complementary: one proves the premise, the other pins production behaviour.

Pre-fix red evidence for the protected-header limit, at `3a81f651` (tests and
constant present, check absent):

```
--- FAIL: TestProtectedHeaderSizeLimit/one_byte_over_the_limit
--- FAIL: TestProtectedHeaderSizeLimitKeyIDCases/escaped_key_id_within_the_key_id_limit_is_refused
--- FAIL: TestProtectedHeaderLimitErrorsDoNotEchoKeyID
```

Pre-fix red evidence for the create/rotate preflight, before `2fa185d6`:
`TestCreateAccountValidatesEncryptionConfigBeforeMutation` and
`TestRotateCredentialValidatesEncryptionConfigBeforeMutation` failed with
connector invocation counts of 1 instead of 0, and the fan-out case returned no
error at all.

## Fixture provenance

`pkg/crypto/providers/jwe/testdata/fixture.json`, produced remotely by
`TestRegenerateXWingJWEFixture` (`JWE_FIXTURE_REGENERATE=1`).

Contains only a synthetic public throwaway key and payload: a 32-byte seed, the
derived 1216-byte X-Wing public key, the plaintext, the AAD, the key id, the
exact flattened JWE JSON, and the primitive profile with recorded provenance
(KEM `0x647a`, KDF `0x0001`, AEAD `0x0003`, `filippo.io/hpke v0.4.0`, go1.27.1).

`TestXWingJWEFixture` re-derives the key from the seed, asserts the recorded
profile ids, asserts the framing shape, asserts the recorded `hpke_version`
matches the linked module (so a library upgrade that changed behaviour fails the
test), and decrypts the committed bytes with the independent reader.

This is SDK-side evidence a second implementation can ingest. It is **not**
proof that any Rust implementation interoperates; that is J10.

## Gaps and limits of this evidence

- The committed fixture is not interoperability proof and no second
  implementation was exercised. Draft-vector comparison was not performed; the
  private suite mapping is not HPKE-4 or HPKE-9, so a shared published vector
  would not apply to this algorithm identifier.
- Fuzzing was a bounded 60-second soak per target, not an extended run.
- `JWE iv`/`JWE tag` are not authenticated inputs. The reader ignores them
  because the profile requires them empty and the HPKE tag is inside
  `ciphertext`; a reader that treated them as ciphertext would disagree with
  this wire format rather than detect tampering.
- The `BATON_COMPAT=1` cross-SDK checkpoint harness (the third leg of
  `make compat-check`) was not run: it builds pinned older SDK trees fetched
  over the network. The two local legs are covered by the broad suite below.
- Independent evidence audit and focused implementation review are the
  supervisor's; this file records only what the implementation side executed.
