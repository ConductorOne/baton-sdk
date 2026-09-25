# JWE SDK evidence

Stage: SDK implementation and verification. J10 (native Rust ingestion) is
deferred to the consuming-implementation integration stage.

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
| J10 | deferred to consuming-implementation integration | Committed fixture is SDK-side evidence, not interop proof |

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
  The proto change is additive only: `CAPABILITY_CREDENTIAL_ENCRYPTION_JWE_XWING_V1
  = 16` (no renumbering; 14 is untouched) and `additional_authenticated_data = 2`
  on `JWKPublicKeyConfig` (field 1 untouched). The capability name is suite
  specific, so it does not claim arbitrary JWE suites (CO-5).
- `filippo.io/hpke v0.4.0` was already pinned and vendored but marked `//
  indirect`. `go mod tidy` moved it to the direct block. `go mod vendor`
  produced no vendor or `go.sum` change and `go mod tidy -diff` is empty. No
  version change and no new crypto dependency.

Local/CI tool version differences, recorded rather than hidden: buf 1.64.0 here
vs 1.72.0 in CI; golangci-lint 2.13.2 matched by installing the pinned release.

### Capability naming (CO-5)

The capability was renamed from `CAPABILITY_CREDENTIAL_ENCRYPTION_JWE` to
`CAPABILITY_CREDENTIAL_ENCRYPTION_JWE_XWING_V1`, because the producer implements
one suite rather than JWE generally. Renaming a wire-visible enum name is only
safe while nothing released depends on it, so this was checked first rather than
assumed:

- The branch head is not an ancestor of `origin/main`, and the pull request is
  open with `mergedAt: null`.
- `origin/main`'s `Capability` enum ends at `CAPABILITY_CREDENTIAL_ISSUE = 14`, so
  values 15 and 16 are both free and the symbol does not exist on `main` at all.

The numeric tag stays 16, so the encoded value does not move. The name does appear
in `Capability_name`, `Capability_value` and the embedded descriptor, so a
consumer that names the capability in its own source needs a coordinated rename;
the producer keeps no alias for the old name. No provider identifier, algorithm
identifier, cryptographic or framing change: the provider identifier stays
`baton/jwe/v1`.

## Declared configuration bounds

`EncryptionConfig.JWKPublicKeyConfig.pub_key` (tag 1) and
`additional_authenticated_data` (tag 2) now declare
`[(validate.rules).bytes = {max_len: 16384}]`. Maximum only: no minimums, no tag
changes, no provider or selection semantics change. The bound on `pub_key`
deliberately covers legacy JWK recipients as well as the JWE X-Wing recipient.

The generated validators are the only enforcement point. The provider ran its
own 16 KiB `pub_key` and authenticated-data checks in parallel with them, which
was two sources of truth for the same number; `recipient` now calls the generated
`jwkConfig.Validate()` before parsing and the duplicate constants and checks are
gone. The failure surfaces as one fixed `InvalidArgument`, so no part of the
rejected configuration reaches the caller. The nonempty-key, UTF-8, strict JSON
and key-material checks remain, as do the plaintext and protected-header bounds,
which no proto field expresses.

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
both are over, and representative legacy RSA, EC and Ed25519 public JWKs plus the
X-Wing AKP JWK all fit inside the bound.

At the provider boundary,
`TestDeclaredFieldBoundsEnforcedByBothEntryPoints` drives both exported entry
points — `ValidateConfig` and `Encrypt` — for each field: a valid padded JWK at
16384 is accepted and still encrypts to a message the recipient can read, and
16385 is refused by both. The earlier test that only compared a provider constant
to the declared number is gone with the constant.

Callers of the parser, inspected rather than assumed: `publicJWKFields` has one
production caller, `recipient`, which runs the generated validator first, so
parsing is still bounded before JSON allocation. The two fuzz targets call it
directly and keep explicit input caps; those caps are per-case cost guards, not
the declared bound, and the comment on each says so.

## Algorithm identifier

The profile identifier is `https://c1.ai/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1`
(CO-3). It appears once in production source, as the `Algorithm` constant in
`pkg/crypto/providers/jwe/jwe.go`.

The identifier is authenticated: it is a member of the protected header, and the
protected header is bound into the HPKE additional authenticated data. The
committed fixture therefore had to be **resealed by the provider**, not
rewritten, and no fallback to the previous identifier exists.

Red and green for the change:

```
# after the constant changed, before the fixture was resealed
--- FAIL: TestXWingJWEFixture
    expected: "https://c1.ai/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1"
    actual  : "https://conductorone.com/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1"

# after resealing with JWE_FIXTURE_REGENERATE=1
ok  github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe
```

The claim that text substitution is invalid is demonstrated by a throwaway
instrument on a disposable copy, using the fixture as it stood before the change
(`git show c6b58b45:`). It relabels the protected header to the new identifier and
the reader rejects the message:

```
relabelled header rejected with: chacha20poly1305: message authentication failed
```

The same property is pinned permanently by the committed tamper matrix case
`TestIndependentReaderAuthenticatesEverySealedField/protected_header_alg`.

### Consumer requirement

Interoperability is not claimable from this fixture alone. A consuming
implementation must adopt the identical `c1.ai` algorithm identifier and read
[the fixture](../../../pkg/crypto/providers/jwe/testdata/fixture.json),
reproducing the recorded plaintext.

Pin the consumer against a released SDK version that contains this fixture, or
against an immutable commit on the main branch that contains it. Do not pin a
branch commit: a branch revision may not stay reachable after a squash merge.
Record the revision that actually reproduced the fixture, and rerun the interop
check against it. No compatibility path for the previous identifier exists, so a
consumer that has not adopted it will fail to decrypt rather than silently
succeed.

## Command log

```
go test ./pkg/crypto/... ./pkg/connectorbuilder/ -count=1
  -> ok: pkg/crypto, pkg/crypto/providers, .../age, .../jwe, .../jwk, pkg/connectorbuilder

go test ./pkg/crypto/... ./pb/... ./pkg/connectorbuilder/ -count=1 -v
  -> 129 top-level PASS, 255 subtest PASS, 0 FAIL, 1 SKIP
     (skip = fixture regeneration, gated behind JWE_FIXTURE_REGENERATE=1)

go test ./pkg/crypto/... ./pkg/connectorbuilder/ ./pb/... -count=1
  -> ok: pkg/crypto, .../providers, .../age, .../jwe, .../jwk,
     pkg/connectorbuilder, pb/c1/connector/v2
     (after the bounds delegation; 0 FAIL)

go test ./pkg/connectorbuilder/ ./pkg/crypto/... ./pb/... -count=1
  -> ok at CO-4 (exported envelope, inlined header serialization, zero-output
     gate); 0 FAIL

go test ./pkg/connectorbuilder/ ./pkg/crypto/... ./pb/... -count=1
  -> ok at CO-5 (capability rename); 0 FAIL
make protogen re-run             -> no drift after the rename
buf lint / format / breaking     -> 0 / 0 / 0

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
     66 ok and 24 with no test files, zero failures
     (re-run at working-branch revision 9ca0ed2a after the identifier change)
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
| Remove the generated-validator delegation (`jwkConfig.Validate()`) | `jwe.go` | declared-bound tests, validation matrix | both at-limit cases still PASS, both over-limit cases FAIL, and the matrix fails on exactly `public JWK over size limit` and `authenticated data over size limit` |
| Remove the zero-output gate from `RotateCredential` | `credentials.go` | zero-output and returned-plaintext tests | the five zero-output rotate cases FAIL; create cases and both empty-list baselines still PASS |
| Remove the zero-output gate from `CreateAccount` | `accounts.go` | zero-output test | the five zero-output create cases FAIL; rotate cases and both empty-list baselines still PASS |

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

Pre-fix red evidence for the create/rotate preflight, before `2fa185d6`: the two
tests then asserted zero connector invocations and failed with counts of 1, and
the fan-out case returned no error at all. CO-4 reversed that policy, so those
tests were replaced rather than kept.

Create and rotate now validate supplied configs only when the connector returned
at least one plaintext value. With a connector configured to mint nothing,
`TestCreateAndRotateSucceedWithUnusedUnusableConfig` shows a nil entry, an
unknown provider, a provider-rejected recipient, a malformed JWE recipient and
JWE fan-out all succeed with one connector invocation and no encrypted data;
`TestCreateAndRotateRejectUnusableConfigWhenPlaintextReturned` shows the same five
shapes fail with `InvalidArgument`, one connector invocation and no response once
the connector does return a plaintext. An empty config list succeeds in both.

Planting the gate's removal fails exactly the zero-output cases on whichever call
site was planted, and leaves the empty-list baseline passing — so the gate, not
an unrelated check, is what the test measures.

Issuance keeps its pre-mint validation. `validateCredentialIssueOutput` already
requires at least one plaintext value, so issuance has no zero-output case to
preserve, and the pre-mint zero-invocation `IssueCredential` tests are unchanged.

## Envelope type and protected-header ordering

The flattened JWE JSON is the exported `FlattenedJWE` rather than an anonymous
struct at the point of use, so a reader can decode the wire format without
declaring it again. go-jose's equivalent, `rawJSONWebEncryption`, is unexported
and marks every member `omitempty`, so it cannot express this profile's
always-present empty `iv` and `tag` — the reason a small local type was exported
instead of reusing the library's. Tests reuse the exported type for the wire
field names; the reader still derives its own HPKE inputs from the raw wire
strings, and the framing test asserts the exact member set from raw JSON, so a
wrong JSON tag cannot pass unnoticed by both sides.

Protected-header member order is not a wire invariant. The HPKE additional data
is the protected string as transmitted, so
`TestIndependentReaderAcceptsAnyProtectedHeaderMemberOrder` seals a message with
`kid` before `alg` — the reverse of the provider's order — and the reader decrypts
it. The test seals with `hpke` directly, so it does not depend on the provider's
own serializer. Relabelling the transmitted header without resealing is still
rejected.

The test is discriminating: a throwaway instrument on a disposable copy showed a
reader that reconstructs the header from parsed members and re-serializes it
rejects that same valid message with
`chacha20poly1305: message authentication failed`. No "never change the order"
warning is needed, and none was added; the ordering statement lives on the
`protectedHeader` type instead.

## Fixture provenance

[`pkg/crypto/providers/jwe/testdata/fixture.json`](../../../pkg/crypto/providers/jwe/testdata/fixture.json),
produced remotely by `TestRegenerateXWingJWEFixture` (`JWE_FIXTURE_REGENERATE=1`).

The fixture was resealed under the `c1.ai` identifier. The working-branch
revision that resealed it, `9ca0ed2a`, is recorded here as provenance of the test
run. It is not a durable consumer pin; see the consumer requirement above.

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
  implementation was exercised; the consumer requirement above is what closes
  that gap.
- Draft-vector comparison was not performed: the private suite mapping is not
  HPKE-4 or HPKE-9, so a shared published vector would not apply to this
  algorithm identifier.
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
