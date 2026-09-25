# JWE credential encryption verification plan

Frozen behavioral baseline, 2026-09-24. SDK implementation stage only.

Risk: HIGH. Incorrect ciphertext can strand a newly minted credential and requires
cross-repository coordination. No storage, scheduler, or hot-path cost changes.
The provider is stateless; each encryption gets a fresh HPKE sender.

| ID | Required behavior | Instrument |
| --- | --- | --- |
| J1 | Generic JWK config carries exact opaque AAD; no application-context parser | Arbitrary binary AAD round trip and protobuf round trip |
| J2 | X-Wing/HKDF-SHA256/ChaCha20-Poly1305 integrated JWE follows draft-22 framing under a private alg | Independent HPKE reader, field assertions, committed fixture |
| J3 | Malformed keys, private keys, unsupported profiles, duplicate JSON members, oversized config and invalid X25519 keys fail before mint | Parser matrix and real IssueCredential call-count assertions |
| J4 | JWE issuance requires one config and one output; failure never reports success or includes plaintext | Fan-out/config matrix, output cardinality tests |
| J5 | JWK and age keep existing no-AAD behavior; unsupported AAD is never discarded | Existing regression suites, direct provider and manager rejection tests |
| J6 | New capability is advertised only for credential issuers; unknown provider never falls back | Connector metadata and dispatch tests |
| J7 | Ciphertext, encapsulation components, protected header and AAD are authenticated | Independent-reader tamper matrix and wrong-key tests |
| J8 | Errors do not contain private-key input or credential bytes | Secret-marker assertions and fuzz parser |
| J9 | Source and generated protos agree, tags are additive, existing consumers compile | Remote generation, compatibility, lint, targeted/race and regression tests |
| J10 | Go JWE is ingested and revealed through native Rust/WASM read APIs | Deferred to the consuming-implementation integration stage; blocks rollout, not SDK draft review |

Run all generation/tests/lint remotely. Commit source before generated artifacts.
Record commands, revisions, coverage gaps and planted-violation results in
evidence.md. Independently review final implementation and evidence before claiming
SDK closure. New protocol fixtures are evidence of SDK behavior until a second
implementation consumes them, not proof of Rust interoperability.

Review routing: edge cases, permutations, error handling, validation ordering,
bounded allocation and stateless concurrent use apply. C1 persistence, cancellation,
replay receipts and provider revocation are deferred to the C1 stage.

Design constraints: use existing HPKE primitives; no key schedule implementation,
vault-field parsing, JWS, decryption service, fallback, or new dependencies. Old
SDKs must be excluded by capability selection before dispatch.

## Implementation-obligation addendum

- JWK parsing is bounded before JSON allocation, rejects duplicate top-level
  members and private material, and does not echo parser input in errors (J3/J8).
- HPKE public-key parsing alone does not reject low-order X25519 points. A fixed
  public validation probe checks ECDH before minting; test both zero and one
  low-order encodings (J3).
- The provider always emits external AAD, even empty. Reader tests must construct
  `protected + "." + aad` independently and retain the HPKE tag in ciphertext (J2/J7).
- Each call owns one HPKE sender; no cached encryption state, closers, locks or
  durable writes. Encryption does not mutate caller-owned inputs (J1/J7).
- The builder checks JWE cardinality before general output validation so malformed
  multi-value output cannot leak field names through unrelated validation errors (J4/J8).
- Capability value 16 avoids the value used by the separate, unmerged
  vault-specific encryption proposal. The existing oneof and its tag numbers stay
  unchanged (J6/J9).
- The legacy JWK provider and shared resolver reject unsupported AAD; callers
  invoking either provider directly must receive the same rejection (J5).

## Change orders

Post-freeze changes to the baseline above. Each is re-routed through the risk
model and carries its own instrument.

### CO-1: recipient preflight on create and rotate (2026-09-24)

Registering `baton/jwe/v1` made it reachable from `CreateAccount` and
`RotateCredential`, which encrypted only after the connector had mutated the
upstream provider. Both paths now call `crypto.ValidateEncryptionConfigs` after
option conversion and before the connector call (J3/J4). Instrument: side-effect
counts asserting zero connector invocations for a malformed JWE recipient, a
legacy JWK recipient carrying authenticated data, and JWE fan-out, plus
compatibility cases pinning that an empty config list and a legacy no-AAD JWK
recipient still reach the connector.

### CO-2: protected-header limit aligned with the consuming reader
(2026-09-24)

The consuming reader caps the decoded protected header at 4096 bytes, while the
provider bounded only the raw `key_id`. `json.Marshal` escapes HTML characters
to six wire bytes each, so a `key_id` inside its own 1024-byte limit could
serialize past 4096 and be rejected by the reader after the credential had been
minted. `MaxProtectedHeaderBytes` now bounds the serialized header, and one
shared serializer feeds both `recipient` preflight and `Encrypt` (J2/J3/J4). The
1024-byte `key_id` limit is unchanged. This narrows accepted input to match the
consumer; it does not change the wire format. Instrument: boundary tests at 4095
/ 4096 / 4097 serialized bytes, an accepting ordinary 1024-byte key id, a
refusing 1024-byte escaped key id, no-echo assertions, and a real
`IssueCredential` case proving zero provider calls.

### CO-3: algorithm identifier moved to the c1.ai domain (2026-09-25)

The profile's algorithm identifier changed from
`https://conductorone.com/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1` to
`https://c1.ai/alg/hpke-xwing-hkdf-sha256-chacha20poly1305/v1`.

This is an authenticated value, not a cosmetic URL: the identifier is a member of
the protected header, and the protected header is bound into the HPKE additional
authenticated data. A message sealed under the old identifier cannot be relabelled
by editing text — the Poly1305 tag stops verifying — so the synthetic fixture is
resealed by the provider rather than rewritten, and no fallback to the previous
identifier is added. Instrument: the fixture test pins the profile identifier to
the provider constant, and a throwaway instrument relabels the pre-change
fixture's header and shows the reader reject it (J2/J7/J9).

Interop is not claimable again until the consuming side adopts the identical
identifier and reads the resealed fixture; see the evidence file.

### CO-4: zero-output create and rotate, envelope type, header ordering
(2026-09-25)

Review of the draft producer changed four things. Recorded before the code, as a
change order, because it reverses a compatibility decision CO-1 made.

**Create/rotate validation moves after the connector call.** CO-1 validated
supplied encryption configs before invoking the connector. That changed the
default path for every existing connector, not only JWE callers: a request with
an unusable config used to succeed whenever the connector returned zero
plaintexts — ActionRequired, InProgress, AlreadyExists and NoPassword flows, and
CI connectors that rotate a credential without returning it. Create and rotate
now validate only when the connector returns at least one plaintext value, so a
zero-output operation succeeds with a config it never uses. A connector that does
return plaintexts still cannot have them encrypted to an unusable recipient.

**IssueCredential keeps pre-mint validation.** The same review suggested gating
issuance the same way. It cannot be: `validateCredentialIssueOutput` already
requires at least one plaintext value, so issuance has no zero-output case to
preserve, and J4 requires invalid configs and JWE fan-out to fail before `Issue`
is called. Changing issuance would weaken that contract to serve a case that does
not exist. Recorded as a declined part of the review, not silently ignored.

**The flattened envelope is an exported type.** The provider declared the JWE
JSON anonymous at the point of use. go-jose's equivalent, `rawJSONWebEncryption`,
is unexported and marks every member `omitempty`, so it cannot express this
profile's always-present empty `iv` and `tag`. `FlattenedJWE` is exported instead,
and the producer emits it.

**Protected-header member order is not a wire invariant.** The reader
authenticates the protected string as transmitted, so sealing and reading work
under any member order; what must not change is the string itself between sealing
and reading. Instrument: seal and decrypt a message whose protected header lists
the members in a different order, alongside the existing case that relabels a
header without resealing and is rejected.

`encodeProtectedHeader` is inlined at its single call site so a marshal failure
is handled rather than ignored; the serialized header is still the same bytes the
size bound is measured on.

### CO-5: capability renamed to name the X-Wing suite (2026-09-25)

Review noted that `CAPABILITY_CREDENTIAL_ENCRYPTION_JWE` overstates what is
supported: the producer implements one suite, not JWE generally. Renamed to
`CAPABILITY_CREDENTIAL_ENCRYPTION_JWE_XWING_V1` with the numeric tag unchanged at
16. No provider identifier, algorithm identifier, cryptographic, or framing
change; the provider identifier stays `baton/jwe/v1`.

Checked before renaming, because a rename of a wire-visible enum name is only
safe while nothing released depends on it:

- Branch head is not an ancestor of `main`, and the PR is open with
  `mergedAt: null`.
- `main`'s `Capability` enum ends at `CAPABILITY_CREDENTIAL_ISSUE = 14`, so
  neither 15 nor 16 is taken and the symbol does not exist on `main` at all.

The enum name is carried in the descriptor as well as the Go identifier, so the
rename is wire-visible to anything reading `Capability_value` or the descriptor
by name; renumbering is what would break wire compatibility, and the tag is
unchanged. A downstream consumer that names the capability in its own source
needs a coordinated rename; the producer does not alias the old name.

Instrument: the capability advertisement test asserts the new symbol on the
issuing resource type and on the connector, and its absence for a resource type
that cannot issue and for a connector with no issuer.
