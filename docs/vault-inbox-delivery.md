# Vault-inbox credential delivery contract

This document describes the current contract between the connector-side
encryption profile (`baton/vault-inbox/v1`) and C1 full-knowledge credential
delivery, as of 2026-09-23. It describes the wire format, validation rules, and
responsibilities of the SDK and the related C1 and Latchkey changes below; it
does not establish that those changes are deployed.

## Definitions

- **Full-knowledge (FK) vault:** a vault in which C1 is an authorized full member
  (`C1_FULL_MEMBER`). C1 can ingest and decrypt secrets through its member
  runtime, subject to vault authorization. Full knowledge does not grant access
  to every C1 worker or bypass authorization for reads.
- **Latchkey / Multipass:** the shared vault cryptography and client
  implementation in the Multipass repository. C1 embeds its member runtime as
  WebAssembly (WASM).
- **Inbox recipient:** the vault's public encryption key and its identifying
  coordinates. A connector can encrypt to it without holding vault membership
  or any vault private keys.
- **Submission envelope:** the encrypted message produced by the SDK using the
  existing inbox wire format. FK delivery uses this format without creating a
  Vault Submission row or requiring external review. Its `submission_id` field
  carries the prepared FK delivery ID.
- **Native secret / Model-B item:** a secret stored in the vault's normal value
  and key-wrapper format, with a secret ID and version ID, readable through the
  ordinary authorized vault APIs. An inbox envelope is not yet a native secret.
- **JWK:** JSON Web Key, the JSON representation of the recipient public key.
  This profile uses the algorithm-key-pair (`AKP`) representation.
- **Attestation:** a signed statement binding an inbox public key to its vault
  and key coordinates.
- **HPKE:** Hybrid Public Key Encryption. Its key encapsulation mechanism (KEM),
  key derivation function (KDF), and authenticated encryption algorithm (AEAD)
  are specified below. Additional authenticated data (AAD) is authenticated but
  not encrypted; HPKE `info` binds the encryption context.

## Implementation references

The wire-format reference was inspected at `multipass@3350d549`:

- `crates/latchkey-mls-core/src/vault_inbox.rs` — `seal_vault_submission`,
  `open_vault_submission`, `binding_bytes`, `vault_inbox_hpke`.
- `crates/latchkey-mls-core/src/framing.rs` — `framed`.
- `crates/latchkey-client-sdk/src/vault_inbox.rs` —
  `SecretSubmissionPayloadV3`, `decode_secret_submission_payload_for_open`.
- `crates/latchkey-client-sdk/src/client.rs` —
  `open_vault_submission_secret_payload`, `accept_vault_submission_as_secret`.

FK delivery also requires these related changes:

- [Multipass #823](https://github.com/ductone/multipass/pull/823) adds member-WASM
  exports to create and attest an inbox key (`latchkey_create_vault_inbox_key`)
  and ingest a submission (`latchkey_seal_vault_submission`). The public key can
  be handed to the connector; the private key remains sealed outside the module.
  Ingestion opens the envelope and seals a native Model-B item inside the module.
- [C1 #26059](https://github.com/ductone/c1/pull/26059) defines recipient lookup
  and persisted delivery coordinates.
- [C1 #26060](https://github.com/ductone/c1/pull/26060) adds vault-service ingress,
  host bindings, and the updated embedded WASM artifact.
- [C1 #26061](https://github.com/ductone/c1/pull/26061) connects issuance dispatch,
  native-secret persistence, and delivery completion.

## Delivery lifecycle

1. An approved ticket fixes an FK vault destination. C1 persists **one prepared
   FK recipient and delivery ID** per (tenant, ticket) *before*
   `ClaimDispatch`, and fixes the destination vault, delivery ID,
   `profile_id`, `inbox_key_id`, `key_generation`, payload scheme, and crypto
   suite on the operation.
2. C1 sends the frozen coordinates plus the inbox public key to the connector in
   an `EncryptionConfig` that selects inbox mode by naming the
   `baton/vault-inbox/v1` provider, carries the inbox key id on `key_id`, and
   puts the recipient JWK on `jwk_public_key_config.pub_key` (see §5.1). That
   config is C1's authority: it comes from the authenticated action transport for
   an approved ticket, and the connector does not and cannot elect a destination
   itself.
3. The connector validates the config **before minting** (see §6), mints the
   credential once, serializes the exact `SecretSubmissionPayloadV3` container
   the inbox reader expects, seals it with the existing X-Wing HPKE profile, and
   returns the envelope as `EncryptedData.encrypted_bytes`.
4. C1 durably records that exact result as `RESULT_ENCRYPTED` and sends those
   bytes to the vault service's authorized FK ingress. The transport worker has
   no decryption keys and preserves the ciphertext byte-for-byte. This path
   creates no Vault Submission row and has no `PENDING_REVIEW` or external review
   step. Recording or transporting the ciphertext is not delivery completion.
5. The authorized C1 full member accepts the envelope automatically. Inside the
   member WASM, it opens the sealed inbox private key, decrypts the submission,
   checks its bindings, and seals the value as a native secret. Neither the
   credential plaintext nor the unsealed private key leaves the module. Before
   marking the operation
   `DELIVERED`, C1 must durably create the native secret, record acceptance, and
   populate the existing full-knowledge delivery instance's `secret_id` and
   `version_id` with the accepted secret and version IDs. Stored ciphertext or
   allocated IDs alone do not satisfy these requirements.

Ordinary authorized reveal is a **mandatory acceptance test** for that ingestion
path — it is how the created native secret is proven to be readable — not a
production prerequisite of delivery. Secret sharing is a separate concern again
and is not part of this contract.

## 1. HPKE instance

`vault_inbox_hpke()`:

| Parameter | Value |
|---|---|
| Mode | Base |
| KEM | `XWingDraft06` (ML-KEM-768 + X25519) |
| KDF | HKDF-SHA256 |
| AEAD | ChaCha20-Poly1305 |

Go side: `filippo.io/hpke` `MLKEM768X25519()` + `HKDFSHA256()` +
`ChaCha20Poly1305()`. The KEM's `0x647a` code point is the current X-Wing
identifier (`XWingDraft06`), **not** `XWingDraft06Obsolete` (`0x004D`). The
public key is exactly 1216 bytes (ML-KEM-768 encapsulation key ‖ X25519), and
the encapsulation is 1120 bytes.

## 2. Binding bytes = HPKE info = AEAD AAD

`binding_bytes` and `framed` are reproduced byte for byte:

```
framed(domain, fields) = for each of [domain] ++ fields:
                          u32_be(len(field)) || field
```

with, in order:

| # | Field | Value |
|---|---|---|
| 0 (domain) | `INFO_PREFIX` | `latchkey/v1/vault-inbox-submission/info` |
| 1 | version | single byte `2` (`ENVELOPE_VERSION`) |
| 2 | alg | `HPKE-Base-X-Wing-Draft06-HKDF-SHA256-ChaCha20Poly1305` |
| 3 | tenant_id | C1 tenant id |
| 4 | vault_boundary_id | approved destination vault |
| 5 | inbox_key_id | active inbox key id |
| 6 | key_generation | ASCII decimal, no padding |
| 7 | payload_scheme | `latchkey.vault_submission.secret.v1` |

The same bytes are passed as **both** the HPKE `info` and the AEAD `aad`
(`hpke.seal(&pk, &aad, &aad, plaintext, …)` / `hpke.open(&enc, &priv, &aad,
&aad, &ct, …)`). Because the framing is injective and `version`/`alg` lead the
fields, a ciphertext can be neither replayed under another tenant, vault, key,
generation, or scheme, nor moved between suites or envelope versions.

A pinned worked example lives in
`pkg/crypto/providers/vaultinbox/testdata/vault-inbox-submission-vector.json`
(`binding_hex`).

## 3. Envelope (`EncryptedData.encrypted_bytes`)

UTF-8 JSON, exactly `VaultInboxSubmissionEnvelope`:

```json
{"version":2,"alg":"HPKE-Base-X-Wing-Draft06-HKDF-SHA256-ChaCha20Poly1305",
 "enc":"<base64url-no-pad encapsulated key>",
 "ciphertext":"<base64url-no-pad AEAD ciphertext>"}
```

The reader rejects any version other than `2` and any `alg` other than the suite
string, then re-derives the binding and AEAD-opens.

## 4. Plaintext payload (inside the AEAD)

UTF-8 JSON, exactly `SecretSubmissionPayloadV3` (serde field names, declaration
order):

```json
{"version":3,"submission_id":"...","display_name":"...","description":"...",
 "content_type":"generic","annotations":{},"value_b64":"<base64url-no-pad>"}
```

- `version` is `3`. The open path accepts **only** v3; the legacy v1/v2 plaintext
  carried no submission-id binding and is rejected with
  `VAULT_SUBMISSION_PAYLOAD_UNSUPPORTED`.
- `submission_id` carries the C1-allocated FK delivery ID, bound inside the sealed
  plaintext so a server cannot relabel one same-vault submission as another.
- `content_type` normalizes empty ⇒ `generic` (`content_type_or_generic`).
- `value_b64` is URL-safe base64 **without** padding.

## 5. `EncryptedData` fields

| Field | Value |
|---|---|
| `provider` | `baton/vault-inbox/v1` |
| `key_id` (deprecated) | empty |
| `key_ids` | exactly `[inbox_key_id]` |
| `name` / `description` / `schema` | copied from `PlaintextData` |
| `encrypted_bytes` | the submission envelope JSON above |

`key_ids` names the **inbox key id**, not the JWK thumbprint. C1 compares it
against the submission's active inbox key id.

`public_key_thumbprint` is **JWK validation, not an independent HPKE binding**: it
is re-derived from the recipient JWK's `alg`/`kty`/`pub` (§5.1) and compared
against the value the JWK claims, so a config cannot claim a thumbprint that does
not belong to the key it names. It is **not** carried as a separate field in the
HPKE `info`/AAD — the AAD binds `inbox_key_id` and `key_generation` (see §2), not
the thumbprint. Treating a thumbprint match as proof of anything beyond JWK
integrity would overstate it. Likewise `submission_id` and `content_type` are
authenticated by being **inside the sealed payload** (§4), not by any outer field.

## 5.1 Recipient config surface

The recipient coordinates reach the connector on the shared `EncryptionConfig`,
not in a dedicated vault-inbox message:

| Field | Value |
|---|---|
| `provider` | `baton/vault-inbox/v1` — the only selector for inbox mode |
| `key_id` | the inbox key id; the single authoritative key id |
| `jwk_public_key_config.pub_key` | the recipient JWK JSON, carrying the extension below |

The JWK holds the public key in its ordinary members (`kty`, `alg`, `pub`) and the
binding context in a namespaced extension member, `baton_vault_inbox`:

| Extension member | Value |
|---|---|
| `version` | `1` |
| `suite` | the suite string; must equal the JWK `alg` |
| `tenant_id` | C1 tenant id |
| `vault_boundary_id` | approved destination vault |
| `key_generation` | non-zero integer |
| `payload_scheme` | `latchkey.vault_submission.secret.v1` |
| `submission_id` | the C1-allocated FK delivery id |
| `public_key_thumbprint` | base64url(SHA-256 of the canonical `{alg,kty,pub}`) |
| `content_type` | optional; empty normalizes to `generic` |

Every extension member is required except `content_type`. The extension
deliberately does **not** carry the inbox key id: `key_id` is the single source,
so two values cannot disagree about which key a ciphertext is bound to. A JWK
`kid` that disagrees with `key_id` is refused rather than silently overridden.

Parsing is strict because the extension is a protocol surface. A member the
extension does not define, a member repeated in the outer JWK or the extension,
trailing content after either object, a wrong version or suite, a non-string or
null JWK member, and private material are all refused before any provider work.
Ordinary optional JOSE metadata (`use`, `key_ops`, and a `kid` that agrees) is
still accepted, so the strictness is scoped to the protocol extension rather than
to all of JOSE. Unknown fields on the shared `EncryptionConfig` stay tolerated, so
that message remains additive for every other provider; unknown fields on the
provider-specific JWK config are refused.

Tradeoffs, relative to carrying the coordinates in a dedicated config message:

- **Additive, no new message.** The recipient rides on an existing shared message,
  so no new proto message or config-version enum is introduced and the classical
  `baton/jwk/v1` and `age` paths keep their current bytes. The inbox wire format
  (§2–§4) is unchanged, so a reader cannot tell which config surface produced an
  envelope.
- **Strictness moves into the SDK.** The binding context is now JSON the connector
  parses and validates itself rather than typed protobuf. The parser refuses
  ambiguity — repeated members, unknown members, trailing content — instead of
  resolving it.
- **Inbox mode is selected by the provider name alone.** A JWK public key config
  without this provider is ordinary classical encryption; inbox mode is never
  inferred from the key type, and a config that names this provider but is
  malformed is refused rather than downgraded.
- **`kid` is coupled to `key_id`.** A serving JWKS whose `kid` differs from the
  inbox key id is refused rather than silently overridden. A producer that emits
  an unrelated `kid` must either match `key_id` or omit it.
- **Attestation is still not verified.** See §6.3.

## 6. Validation split: pre-mint vs post-mint

The distinction matters because only the pre-mint half can refuse an issuance
**before** a provider credential exists. Nothing in the post-mint half prevents a
mint; it refuses to hand back a usable result after one.

### 6.1 Pre-mint — refused before the provider runs (issuer calls = 0)

Config shape and capability, all evaluated before the provider is invoked:

- the JWK extension's version and suite are the supported ones, and a JWK `kid`,
  if present, agrees with `key_id`;
- every binding identifier is non-empty, bounded, and free of control
  characters; `key_generation` is non-zero;
- `payload_scheme` is exactly the one scheme this provider emits;
- the public JWK is exactly one public AKP JWK, `alg` exactly the suite string,
  no `priv`, `pub` exactly 1216 bytes, parsing as an X-Wing key, and the X25519
  component is not a low-order point;
- the extension's `public_key_thumbprint` re-derives from the JWK's
  `alg`/`kty`/`pub` (§5.1);
- the selected descriptor **advertises** the requested vault-inbox profile;
- the vault-inbox recipient is the **only** encryption config — a mixed or
  duplicate recipient set is refused before anything is minted.

Which of those run on which path:

| Path | Pre-mint gates |
|---|---|
| `IssueCredential` | all of the above, unconditionally |
| registered actions | config shape only — the cardinality rule is post-mint (§6.2) |
| `CreateAccount` | config shape when a vault-inbox recipient is present, plus the credential-option rule below |
| `RotateCredential` | the same config-shape check, plus the rotate credential-option rule below |

The config-shape check is scoped to a recipient that names this provider, since
routing keys on the provider name, so every other recipient type keeps its
existing create/rotate behaviour.

**Credential options.** A vault-inbox recipient must be paired with options that
actually produce a value, and this is refused before the account or the rotation
happens, because afterwards the only outcomes are a failed call for something
that really happened and a retry that returns `AlreadyExists`:

- `CreateAccount` requires `RandomPassword`. `NoPassword` and `Sso` create an
  account with no credential at all, and `EncryptedPassword` carries material the
  caller already holds rather than something the connector mints.
- `RotateCredential` accepts `RandomPassword` or no options at all — a rotation
  with nothing set is a supported shape where the connector mints its own
  replacement — and refuses the same three.

### 6.2 Post-mint — refused after the provider has already minted

Output cardinality and size, evaluated once the connector has produced values.
The cardinality rule differs by path because the contracts differ:

| Path | Rule | A zero-value result |
|---|---|---|
| `IssueCredential`, registered actions | exactly one plaintext value | refused |
| `CreateAccount`, `SuccessResult` | exactly one plaintext value | refused — a success with no credential would seal no submission and report success |
| `CreateAccount`, non-success results | at most one plaintext value | **allowed** — `AlreadyExists`, `ActionRequired`, and `InProgress` carry none by contract, and the structured result is preserved |
| `RotateCredential` | exactly one plaintext value | refused |

Two or more values are refused on every path, because they would seal several
complete submission envelopes bound to a single submission id. Size is checked on
all of them:

- the display name and description are within the submission row's limits;
- the sealed envelope is within the inbox's 2 MiB cap.

An overlength or wrong-cardinality failure is therefore **post-mint** by
construction: the provider credential exists, and the failure surfaces so C1 can
revoke that provider credential and clean up its submission (see §7) instead of
silently delivering a partial or unusable submission.

### 6.3 What the SDK does not verify

The SDK currently trusts the recipient configuration supplied through the
authenticated C1 action transport. It checks the JWK and thumbprint but does not
verify attestation signatures. A JWK or matching thumbprint alone does not
authenticate the destination.

For device-free FK vaults, Multipass #823 lets the C1 full member create the
inbox key and attest it with its existing MLS leaf signing key, using composite
ML-DSA-65 + Ed25519. This does not require an owner device.

Connector-side attestation verification can be added later: the connector could
verify the recipient public key's attestation chain against a known, trusted
Multipass/Latchkey root key. That extension would need the trust anchor and
chain-validation rules, including the binding to the member signing key; it is
not implemented by this SDK profile.

## 7. Provider revocation and submission cleanup

Delivery failures are handled by **exact, bounded** actions, never by a blanket
cleanup and never by re-minting:

- The provider credential the issuance created is revoked **exactly** — the
  specific credential, not a sibling, and not anything in a shared vault.
- This ticket's exact delivery ciphertext and associated submission material are
  cleaned up; FK delivery has no Vault Submission row. When the native version
  already exists, cleanup targets that exact version too.
- Sibling submissions, sibling credentials, and other vault members' data are
  left untouched.
- No replacement credential is minted to cover the failure. C1 does not re-mint
  once a live provider key exists; a failed credential stays revoked-or-flagged
  rather than quietly replaced, so a live key is never orphaned.
- Revocation failures stay visible and retryable rather than being swallowed.

## 8. What is explicitly out of scope

- No new inbox cryptographic wire format. Rust member-runtime exports and an
  updated C1 WASM artifact are required, as described in Implementation references.
- No C1-side decrypt/reseal in the transport worker. The authorized member WASM
  performs ingestion; secret sharing is separate.
- No age profile change, no Paper fallback, no remint after a live key exists.
- No requester-selected destination: the destination is the admin-approved
  vault frozen on the approved ticket.

## 9. Pinned interop fixture

`pkg/crypto/providers/vaultinbox/testdata/vault-inbox-submission-vector.json`
holds a submission sealed by `filippo.io/hpke` under the fixed `0x42` X-Wing
seed, plus the exact binding bytes. The matching Rust check is
`crates/latchkey-mls-core/tests/vault_inbox_go_submission_vector.rs` (test-only,
no production change): it feeds `envelope_json` and the binding coordinates to
the unmodified `open_vault_submission`, then decodes the recovered plaintext
with `decode_secret_submission_payload_for_open` and compares it to
`expected_plaintext`. The Rust test asserts the committed `binding_hex` too, so
Go and Rust must agree on the AAD bytes, not merely on round-tripping through
their own implementations.
