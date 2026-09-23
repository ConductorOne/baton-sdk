# Vault-inbox credential delivery: frozen wire contract

This document freezes the mapping between the connector-side encryption profile
(`baton/vault-inbox/v1`) and the shipped Latchkey vault-inbox reader. It is the
compatibility contract both halves of the delivery transport are written
against. Nothing here is a new format: every byte is produced by an existing
primitive, and the reader at the other end is unmodified.

## What this replaces

Earlier drafts (`baton/full-knowledge-vault/v1`, `FullKnowledgeVaultConfig`,
`FullKnowledgeCredentialEnvelope`, `FullKnowledgeSecretPayloadV2`, and the
age-to-FK / native-CEK-import designs) are **superseded and not implemented
here**. They introduced a new envelope and a native item-value payload that the
shipped inbox reader does not know. This contract deliberately does none of
that: no new cryptographic envelope, no CEK import, no ABI or embedded WASM
artifact, no age format or profile extension, no classical ECDH fallback, and no
synthetic native secret or version id at delivery time.

## Authoritative reader

`multipass@3350d549`, unchanged:

- `crates/latchkey-mls-core/src/vault_inbox.rs` — `seal_vault_submission`,
  `open_vault_submission`, `binding_bytes`, `vault_inbox_hpke`.
- `crates/latchkey-mls-core/src/framing.rs` — `framed`.
- `crates/latchkey-client-sdk/src/vault_inbox.rs` —
  `SecretSubmissionPayloadV3`, `decode_secret_submission_payload_for_open`.
- `crates/latchkey-client-sdk/src/client.rs` —
  `open_vault_submission_secret_payload`, `accept_vault_submission_as_secret`.

## Delivery lifecycle

1. An approved ticket fixes an FK vault destination. C1 gets or creates **one
   durable prepared inbox submission** per (tenant, ticket) *before*
   `ClaimDispatch`, and freezes the destination vault, submission id,
   `profile_id`, `inbox_key_id`, `key_generation`, payload scheme, and crypto
   suite on the operation.
2. C1 sends the frozen coordinates plus the inbox public key to the connector in
   an `EncryptionConfig.vault_inbox_recipient_config`. That config is C1's
   authority: it comes from the authenticated action transport for an approved
   ticket, and the connector does not and cannot elect a destination itself.
3. The connector validates the config **before minting** (see §6), mints the
   credential once, serializes the exact `SecretSubmissionPayloadV3` container
   the inbox reader expects, seals it with the existing X-Wing HPKE profile, and
   returns the envelope as `EncryptedData.encrypted_bytes`.
4. C1 durably records that exact result as `RESULT_ENCRYPTED`, then uploads and
   registers those exact bytes against the prepared submission. The submission
   reaches `PENDING_REVIEW`.

**Registration is intermediate, not completion.** `PENDING_REVIEW` means the
ciphertext is durably stored and reviewable; it is **not** `DELIVERED`, and it is
not a native secret. The transport is keyless: between the connector's output and
the registered submission the ciphertext is copied **byte-identically** and is
never decrypted, resealed, or re-encoded.

5. A **separate** authorized slice performs member ingestion: an authorized C1
   vault member decrypts the submission, creates a **native secret** through the
   ordinary secret path, and records acceptance. Before marking the operation
   `DELIVERED`, C1 must durably create the native secret, record acceptance, and
   populate the existing full-knowledge delivery instance's `secret_id` and
   `version_id` with the accepted secret and version IDs. Inbox registration or
   allocated IDs alone do not satisfy these requirements.

Ordinary authorized reveal is a **mandatory acceptance test** for that ingestion
slice — it is how the created native secret is proven to be readable — not a
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
- `submission_id` is the C1-allocated submission, bound inside the sealed
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
is re-derived from `public_jwk_json` and compared against the recipient JWK, so a
config cannot claim a thumbprint that does not belong to the key it names. It is
**not** carried as a separate field in the HPKE `info`/AAD — the AAD binds
`inbox_key_id` and `key_generation` (see §2), not the thumbprint. Treating a
thumbprint match as proof of anything beyond JWK integrity would overstate it.
Likewise `submission_id` and `content_type` are authenticated by being **inside
the sealed payload** (§4), not by any outer field.

## 6. Validation split: pre-mint vs post-mint

The distinction matters because only the pre-mint half can refuse an issuance
**before** a provider credential exists. Nothing in the post-mint half prevents a
mint; it refuses to hand back a usable result after one.

### 6.1 Pre-mint — refused before the provider runs (issuer calls = 0)

Config shape and capability, all evaluated before the provider is invoked:

- config version and suite are the supported ones;
- every binding identifier is non-empty, bounded, and free of control
  characters; `key_generation` is non-zero;
- `payload_scheme` is exactly the one scheme this provider emits;
- the public JWK is exactly one public AKP JWK, `alg` exactly the suite string,
  no `priv`, `pub` exactly 1216 bytes, parsing as an X-Wing key, and the X25519
  component is not a low-order point;
- `public_key_thumbprint` re-derives from `public_jwk_json` (§5);
- the selected descriptor **advertises** the requested vault-inbox profile;
- the vault-inbox recipient is the **only** encryption config — a mixed or
  duplicate recipient set is refused before anything is minted.

Which of those run on which path:

| Path | Pre-mint gates |
|---|---|
| `IssueCredential` | all of the above, unconditionally |
| registered actions | config shape, plus the cardinality rule in §6.2 |
| `CreateAccount` | config shape when a vault-inbox recipient is present, plus the credential-option rule below |
| `RotateCredential` | the same config-shape check, plus the rotate credential-option rule below |

The config-shape check is scoped to a recipient that selects this profile — by
the inner message **or** by the provider name, since routing keys on the provider
name first — so every other recipient type keeps its existing create/rotate
behaviour.

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

**Attestation signature verification is not here.** The inbox public key is
authenticated by an owner-device composite ML-DSA-65 + Ed25519 attestation that
only the member's Latchkey client can verify against its registered device key;
C1 validates the stored attestation structurally when it serves the profile. The
connector does not verify attestation signatures, does not treat a JWK as
self-authenticating, and does not invent, re-sign, or substitute keys. This is
the same trust root as the existing inbox submission flow, unchanged.

## 7. Provider revocation and submission cleanup

Delivery failures are handled by **exact, bounded** actions, never by a blanket
cleanup and never by re-minting:

- The provider credential the issuance created is revoked **exactly** — the
  specific credential, not a sibling, and not anything in a shared vault.
- The **exact** inbox submission prepared for this ticket is cleaned up. When the
  native version already exists, that exact native version is cleaned up too.
- Sibling submissions, sibling credentials, and other vault members' data are
  left untouched.
- No replacement credential is minted to cover the failure. C1 does not re-mint
  once a live provider key exists; a failed credential stays revoked-or-flagged
  rather than quietly replaced, so a live key is never orphaned.
- Revocation failures stay visible and retryable rather than being swallowed.

## 8. What is explicitly out of scope

- No Rust production code or artifact changes; the reader is unmodified.
- No C1-side decrypt/reseal in the transport. Member ingestion (§5) is a separate
  authorized slice, and sharing is separate again.
- No age profile change, no Paper fallback, no remint after a live key exists.
- No requester-selected destination: the destination is the admin-approved
  vault frozen on the approved ticket.
- **The wire profile is unchanged by any of this.** The corrections here are to
  the contract's description and to the SDK's tests; the bytes, field numbers,
  suite, framing, and envelope stay exactly as specified in §§1–5.

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
