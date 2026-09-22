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

## Delivery lifecycle (end state)

1. An approved ticket fixes an FK vault destination. C1 gets or creates **one
   durable prepared inbox submission** per (tenant, ticket) *before*
   `ClaimDispatch`, and freezes the destination vault, submission id,
   `profile_id`, `inbox_key_id`, `key_generation`, payload scheme, and crypto
   suite on the operation.
2. C1 sends the frozen coordinates plus the inbox public key to the connector in
   an `EncryptionConfig.vault_inbox_recipient_config`.
3. The connector validates the config before minting, mints the credential
   once, serializes the exact `SecretSubmissionPayloadV3` container the inbox
   reader expects, seals it with the existing X-Wing HPKE profile, and returns
   the envelope as `EncryptedData.encrypted_bytes`.
4. C1 durably records that exact result as `RESULT_ENCRYPTED`, then uploads and
   registers those exact bytes against the prepared submission. The operation
   becomes `DELIVERED` only after durable registration.

**Delivery ends at the durable inbox submission.** There is no C1-side decrypt,
reseed, ingestion daemon, sharing, or native-secret creation during delivery.

5. Later, an authorized active vault member uses the existing
   `accept_vault_submission_as_secret` flow: open, create a native secret through
   the ordinary secret path, and record acceptance. Ingestion automation and
   sharing remain out of scope.

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
against the submission's active inbox key id; the thumbprint is separately bound
inside the HPKE binding (via the attested key) and inside the attestation. The
two identifiers must agree with what C1 already stores on the
`LatchkeyVaultSubmission` row.

## 6. Validation split (pre-mint)

The connector validates everything it can without a trust anchor, before the
provider runs:

- profile/config version and suite are the supported ones;
- every binding identifier is non-empty, bounded, and free of control
  characters; `key_generation` is non-zero;
- the public JWK is exactly one public AKP JWK, `alg` exactly the suite string,
  no `priv`, `pub` exactly 1216 bytes, parsing as an X-Wing key, and the X25519
  component is not a low-order point;
- `public_key_thumbprint` re-derives from `public_jwk_json` as
  `base64url(SHA-256({"alg":…,"kty":…,"pub":…}))`;
- the vault-inbox recipient is the only encryption config, and the issuance
  yields exactly one plaintext value.

**Attestation signature verification stays where it is.** The inbox public key is
authenticated by an owner-device composite ML-DSA-65 + Ed25519 attestation that
only the member's Latchkey client can verify against its registered device key;
C1 already validates the stored attestation structurally when it serves the
profile. The connector must not treat a JWK as self-authenticating and does not
invent, re-sign, or substitute keys. This is the same trust root as the existing
inbox submission flow, unchanged.

## 7. What is explicitly out of scope

- No Rust production code or artifact changes; the reader is unmodified.
- No C1-side decrypt/reseal, ingestion daemon, sharing, or native-secret
  creation at delivery.
- No age profile change, no Paper fallback, no remint after a live key exists.
- No requester-selected destination: the destination is the admin-approved
  vault frozen on the approved ticket.

## 8. Pinned interop fixture

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
