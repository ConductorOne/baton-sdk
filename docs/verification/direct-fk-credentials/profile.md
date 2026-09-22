# FK credential profile 1

Reference: filippo.io/hpke v0.4.0 vendored at SDK f618d4978593.
The wire profile is pinned to this implementation, not a floating draft.
Its reference document names hpke-pq-03, hybrid-kems-07,
concrete-hybrid-kems-02 and hpke-hpke-02.

## Capsule suite 1

HPKE base mode, MLKEM768X25519 (0x647a), HKDF-SHA256 (0x0001),
ChaCha20-Poly1305 (0x0003). One message per HPKE context, sequence zero.
The 1216-byte public key is ML-KEM-768 public key || X25519 public key.
The 1120-byte encapsulation is ML-KEM-768 ciphertext || X25519 public key.
Private keys are 32-byte seeds expanded by the pinned implementation.
Do not substitute HPKE's SHAKE256 KDF or derive a seed with DeriveKeyPair.

Authenticated context starts with ASCII `c1/fk-credential-key-capsule/v1`
(no NUL or length prefix), followed by the following fields in order.
`u32` means four-byte unsigned big-endian; `field` means u32 byte length
followed by exact bytes. Text is valid UTF-8 with no normalization.

| Field | Encoding |
| --- | --- |
| protocol_version | u32, exactly 1 |
| tenant_id | field |
| ticket_id | field |
| vault_id | field |
| vault_boundary_id | field |
| secret_id | field |
| version_id | field |
| content_type | field |
| preparation_id | field |
| key_id | field |
| key_capsule_suite | u32, exactly 1 |
| key_capsule_public_key | field, exactly 1216 bytes |
| value_suite | u32, exactly 1 |
| value_ciphertext_sha256 | exactly 32 bytes, no length prefix |

Each identifier is 1..1024 UTF-8 bytes, without control characters or leading
or trailing whitespace. Content type is 1..128 ASCII bytes matching
`^[a-z0-9]+([._-][a-z0-9]+)*$`, as in native SecretPayloadV2. Empty does not
default to generic. All profile integers reject zero and unknown values.

Use the complete context as both HPKE info and AAD. Capsule plaintext is
u32(1) || field(context) || CEK[32]. It has no trailing bytes. The receiver
reconstructs context from the persisted preparation and exact value digest,
compares the embedded context, then imports the CEK. Outer envelope data
never supplies authorization or overrides persisted preparation data.

X-Wing is hybrid public-key cryptography. SHA3-256/SHAKE256 inside X-Wing,
SHA-256/HKDF and ChaCha20-Poly1305 are symmetric/hash primitives. HPKE base
mode does not authenticate the sender. The authenticated C1 connector action
transport supplies destination/configuration authority; malicious C1 is
outside this full-knowledge model. No signing primitive is added.

## Native value suite 1

Reference: multipass 03c98d9f30b7, value_crypto.rs ItemAddress and
ItemKeyPair::seal_value_with_rng. Generate a fresh 32-byte CEK and a fresh
12-byte nonce from the OS CSPRNG for each value.

Binding = ASCII `latchkey/v1/item-value/` || field(tenant_id) ||
field(secret_id) || field(version_id) || field(content_type).
HKDF-SHA256 uses the CEK as IKM, absent salt, binding as info, output 32 bytes.
ChaCha20-Poly1305 uses that key, the sampled nonce and binding as AAD.
Native ciphertext is 0x06 || nonce[12] || ciphertext || tag[16].
Neither vault identifier occurs in native value derivation; both are bound
in the capsule. They must never be substituted for one another.

Plaintext is native SecretPayloadV2 protobuf wire data: uint32 field 1 = 2,
string field 2 = exact content_type, bytes field 3 = credential bytes.
The value is nonempty and at most 1 MiB; framing adds payload overhead.
Protobuf serialization is not used as canonical authenticated context.

## SDK carrier and issuance

EncryptionConfig arm 102 carries FullKnowledgeVaultConfig, fields 1..13 in
the context table order, excluding the derived ciphertext digest.
Provider is `baton/full-knowledge-vault/v1`. EncryptedData.key_ids contains
exactly the preparation's key_id; deprecated key_id is empty. The outer
EncryptionConfig.key_id may be empty or equal to that key_id.

EncryptedData.encrypted_bytes is FullKnowledgeCredentialEnvelope protobuf:
version 1, complete public config, native value ciphertext, HPKE encapsulation,
and capsule ciphertext. Only exact version 1 is supported. The preparation
config is repeated for binding checks, never as a source of authorization.

An issuance descriptor explicitly advertises protocol version 1, promising
exactly one output value. Require exactly one config and reject mixed/fan-out
requests before Issue. Unexpected output cardinality is an issuance error
requiring compensation/reconciliation; it is never a reason to remint.

## Acceptance status

This document defines the implementation target. Cross-language capsule,
native value and Model-B vectors are still required before C1 consumers or
admission can be enabled. A successful Go self-round-trip is insufficient.
