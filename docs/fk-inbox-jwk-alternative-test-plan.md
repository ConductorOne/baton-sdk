# Test plan: JWK-config alternative to the vault-inbox recipient config

Branch: `highb/IGA-4417/fk-inbox-jwk-alternative`, based at `ae9de8d6`
Status: **recorded before implementation**, as the high-risk process requires.

This plan is bounded on purpose. It names the checks that can actually fail and
what each one proves. Checks that could only pass (asserting a value was copied,
a field survived a round trip, a constructor returned non-nil) are deliberately
absent; they cost review attention and catch nothing.

## What is being changed

The alternative selects inbox HPKE through the *existing* JWK config surface —
`EncryptionConfig` + `provider` + `JWKPublicKeyConfig.pub_key` + `key_id` — with
the inbox binding context carried in a typed, namespaced JWK extension member
(`baton_vault_inbox`) on the public JWK document, instead of the dedicated
`VaultInboxRecipientConfig` oneof arm (`= 102`).

The load-bearing claim is **wire equivalence**: given the same inputs and the
same injected randomness, the JWK-configured provider must emit byte-identical
`EncryptedData.encrypted_bytes` to the arm-102 path at `ae9de8d6`. Everything
below exists to either establish that or to bound where it can fail.

## Equivalence

- **E1 — byte-identical seal.** With a fixed plaintext, fixed recipient key, and
  deterministically injected HPKE randomness, the JWK-config path and the
  arm-102 path produce identical `encrypted_bytes`.
- **E2 — binding framing unchanged.** The derived binding bytes equal the bytes
  used by the arm-102 path for the same coordinates, and equal the committed
  `binding_hex` vector.
- **E3 — thumbprint canonicalization unchanged.** The inbox thumbprint is still
  `base64url(SHA-256({"alg","kty","pub"}))` and **excludes** the extension. A
  pinned vector asserts the same digest as before, so a legitimate key's
  thumbprint cannot shift when the extension is added or removed.
- **E4 — committed vectors still open.** The existing pinned payload/ciphertext
  vectors decode and open unchanged.

## Extension parsing — refused before any provider work

Each case asserts the refusal happens with **zero** provider invocations, not
merely that an error came back somewhere.

- **P1 — malformed extension JSON.** Truncated, non-object, or non-JSON.
- **P2 — trailing JSON.** Valid extension followed by a second value or trailing
  bytes.
- **P3 — unknown extension field.** A member the parser does not define is
  refused rather than ignored. This is the difference between a strict protocol
  surface and one that silently accepts a future field it cannot honour.
- **P4 — duplicate extension field.** The same member twice, including the case
  where the duplicate carries a different value, so "last wins" cannot decide
  security-relevant context.
- **P5 — duplicate top-level security-sensitive JWK member.** A repeated `alg`,
  `kty`, or `pub` on the outer JWK — the encoding that lets two readers of the
  same document disagree about which key is being used.
- **P6 — unsupported version / suite.** Version and suite values the provider
  does not implement.
- **P7 — missing required IDs.** Each of tenant, vault boundary, generation,
  payload scheme, delivery id, thumbprint, content type in turn.
- **P8 — private material present.** A `priv` member, or any private key
  material, on the recipient document.
- **P9 — empty / non-object extension where required, and extension absent.**
- **P10 — ordinary optional JOSE metadata still accepted.** Metadata that is safe
  to ignore must not become a hard failure, so P3's strictness is shown to be
  scoped to the protocol extension rather than to all of JOSE.

## Provider and gate behaviour

- **G1 — missing or wrong provider.** An extended JWK with no provider, or with
  `baton/jwk/v1`, must not silently fall through to classical encryption and must
  not mint. This is the downgrade path, and it is the single most important gate
  here.
- **G2 — malformed config is still recognised as this provider.** Recognition
  must not depend on a config that parsed, or a corrupt config becomes an
  unclassified input.
- **G3 — capability advertisement.** A descriptor that does not advertise the
  profile is refused.
- **G4 — exclusivity / cardinality.** A single recipient config is required; a
  mixed or duplicate recipient set is refused. Post-mint cardinality handling is
  unchanged per path.
- **G5 — conflicting `kid`.** An outer `key_id` plus a disagreeing JWK `kid` is
  refused. `key_id` remains the single authoritative inbox key id.
- **G6 — create/rotate/actions behaviour preserved.** The pre-mint gates that
  apply to those paths keep applying.

## Regressions that must not move

- **R1 — legacy `baton/jwk/v1` behaviour.** Byte-for-byte unchanged, including
  its thumbprint and key-id conventions.
- **R2 — `age` provider.** Byte-for-byte unchanged.
- **R3 — no leftover arm-102 dependency** in production code or tests.

## Cryptography reachability

- **C1 — HPKE round trip.** Seal then open with the fixture private key returns
  the plaintext.
- **C2 — every binding component is authenticated.** Tamper tenant, vault
  boundary, inbox key id, generation, payload scheme, and suite in turn; each
  must fail to open, proving each is actually bound rather than merely present.
- **C3 — random seal.** Two seals over identical inputs differ, so C1 cannot
  pass by ignoring the injected randomness.

## Explicitly out of scope

- **C1 Rust ingestion and native-secret reveal.** That is a separate required
  integration lane (C1 #26059/#26060/#26061 and Multipass #823). No Go test here
  will be presented as evidence for it. The Go-side claim stops at the exact
  bytes the Rust reader consumes.
- **Recipient attestation verification.** Not implemented in this profile and not
  claimed. The SDK trusts the authenticated C1 action transport; a JWK and a
  matching thumbprint do not authenticate the destination.

## Known evidence gaps at the time of writing

- The provider-mutation harness used previously proves a gate *runs*, not that it
  *refuses*. Every negative case above will be mutation-checked: the gate is
  disabled and the test must fail. A case that passes with its subject deleted is
  reported as a gap, not counted as coverage.
- A gate over a comment-only change would be a vacuous check. This plan has no
  such case; the arm-102 exclusive-encryption test from the PR1145 branch relies
  on which class is registered and cannot detect commentary, so it is expected to
  be rewritten rather than ported as-is.

## Process constraints

All generation, tests, lint, and builds run remotely or through CI. Source is
committed and pushed **before** regeneration; generated output lands in a
separate commit. No force-push, no merge, no PR unless requested.
