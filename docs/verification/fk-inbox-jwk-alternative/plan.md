# FK Inbox JWK Alternative Verification Plan

Status: frozen implementation-blind baseline. Committed before implementation
inspection, per `docs/BUG_CATCHING.md` §2 and `docs/REVIEW_CHECKLIST.md`. A plan
reconstructed from a finished diff is not preregistration.

## Risk

HIGH on every axis, and the ask is therefore prevention rather than review.

- **Silence.** A wrong binding still produces a well-formed envelope that seals
  and uploads. Nothing crashes; the credential is simply sealed to a context that
  does not match the vault, and the failure surfaces only when a member tries to
  open it, if then.
- **Durability.** `EncryptedData.encrypted_bytes` is a wire type read by Latchkey
  readers and by SDK versions that do not exist yet. Remediation cannot retract a
  ciphertext already committed to a vault.
- **Version-pair dependence.** Yes, on both sides: the config surface a C1 build
  can construct, and the reader that opens the result.
- **Consumer distance.** The c1 platform and the Rust member runtime, in other
  repositories, on their own release cadence.

Consequence rung 4-5: coordinating a contract change across repos we do not
control, and credential operations that cannot be undone.

This alternative is an experimental config API, not a deployed compatibility
promise. It is additive: the existing `baton/jwk/v1` and `age` paths keep their
current bytes, and inbox mode is never inferred from key type.

## Contract

- Inbox HPKE is selected by the explicit provider `baton/vault-inbox/v1` on
  `EncryptionConfig`. No other value selects it.
- The recipient public key is the ordinary AKP JWK on
  `JWKPublicKeyConfig.pub_key`: `alg`, `kty`, `pub` carry the actual key material.
- The inbox binding context travels in a typed, namespaced JWK extension member
  (`baton_vault_inbox`), not in protobuf and not packed into `kid`.
- The extension carries: version, suite, tenant, vault boundary, key generation,
  payload scheme, delivery id, expected public key thumbprint, content type.
  Every field is required, bounded, and validated before any provider work.
- `key_id` on `EncryptedData` remains the single authoritative inbox key id. A
  JWK `kid` that disagrees is refused; a `kid` is never used to carry context.
- The inbox thumbprint stays exactly the Latchkey canonicalization of
  `{alg,kty,pub}` and **excludes** the extension, so adding the context cannot
  move a legitimate key's thumbprint.
- Given the same coordinates and the same injected randomness, the JWK-configured
  provider emits **byte-identical** `encrypted_bytes` to the arm-102 path at
  `ae9de8d6`. The binding framing, HPKE sealing, payload serialization, and
  envelope encoding are reused unchanged, not reimplemented.
- A config that names this provider but is malformed, missing, or wrong-provider
  must fail before mint. It must not downgrade to classical encryption, and it
  must not mint and then refuse to hand back a result.
- Capability advertisement, single-config, and post-mint cardinality rules are
  preserved, including the per-path differences for create, rotate, and
  registered actions.
- `baton/jwk/v1` and `age` behavior, including their thumbprints and key-id
  conventions, is unchanged.

## Coverage model

Executable dimensions:

- selection: provider present/missing/wrong/empty, on an extended JWK;
- extension: absent / valid / malformed JSON / trailing JSON / unknown member /
  duplicate member / unsupported version / unsupported suite / each required id
  missing / private material present;
- outer JWK: duplicate `alg`/`kty`/`pub`; conflicting `kid`; ordinary optional
  JOSE metadata present;
- coordinates: each of tenant, vault boundary, inbox key id, generation, payload
  scheme tampered in turn;
- entropy: injected determinism for equivalence, real randomness for round trip;
- legacy paths: `baton/jwk/v1`, `age`, unchanged;
- gate path: `IssueCredential`, registered actions, `CreateAccount`,
  `RotateCredential`.

Cross-cutting: extension-shape and outer-JWK rows apply across every gate path;
a representative reduction is acceptable only after implementation inspection
confirms the paths share the same parser. That reduction is recorded in
`evidence.md`, not assumed here.

## Criteria

- C1: The JWK-configured provider emits byte-identical inbox wire bytes to the
  arm-102 path at `ae9de8d6` for the same coordinates and injected randomness.
- C2: The binding bytes equal the committed vector and are unchanged.
- C3: The inbox thumbprint is unchanged and excludes the extension.
- C4: Every malformed, unknown, duplicate, trailing, unsupported, incomplete, or
  private-material case is refused before the provider is invoked (issuer calls
  observed as zero), not merely refused somewhere.
- C5: A missing or wrong provider on an extended JWK neither downgrades to
  classical encryption nor mints.
- C6: Capability advertisement, exclusivity, and per-path cardinality are
  preserved.
- C7: A conflicting JWK `kid` is refused and `key_id` is the only key id used.
- C8: `baton/jwk/v1` and `age` produce unchanged bytes.
- C9: HPKE round trip succeeds, two seals differ, and tampering any single
  binding component fails to open.
- C10: No leftover arm-102 dependency remains in production code or tests.
- C11: The alternative reduces or relocates proto schema and code, measured
  against `ae9de8d6` and main, and the delta is reported either way.

## Instruments

- Pinned vectors and a determinism-injecting test cover C1-C3 and C9.
- Table-driven package tests over the coverage model cover C4-C8.
- Every negative case is mutation-checked: the gate is disabled and the test must
  fail. A case that passes with its subject deleted is recorded as a gap, not
  counted as coverage. This environment has previously produced gates that were
  proven to *run* but not to *refuse*, so the mutation check is the instrument,
  not a formality.
- `make protogen` plus Buf lint and breaking cover the schema surface of C10-C11.
- A diff measurement against `ae9de8d6` and main covers C11.

C1 Rust ingestion and native-secret reveal are a **separate required lane** and
are not covered by any instrument here.

## Proto decision (recorded before implementing)

`buf.yaml` enables `ENUM_VALUE_NO_DELETE_UNLESS_{NAME,NUMBER}_RESERVED`,
`FIELD_NO_DELETE_UNLESS_{NAME,NUMBER}_RESERVED`, and `FILE`, while excepting
`EXTENSION_NO_DELETE` and `FIELD_NO_DELETE`. The operative rule from
`.claude/skills/ci-review.md` is explicit:

> "Do not remove active fields in place. Deprecate first, keep old numbers
> reserved, and regenerate checked-in Go output."

and

> "Any proto source change must include matching generated `pb/.../*.pb.go`
> changes."

Baseline check:

| Baseline | Contains arm 102? |
|---|---|
| `origin/main` (`15119140`) | No |
| Latest release `v0.31.0` | No |
| PR branch base `ae9de8d6` | Yes |

Arm 102 was never released, so against the released baseline removal breaks
nothing a consumer could have compiled against. Against the PR branch it is a
breaking removal and `buf breaking` would flag it. The repository rule for
in-place removal is reservation, so the field number is reserved rather than
reused or silently dropped, and generated output lands with the proto source as
the rule requires.

## Change orders

Change orders are added here, with their affected criteria, as implementation
inspection or review surfaces them.

### CO-1 — unknown-field refusal is scoped to the provider-specific JWK config

Affects C4, C6.

The first implementation refused unknown fields on the shared
`EncryptionConfig`. The arm-102 test asserted the opposite — that the shared
message stays additive — and the plan's contract requires `baton/jwk/v1` and
`age` to be unaffected. Refusing unknown fields on a message shared with every
other provider would make a future field a breaking change for this profile.

The refusal now applies to the provider-specific `JWKPublicKeyConfig` only, which
is where the contents are frozen into the binding. An unknown field on
`EncryptionConfig` with this provider is tolerated, as before.

### CO-2 — a JWK `kid` that disagrees with `key_id` is refused

Affects C7.

C7 requires that "a JWK `kid` that disagrees [with `key_id`] is refused". The
first implementation read no `kid` at all. The parser now reads an optional `kid`
and refuses a non-empty value that differs from `key_id`; a matching or absent
`kid` is accepted.

This is a producer-facing change: a JWK that carries a `kid` unrelated to the
inbox key id is now refused rather than ignored. It is recorded in §5.1 of
`docs/vault-inbox-delivery.md`.
