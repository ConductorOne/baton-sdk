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
| J10 | Go JWE is ingested and revealed through native Rust/WASM read APIs | Deferred to C1/Multipass integration stage; blocks rollout, not SDK draft review |

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
