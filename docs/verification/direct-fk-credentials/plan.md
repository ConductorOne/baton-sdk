# Direct FK credential encryption

Baseline: SDK f618d497859307182df122e560696ec458ab9aa4. Scope spans the
SDK, Datadog, C1 and the Latchkey member runtime. This is HIGH risk: an
incorrect contract can strand a provider credential or publish a secret to
the wrong destination. No declared sync hot path changes.

## Decisions

The user approved proceeding without a formal security review and selected
the SDK's vendored Go HPKE implementation as the compatibility reference.
Pin filippo.io/hpke v0.4.0, MLKEM768X25519, HKDF-SHA256 and
ChaCha20-Poly1305. Do not infer compatibility with Rust from suite names.
Authenticated C1 action transport supplies configuration authority.
Admission remains disabled until the cross-repository acceptance tests pass.
Do not modify the running PR2 task or branch.

All tests, lint and generation run remotely. Commit source changes before
generation, push, then create a Squire environment using OMP and
deepseek-4.1-flash for verification. Generated changes get a separate commit.

## Stages and obligations

| ID | Stage | Contract and required evidence |
| --- | --- | --- |
| P1 | Profile | Pin native framing, capsule context, field order, widths, size bounds and rejection rules. Check fixed Go/Rust vectors, including separate tampering of both KEM components. |
| P2 | Runtime | Imported CEK plus fresh MEK and SecretKeyKey yields ordinary Model-B reads. Test normal read, wrong address and wrong key; assert import never decrypts or reseals values. |
| P3 | SDK | Add unused config arm 102, typed result and explicit capability. Proto lint, breaking checks and regenerated descriptors agree. Existing age/JWK encodings remain unchanged. |
| P4 | SDK | Validate complete configuration, supported profile, capability and single-config contract before Issue. Test provider call count is zero for every rejection. |
| P5 | SDK | Exactly one provider value succeeds; zero/multiple values return an error without plaintext. Exercise the builder boundary and verify issuance is not retried. |
| P6 | C1 prepare | Durable get-or-create by tenant/ticket freezes address and destination before dispatch. Test conflicting retries, live authority, rotation and protected private-key custody. |
| P7 | C1 import | Import requires the exact persisted result, action and destination. Receipt binds every identifier and result digest. Test identical replay and conflicting result bytes. |
| P8 | C1 commit | Objects and wrappers precede conditional publication. Cancellation/expiration competes with publication at one durable coordination boundary. Crash at every write cut and replay twice. |
| P9 | C1 cleanup | Durable cleanup removes preparation keys after commit/terminal cleanup. Compensation affects the exact issuance version and preserves sibling objects. |
| P10 | Integration | Datadog advertises only the supported single-value profile. Go seals, Rust opens through ordinary FK APIs, and stored value bytes equal connector bytes. |
| P11 | Rollout | Paper regression and old/new connector matrix reject unsupported FK before mint. No fallback, hard-reset remint, or success based only on allocated IDs. Record deployed C1/WASM and released client revisions. |

Each criterion starts evidence incomplete. Stage-specific evidence must name
the command, tested revision, assertion, covered cases and remaining gaps.
Validate rejection instruments with a planted bypass. Disposition uncovered
changed branches and obtain independent review before claiming completion.

Source reconnaissance preceded this plan; it is not an implementation-blind
preregistration. No implementation or verification results exist at baseline.
