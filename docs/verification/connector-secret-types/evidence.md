# Evidence ledger

Base: released v0.42.3, commit 2e045d482d592a9462b5f87c70a6113fe87ec54a. Source declaration/guard: 528830db; generated protobuf: 9a550250; immutable advertised schema correction: 4483426d. No release is claimed.

| Criterion | Executable artifact and assertion | Status |
| --- | --- | --- |
| Declaration presence and schema round-trip | TestSecretContentTypeSchemaRoundTrip, binary/JSON equality and HasSecretContentType absent/present; explicit empty ValidateAll refusal | Verified to stated coverage |
| Illegal declarations | TestSecretContentTypeRegistrationRejectsInvalidDeclarations, global/resource and ordinary/encrypted registration; no published action on invalid input/public/empty/grammar/length | Verified to stated coverage |
| Pre-handler downgrade prevention | TestTypedSecretOutputRefusesUnacknowledgedCallerBeforeHandler, global/resource; missing/opaque/generic/wrong type/purpose/version/ambiguous/invalid JSON plus unsupported recipients/cardinality; empty action ID, handler counter zero, no allocation | Verified to stated coverage |
| Exact structured bytes and immutable registration type | TestTypedSecretOutputEncryptsExactHelperBytesWithFrozenDeclaration, actual manager/helper/JWE; mutate original schema after registration; independently open X-Wing HPKE bytes and compare canonical username/password JSON | Verified to stated coverage |
| Legacy generic compatibility | TestGenericSecretOutputKeepsOpaqueContexts plus existing ActionHandlerWithSecrets tests; absent/explicit generic and old age multi-output/recipient paths | Verified to stated coverage |
| Instrument adequacy | Removed new guard temporarily, ran account/generic regression: failed because expected error was nil; restored source byte-for-byte | Verified bounded mutation |
| Source generation | make protofmt; make protogen exit0. Only config.pb.go, config.pb.validate.go, config_protoopaque.pb.go changed; generated separately | Verified |
| Full package and race | go test -race -v ./pkg/actions -count=1; all named package tests ran/pass, no skips; 11.238s final package execution | Verified to stated coverage |
| Cross-repository integration | C1 consumer adoption, real member/Store/reveal/canonical codec/renderer | Deferred to consumer integration stage; not an SDK-only claim |

Initial actual invocation test failed on JWE+Paper due to the pre-existing singleton-JWE validator. That failed assumption is preserved in the change-order history; tests now assert the released contract rather than expanding native/shared validation. The caller context guard establishes agreement, not server authentication or payload admission. No provider operations were executed.

Snapshot correction: TestSecretOutputAdvertisedSchemaIsAnImmutableSnapshot failed in all six global/resource × absent/generic/password_v2 cases before the fix. The final full race run passes original-pointer, listing-copy and getter-copy mutation controls while actual invocation agrees with the unchanged advertised declaration. Proto inputs/generated files are unchanged by this correction; no regeneration was run.
