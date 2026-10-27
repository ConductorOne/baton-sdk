# Connector-declared secret action output types

## Frozen behavior

A connector declares the content type of each secret action output before its handler runs. The connector owns the value bytes and codec. The caller chooses the destination and acknowledges the declared content type before producer side effects. One secret output can contain a document with multiple fields.

An absent declaration preserves existing generic output behavior. A present empty declaration, declarations on inputs or public outputs, and malformed identifiers are invalid. Existing arbitrary encryption context behavior remains compatible for undeclared/generic outputs. Explicit structured outputs require a matching typed destination acknowledgement before action allocation and handler execution; an older caller that supplies generic context cannot silently reinterpret a structured output.

The action schema at registration and invocation is an immutable snapshot. Later caller mutations of schema pointers or encryption configurations cannot change an invocation's declaration or authenticated context.

Secret bytes remain separate from public return fields and are encrypted without conversion. This change introduces no credential issuance protocol, typed input support, provider lifecycle, release or activation.

## Risk and verification

HIGH: a mixed-version caller can otherwise silently change durable payload meaning after a producer performs an external effect. Prevention must run before the handler; post-result checking cannot reverse that effect. No grant-scale or artifact-open hot path changes: checks are linear in schema fields and encryption configurations, with a bounded context parse only for explicit typed outputs.

| Criterion | Required instrument | Initial status |
| --- | --- | --- |
| Declaration absent/present round-trip | Descriptor and binary/JSON schema round-trip | Evidence incomplete |
| Invalid placement/value refused | Global/resource registration tables; assert no schema published | Evidence incomplete |
| Old generic caller refuses typed output | Invoke actual ActionManager with generic/missing/wrong context; producer-call count zero | Evidence incomplete |
| Matching typed context reaches producer | Actual manager, real JWE recipient and encrypted structured bytes | Evidence incomplete |
| Legacy generic compatibility | Existing secret-action tests plus arbitrary-context controls | Evidence incomplete |
| Immutable declaration/config | Mutate original schema/config after registration/dispatch; verify frozen output | Evidence incomplete |
| Result boundaries | Public-secret collision, undeclared/duplicate/required result checks | Existing tests; current change evidence incomplete |
| Generated compatibility | Owning protobuf generation and diff, source committed before generated | Evidence incomplete |

Instrument adequacy: demonstrate the mixed-version regression fails when its pre-handler acknowledgement check is removed. Review changed branch coverage and preserve explicit gaps. Registration global/resource paths are a required cross-product; ordinary/encrypted handler registration both enforce declaration placement. No test contacts an upstream provider.

## Boundary resolution

Exact additive field and acknowledgement interpretation require architecture disposition before schema or authority changes. Source inspection was explicitly required to establish whether existing APIs suffice; it preceded this plan. Preserve that exception instead of claiming implementation-blind discovery.

## Change orders

Append any contract or enforcement ownership change here before implementation.

### Change order 1: resolved declaration and caller acknowledgement

Architecture disposition selects optional config.Field.secret_content_type=8 and a pre-handler check of existing full-knowledge JWE context purpose/version/type. Check every JWE recipient, reject duplicate JSON keys, and require one secret return for non-generic types. A matching JWE recipient may coexist with Paper/age recipients. Generic declarations retain opaque contexts and existing multi-output transport. The guard establishes type agreement only, not server authentication or payload validity. Tests cover global/resource invocation, invalid placement, mixed recipients and immutable registered type metadata. Full catalog ownership remains with the consumer; SDK checks syntax only.

### Change order 2: preserve existing singleton-JWE transport

Executed invocation evidence showed that released ValidateEncryptionConfigs rejects any JWE recipient in a configuration set with length other than one. The earlier JWE+Paper positive assumption was counterfactual. Architecture correction preserves that existing restriction; JWE+age and multi-JWE are rejection controls before action allocation/handler effects. The positive typed path uses exactly one JWE recipient and one secret output. Generic age-only multi-recipient/multi-output behavior is unchanged. NewEncryptionManager does not itself enforce this configuration validation; the common ActionManager invocation boundary does, before construction and handler dispatch. No shared/native crypto API changes are introduced.

### Change order 3: immutable advertised schema

A source check identified a correction required by the original immutable-registration criterion: the guard copied type strings, but registration and schema getters retained shared mutable schema pointers. The new six-case global/resource regression failed when caller mutation changed the advertised absent/generic/password declaration into api_key_v2 while the registered handler retained its original type. Publication and read APIs now clone schemas. Mutations of the original, listed and fetched schemas cannot alter advertised/enforced agreement. This adds linear copying in action schema size on registration/discovery, outside declared artifact-open/grant-scale hot paths; no provider, store or credential lifecycle changes.
