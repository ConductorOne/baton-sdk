# Field metadata transition

`config.Field.content_type` is MIME metadata. Logical delivery types use one
`multipass-type-` tag, for example `multipass-type-api_key_v2`. Other tags do
not select a type. The earlier prototype used Field 8 for a bare logical type;
that encoding is incompatible and valid bare values are rejected. JSON using
`secret_content_type` on a Field is also rejected by the legacy transport.
Native `request_schema` and descriptor `secret_content_type` remain readable;
when new and legacy declarations coexist they must agree. An absent return
field preserves the legacy issuance contract. A declared native return is one
named required secret string whose name must match the plaintext result.

Request values remain nonsecret. Hosts apply defaults before approval; SDK
validation does not apply defaults. Payload bytes are opaque and are not
validated against MIME or logical type metadata.
