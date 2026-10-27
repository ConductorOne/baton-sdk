# Secret payload contracts

Multipass owns the format of every secret payload type it renders. This
directory is the contract consumers copy.

| Path | Authored | Purpose |
|---|---|---|
| `schemas/<secret_content_type>.schema.json` | by hand | JSON Schema 2020-12 for the stored JSON document of the five JSON types. Consumers copy these files unchanged. |
| `contracts.json` | by hand | Index of all eight types: codec, schema path, text-codec rules, size limits, stored-value compatibility. |
| `fixtures/` | generated from the real Rust decoder | Valid and invalid stored payloads with the receiver's verdicts. `scripts/secret-contracts.sh generate` rewrites them; `check` fails when they drift. |

The schemas are not generated from Rust. The Rust receiver
(`crates/latchkey-client-sdk/src/secret_types`) is what stored values actually
meet, so two tests keep it and the schemas in agreement:
`hand_authored_schemas_agree_with_the_receiver_catalog` and
`contracts_index_agrees_with_the_receiver_catalog`. Changing a field means
editing the schema, the receiver catalog and the fixtures together.

## Naming

`secret_content_type` names the secret format in schemas, helpers and docs:
`password_v2`, `api_key_v2`, `ssh_key`, `oauth_client_secret_v2`,
`certificate`, `hotp`, `note`, `generic`. It is the value already carried by
the protocol field `content_type` (`SecretTypeDefinition.content_type`).
The optional plaintext/encrypted-data `schema` describes the plaintext independently. Nothing on the wire, in stored bytes
or in authenticated context is renamed; consumers map the field explicitly.

## What a schema asserts and what it annotates

JSON Schema asserts structure: keys, types, required, lengths, item counts,
uniqueness and patterns. In the 2020-12 dialect, `format: date` is an
annotation by default. Consumers that require date rejection at the schema
layer must enable format assertion (such as `AssertFormat`), or use a native
date check. Keep the native `rfc3339_full_date` check in either case.
Property order has no semantic meaning and does not define encoder order.
The schemas also carry annotations a native check enforces:

- `x-non-blank`: the receiver requires a string containing a non-whitespace character, using Rust `str::trim` semantics.
- `x-semantic`: ids of native checks (`http_url`, `ssh_private_key`, ...).
- `x-max-utf8-bytes`, `x-max-document-bytes`: byte caps beyond `maxLength`.
- `x-secret-content-type`: the type the schema describes.

Text codecs (`hotp`, `note`, `generic`) store the field's UTF-8 bytes
verbatim and have no schema. `generic` may also hold bytes that are not UTF-8,
and any type outside this list (including legacy ones such as `totp`) is kept
as an opaque value and never rejected on read. A blank optional string or an
empty optional list is ignored by the receiver, so schemas describe the
document after those are dropped; a producer omits them.

This directory carries encoding-independent contracts only. The cross-language
encoding vectors are `specs/secret-types-v1.json`.
