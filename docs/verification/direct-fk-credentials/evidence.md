# Direct FK credential verification evidence

Remote verification run on 2026-09-22. Every command below ran in a Squire
environment, never on a laptop. This file records what was actually executed
and observed; it does not restate the plan's intent as if it had happened.

## Revisions

| Repository | Branch | Revision |
| --- | --- | --- |
| baton-sdk | `highb/iga-4417-add-xwing` | `7ca9cb42` (my generated commits: `d016789c`, `8dc4be33`, `4c72bed9`) |
| multipass | `highb/IGA-4417/direct-fk-key-import` | `8dd1e677` (my commits: `ef1e73ec`, `15bae810`, `27341b4d`, `809cd3cb`) |
| baton-datadog | `highb/IGA-4417/direct-fk-credentials` | `c7fc3504` (SDK pinned at `7ca9cb42`; my commits: `ab6c6f03`, `1ebfbf38`, `5162f7d7`, `c7fc3504`) |

Baselines: multipass `03c98d9f30b7c95095d41e5718c1d8f0583bb06d` for the native
value suite, SDK `f618d497` for the profile, multipass
`aa5b804b53e6bd7789a7ed7b16d415b140690cd1` for the member runtime.

## Toolchain

| Tool | Version |
| --- | --- |
| go | go1.27.1 linux/arm64 |
| buf | 1.64.0 (CI pins the `bufbuild/buf-action` at 1.72.0) |
| golangci-lint | v2.13.2 built with go1.27.1 (the version `ci.yaml` pins) |
| rustc (multipass root) | 1.93.0 (`rust-toolchain.toml`) |
| rustc (`crates/latchkey-mls-core`) | 1.96.1 (`crates/latchkey-mls-core/rust-toolchain.toml`) |
| hpke-rs | 0.7.0 (`hazmat`, `libcrux`, `std`) |
| filippo.io/hpke | v0.4.0 (vendored) |
| node / python3 | v24.13.0 / 3.13.12 |

## Commands and results

All commands ran in `/data/squire/src/baton-sdk` unless stated otherwise.

| Command | Exit | Result |
| --- | --- | --- |
| `make protogen` | 0 | Regenerated `connector.pb.go`, `connector_protoopaque.pb.go`, `resource.pb.go`, `resource.pb.validate.go`, `resource_protoopaque.pb.go`; committed separately and pushed. A second run for the `EncryptedData` comment change regenerated only `resource.pb.go` and `resource_protoopaque.pb.go`. |
| `buf lint` | 0 | clean |
| `buf format -d --exit-code` | 0 | clean |
| `buf breaking --against https://github.com/conductorone/baton-sdk.git#branch=main` | 0 | no breaking change |
| `go test ./pkg/crypto/... ./pkg/connectorbuilder/...` | 0 | all packages ok |
| `go test -tags=baton_lambda_support ./...` | 0 | every package ok, no failures |
| `make lint` (`golangci-lint v2.13.2 run --timeout=3m`) | 0 | clean at `7ca9cb42`; it was red with 6 FK findings before `f726ded8`, see below |
| `go test -run TestInteropVectors ./pkg/crypto/providers/fullknowledge/` | 0 | PASS |
| `cargo test --locked credential_import_matches_sdk_interop_vectors` (multipass `crates/latchkey-mls-core`) | 0 | PASS |
| `go build ./...` + `go test ./pkg/connector/...` (baton-datadog) | 0 | builds, tests pass |
| `go test ./...` (baton-datadog, final pin) | 0 | all packages pass |
| `golangci-lint run --timeout=6m` (baton-datadog, final pin) | 1 | the same 10 `goconst` findings, all pre-existing |
| `make test/mls` (multipass root, rustc 1.93.0) | 0 | default lane 197 passed, `member-runtime` lane 350 passed, plus the integration, UI and doc-test suites |
| `make test/mls-wasm` (multipass root, rustc 1.93.0) | 0 | 62 passed at `8dd1e677` |
| `RUSTUP_TOOLCHAIN=1.96.1 cargo test --manifest-path <member-wasm> --locked --no-default-features` | 0 | 62 passed; the CI toolchain for that job |
| `make lint/mls-wasm` (`RUSTUP_TOOLCHAIN=1.96.1`, clippy `-D warnings`) | 0 | clean |
| `make check/mls-wasm` (`RUSTUP_TOOLCHAIN=1.96.1`) | 0 | surface and host-RNG checks pass; local digest `5e0d6aa2…`; the x86_64-canonical pin comparison is skipped |
| `make fmt-check/mls` (multipass root) | 0 | clean |
| `RUSTUP_TOOLCHAIN=1.96.1 make fmt-check/mls-wasm` | 0 | clean |

### SDK lint

At the revision this run first tested (`39fe1757`) `make lint` exited 2 with six
findings in the FK source, none of which reproduce on `main`:

```
pkg/crypto/providers/fullknowledge/fullknowledge.go:155:66: G115: integer overflow conversion int32 -> uint32 (gosec)
pkg/crypto/providers/fullknowledge/fullknowledge.go:160:45: G115: integer overflow conversion int32 -> uint32 (gosec)
pkg/crypto/providers/fullknowledge/fullknowledge.go:162:45: G115: integer overflow conversion int32 -> uint32 (gosec)
pkg/crypto/providers/fullknowledge/fullknowledge.go:194:49: G115: integer overflow conversion int -> uint32 (gosec)
pkg/crypto/providers/fullknowledge/fullknowledge_test.go:101:25: G115: integer overflow conversion int -> uint32 (gosec)
pkg/crypto/providers/fullknowledge/fullknowledge_test.go:89:41: nolintlint: directive `//nolint:staticcheck ...` is unused for linter "staticcheck"
```

They were reported rather than patched, because they are production source. The
branch owner fixed them in `f726ded8`, and `make lint` on `7ca9cb42` exits 0
with no findings.

The verification test file contributed five more (`gosec` G115/G101/G306) on
its first draft. Those were removed in the verification commit: the fixture's
config fields are typed with the proto enum types so no narrowing cast is
needed, the credential constant was renamed, and the generator writes with
`0o600`.

## Member-WASM lane

CI builds and tests the member-WASM crate through the root Makefile under rustc
1.96.1 (`dtolnay/rust-toolchain@1.96.1` in `ci.yml`), targeting
`wasm32-unknown-unknown`. Every gate that job runs passes at `8dd1e677`:

| Gate | Exit |
| --- | --- |
| `make test/mls-wasm` (rustc 1.93.0, as the root Makefile resolves it) | 0 — 62 passed |
| `RUSTUP_TOOLCHAIN=1.96.1 cargo test --manifest-path <member-wasm> --locked --no-default-features` | 0 — 62 passed |
| `RUSTUP_TOOLCHAIN=1.96.1 make lint/mls-wasm` (clippy, `-D warnings`) | 0 |
| `RUSTUP_TOOLCHAIN=1.96.1 make fmt-check/mls-wasm` | 0 |
| `RUSTUP_TOOLCHAIN=1.96.1 make check/mls-wasm` | 0 — see the artifact section |

The two Rust gates must run under 1.96.1 to match CI: with the repository root
selected as the working directory, rustup resolves the root
`rust-toolchain.toml` (1.93.0), which has no `wasm32-unknown-unknown` standard
library installed, and `make check/mls-wasm` fails with `can't find crate for
core`. That is a toolchain-selection artefact, not a source defect.

### The E0433 defect, and its baseline

At `809cd3cb` (and therefore at every revision from `b313a8b5` up to the fix)
`make test/mls-wasm` failed to compile:

```
error[E0433]: failed to resolve: use of unresolved module or unlinked crate `ffi`
    --> src/wasm_abi.rs:3402:43
     |
3402 | const _: extern "C" fn(u32, u32) -> u64 = ffi::latchkey_import_credential_value;
     |                                           ^^^ use of unresolved module or unlinked crate `ffi`
```

Every neighbouring line in that block carries `#[cfg(target_arch = "wasm32")]`;
the line added with the credential-import export did not, and `mod ffi` is
itself wasm-only, so any non-wasm build of the crate failed. This is production
source, so this run did not change it; it was reported and the branch owner
fixed it in `8dd1e677` with exactly that one attribute.

Baseline, run at the branch's parent `aa5b804b` with the same working
directory, toolchain and features:
`cargo test --manifest-path crates/latchkey-mls-core/crates/latchkey-mls-member-wasm/Cargo.toml --locked --no-default-features`
exits 0 with no such error. The defect is introduced by this branch's own
change, not inherited.

The fix is wasm32-neutral: `make check/mls-wasm` reports the same local
normalized digest, `5e0d6aa242e086d51e9eb6b8bba26fb1543dba4dbaba542a9467006b093fa67e`,
both before and after `8dd1e677`, because the attribute only removes a
non-wasm32 assertion.

### Toolchain lanes

`make -C crates/latchkey-mls-core test` is not a valid entry point on this
branch. Running from inside the crate selects that crate's
`rust-toolchain.toml` (1.96.1), and the trybuild snapshot
`crates/latchkey-mls-core/tests/ui/zk_member_runtime_absent.stderr` was
regenerated for 1.93.0 by `c35374f1`, so the run dies on compiler diagnostic
wording (`no function or associated item named` versus `no associated function
or constant named`) before reaching this change.

Baseline, at the branch's parent `aa5b804b` with the same working directory,
toolchain and features, via `cargo test --locked --test
zk_member_runtime_absent`:

```
test mutating_surface_is_absent_without_member_runtime ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
```

The same failure on the parent means the mismatch is a toolchain-lane artefact,
not something this branch introduced. The canonical native lane is the root
`make test/mls`, which uses 1.93.0 and passes.

`make check/mls-wasm` builds the release artifact for `wasm32-unknown-unknown`,
runs the surface check and the compiled-module host-RNG check, and both pass:

```
member wasm surface ok: exports latchkey_add_member, latchkey_alloc,
  latchkey_assemble_c1_key_package, latchkey_commit_entry_operation,
  latchkey_derive_exporter, latchkey_found_full_member_vault, latchkey_free,
  latchkey_import_credential_value, latchkey_join_from_welcome,
  latchkey_prepare_c1_key_package, latchkey_process_inbound,
  latchkey_remove_member, latchkey_reveal_metadata, latchkey_reveal_value,
  latchkey_seal_value, mls_core_git_sha; imports env.latchkey_host_random
member wasm host RNG, sensitive request, and full-member lifecycle runtime ok:
  69 calls, 21000-byte prepare response
```

The host-RNG check runs the credential import through the compiled module: it
seals a native value with a fixed CEK and nonce under Node's `crypto`, imports
that CEK, opens the value through ordinary reveal, asserts the ciphertext is
returned unchanged, and rejects an invalid protocol version, a short CEK, a
truncated frame, and a tampered value tag.

`make fmt-check/mls` passes under the root (1.93.0) rustfmt.

### Artifact

A source-attributed test build from a clean tree at `8dd1e677`, produced by
replicating `build-member-wasm-release.sh` with only its `x86_64` and
clean-tree gates relaxed:

| Field | Value |
| --- | --- |
| wasm sha256 | `0bd48820742b520b4bebc743ce0d118a160439747ee774a66cf8a31865cb6dac` |
| wasm size | 3817178 bytes |
| manifest | `member-wasm-manifest.json`, source commit `8dd1e677…` (`mls_core_gitinfo`, provenance version 1) |
| new export | `latchkey_import_credential_value(i32, i32) -> i64` |
| abi | `latchkey-member` version 1 |
| fixtures | `c1_host_stateful_fixture.json`, `c1_host_stateful_join_request.pb` |

The artifact is **not** a canonical release bundle. `member-wasm.sha256` pins
`50213f624ac6ab237aae6ceef4c51b0a5e49ca4d9324d28d69091bdf303650e1` for x86_64
Linux, `check-member-wasm-artifact.sh` refuses to compare that pin on any other
host, and `build-member-wasm-release.sh` refuses to build on any host that is
not `x86_64`. So no canonical bundle, and therefore no embedded-release
verification, can be produced from this environment.

The canonical bundle is produced only by the `mls-wasm` job in `ci.yml`, which
runs on `ubuntu-latest` and uploads
`latchkey-member-wasm-<head sha>` (contents of `${RUNNER_TEMP}/latchkey-member-wasm`:
the normalized wasm, `member-wasm-manifest.json`, the gzipped transport copy and
the stateful fixtures) with 14-day retention. Its digest is the value that must
match `member-wasm.sha256`. The canonical run for this branch is CI run
`35759163562`; until that artifact exists, embedded-release verification is
blocked and this test build must not be used in its place.

## Interoperability evidence

The vector set is committed at
`pkg/crypto/providers/fullknowledge/testdata/direct-fk-vectors.json` and
mirrored byte-identically at
`crates/latchkey-mls-core/testdata/direct-fk-vectors.json`;
sha256 `963fb61af31cd0d3a4b0014c94bd45b8e1fd80b9f0db9f28c99c91bc9941d2ac`.
Every key in it is synthetic: a literal 32-byte seed, a literal 32-byte CEK,
a literal 12-byte nonce.

### Reproducing

```sh
# regenerate the Go half (preserves the Rust half already in the file)
cd baton-sdk && FK_INTEROP_WRITE=1 go test -run TestGenerateInteropVectors ./pkg/crypto/providers/fullknowledge/
# verify both halves
cd baton-sdk && go test -run TestInteropVectors ./pkg/crypto/providers/fullknowledge/
cd multipass && cargo test --manifest-path crates/latchkey-mls-core/Cargo.toml --locked -p latchkey-mls-core credential_import_matches_sdk_interop_vectors
```

The multipass reference for the native value suite is
`03c98d9f30b7c95095d41e5718c1d8f0583bb06d`. Between that revision and the
verified branch, `crates/latchkey-mls-core/src/value_crypto.rs` changed only by
addition — the diff removes no line — so the `seal_value` path the vectors
exercise is the reference path, not a rewritten one.

Fixed inputs: recipient seed `9d1f0f0e…`, CEK `0e2b7a5d…`, nonce
`a0a1a2a3a4a5a6a7a8a9aaab`, address `tenant-1` / `secret-1` / `version-1` /
`generic`, payload = the `SecretPayloadV2` wire bytes for
`envelope_version=2, content_type="generic", value="direct-fk-credential-value"`.

**Native value.** multipass `ItemKeyPair::seal_value_with_rng` with an injected
nonce reproduces Go `sealValue` byte-for-byte:
`06a0a1a2a3a4a5a6a7a8a9aaab5379f35552ec72909cbcf4f26eb0c51b60b12671d7a927b1a10f85facbc8e8f99e0a3d2e84618783e88ec00447b853cc563e4b674b9070`,
sha256 `8ce599f6986e5d15816d9c0221c997f4f02407808d50a0a4b85b9fc661c98b15`. The
Rust test also asserts the seal path draws the nonce exactly once, so a silent
extra entropy draw fails the test rather than passing on a lucky vector.

**Capsule framing.** The Rust test rebuilds the authenticated context and the
capsule plaintext field by field from the fixture's recorded config values —
the ASCII domain `c1/fk-credential-key-capsule/v1` with no NUL or length
prefix, `u32(1)`, the nine identifiers in profile order with `u32` byte-length
framing, `u32` capsule suite, the length-framed 1216-byte public key, `u32`
value suite, then the bare 32-byte value ciphertext digest — and both equal
the committed bytes. Context is 1406 bytes; capsule plaintext is
`u32(1) || field(context) || CEK[32]`.

**HPKE.** Compatible, established by byte-level agreement rather than by suite
names. `hpke-rs 0.7.0` `KemAlgorithm::XWingDraft06` (code point `0x647a`,
libcrux backend) with HKDF-SHA256 and ChaCha20-Poly1305 opens a capsule sealed
by the pinned `filippo.io/hpke` v0.4.0 `MLKEM768X25519`, with the profile
context supplied as **both** `info` and AAD and the recipient key given as the
raw 32-byte seed. The reverse direction is also verified: the Go test opens the
capsule hpke-rs sealed. No transcript or key-schedule difference was found.
hpke-rs 0.7.0 additionally ships `XWingDraft06Obsolete` at the obsolete code
point `0x004D`; it was not used.

**Tampering.** Encapsulation is ML-KEM-768 ciphertext (1088 bytes) || X25519
public key (32 bytes). Flipping one bit at offset 0 fails the open; flipping one
bit at offset 1088 fails the open. Tampering the capsule ciphertext fails, a
different `info` fails, and a different AAD fails. Applied to capsules from both
implementations.

**Model-B.** `import_credential_value` with the Go CEK plus a freshly generated
MEK and SecretKeyKey, then `open_model_b_value`, returns the exact Go payload,
and the returned value ciphertext is byte-identical to the input — import never
decrypts or reseals. Failing negatives: a different vault boundary, a different
item id, and a value sealed under a different CEK. Two imports of the same
input produce different envelopes, so each import mints fresh key material.

## Obligation status

Each row is the state of the *evidence*, not of the intent. Nothing in the C1
column was implemented during this run, and none of it is claimed.

| ID | Stage | Status | What is actually established |
| --- | --- | --- | --- |
| P1 | Profile | PARTIAL | Field order, widths and framing are pinned by a cross-language vector and by a Rust re-derivation from the recorded config; size bounds and rejection rules are covered only by the Go-side rejection table; both KEM components have independent tamper checks. No formal security review (the user waived it). |
| P2 | Runtime | PARTIAL | Native imported-CEK tests cover the normal read, wrong address, wrong key and the no-decrypt/no-reseal property; the compiled-WASM host-RNG test exercises the ABI path. Not covered: C1-side persistence and idempotency of the import. |
| P3 | SDK | PARTIAL | Config arm 102 exists, descriptors are regenerated and `buf`-clean, and the age/JWK encodings are unchanged (their suites pass). The "explicit capability" is the `full_knowledge_vault_profiles` descriptor field. No typed result type exists. |
| P4 | SDK | EVIDENCED | Complete configuration, supported profile, capability and single-config validation all run before `Issue`; every rejection case asserts the issuer never saw an input, so the provider call count is zero. |
| P5 | SDK | PARTIAL | Exactly one provider value succeeds; zero and multiple values return an error that does not carry the plaintext. "Issuance is not retried" is not exercised — there is no retry harness in this scope. |
| P6 | C1 prepare | NOT IMPLEMENTED | No C1 source is in scope in this run. |
| P7 | C1 import | NOT IMPLEMENTED | No C1 source is in scope in this run. |
| P8 | C1 commit | NOT IMPLEMENTED | No C1 source is in scope in this run. |
| P9 | C1 cleanup | NOT IMPLEMENTED | No C1 source is in scope in this run. |
| P10 | Integration | PARTIAL | Datadog advertises only the supported single-value profile, and Go-seal → Rust-open is proven through the ordinary Model-B read path at the crypto layer. End-to-end through C1 storage, and stored value bytes matching connector bytes in a live tenant, are not proven. |
| P11 | Rollout | NOT IMPLEMENTED | No paper regression, no old/new connector matrix, no retained-WASM remint check. The deployed-revision baseline is recorded read-only in the multipass feature notes, not here. |

No rejection instrument was validated with a planted bypass. Uncovered changed
branches were not dispositioned. Independent review has not happened.

## Not established

- Cross-language agreement is a crypto-layer result. It says nothing about C1
  issuance, persistence, publication, or admission.
- A successful Go self-round-trip and a successful Go/Rust vector are still not
  a cold-start end-to-end result.
- The Datadog connector advertises the profile; that is capability metadata, not
  a working delivery path.
- The deployment baseline in the multipass notes is a read-only record of which
  revisions serve traffic. It is not evidence that any deployment completed and
  not evidence of interoperability.
