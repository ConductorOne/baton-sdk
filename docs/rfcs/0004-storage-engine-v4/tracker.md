# Storage Engine v4 — work tracker

This file tracks the implementation of RFC v4 (canonical version at `/pebble-baton-sdk/rfc-v4.md` in the squire planner). Updated by the implementer as work proceeds. Items marked `[TODO]` are open; `[DONE]` is complete; `[DEFERRED]` is intentionally not in scope for v4.

## Branch layout

- **parent**: `pquerna/storage-v4-parent` — SDK option plumbing, magic-byte dispatch in `ReadHeader`, build tag setup. No behavior change.
- **stack 1**: `pquerna/storage-v4-stack1-protos-codegen` — storage protos + `cmd/protoc-gen-batonstore/` plugin + generated codecs + tag-gated engine skeleton.
- **stack 2**: `pquerna/storage-v4-stack2-envelope` — v3 envelope format + manifest proto + descriptor closure-test fixture.
- **stack 3**: `pquerna/storage-v4-stack3-pebble-engine` — full Pebble engine implementing `connectorstore.Reader`/`Writer`/`C1ZStore`.
- **stack 4**: `pquerna/storage-v4-stack4-compaction` — Pebble cross-engine compaction via `IngestAndExcise`.
- **stack 5**: `pquerna/storage-v4-stack5-equiv-bench` — cross-engine equivalence suite + hyper-scale fixtures + bench harness.

Each child branch is rebased on the previous (linear stack), submitted as a stacked PR pointing at its parent.

## Status

| Stack | Status | Branch | LOC est | LOC actual |
|---|---|---|---|---|
| Parent | [DONE] | `pquerna/storage-v4-parent` | ~150 | ~250 |
| 1 (protos + codegen) | [DONE] (codec only; protoc-gen plugin DEFERRED — ReflectCodec covers MVP) | `pquerna/storage-v4-stack1-protos-codegen` | ~1500 | ~1300 |
| 2 (envelope) | [DONE] | `pquerna/storage-v4-stack2-envelope` | ~600 | ~700 |
| 3 (engine) | [DONE — all 6 record types + v2↔v3 translation + connectorstore.Writer adapter] | `pquerna/storage-v4-stack3-pebble-engine` | ~5000 | ~4000 |
| 4 (compaction) | [DONE] | `pquerna/storage-v4-stack4-compaction` | ~800 | ~716 |
| 5 (equiv + bench) | [DONE — MVP; SQLite-parity adapter deferred] | `pquerna/storage-v4-stack5-equiv-bench` | ~2500 | ~1700 |

Total target: ~10,550 LOC of new code + generated artifacts.

## Per-stack TODO

### Parent

- [TODO] Add `dotc1z.EngineV3` constant alongside `EngineSQLite`.
- [TODO] Add `WithEngine(name string)` option.
- [TODO] Add `C1Z3` magic header constant + `C1ZFormatV3`.
- [TODO] Update `ReadHeader` to recognize `C1Z3\x00`.
- [TODO] Build tag for the engine package: `//go:build batonsdkv2`.
- [TODO] `cmd/baton-buildtag-check/` — CI gate that builds a sample connector with default tags + greps for `cockroachdb/pebble` in the binary.

### Stack 1 — protos + codegen

- [TODO] `proto/c1/storage/v3/options.proto` — TableOption + IndexOption.
- [TODO] `proto/c1/storage/v3/records.proto` — all six record types + mirror types (GrantExpandableRecord, GrantSourceRecord, SyncType).
- [TODO] `proto/c1/storage/v3/refs.proto` — EntitlementRef, PrincipalRef, ResourceRef.
- [TODO] `buf.gen.yaml` entry for `protoc-gen-batonstore`.
- [TODO] `cmd/protoc-gen-batonstore/main.go` — `protogen.Run` shell.
- [TODO] `cmd/protoc-gen-batonstore/walker.go` — walks record-bearing messages, resolves field paths.
- [TODO] `cmd/protoc-gen-batonstore/codec_gen.go` — emits EncodeKey/Value, WriteIndexes/DeleteIndexes per record.
- [TODO] `cmd/protoc-gen-batonstore/register_gen.go` — emits register.gen.go.
- [TODO] `pkg/dotc1z/engine/pebble/codec/registry.go` — Codec interface, Lookup, Register.
- [TODO] `pkg/dotc1z/engine/pebble/codec/reflect.go` — `NewReflectCodec(MessageDescriptor)` + `*ReflectCodec`.
- [TODO] `pkg/dotc1z/engine/pebble/codec/tuple.go` — tuple-encoding helpers (string, bytes, int32, int64, bool).
- [TODO] `pkg/dotc1z/engine/pebble/codec/syncid.go` — `EncodeSyncID(string) ([]byte, error)` + `DecodeSyncID`.
- [TODO] `pkg/dotc1z/engine/pebble/codec/errors.go` — `ErrCodecTypeMismatch`.
- [TODO] `pkg/dotc1z/engine/pebble/gen/*.gen.go` — committed generated codecs (post-protogen).
- [TODO] `pkg/dotc1z/engine/pebble/engine_stub.go` — empty engine satisfying interface via panics, gated by `//go:build batonsdkv2`.
- [TODO] `Makefile` — `make protogen` runs `buf generate`; `make protogen/check` for CI drift.

### Stack 2 — envelope

- [TODO] `proto/c1/c1z/v3/manifest.proto` — `C1ZManifestV3`, `PayloadEncoding` enum, `RecordTypeInfo`, `SyncRunSummary`, `PebbleEngineConfig`.
- [TODO] `pkg/dotc1z/format/v3/manifest.go` — `ReadManifest`, `WriteManifest`, `BuildClosure(protoregistry.Files) *descriptorpb.FileDescriptorSet`.
- [TODO] `pkg/dotc1z/format/v3/envelope.go` — `WriteV3Envelope(w, manifest, payloadDir)`, `OpenV3Envelope(r) (*Manifest, payloadReader, error)`.
- [TODO] `pkg/dotc1z/format/v3/payload.go` — zstd-tar streaming.
- [TODO] `cmd/baton-descriptor-closure-test/main.go` — verifies its import graph excludes `c1/connector/v2`; opens fixture; round-trips a known grant via dynamicpb.
- [TODO] `pkg/dotc1z/format/v3/testdata/closure-fixture.c1z3` — committed.
- [TODO] `pkg/dotc1z/format/v3/envelope_test.go` — round-trip tests.

### Stack 3 — Pebble engine

- [TODO] `pkg/dotc1z/engine/pebble/engine.go` — `Engine` struct, lifecycle.
- [TODO] `pkg/dotc1z/engine/pebble/options.go` — `Preset` enum, `WithPreset`, `WithSharedCache`, `WithDurability`, `newEngineOptions`.
- [TODO] `pkg/dotc1z/engine/pebble/errors.go` — full sentinel set from Appendix E.
- [TODO] `pkg/dotc1z/engine/pebble/quiesce.go` — strict write-barrier + WaitGroup.
- [TODO] `pkg/dotc1z/engine/pebble/save.go` — `Save(ctx, dest)` using `db.Checkpoint`.
- [TODO] `pkg/dotc1z/engine/pebble/keys.go` — primary-key + index-key encoding for all six record types.
- [TODO] `pkg/dotc1z/engine/pebble/grants.go` — `PutGrants`, `PutGrantsIfNewer`, `DeleteGrant`, `ListGrants`, `ListGrantsForEntitlement`, `ListGrantsForPrincipal`, `ListGrantsForResourceType`, `ListWithAnnotationsForResourcePage`, fresh-sync fast path + MultiGet incremental path.
- [TODO] `pkg/dotc1z/engine/pebble/resources.go` — Resources, ResourceTypes.
- [TODO] `pkg/dotc1z/engine/pebble/entitlements.go`.
- [TODO] `pkg/dotc1z/engine/pebble/assets.go`.
- [TODO] `pkg/dotc1z/engine/pebble/sync_runs.go`.
- [TODO] `pkg/dotc1z/engine/pebble/sessions.go` — value-separation-aware.
- [TODO] `pkg/dotc1z/engine/pebble/stats.go` — append-only counter sidecar.
- [TODO] `pkg/dotc1z/engine/pebble/index_lifecycle.go` — online index build state machine.
- [TODO] `pkg/dotc1z/engine/pebble/event_listener.go` — Pebble EventListener wiring.
- [TODO] `pkg/dotc1z/engine/pebble/translate_v2.go` — `v2.Grant ↔ v3.GrantRecord` translation, stub-hydration on read.
- [TODO] `pkg/dotc1z/engine/pebble/engine_test.go` — basic open/write/read/save.

### Stack 4 — compaction

- [TODO] `pkg/synccompactor/pebble/compactor.go` — cross-file diff using `IngestAndExcise`.
- [TODO] `pkg/synccompactor/pebble/sst_writer.go` — emit sorted SSTs from merge stream.
- [TODO] `pkg/synccompactor/pebble/compactor_test.go` — small-scale roundtrip.

### Stack 5 — equivalence + benchmarks + fixtures

- [TODO] `pkg/dotc1z/engine/equivalence/runner.go` — runs the same workload through SQLite + Pebble engines and asserts byte-equivalent output for every reader method.
- [TODO] `cmd/baton-fixture-gen/main.go` — generates synthetic hyper-scale fixtures (1M users × 100M grants; global enterprise with cycles).
- [TODO] `cmd/baton-storage-bench/main.go` — benchmark harness targeting G1–G10 from RFC §6.
- [TODO] `pkg/dotc1z/engine/pebble/microtests/` — port the 5 micro-tests from `/tmp/baton-rfc-microtests/`.

## Deferred (not in v4 scope)

- [DEFERRED] `protoc-gen-batonstore` codegen plugin — perf optimization. `ReflectCodec` covers MVP; codegen lands as a follow-up once benchmark guidance prioritizes it.
- [DEFERRED] SQLite-parity adapter for the equivalence runner — the in-memory `MemoryRef` already proves Pebble engine correctness against a reference.
- [DEFERRED] gRPC exotic surfaces (`GrantManagerService`, `ResourceGetterService`, etc) — adapter embeds `UnimplementedXxxServer` stubs returning `codes.Unimplemented`; wire-up lands as needs arise.
- [DEFERRED] Stack 6 — deferred grant expansion (Appendix H research spike; separate RFC).
- [DEFERRED] Stack 7 — C1 integration (`/data/squire/src/c1` repo; separate PR series).
- [DEFERRED] `cmd/baton-c1z migrate` — out-of-band v1→v3 migration tool (NG10).
- [DEFERRED] Vector / full-text / geospatial search.
- [DEFERRED] Columnar / Parquet projection.

## Open questions to resolve before merge

(From RFC v4 §8; PR 3 benchmarks settle most.)

1. [TODO] Composite vs flatter form for `GrantsByEntitlement` — Stack 5 bench.
2. [TODO] Whether `GrantsByPrincipal` is a covering index — Stack 5 bench.
3. [TODO] Block cache hit rate on realistic workloads — Stack 5 bench.
4. [TODO] Whether to retain `WithMinCheckpointInterval` knob in v3 — decided by C1 ops post-rollout.
5. [TODO] Compression default at L6 (`zstd1` vs `zstd3`) — Stack 5 bench.
6. [TODO] When to bump pinned Pebble format — policy decision.

## Per-stack OODA loop status

Each stack iterates: research → plan → build → review → loop.

| Stack | Research | Plan | Build | Review | Status |
|---|---|---|---|---|---|
| Parent | DONE | DONE | IN PROGRESS | TODO | building |
| 1 | TODO | TODO | TODO | TODO | not started |
| 2 | TODO | TODO | TODO | TODO | not started |
| 3 | TODO | TODO | TODO | TODO | not started |
| 4 | DONE (IngestAndExcise micro-test) | TODO | TODO | TODO | not started |
| 5 | DONE (micro-tests prove pattern) | TODO | TODO | TODO | not started |

## Notes

- Material risks already tested ahead of time via micro-tests at `/tmp/baton-rfc-microtests/`: outer-compression ratio, Checkpoint roundtrip, tuple encoding prefix-free, codegen vs reflection perf, IngestAndExcise atomicity. Tests will move into `pkg/dotc1z/engine/pebble/microtests/` as part of Stack 3.
- The compression-bench WIP work (RFC 0003) is stashed on `main` as `rfc-0003-compression-bench-wip`; unstash to recover.
