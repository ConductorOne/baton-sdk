# Storage Engine v4 — work tracker

Current implementation status. The canonical RFC text lives in
`follow-ups-rfc.md`. The original multi-stack plan that this file
previously described was collapsed into a single squashed PR
(`pquerna/storage-v4-combined` → PR #874). Work now lands as
follow-up commits S0–S5 layered on top of that PR.

## PR #874 baseline

- Branch: `pquerna/storage-v4-combined`
- PR: https://github.com/ConductorOne/baton-sdk/pull/874
- Carries: parent option plumbing, v3 protos, v3 envelope, Pebble
  engine (all six record types + v2↔v3 translation +
  `connectorstore.Writer` adapter), compaction
  (`IngestAndExcise`), equivalence runner (in-memory reference),
  microtests.

## Follow-up commits (S0–S5)

S0 (docs)     `follow-ups-rfc.md` + this tracker. **DONE.**
S1 (v3 container)     TAR + TAR_ZSTD payload encodings, `dotc1z.WithPayloadEncoding`. **DONE.** commit `7c522722`.
S2 (write codec)      vtprotobuf MarshalVT auto-detect on the write path (`marshalRecord` branches on `vtMarshaler`). **DONE.** commit `a832d6af`.
S3 (write perf)       Tier-A cherry-picks: L0CompactionThreshold 2→8, resolveSyncBytes hoist, grantTranslateArena, parallel V2→V3 translation. **DONE.** commit `77ca3360`.
S4 (read perf)        UnmarshalVT auto-detect on read (`unmarshalRecord`) + parallel `ExtractZstdTar`. **DONE.** commit `5c1deb44`.
S5 (read arena)       UnmarshalVT + nested-message arena. **DONE.** commit `aad1c718`.

## Round 2 follow-ups (deferred items recovered)

S3 Tier-B/C (split + dedup + skipGet)  **DONE** for Grants, Resources, Entitlements. commits `deab18c1` + `44f877a7`.
baton-demo realistic Sync() bench       **DONE** as `BenchmarkSyncShape_BatonDemo` in pkg/dotc1z/engine/pebble/. Approximates baton-demo's 5k user / 200 group / 25 role / 100k grant data shape. Runs against both engines: SQLite ~3100 ms/100k-grants vs Pebble ~310 ms — Pebble ≈10× faster, c1z output ≈40% smaller. Selectable via SYNC_BENCH_ENGINE env var (sqlite | pebble | both).

## Deferred (out of scope for this RFC, retained in the queue)

- **Pebble C1ZStore adapter.** The Pebble engine currently exposes
  only `connectorstore.Writer`. The full `dotc1z.C1ZStore`
  interface (Grants() sub-store, SyncMeta(), FileOps()) is still
  SQLite-only. Plumbing Pebble through the canonical
  `pkg/sync.NewSyncer` path needs a thin adapter that satisfies
  these sub-store interfaces. Until that lands, the
  `BenchmarkSyncShape_BatonDemo` bench drives the engine's writer
  surface directly (which is the same per-call shape `pkg/sync`
  would produce) rather than running through `pkg/sync` itself.
- **Autoresearch parallel-build / parallel-idx-sort cherry-picks**
  (99c76cd2 / de099547 / a864d686 / 3d660b9d / 8525c149 / 9b8fc472
  / 4995f17e). The simpler split-batch + skipGet + dedup pieces
  landed (S3 Tier-B/C); the additional parallel-build + 4-way
  parallel idx-sort + k-way merge layers are ≥1024-record-only
  perf optimizations whose ROI depends on connector page sizes
  we don't yet have production data for. Re-evaluate after we
  have C1 ops running on Pebble.
- **§3b parallel read pool.** Same argument: the foundational
  UnmarshalVT + arena lands in S4 + S5; an additional decode
  worker pool is incremental and worth measuring before adding.
- **`cmd/baton-c1z migrate`** — out-of-band v1→v3 migration tool.
- **SQLite-parity adapter** for the equivalence runner.
- **`protoc-gen-batonstore`** codegen plugin (ReflectCodec covers
  the MVP).
- **Stack-7 C1 integration** (separate repo).
- **Vector / full-text / geospatial search**, **columnar /
  Parquet projection**.

## Open follow-up benches

- **baton-demo realistic Sync() bench** — clone
  `git@github.com:ConductorOne/baton-demo.git` and build a `make`
  target that runs the full Sync() stack against SQLite vs Pebble
  on the same dataset. The current `pebble_writepack_*` benches
  drive PutGrants directly; a Sync()-shaped bench exercises the
  adapter, sync state machine, paged iteration, and gives us a
  production-realistic comparison number.

## Material risks

- MarshalVT/UnmarshalVT must produce byte-identical output to
  proto.Marshal / Unmarshal. The equivalence runner exercises this
  on every commit; any field-ordering or default-value bug surfaces
  immediately.
- Reserved wire numbers 1 and 2 in `PayloadEncoding` must never be
  reused (verified by `TestWriteEnvelopeRejectsReservedEncoding` /
  `TestReadEnvelopeRejectsReservedEncoding`).
- The parallel ExtractZstdTar buffers full file contents in memory
  before dispatching to writers; if Pebble's max-entry size ever
  exceeds `maxTarEntryBytes`, the read path will error out cleanly
  rather than OOM.
