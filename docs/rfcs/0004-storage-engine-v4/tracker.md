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
S5 (read arena)       UnmarshalVT + nested-message arena. **DEFERRED.** See "Deferred" below.

## Deferred (out of scope for this RFC, retained in the queue)

- **S5 (nested-arena decoder).** The arena trick (pre-allocated
  EntitlementRef + PrincipalRef slots) needs either a custom
  decoder wrapping protowire or generated code that vtprotobuf
  doesn't emit out of the box. S4's UnmarshalVT auto-detect already
  captures the larger of the two wins (2–3× over reflection). The
  remaining arena delta is incremental; best handled after we have
  a production benchmark (see `baton-demo`) confirming nested-alloc
  GC pressure is actually the next bottleneck.
- **S3 Tier-B/C.** The `split_batch.go` generic helper (§3a.1)
  and the autoresearch parallel-build/parallel-idx-sort cherry-picks
  (b1a1700c / 99c76cd2 / de099547 / a864d686 / 8525c149 / 4995f17e)
  depend on infrastructure that doesn't exist on the squashed PR —
  they're effectively new design work, not cherry-picks. Tracked
  separately; not blocking RFC closure.
- **§3b parallel read pool / split-tar parallel write.** Same
  argument as S5: the foundational UnmarshalVT win lands in S4;
  the worker-pool optimizations are incremental and need
  production-realistic benchmarks to justify the complexity.
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
