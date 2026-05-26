# Storage Engine v4 — work tracker

Current implementation status. The canonical RFC text lives in
`follow-ups-rfc.md`. The original multi-stack plan that this file
previously described was collapsed into a single squashed PR
(`pquerna/storage-v4-combined` → PR #874). Work now lands as
follow-up commits S0–S5 layered on top of that PR.

## PR #874 baseline

- Branch: `pquerna/storage-v4-combined`
- PR: https://github.com/ConductorOne/baton-sdk/pull/874
- State: posted; CI green at the previous push.
- Carries: parent option plumbing, v3 protos, v3 envelope, Pebble
  engine (all six record types + v2↔v3 translation +
  `connectorstore.Writer` adapter), compaction
  (`IngestAndExcise`), equivalence runner (in-memory reference),
  microtests.

## Follow-up commits (S0–S5)

S0 (docs)    `follow-ups-rfc.md` + this tracker. **DONE.**
S1 (v3 container)   TAR + TAR_ZSTD payload encodings, `dotc1z.WithPayloadEncoding`. **DONE.** Pushed 2026-05-26 (commit `7c522722`).
S2 (write codec)    vtprotobuf MarshalVT on the write path (PutGrant, PutResource, PutEntitlement, PutAsset, etc.). **IN PROGRESS.** Cherry-pick of 4 autoresearch commits from `autoresearch/pebble-proto-codegen-20260525`.
S3 (write batches)  Cherry-pick autoresearch write-side perf wins, squashed; includes §3a.1 split-batches generalization (Grant ✓; Resource and Entitlement extension). **PENDING.**
S4 (read perf)      Cherry-pick autoresearch read-side wins (excluding the hand-rolled wire decoder), squashed. **PENDING.**
S5 (read codec)     UnmarshalVT + nested-arena decoder, behind a fallback if benchmarks show > 1.10× hand-rolled. **PENDING.** Gate: must equal-or-beat the hand-rolled decoder we are not landing.

## Open follow-ups (deferred to a later PR)

- v2-grants v1→v3 migration tool (`cmd/baton-c1z migrate`).
- SQLite-parity adapter for the equivalence runner.
- protoc-gen-batonstore codegen plugin (perf-only; ReflectCodec is the MVP).
- Stack-7 C1 integration (separate repo).
- Vector / full-text search; columnar projection.

## Material risks

- vtprotobuf MarshalVT changes the wire output deterministically;
  any field-ordering bug surfaces immediately in the equivalence
  runner.
- S5 decoder rollback path: if benchmarks regress vs hand-rolled,
  the fallback is the stock UnmarshalVT (still a win over reflect).
- Reserved wire numbers 1 and 2 in `PayloadEncoding` must never be
  reused.
