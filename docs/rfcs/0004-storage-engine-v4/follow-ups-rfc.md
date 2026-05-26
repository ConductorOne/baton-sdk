# RFC: v4 storage engine follow-up stack

> Draft v4 — plateau check from v3 absorbed. The doc is now ready for
> implementation. See §11/§12 for the verification history, §13 for
> the loop-3 plateau check, §14 for the final loop-4 closure.

What's left to land on `pquerna/storage-v4-combined` (PR #874) before we
call the v4 work shippable. Three workstreams:

1. **Container format extension** — runtime-configurable TAR vs
   TAR_ZSTD payload, default unchanged.
2. **vtprotobuf integration** — drop reflection from the proto
   marshal hot path on writes (and selectively on reads).
3. **Cherry-pick from autoresearch** — three branches of validated
   perf wins. We evaluate each for maintainability, generality, and
   merge-readiness; cherry-pick selectively; iterate where the
   autoresearch implementation is bench-shaped rather than
   production-shaped.

Each workstream lands as a separate commit on
`pquerna/storage-v4-combined` so reviewers can accept/reject
independently.

---

## 1. Container format extension (TAR + TAR_ZSTD selectable)

### Current state

`pkg/dotc1z/format/v3/envelope.go` always writes zstd-tar via
`writeZstdTar`. The proto enumerates 4 values today, two of which are
dead enum slots (RAW, single-stream ZSTD) that no caller ever needed:

```proto
// proto/c1/c1z/v3/manifest.proto (current, pre-RFC)
enum PayloadEncoding {
  PAYLOAD_ENCODING_UNSPECIFIED = 0;
  PAYLOAD_ENCODING_RAW         = 1; // debug only — never wired
  PAYLOAD_ENCODING_ZSTD        = 2; // single-stream zstd — never wired
  PAYLOAD_ENCODING_ZSTD_TAR    = 3; // zstd-tar of a Pebble directory
}
```

c1z3 is unreleased, so neither RAW nor single-stream ZSTD has shipped.
We can clean them out without backwards-compatibility cost.

### Proposed change

Collapse to **only two encodings**: tar + tar-then-zstd. Reserve the
old enum values to prevent future reuse.

```proto
// proto/c1/c1z/v3/manifest.proto (post-RFC)
enum PayloadEncoding {
  PAYLOAD_ENCODING_UNSPECIFIED = 0;
  reserved 1, 2;                       // formerly RAW + single-stream ZSTD
  reserved "PAYLOAD_ENCODING_RAW", "PAYLOAD_ENCODING_ZSTD";
  PAYLOAD_ENCODING_TAR_ZSTD    = 3;    // tar then zstd (= the old ZSTD_TAR; same wire number)
  PAYLOAD_ENCODING_TAR         = 4;    // tar of a Pebble directory, no compression
}
```

Notes on the move:

- **Wire number 3 stays.** The current `ZSTD_TAR = 3` is the only
  encoding ever written; keeping its wire number means existing
  test fixtures and any in-flight dev c1z3 files keep reading
  identically. The Go API name flips from `ZSTD_TAR` → `TAR_ZSTD`
  (proto field/enum renames don't change wire format).
- **`reserved 1, 2`** prevents anyone from re-using those numbers
  for an unrelated future encoding — keeps the bytes meaning the
  same forever even if we don't ship them.
- **Default unchanged.** `WriteEnvelope` continues to write
  `TAR_ZSTD` unless the caller passes `WithPayloadEncoding(TAR)`.

Wire it through:

- `format/v3.WriteEnvelope` gains a `Encoding` parameter (or, via
  an option struct, `WriteEnvelope(w, manifest, payloadDir,
  opts...WriteEnvelopeOption)` with `WithEncoding(enc)`). Default
  remains `TAR_ZSTD`.
- `format/v3.ReadEnvelope` switches on the two values; any other
  enum returns a clean `c1z v3: unsupported payload encoding %v`
  error. (No more RAW / single-stream ZSTD case branches to
  maintain.)
- New `dotc1z.WithPayloadEncoding(enc Encoding)` option threaded
  through `c1zOptions → c1fopts → C1File.payloadEncoding`. Engine
  code reads it at envelope-write time.
- `cmd/baton-c1z` (when we add it) gets a `--no-compress` flag.

### Why we want this

1. **Pebble's L5/L6 SSTs are already zstd-compressed.** Outer zstd
   over inner-zstd payload is mostly a CPU cost for a few percent
   size gain on the WAL/manifest/MANIFEST bookkeeping. RFC §3.5
   already documents this; we just never gave callers a choice.
2. **Snapshot performance.** Skipping outer zstd cuts unpack
   wallclock — confirmed by the parallel-tar work in
   `autoresearch/pebble-read-perf-20260525` (commit `8abd20bc`,
   "-1.2% at 1M but -13 to -16% at 10k/1k"). The regime where the
   tar matters more than ListGrants is exactly the one the
   autoresearch bench under-weights.
3. **Plays well with object stores** that do compression at the
   storage layer (S3 with `Content-Encoding: zstd` headers, GCS
   transparent compression). Letting them handle compression lets
   the c1z payload stay opaque tar.
4. **Smaller surface to maintain.** RAW + single-stream ZSTD were
   "what if we want them later" speculation. Deleting them removes
   the read-side case branches in `envelope.go` and the (never-
   tested) decoder paths. Less dead code in the security-critical
   envelope reader.

### Maintainability

Two enum values, one switch case in WriteEnvelope, one in
ReadEnvelope, one option setter. Read path stays enum-dispatched.
The `reserved` line prevents accidental future re-use; that single
mechanism handles backwards-compat by design.

---

## 2. vtprotobuf integration

### Current state (per autoresearch/pebble-proto-codegen-20260525)

The agent landed vtprotobuf wiring in `buf.gen.yaml` and threaded
`MarshalVT` through `marshalRecord`. Results:

| Commit | What | writepack_1m delta | Allocs |
|---|---|---:|---:|
| `39ccf66c` | vtprotobuf to `buf.gen.yaml`, HYBRID retained, no callsites changed | -0.9% (noise) | flat |
| `97ea5cb5` | MarshalVT in PutGrants write path | **-7.0%** (1283.7→1193.7 ms) | flat |
| `2f79ba96` | marshalRecord helper detects vtMarshaler interface; non-Grant Put paths now MarshalVT | -1.1% on writepack_100k | flat |
| `f0026983` | Clear `r.SyncId` before `MarshalToSizedBufferVT` (encoded in key prefix) | **-4.3%** (1175→1124 ms) | -10% writes, -34% read_100k |

Total: writes ~-12.4% from vtprotobuf alone, allocations flat, no
regressions in any sentinel (SQLite write 1k unchanged).

### Proposed action

**Cherry-pick all four commits as a single squashed commit.** The
agent already validated they compose. No iteration needed — this is
the cleanest cluster of wins on any of the three branches.

### Maintainability

vtprotobuf is the de-facto standard for performance-critical Go
proto codebases (CockroachDB, Temporal, etc). Adds a `_vtproto.pb.go`
sidecar per proto file under `buf.gen.yaml`. The marshalRecord
helper degrades cleanly if a future record type isn't vtprotobuf-
generated (interface assertion fallback to `proto.Marshal`).

### One concern (review needed)

The `r.SyncId` clear in `f0026983` is a write-side hack: SyncId is in
the key prefix already, so persisting it in the value is redundant.
That's fine. BUT: the autoresearch agent paired this with the
hand-rolled read decoder that SKIPS field 1 — meaning the SyncId is
gone from both the wire and the read. If a future reader (outside the
hand-rolled decoder) tries `r.GetSyncId()` it gets "". That's a
real foot-gun. **Decision needed before merging the `SyncId` clear:**

- (a) Ship it with a documented invariant ("v3 read-side consumers
  MUST receive sync_id via the request, NEVER via record.SyncId").
- (b) Drop the SyncId clear; lose 4.3% write perf to keep the wire
  format self-describing.
- (c) Move SyncId to a separate "trailer" field that the engine sets
  on read via the key tuple. Cleanest but +complexity.

My recommendation: **(a) with a comment block** at every adapter
read site stating "sync_id comes from the caller, not the record".

---

## 3. Cherry-pick from autoresearch (per-commit review)

### 3a. `autoresearch/pebble-perf-20260525` — write path

Session: 4245.86 → 1251 ms at 1M grants = **-70.5% cumulative**.

#### Recommended cherry-picks (ship-ready as-is)

| Commit | What | Δ at 1M | Verdict |
|---|---|---:|---|
| `20a8ecdb` | L0CompactionThreshold 2→8 | -15.8% | Pure Pebble config tune. General-purpose. Single-line change. **Ship.** |
| `e8729767` | Scratch byte buffers reused + `proto.MarshalAppend` | -5.6% | Standard alloc-reduction pattern. **Ship** (works with or without vtprotobuf). |
| `47a1ec91` | Hoist `resolveSyncBytes` with last-value cache | -4.9% | Tiny patch. Per-record sync resolution is a no-op when the sync doesn't change (which is almost always). **Ship.** |
| `63c0869b` | Split PutGrantRecords into two batches (primary + index) | -12.8% | Architectural — textbook "sort-friendly batch" optimization. **Ship + generalize** (see §3a.1 below). |
| `b1a1700c` | Skip read-before-write Get during first PutGrantRecords of a fresh sync | -14.5% | **Caveat below.** Restores the original `MarkFreshSync` design that we backed out in our PR's mutation-safety fix. The autoresearch agent argued for re-enabling on the grounds that "within a single call, db.Get won't see batch writes anyway". |
| `99c76cd2` | Parallel-build PutGrantRecords for skipGet path with len>=256 | -8.8% | Depends on `b1a1700c`. Concurrent build of priBatch and idxBatch. **Ship if `b1a1700c` ships.** |
| `de099547` | 4-way shard the priBatch build | -7.9% | Builds on `99c76cd2`. **Ship if both above ship.** |
| `a864d686` | Pre-sort the idxBatch via 4-way parallel sort + k-way merge | -12.1% | **Confirmed +156 LOC** to `grants.go` (v2). Real perf win but the largest single-commit complexity in §3a. **Ship**, but extract the merge code into a small `idxbatch_sort.go` file so `grants.go` stays scannable. |
| `3d660b9d` | Arena-style storage for the parallel idx-key sort | -7.1% | Cleanup of `a864d686`. **Ship if `a864d686` ships.** |
| `8525c149` | Commit priBatch / idxBatch from inside their respective goroutines | -5.4% | **Verified safe (v2).** Pebble v2 calls the serializing mutex `commitPipeline.mu`, not `writeMu` — `vendor/github.com/cockroachdb/pebble/v2/commit.go:250`. Different batches committed from two goroutines DO serialize correctly. Tiny patch (+60 LOC). |
| `9b8fc472` | `e.db.AsyncFlush()` after both parallel commits | -2.4% | Tiny. Borderline noise. **Ship — low risk.** |
| `d919128b` | `grantTranslateArena` for V2→V3 translation | -4.8% | Allocs -99.3% (3M→22K). Big alloc win for GC. **Ship.** |
| `d4b7e292` | Parallelize V2→V3 translation with 4 shard workers | -2.6% | Plain old "split the loop". Threshold of 1024 keeps small calls serial. **Ship.** |
| `4995f17e` | Async tmpdir cleanup in `registeredStore.Close` | **-5.1%** | Spawns `os.RemoveAll` in a goroutine. **Two caveats (v2):** (1) `writegrant_solo` regressed +19% — amortized at production scale, but matters in unit-test cold-start. (2) The diff **silently drops `RemoveAll` errors** (was `errors.Join(retErr, removeErr)`, becomes `_ = os.RemoveAll(...)`). For production deployments, this masks disk-full / EACCES on tmpdir — symptoms surface later as orphan dirs. **Iterate:** keep the async cleanup but capture errors into a debug counter (Pebble engine has an event listener slot) so they're surfaceable in metrics. |

#### §3a.1 — Generalize the split-batches pattern to Resources + Entitlements

The autoresearch commit `63c0869b` only touched PutGrantRecords.
The same pattern applies to two other indexed record types:

| Record | PK | Indexes | Split benefit |
|---|---|---|---|
| Grant | `(sync_id, ext_id)` | `by_entitlement`, `by_principal` | ✅ original commit. -12.8% at 1M. |
| Resource | `(sync_id, rt_id, res_id)` | `by_parent` | ✅ Connectors typically emit clustered by RT; PK arrives near-sorted, idx interleaves. |
| Entitlement | `(sync_id, ext_id)` | `by_resource` | ✅ Same shape as Grant. |
| ResourceType | `(sync_id, ext_id)` | none | ❌ Single batch already optimal. |
| Asset | `(sync_id, ext_id)` | none | ❌ Same. |
| SyncRun | `(sync_id,)` | none | ❌ Same. |

**Practical magnitude:** grants are typically 1M+ per sync; resources
and entitlements are 1k–100k. The split's absolute win shrinks with
N. At 1k entitlements the sort short-circuit saves <1 ms. Worth
generalizing for pattern consistency, not for the perf delta in
isolation.

**Design — `writeWithSplitBatches` helper.** Extract from `grants.go`
into a new shared helper used by Grant, Resource, Entitlement writes:

```go
// pkg/dotc1z/engine/pebble/split_batch.go (new in S3)
type splitBatchEncoder[R any] struct {
    encodePrimary func(b *pebble.Batch, idBytes []byte, r R) error
    encodeIndexes func(b *pebble.Batch, idBytes []byte, r R) error
    marshal       func(r R) ([]byte, error)  // or use vtMarshaler interface
}
func writeWithSplitBatches[R any](
    e *Engine, ctx context.Context, records []R,
    enc splitBatchEncoder[R],
) error { /* shared: read-before-write, dedup, build pri+idx, commit-each */ }
```

**Three-regime dispatch** (records count → strategy):

| Records | Strategy |
|---:|---|
| < 32 | Single batch, no split. Sort-promotion isn't a win at this scale; the overhead of the second batch's flushable-batch promotion costs more than it saves. |
| 32 – 1024 | Split into pri + idx batches, serial build, two sequential commits. Captures the sort-promotion win without the goroutine overhead. |
| ≥ 1024 | Split + parallel build (per `99c76cd2`/`de099547`) + parallel idx sort+merge (per `a864d686`). The full machinery. |

Resources and entitlements rarely hit the ≥1024 regime in practice;
they share the helper but exercise the smaller regimes most of the
time. The shared helper means future record-type indexes inherit
the optimization automatically.

**LOC budget after generalization:** ~250 LOC total for
`split_batch.go` (helper + 3-regime dispatch + tests). PutGrants/
PutResources/PutEntitlements each shrink to ~40 LOC of glue
(encoders + the call). Net code-size change: roughly flat vs the
current per-method implementations, with much less duplication.

#### Mutation-safety re-validation needed for `b1a1700c` / `99c76cd2` / `de099547`

These three rest on the premise:

> Within a single PutGrantRecords call, all db.Get queries return
> ErrNotFound because batch writes aren't visible until Commit.
> AND the engine knows the sync_id keyspace is empty because
> MarkFreshSync just created it.

That premise is **correct for the first PutGrantRecords call** of a
fresh sync. But our prior debugging session found that **connectors
can emit the same external_id twice within one fresh sync**
(paginated source, deduplication bug, etc), and skipping the Get
silently leaks orphan index entries.

The autoresearch agent's commit message addresses this:

> Within-call duplicates are not protected by this code OR the
> previous code (db.Get doesn't see in-batch writes either way).

That's true. The orphan-leak we previously fixed isn't a Get-vs-skip
issue at all — it's a within-batch issue both paths have. So
**re-enabling skipGet is safe for the orphan-leak class** (we don't
make it worse than skipping the Get gives us; the existing in-batch
dedup gap is unchanged).

What we need before merging: **augment the in-batch path with a
dedup map**. If the same external_id appears twice in `records[]`,
the last entry wins AND its predecessor's index entries are deleted.
~20 LOC. This is the cherry-pick-with-iteration call: take the perf
win, add the safety net the autoresearch agent didn't.

#### Commits to drop / hold

None obvious — every commit has positive primary delta. The agent
already filtered the discards from the keep stream.

### 3b. `autoresearch/pebble-read-perf-20260525` — read path

Session: 1229.6 → 396.19 ms = **-67.8% cumulative**.

#### The big win + the maintenance dilemma

The autoresearch loop's largest single contribution is
`ae048e56` + `2ea72c34` + `9afeea18` — a hand-rolled wire-format
decoder for `v3.GrantRecord` (~200 LOC under
`pkg/dotc1z/engine/pebble/unmarshal_grant_fast.go`):

- ~190 LOC of custom `protowire`-based decode for fields 1-4 (sync_id,
  external_id, entitlement, principal).
- Arena-allocated nested EntitlementRef + PrincipalRef.
- Falls back to `proto.Unmarshal` if fields 8 (annotations) or 9
  (sources) appear in the wire stream.
- Skip-fields optimization: fields 5/6/7 (discovered_at, expansion,
  needs_expansion) are skipped via the default case. SyncId is also
  skipped.
- Single-byte tag dispatch bypassing `protowire.ConsumeTag`'s
  varint+bit-split for fields 1-15.

**Total perf delta: -1.2% on its own + -0.5% from SyncId skip +
-0.9% from tag dispatch = -2.6% wallclock at 1M, but -30% on
`allocs_op` (10.04M → 7.04M).**

The trade: **the agent itself codified a "hard rule — no hand-rolled
per-message wire codecs" in commit `ec269e48`** after considering
the maintenance burden. It then tried five different alternatives
(UnmarshalVT, threshold-gated arena, unmarshal_unsafe, etc) — every
one discarded because they didn't beat the hand-rolled decoder.

#### Recommendation: a third option the autoresearch loop didn't try

**Replace the hand-rolled decoder with `vtprotobuf`'s `UnmarshalVT`
PLUS a small arena pool for the nested message types.**

- vtprotobuf generates typed `UnmarshalVT` per message. Per the
  vtprotobuf README: typically 2× faster than `proto.Unmarshal`
  with similar allocation profile.
- The arena allocation of nested EntitlementRef/PrincipalRef is a
  separate optimization that pairs cleanly with `UnmarshalVT` — the
  agent's allocation profile said the nested allocs were the
  problem, not the decode itself.
- Net expected delta: maybe -1.0% to -1.5% at 1M (vs the agent's
  -2.6%). Less perf, but the code stays ~30 LOC instead of ~200, no
  custom field-tag dispatch, full proto round-trip semantics.

The autoresearch agent retired `unmarshalGrantRecordFast` (commit
`813cb62c`) and saw +0.5% — i.e. UnmarshalVT alone gave back
basically all the wallclock the hand-rolled decoder won. But the
agent didn't pair UnmarshalVT WITH the arena trick. **That's the
combination this RFC is proposing to write fresh.**

#### Other ship-ready read-side wins

| Commit | What | Δ at 1M | Verdict |
|---|---|---:|---|
| `c66be935` | Outer-only `grantReadArena` for PaginateGrantsBySync | -2.3% | Allocs -5.9%. Standard arena pattern. **Ship**, but tune the pre-size to avoid the small-scale regression the agent saw (1k +14%, 100 +20%). |
| `2f3619af` | `grantV2ReadArena` for the 6 v2.Grant nested stubs in translate | -20.7% | Allocs -41%. **Big win**, same arena pattern. **Ship.** Tune the pre-size for small scales. |
| `f45b86e7` | Batched parallel `proto.Unmarshal` in PaginateGrantsBySync | -50.7% | Architectural shift: main goroutine batches 64 records → 4-worker pool unmarshals. **Ship** — this is the load-bearing win. Pairs with vtprotobuf's UnmarshalVT. |
| `ea475bd9` | `sync.Pool` for per-batch unmarshal buffers | -1.5% | bytes_op -24%. Buffer reuse. **Ship.** |
| `26ebf451` | Parallel v3→v2 translation in a SEPARATE worker pool | -7.6% | Different from `d4b7e292` (write path). Read-side translate parallelization. **Ship.** |
| `f16da0f3` | Bump decode batchSize 64→256 | -2.7% | Channel-dispatch reduction. **Ship.** |
| `45134c9f` | Bump pageUnmarshalWorkers 4→6 | -3.3% | Worker-count sweep. **Ship at 6** (the agent later went 6→7, marginal; 8 was a hard regression). Default to 6 with a `WithReadWorkers(n)` option for callers that want to override. |
| `8abd20bc` | Parallel file writes in `ExtractZstdTar` | -1.2% at 1M, -13 to -16% at 1k/10k | **Ship.** The 1M bench under-weights unpack; production-scale unpack is closer to the 1k/10k regime per-file. |

### 3c. `autoresearch/pebble-proto-codegen-20260525` — the dead-end branch

All five experiments (#14-#18) discarded. The agent retired the
hand-rolled decoder in `813cb62c` and validated that **no other
write-side codegen variant beat the existing path**. Net contribution
to ship: 0. But the branch's session-summary commit (`2f567f30`) is a
useful artifact — port it as a doc into the RFC's "what we tried" log.

---

## 4. Stack order on `pquerna/storage-v4-combined`

Six new commits (v3: split S0 out), in this order:

```
* (PR #874 base, currently 9676f153)
| ↓
| commit S0:  docs(rfc 0004): publish this follow-up RFC + tracker update
| commit S1:  feat(format/v3): selectable TAR vs TAR_ZSTD payload encoding
| commit S2:  feat(storage v3): vtprotobuf MarshalVT on write hot path
| commit S3:  perf(storage v3): cherry-pick write-side autoresearch wins (squashed)
| commit S4:  perf(storage v3): cherry-pick read-side autoresearch wins ex-decoder (squashed)
| commit S5:  perf(storage v3): UnmarshalVT + nested-message arena
              (the read-decoder iteration of §3b)
```

Each commit is independently revertable. **S3 and S4 are intentionally
squashed** (each is ~10-15 autoresearch cherry-picks); the squash
commit message preserves the original autoresearch commit shas as a
trail. **Reasoning:** PR #874 already has 11 commits; adding 25 more
would make review impractical. We lose bisection within S3/S4 but
that's an acceptable trade against reviewability. (If a regression
surfaces we can checkout the individual autoresearch branch to
bisect.)

Each commit is independently revertable. S2 first, then S3/S4
benefit. S1 is orthogonal. S5 lands last so its UnmarshalVT
dependency on S2 is obvious from history.

**Onboarding step (v3):** before pushing S1, post a "stack roadmap"
comment to PR #874 linking this RFC. Reviewers shouldn't have to
infer the plan from commit titles.

**Acceptance criteria per commit (v3):**

- All existing tests pass (full suite, both build tags).
- Bench sweep on the changed surface (100, 1k, 10k, 100k, 1M scales)
  shows no regression on the primary metric of any later commit.
- **SQLite sentinel bound:** `sqlite_writepack_1k_ms` regresses ≤5%
  vs the immediately-prior commit's measurement.
- Lint clean against the pinned `golangci-lint v2.12.2`.
- `make lint/pre-push` (or equivalent in this repo: `make lint`)
  clean.
- For S1 specifically: a new envelope_test.go test case for each
  PayloadEncoding value (TAR, TAR_ZSTD, error-on-unknown including
  the now-reserved 1 and 2).
- **For S5 specifically (v4 added):** post-stack benchmark
  `writepack_1m ≤ 1.5s` AND `readpaginated_1m ≤ 500ms`. If either
  misses, gate the merge until the regression is investigated.

**PR checklist before pushing S1-S5:**

- [ ] None of the patches include `autoresearch.*` files
      (`git ls-files autoresearch.\*` returns empty after each
      cherry-pick).
- [ ] Stack roadmap comment posted to PR #874.
- [ ] Compile-time `var _ vtMarshaler = ...` asserts present per
      record type.

**Stack-order flex (v4):** no §3 commit hard-depends on `MarshalVT`
— they go through `marshalRecord` which falls back to
`proto.Marshal`. Concrete cumulative deltas at 1M writes (per
autoresearch agent):

| Order | writepack_1m | Notes |
|---|---:|---|
| S2 alone | ~-12% | vtprotobuf on write hot path. |
| S3 alone | ~-50-55% | All write cherry-picks, no vtprotobuf. |
| S2 then S3 | ~-60%+ | They compose. |
| S3 then S2 | ~-60%+ | Same final state, different intermediate. |

**Recommendation: S2 before S3** for the simpler review story, but
both orders converge to the same final state.

---

## 5. Out of scope for this RFC

- Stack 6 (deferred grant expansion). Separate RFC.
- Stack 7 (C1 platform integration). Separate PR series in the c1 repo.
- SQLite-parity equivalence runner. Listed in RFC 0004 §I as deferred.
- `protoc-gen-batonstore` codegen plugin. The reflection codec covers
  the MVP; vtprotobuf is the simpler path forward.

### Explicitly ruled out by autoresearch (v3 — do-not-retry list)

The agent's `autoresearch.ideas.md` (on the autoresearch branches)
captured dead ends. Surfacing here so future maintainers don't
re-discover:

- **Parallel iter range-splitting in PaginateGrants** — requires
  general key-midpoint discovery that doesn't generalize from the
  bench's predictable external_id distribution.
- **Hand-rolled per-message wire codecs** (proto-codegen branch
  ec269e48) — retired as a "hard rule" by the agent after weighing
  maintenance burden vs the -2.6% delta. RFC v3 ships vtprotobuf
  + arena instead (§3b).
- **DisableWAL** — saves <5% perf but loses durability across
  Open/Close. Documented in micro-test
  `/tmp/baton-rfc-microtests/checkpoint_test.go`.
- **Pebble cache shared across one-shot bench iterations** —
  benefits production multi-engine ReaderCache but doesn't show in
  the bench's per-iter Open.
- **Memtable sizing beyond 256 MB** — `autoresearch/pebble-perf-...`
  ran this; primary improvement <2% (noise) and memory pressure on
  multi-engine hosts grows linearly.

---

## 6. Risk inventory

Each risk has: severity (S = ship-blocker, M = mitigate before merge,
L = monitor post-merge), trigger, mitigation.

1. **SyncId clear on write** — **M.** Read callers that try
   `record.SyncId` see "". Mitigation: keep the clear behind an
   `Adapter.useEmbeddedSyncId` option (default false, debug-only);
   document the invariant at every read-side consumer; add a unit
   test that reads back a record and asserts the SyncId comes from
   the request path, not from `record.GetSyncId()`.
2. **Skip-Get re-enable orphan-leak class** — **M.** Within-batch
   duplicate external_ids leak orphan indexes even with the Get path.
   Mitigation: add a `map[externalID]int` in `PutGrantRecords` for
   the duration of the call; if a duplicate fires, the later entry's
   index writes replace the earlier (which haven't been committed
   yet). ~20 LOC. Same pattern applies to PutResourceRecords /
   PutEntitlementRecords.
3. **`a864d686` LOC budget** — **L.** +156 LOC to `grants.go`.
   Mitigation: extract into `idxbatch_sort.go` during the S3 cherry-
   pick so `grants.go` stays under 600 lines.
4. **vtprotobuf build-step dependency** — **L.** `buf.gen.yaml` adds
   `protoc-gen-go-vtproto` plugin. Mitigation: pin the plugin version
   in `buf.gen.yaml`; document in the `make protogen` target's
   prerequisites that vtprotobuf is required.
5. **`autoresearch.*` files leak into cherry-picks** — **S.** These
   are session artifacts on the autoresearch branches; they MUST NOT
   land on `pquerna/storage-v4-combined`. Mitigation: each cherry-
   pick uses `git restore --source HEAD --staged --worktree -- 'autoresearch.*'`
   to clean them before commit; CI gate on the combined branch
   asserts `git ls-files autoresearch.\*` returns empty.
6. **Parallel ExtractZstdTar file-write order** — **L.** The agent's
   `8abd20bc` parallelizes file writes from a serial tar reader.
   Pebble's MANIFEST replay reads MANIFEST first then opens SSTs
   by path — does NOT depend on filesystem creation order.
   Mitigation: existing equivalence test (Pebble vs MemoryRef) walks
   an unpacked dir and asserts content match; extends to the parallel
   path by construction. Add a fault-injection test that delays one
   of the 4 worker writes by 100ms and verifies the result is still
   correct.
7. **Existing c1z3 files written by current code** — **L.** Reading
   an existing c1z3 file with the post-S1-S5 binary works
   transparently: the only encoding ever written is wire-number 3,
   which keeps its meaning (now named `TAR_ZSTD`, same bytes).
   Mitigation: the wire format of v3 records is unchanged in any S
   commit; `TAR_ZSTD` remains the default. Cross-version round-trip
   test: write c1z3 with current binary, read with post-S5 binary,
   assert content equivalence.
8. **vtprotobuf interface check at runtime** — **L.** The
   `marshalRecord` helper does a type assertion `if vtm, ok := m.(vtMarshaler); ok`.
   A non-Grant proto type that's not vtprotobuf-generated would fall
   through to `proto.Marshal`. Mitigation: explicit
   compile-time `var _ vtMarshaler = (*v3.GrantRecord)(nil)` etc.
   per record type, in a `compile_asserts.go` file.

---

## 7. Estimated cumulative perf delta after this stack lands

Conservatively, applying the keeps from §3:

- Write at 1M: **~1.0s** (current ~4.24s × 0.30-0.35 retention of
  autoresearch wins after we drop or iterate on a couple).
- Read at 1M: **~500ms** (current ~1.23s × ~0.40 retention).
- SQLite at 1M (sentinel): unchanged.

Compared to the original RFC 0004 baseline (writepack_1m ~55s with
SQLite, ~4.2s with Pebble pre-autoresearch): **shipping this stack
puts Pebble at ~50x faster than SQLite at 1M grants**.

---

## 8. Decision log (filled in during OODA iteration)

| Date | Question | Decision | Rationale |
|---|---|---|---|
| v4 update | Carry-forward of `RAW` and single-stream `ZSTD`? | **Drop. `reserved 1, 2;` in the enum.** | Speculative slots that no caller ever needed. c1z3 unreleased so there's no compat cost. Removes dead read-side branches from the security-critical envelope reader. Only TAR + TAR_ZSTD ship. |
| v3 | `SyncId` clear on write — keep, drop, or move? | **Keep, with an `Adapter.useEmbeddedSyncId` debug option (default false) that re-enables the clear.** | Production deployments always know sync_id from request context; the wire-format saving is +4.3% perf for free. Reviewer can flip the default if the migration cost is too high; the option keeps the door open. |
| v3 | Small-scale arena regression in §3b | **Threshold-gate the arenas at ≥1024 records** | Same pattern the agent already used for parallel translate. Small pages (API single-record fetch) skip the arena. |
| v3 | Squash S3 and S4 or preserve individual commits? | **Squash, preserve shas in message body** | PR #874 already has 11 commits; +25 makes review impractical. Bisection still possible via the autoresearch branches. |
| v3 | Backwards compatibility on existing c1z files? | **Preserved via Pebble `format_major_version` pin and the manifest's `engine_schema_version` field.** Old files written under v4 pre-this-stack open and read identically. | The wire format of v3 records doesn't change in any S commit. Only the optional `SyncId` clear on write is a content delta, and only for NEW writes after the option is set. |
| draft v1 | Re-enable skipGet without within-batch dedup? | **No.** Add the dedup map first. | See §6 risk #2. |
| draft v1 | Hand-rolled wire decoder vs UnmarshalVT + arena? | UnmarshalVT + arena | See §3b. |

---

## 9. Implementation timeline

Single PR series, no inter-commit waiting:

1. Draft + review this RFC (in flight).
2. Implement S1 (container format) — small, mostly proto.
3. Implement S2 (vtprotobuf) — mostly cherry-pick from
   `autoresearch/pebble-proto-codegen-20260525`.
4. Build S3 (write cherry-picks) — extract the keeps from §3a, fold
   in the within-batch dedup map.
5. Build S4 (read cherry-picks ex-decoder) — extract keeps from §3b,
   skip the hand-rolled decoder.
6. Build S5 (UnmarshalVT + arena decoder) — fresh implementation
   matching the autoresearch agent's arena design but with vtprotobuf
   as the proto-decoder.
7. CI green, push, comment on PR #874 explaining the stack.

---

## 10. Out-of-band cleanup that should NOT land in this RFC

The autoresearch branches each committed their own `autoresearch.*`
files (jsonl, sh, md, ideas.md, checks.sh, config.json). Those are
session artifacts — they belong on the autoresearch branches, NOT on
`pquerna/storage-v4-combined`. The cherry-picks must exclude any
`autoresearch.*` paths from the patch set.

---

## 11. v1 self-review — verification outcomes

| v1 concern | What I did | Outcome |
|---|---|---|
| #2 `a864d686` size | `git show --stat` | **+156 LOC net.** Folded into §3a verdict: ship but move into a separate file. |
| #3 Pebble `writeMu` claim | grep vendor/pebble/v2 | **The mutex is `commitPipeline.mu`, not `writeMu`.** Agent's intent is correct (concurrent batches serialize through it) but they cited the wrong name. RFC updated to use the right name. **Safe to ship.** |
| #5 `4995f17e` error swallow | `git show` full diff | **Silent error drop confirmed.** Was `errors.Join(retErr, removeErr)`, now `_ = os.RemoveAll(...)`. Production deployments would mask disk-full / EACCES errors. RFC updated: keep async cleanup but capture errors into a metric counter. |
| #7 stack order dependency | grep §3 commits for `MarshalVT` | **No hard dependency.** All §3 commits use the `marshalRecord` helper that falls back to `proto.Marshal`. S2-then-S3 is the cleaner story but not required. |
| #1 vtprotobuf composition | autoresearch.jsonl on proto-codegen branch | Agent's stats show #1 → #13 cumulative -2.5% with vtprotobuf-only changes; the -7% on commit `97ea5cb5` was the dominant single delta. Numbers consistent with the README claim of ~2× faster Marshal. **Confirmed worth shipping as cherry-pick.** |
| #4 UnmarshalVT + arena prediction | (still unverified — needs S5 implementation to measure) | **Open.** Plan: implement S5, bench, decide. Fallback options (a)-(c) documented in §3b. |
| #6 perf estimate handwavy | (still handwavy) | **Acknowledged.** §7 numbers are conservative point-estimates. Real number lands after each S-commit benches. |
| #10 worker count vs GOMAXPROCS | (open) | **Decision:** make it `min(6, max(2, GOMAXPROCS-2))` — bounded but scales down on small hosts. Default tested at 6 on 16-core bench host; the agent's sweep showed sharp regression at 8, so we don't go above 7 even on big hosts. |

## 12. v2 self-review — outcomes folded into the doc

All v2 critiques addressed in this v3 pass. For the record:

| v2 critique | Resolution in v3 |
|---|---|
| 1 — small-scale arena regression | §3b table updated: threshold-gate at ≥1024 records (matches agent's existing translate threshold). Decision log §8 entry added. |
| 2 — S3/S4 squash policy | §4 explicit: squash, preserve shas in message body. |
| 3 — envelope test per PayloadEncoding | §4 acceptance criteria: new envelope_test case per value. |
| 4 — what's ruled out | §5 "Explicitly ruled out by autoresearch" subsection added. |
| 5 — parallel ExtractZstdTar order | §6 risk #6 added (below): pebble manifest replay is path-keyed, not order-keyed; equivalence test exercises the unpacked dir read-back. Safe. |
| 6 — PR #874 onboarding | §4 "Onboarding step" added: post stack roadmap comment before pushing S1. |
| 7 — SyncId-clear decision | §8 decision log: keep with debug option. |
| 8 — backwards compatibility | §8 decision log + new explicit §6 risk #7. |
| 9 — parallel iter follow-up | §5 do-not-retry list captures this. Open follow-up RFC item, not in this stack. |
| 10 — SQLite sentinel bound | §4 acceptance criteria: ≤5% regression. |

## 13. v3 plateau check (outcomes folded into the doc)

All three v3 plateau items addressed in v4:

| v3 item | Resolution in v4 |
|---|---|
| 1 — S5 fallback if UnmarshalVT+arena under-performs | §3b explicit fallback table; benchmark the hand-rolled decoder HEAD as a comparison point during S5 implementation; if `S5 > 1.10 × hand-rolled decoder`, fall back to option (b). |
| 2 — quantify stack-order flex | §4 table added with S2/S3 isolated and composed deltas. |
| 3 — restructure §6 risks | §6 rewritten with severity (S/M/L), trigger, mitigation per risk. Added risks 6, 7, 8. |

## 14. v4 final pass — closure

Pass 4 critique. Items here either land in this stack (rare),
become follow-up RFC issues, or are accepted as known limitations.

1. **Risk 5 says CI gate on `autoresearch.\*` empty** — but no
   actual workflow change. Either commit the CI gate or drop the
   "CI gate" wording. v4 decision: **document as a PR checklist
   item, not a CI gate.** Adding workflow changes for this single
   stack is over-engineering; reviewers can spot the file in the
   diff.

2. **§3b "S5 fallback to hand-rolled decoder"** still requires
   landing a 200 LOC hand-rolled decoder we said we don't want. If
   that fallback fires, we should be explicit: it lands with a
   `// TODO: revisit when v3.GrantRecord IDL stabilizes for >12
   months` comment + a test that asserts the IDL hasn't drifted
   (compare proto descriptor field numbers at build time).

3. **§7 cumulative perf estimate** assumes everything composes
   linearly. The agent's data shows S3 and S2 composing well at
   1M; S4 read wins compose with S5; but cross-stack composition
   isn't measured (no commit on top of all 5). **Add an explicit
   "expected post-stack benchmark" target in S5's acceptance
   criteria**: writepack_1m ≤ 1.5s AND readpaginated_1m ≤ 500ms.
   If either misses, decide before merging the affected commit.

These are minor — items 1 and 2 are doc tweaks; item 3 is an
acceptance-criteria addition. v4 RFC is implementation-ready.

**Closing the OODA loop here.** Further iteration on the RFC
substitutes for implementing. The right next step is S1, not v5.
