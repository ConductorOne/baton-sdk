# Ideas backlog — Pebble engine read perf

Free-form scratch. Append new ideas as bullets; mark tried ones with
status (kept / discarded / crashed) so we don't repeat them.

**Required reading from the prior session**:
`docs/rfcs/0004-storage-engine-v4/autoresearch-archive/writepack/autoresearch.ideas.md`
contains the do-not-retry catalogue from the WritePack session. Several
of those closed axes likely apply here too:

- Parallel-large-alloc across goroutines (heap arena serializes — verified 3×).
- Tournament tree / prefix-skip wrappers around `bytes.Compare`
  (Go SIMD'd `cmpbody` is faster than the wrapper for k=4 or short skip).
- Naive parallel-then-serial pipelines that don't actually overlap
  (e.g. read-all-then-write-all).
- Touching durability semantics for marginal gains.

## Read-path-specific ideas (priority order, profile-confirm before pursuing)

### P1 — likely big wins (untried, large surface)

- **`ExtractZstdTar` parallelism** — single-threaded zstd decode +
  tar walk + per-file `os.OpenFile` + `io.Copy` dominates per-iter
  cost at large scales. For 1 M-grant `.c1z` of ~500 MB the extraction
  is most of the wallclock. Possible:
  - `zstd.WithDecoderConcurrency(0)` (untried for reads; tested under
    writes #9/#35/#46 and was flat there because the OUTER zstd was
    already barely needed over pre-Snappy SST data — for READS we're
    decoding the OUTER zstd of fresh tar contents, different problem).
  - Parallel tar-entry writes to destination dir (workers consume from
    the tar stream's decoded byte ranges, each writes one file).
  - Skip zstd entirely via streaming decompression that avoids the
    intermediate file write — extract into memory + open Pebble against
    an in-memory FS. Pebble supports `vfs.MemFS` via the `FS` option.
- **In-memory Pebble FS for reads** — skip the extract-to-tmpdir step
  entirely. Decompress the c1z payload straight into a `vfs.MemFS` and
  point Pebble at it. Saves the entire tar-extraction wallclock AND
  the subsequent Pebble file-open syscalls (memory-backed FS is much
  faster). Big change but potentially huge win.

### P1 — likely big wins (untried, smaller surface)

- **`V3GrantToV2` arena** — analogous to `V2GrantToV3` from the
  WritePack session (#41 there). Each `ListGrants` page hydrates
  `len(page)` v2.Grants. For 100 pages × 10 k grants each = 1 M
  allocations of `v2.Grant + Entitlement_stub + Resource_stub + ResourceId`,
  which is several per grant. Arena to collapse them.
- **Pebble block cache warming** — the 256 MiB block cache starts cold
  on each `NewStore`. If the data fits in cache, fully warmed reads are
  much faster than cold reads. Warm via a deliberate prefetch read at
  Open time, or trade some setup cost for amortized win.

### P2 — moderate

- **Pagination cursor decoding** — `paginate.go` decodes the
  `PageToken` on every page boundary. 100 pages for 1 M grants. Cost
  per decode probably tiny but compounds.
- **`IterateGrantsBySync` allocations** — per-grant `proto.Unmarshal`
  into a fresh `v3.GrantRecord` for every iteration step. Could pool
  these (arena-style) but it's a streaming iterator API.
- **`pebble.IterOptions`** — currently we set `LowerBound`/`UpperBound`
  for the grant primary keyspace. Could enable `KeyTypePoint` only (no
  range keys for this iteration). Already implicit.

### P3 — speculative

- **Bloom filters on L0/L1 for reads** — discarded in the write
  session (#8) because fresh-sync writes have no Get hits. For reads,
  if we did point-Gets, blooms could help — but the read path is a
  range scan, not point Gets. Probably still useless. Skip.
- **`pebble.Options.MaxOpenFiles`** — currently 1024. At ~265 L0 SSTs
  for a 1M-grant sync this is fine. For larger syncs we might hit the
  limit. Not relevant at the bench's current scales.
- **Custom `IterOptions.LowerBound`/`UpperBound` for the by-principal
  index** — currently the primary key scan walks `v3|G|sync|...`. If
  reads used the by-principal index by default, performance might
  differ. Probably not — primary scan is the right answer for full
  enumeration. Skip.

## Tried — see jsonl for verdicts

### Kept

- **#51 outer-only grantReadArena** (-2.3% primary). Collapses the 1 M
  v3.GrantRecord outer allocations to one slice alloc per page. Small-
  scale regression (1k +14 %, 100 +20 %) is the known arena-over-allocation
  tradeoff. WritePack + SQLite sentinels flat.
- **#53 grantV2ReadArena** (-20.7 % primary). Arena-allocates the 6
  v2.Grant nested stubs (Grant + Entitlement + 2 Resources + 2 ResourceIds)
  in adapter.ListGrants. Pre-sized to len(records) so no waste at any scale
  for the arena itself (small-scale regression unchanged from #51, came from
  the OUTER GrantRecord arena, not this one). Allocs/op 17M→10M.

### Discarded

- **#50 grantReadArena with pre-populated nested fields** — proto.Unmarshal
  didn't reuse the pre-populated EntitlementRef/PrincipalRef/Timestamp
  pointers despite the consumeMessageInfo source-level read suggesting it
  should. Only the outer GrantRecord was reused (-1 alloc/grant), while the
  unused pre-populated arenas added bytes_op +15 % and regressed smaller
  scales +28-40 %. Probable causes recorded in the jsonl ASI.
- **#52 slab-style growable arena** — attempted to fix #51's small-scale
  regression by sizing arenas to actual records via doubling-slab strategy.
  Slab management overhead (per-call cap check + slice-header sync) cancelled
  the saved memclr at small scales. Fixed-size arena from #51 is the better
  tradeoff.
