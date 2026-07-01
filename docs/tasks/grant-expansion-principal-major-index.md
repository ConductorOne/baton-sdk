# Grant Expansion: Principal-Major Index Pass

Status: MEASURED — not adopted as default. Implemented + parity-gated behind
`BATON_EXPAND_BY_PRINCIPAL_PASS`; the deferred sort is faster on the whale (see
§7.1). The shipping win is skip-Get + deferred index build, not this pass.

Scope:
- `pkg/sync/expand` — a second, principal-major evaluation that emits the
  principal-keyed grant index families in sorted order.
- `pkg/dotc1z/engine/pebble` — the sorted-ingest sink for those families.

Baseline / oracle: the entitlement-major `RunTopologicalMergeProjection`
("Pass 1") and the deferred sorted-index build (`BuildDeferredGrantIndexes`).

## 1. Motivation

After deferring the scattered index families and skipping the read-before-write
Get, whale expansion is ~44% faster and the LSM stays healthy (read-amp 4 vs
25). The remaining build cost is an **external sort**: `BuildDeferredGrantIndexes`
scans `by_entitlement` (~12 GB), spills the two principal families, and k-way
merges them — ~1m40 of which ~45 GB is the spill round-trip (write runs + read
runs). That round-trip exists only to **transpose** entitlement-major data into
principal order.

The transpose is unavoidable *if we read Pass 1's output*. It becomes
unnecessary if we **produce the principal-keyed keys in principal order in the
first place** — i.e. evaluate the closure a second time, principal-major,
emitting `by_principal` / `by_principal_resource_type` already sorted. No spill,
no merge: emit → write SST → ingest.

This pass runs **in addition to** Pass 1, not instead of it. Pass 1 still
computes the expanded grants, their `Sources`, the primaries, `by_entitlement`,
and `by_entitlement_resource`. Pass 2 produces only the **`by_principal`** index
family (key-only) — see §2.1 for why `by_principal_resource_type` is NOT
principal-native and stays on the `by_entitlement`-derived build.

Trade, stated up front:
- **Win:** replaces ~45 GB of spill IO with an in-memory reachability recompute
  over the tiny entitlement graph plus a direct sorted SST write (~14 GB). Trades
  IO for CPU; favorable because the graph is small.
- **Cost:** a second evaluator that must produce a `(principal, destination)`
  set **byte-identical** to Pass 1's synthesized grants. Parity is the risk, and
  `shallow` edges are the sharp corner.

## 2. What Pass 2 must produce

For every grant Pass 1 **synthesizes**, `by_principal` and
`by_principal_resource_type` need one key each:

```
by_principal:               principal_rt | principal_id | external_id
by_principal_resource_type: principal_rt | external_id
external_id (synthesized) = destination_entitlement_id : principal_rt : principal_id
```

### 2.1 Only `by_principal` is principal-native (sort-order analysis)

The two families key the same synthesized grants but in **different sort
orders**, so the principal-major loop cannot emit both born-sorted:

- `by_principal` sorts by `(principal_rt, principal_id, external_id)`. Iterating
  principals in `(rt, id)` order and emitting destinations `D` in sorted order
  within each principal produces `external_id = D:rt:id` in ascending order —
  i.e. **exactly** `by_principal`'s key order. Born sorted by Pass 2. ✓
- `by_principal_resource_type` sorts by `(principal_rt, external_id)` =
  `(principal_rt, D, rt, id)` — **destination-entitlement-major within a
  resource type**, NOT principal-major. The principal loop emits `(rt, id, D)`,
  which is the wrong order. ✗

So Pass 2 owns only `by_principal` — and that is the win, because `by_principal`
is the scattered family that dominates the spill. `by_principal_resource_type`'s
per-`rt` order is `(D, id)`, which is exactly what a `by_entitlement` scan
already yields: build it by routing each `by_entitlement` entry into a per-`rt`
bucket (a small controlled vocabulary) and concatenating buckets in `rt` order —
**no large sort**. That is the cheap part of the existing deferred build and it
stays there.

A synthesized grant exists for `(D, P)` iff:
1. `P` **reaches** destination entitlement `D` through the expansion graph
   (respecting edge filters), and
2. `P` has **no base grant** on `D` (if it did, Pass 1 takes the base-update arm,
   the key is invariant, and the entry already exists from the connector sync).

So Pass 2 emits `by_principal(P, D:rt:res)` for each `(P, D)` with `D ∈
reach(P) \ base(P)`, iterating `P` in sorted principal order. Base grants'
principal-index entries already exist inline; Pass 2 only adds the synthesized
ones.

The `external_id` reconstruction couples Pass 2 to `grant.NewGrantID`
(`entitlement_id:rt:res`). This is the same coupling the "minimal-tuple spill"
needed, and it is clean precisely for synthesized grants (deterministic id).

## 3. Inputs / data structures

### 3.1 Base relation, transposed (small)

`base(P)` = the set of entitlements `P` is directly granted (its connector
grants), and the entry points for reachability. Built once, in principal order:

- Source: the pre-expansion grants. ~3.68M on the whale (vs 54M expanded), so a
  small sort or a scan of the base `by_principal`/`by_entitlement` index.
- We need `principal → {entitlement_id}` in **sorted principal order**. The base
  `by_entitlement` index gives `(entitlement_id → principal)`; transposing it is
  a small sort (3.68M, not 54M). Alternatively read base `by_principal` (already
  principal-sorted) and resolve each grant's entitlement.

This transpose is the only sort in Pass 2, and it is over the *input* (base),
which is fundamentally smaller than the *output* (expanded). That is the whole
point: sort the small thing, not the big thing.

### 3.2 Filtered descendant bitsets (precomputed, per resource type)

Reachability respects two edge filters from `Edge` (graph.go):
- `ResourceTypeIDs`: edge propagates `P` only if `P`'s principal type is in the
  set (or the set is empty).
- `IsShallow`: edge propagates only grants that are **direct on the source**.

Directness, from `isGrantDirectOnEntitlement`: `P` is direct on `S` iff `P` has
a **base** grant on `S` (a synthesized grant on `S` has `Sources = {parents}`,
never `S` itself, so it is indirect on `S`). Therefore a shallow edge `S→D`
fires for `P` only when `S ∈ base(P)`.

Consequence: directness is consumed after one hop. From a **direct entry** node
(∈ base), all out-edges fire (shallow + non-shallow). From an **indirectly
reached** node, only **non-shallow** out-edges fire. That is a static function
of `(source node, resource type)`:

```
nonShallowDesc_T(node)   = closure over T-active, non-shallow edges
descDirect_T(S)          = ⋃ over T-active out-neighbors X of S of
                              ({X} ∪ nonShallowDesc_T(X))
```

Precompute `descDirect_T(S)` as a bitset over nodes, for each resource type `T`
(a small controlled vocabulary) and each source node `S` (~1716). Cheap: a few
thousand small bitsets, built from the graph once.

### 3.3 Node vs entitlement granularity (parity-critical)

Edges are node→node; nodes hold multiple entitlement IDs (collapsed cycles).
Pass 1 (`GetExpandableDescendantEntitlements`) yields **every entitlement ID in
each reachable destination node**, and treats every source entitlement in a node
as an entry. Pass 2 must mirror this exactly:
- `base(P)` maps each base-granted entitlement to its node; `P` is direct on
  that entitlement (and thus that node is a direct entry).
- `reach(P)` is a set of nodes; the destination entitlements are the union of
  `EntitlementIDs` over reachable nodes.
- Collapsed-cycle members do **not** propagate to each other (internal edges are
  deleted in `FixCycles`); the precomputed descendant sets already exclude them.

## 4. Algorithm

```
precompute descDirect_T(S) bitsets for every (type T, source node S)
base = transpose(base grants) -> principal -> {entitlement_id}, sorted by principal

open by_principal SST writer
for each principal P in sorted order:           # from the base transpose
    T = P.resource_type
    entries = base[P]                            # entitlements P is direct on
    R = union over (e in entries) of descDirect_T(node(e))   # reachable nodes
    dests = union of EntitlementIDs over nodes in R          # destination entitlements
    for D in sorted(dests):
        if D in entries: continue                # base grant exists -> base-update, skip
        ext = D + ":" + P.rt + ":" + P.id
        write by_principal key (P.rt, P.id, ext)
close + ingest by_principal SST (disjoint from base entries by external_id; lands in L0)
```

Sorted output falls out of (a) iterating `P` in principal order and (b) emitting
`D` in sorted order within `P`: `by_principal` is keyed `(rt, id, ext)`, so the
outer principal loop then inner `ext` is its native order. Assert strict
ordering in the SST writer as a safety net (as the projection writer already
does). `by_principal_resource_type` is built separately from the `by_entitlement`
scan (§2.1), not here.

Memory: `O(graph bitsets + one principal's dest set + SST write buffer)`. No
spill. Bounded regardless of grant count.

## 5. Plug-in point

- Pass 1 runs as today and writes `by_entitlement`, `by_entitlement_resource`,
  primaries inline (defer mode keeps `by_entitlement` inline).
- Pass 2 owns **only `by_principal`**: it runs at EndSync (same hook as
  `BuildDeferredGrantIndexes`), gated by an env var initially, and ingests the
  one `by_principal` SST.
- `by_principal_resource_type` and `by_entitlement_resource` stay on the
  `by_entitlement`-derived deferred build (§2.1) — both are entitlement-native
  (per-`rt` order is `(D, id)`), so they need only bucket routing, no large sort.

### 5.1 Graph availability (the engine has no graph)

`BuildDeferredGrantIndexes` runs inside the engine and derives everything from
the `by_entitlement` keyspace, so it needs no graph. Pass 2 **does** need the
collapsed `EntitlementGraph` (reachability) and the base relation
(`principal → direct entitlements`), both of which live in `pkg/sync/expand`.
So Pass 2 cannot be a pure engine-side `EndSync` step like the deferred build.

The split:
- `pkg/sync/expand` drives Pass 2: it holds the graph, builds the base
  transpose, iterates principals in sorted order, and produces the sorted
  `(principalRT, principalID, externalID)` emission stream.
- `pkg/dotc1z/engine/pebble` exposes a sorted-ingest sink (an optional store
  interface) that accepts the pre-sorted `by_principal` keys and writes/ingests
  the SST. The expander calls it after Pass 1 completes, before/at EndSync.

The base relation must be captured from the **pre-expansion** grants (after
expansion, the principal/entitlement indexes also contain synthesized entries).
Cleanest source: capture `principal → direct entitlements` while the graph is
being built from base grants, or do a dedicated base-only scan (base grants lack
the expansion provenance that synthesized grants carry). This is open question
§8.3 and is the main remaining wiring decision.

## 6. Parity

Pass 2's emitted `(P, D)` set must equal Pass 1's synthesized set exactly. Gate
with the existing differential harness
(`topological_merge_differential_test.go`): run Pass 1, dump its `by_principal`
keyspace, run Pass 2, and assert byte-identical key sets across the corpus —
especially:
- shallow edges (the directness-consumed-after-one-hop semantics),
- resource-type filters,
- collapsed cycles / multi-entitlement nodes,
- principals with both direct and indirect paths to the same destination
  (base-update exclusion),
- diamonds / reconvergence (a destination reached by multiple entries — emitted
  once),
- nil/malformed principals (skipped identically).

No ship without a green parity gate, same bar as the streaming-merge work.

## 7. Tradeoffs vs the deferred sort

| | deferred sort (current) | principal-major Pass 2 |
|---|---|---|
| transpose | reads Pass 1 output (`by_entitlement`, 12GB) + spill/merge (~45GB IO) | recompute reachability in-memory; sort only the base (~3.68M) |
| later sort of 54M keys | yes (the spill+merge) | no (born sorted) |
| parity risk | none (reads truth) | second evaluator must match Pass 1 |
| hard corner | — | `shallow` path-directness |
| IO | ~45GB spill round-trip | ~14GB SST write only |
| reuses | existing spill infra | new evaluator + bitset precompute |

Choose Pass 2 when IO is the binding constraint and the parity harness is in
place; keep the deferred sort as the safe fallback (it cannot disagree with the
grants Pass 1 actually wrote).

### 7.1 Measured verdict (whale, 54M synthesized grants): deferred sort wins

Status: **MEASURED — do not adopt Pass 2 as default.** Parity held exactly
(`emitted == synthesized == 54,049,326`) across all configs, so the experiment is
sound; it just isn't faster.

End-to-end on the whale (`whale_rolled_back_960.pebble.c1z`), skip-Get on:

| index build for `by_principal` | cost |
|---|---|
| **Pass 2** (principal-major) | base scan **3.5s** + emit/ingest **50.4s** ≈ **54s** |
| **deferred sort** (incumbent) | shares the one `by_entitlement` scan (21.3s, amortized across families) + parallel k-way merge (~20s/family); the deferred build did **two** families in **43.1s total** |

Why Pass 2 lost, against its own §1 hypothesis:
- The transpose "sort" it set out to eliminate is **cheap** once the deferred
  build derives keys decode-free from `by_entitlement` and merges families in
  parallel (~43s for two families, scan amortized). The ~45GB spill round-trip
  the design feared is not the steady-state cost.
- Pass 2 adds costs the deferred sort does not: a **separate** pre-expansion base
  scan, a **reachability recompute** over ~214K principals, and a **standalone**
  full SST build + ingest that cannot amortize the `by_entitlement` scan the
  deferred build already pays for the other families.
- Net: Pass 2's ~54s for one family exceeds the deferred build's *marginal*
  ~15–20s to fold `by_principal` into its existing scan+merge.

The real expansion wins are orthogonal to Pass 2 and stack on the deferred sort:
skip-Get (`rbw-get` 2m24s → 8ms; 54M no-op Gets eliminated) + deferred indexing
(`idx-commit` 2m08s → 11.6s) took whale expansion **9m14s → 3m39s (2.5×)**.

Recommendation: ship **skip-Get (after the injective grant-id fix) + deferred
index build**; keep `by_principal` in the deferred build. Pass 2 stays behind
`BATON_EXPAND_BY_PRINCIPAL_PASS` as a tested, parity-gated option for a future
case where `by_principal` is the *only* deferred family (no scan to amortize) —
not the default.

## 8. Open questions

1. How many distinct principals receive expanded grants on the whale? (Bounds the
   per-principal loop count; the 54M emissions dominate either way.)
   **Answered:** ~214,196 principals with 1,309,067 direct source entries; base
   scan is 3.5s. The per-principal loop is cheap; emission/ingest dominates.
2. Does any production path rely on a grant id that is **not**
   `NewGrantID(principal, entitlement)`? If connectors emit custom synthesized
   ids, the `external_id` reconstruction breaks — Pass 2 assumes the
   deterministic form for synthesized grants (true today; the injective-id work
   would make it contractual).
3. Can the base transpose reuse the base `by_principal` index directly (already
   principal-sorted) by resolving entitlements, avoiding even the small base
   sort?
4. Is Pass 2's reachability recompute cheaper than the deferred sort's ~1m40 in
   wall time, not just IO? Needs a measured prototype behind the env var.
   **Answered: no.** The deferred build is now ~43s for two families (scan
   amortized, decode-free, parallel merge), and `by_principal`'s marginal add to
   it is ~15–20s — cheaper than Pass 2's standalone ~54s (base scan + recompute +
   un-amortized SST build/ingest). See §7.1. Deferred sort wins.
5. Does Pass 2 compose with a future injective-key change (escaped-tuple grant
   identity), which would also change `external_id` reconstruction?

## 9. Relationship to other expansion work

- Supersedes the spill sorter for the principal families if adopted (no sort).
- Orthogonal to skip-Get (write-path) and to the injective-id correctness fix,
  though injective ids make assumption (2) contractual.
- This is the "compute over a different aggregate, in addition" idea: Pass 1 is
  entitlement-major (for `by_entitlement` + primaries), Pass 2 is principal-major
  (for the principal families) — each family born in its native order.
