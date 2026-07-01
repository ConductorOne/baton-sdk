# Grant Expansion: Streaming Topological Merge

Status: proposed / design

Scope:
- `pkg/sync/expand` for the evaluation algorithm.
- `pkg/dotc1z/engine/pebble` for any optimized stream/write primitives.

Baseline / oracle: the current `Expander` in `pkg/sync/expand/expander.go`.

This document is intentionally conservative. It separates:

1. semantics that are already true in the current code,
2. a first implementation that can be validated against the current store APIs,
3. later Pebble-specific optimizations that need their own measurements and design gates.

## 1. Problem

Grant expansion computes a transitive closure over the entitlement graph. For an
edge `S -> D`, grants on source entitlement `S` may create or update grants on
destination entitlement `D`, subject to edge-local filters:

- `ResourceTypeIDs` restricts which source principals propagate.
- `Shallow` propagates only grants that are direct on `S`.
- `Sources` records provenance: which source entitlement contributed, and
  whether that contribution was direct.

The current implementation is source-driven:

1. choose currently-expandable source entitlements,
2. read one page of source grants,
3. fan that page to a batch of destination entitlements,
4. for each destination, determine whether each principal already has a grant,
5. write created/updated destination grants,
6. repeat until all edges are expanded.

That shape is good for high fan-out from one source because a source page is
read once and applied to multiple destinations. It is poor for high fan-in to
one large destination because the same destination can be probed and rewritten
once per incoming source.

The target algorithm evaluates each destination from all finalized parents in
principal order. It should:

- remove expansion-time per-principal descendant probes,
- reduce each destination once after all parents are finalized,
- keep memory bounded by merge frontiers and pages, not by grant-set size,
- preserve byte-level output semantics of the current expander.

Important boundary: with the current Pebble APIs, `by_entitlement` scans
materialize full grant records by doing a primary-key `Get` for each index row
(`PaginateGrantsByEntitlement`, `pkg/dotc1z/engine/pebble/paginate.go`). The
first implementation can still validate the algorithm and remove the current
destination existence probes, but it does **not** by itself eliminate every
Pebble point `Get`. Eliminating those remaining materialization Gets requires a
separate engine-level stream/index design (Section 10).

## 2. Code-derived invariants

These are not assumptions; they are read from the current code.

### 2.1 Graph collapse

- Cycles are collapsed by `FixCycles` / `fixCycle`.
- A collapsed `Node` contains multiple entitlement IDs.
- Edges are node-to-node, not entitlement-to-entitlement.
- When a cyclic component is collapsed, internal edges are deleted; no self-edge
  is added. Members of the same collapsed node therefore do **not** propagate to
  each other.
- `fixCycle` unions `ResourceTypeIDs` across collapsed incoming/outgoing edges
  and sets the re-created edges' `IsShallow` to `false`. The new algorithm must
  consume the post-collapse graph exactly as it exists, not reinterpret the
  pre-collapse edges.

### 2.2 Node/entitlement granularity

The graph schedules at node granularity, but grants are per entitlement.

For an edge `Ns -> Nd`, current expansion effectively considers every pair:

```
source entitlement s in Ns.EntitlementIDs
destination entitlement d in Nd.EntitlementIDs
```

This follows from:

- `GetExpandableEntitlements`: yields every entitlement ID inside an expandable
  source node.
- `GetExpandableDescendantEntitlements`: for a source entitlement, finds its
  source node, then yields every entitlement ID inside each destination node.

The new evaluator must preserve that pairwise member behavior.

### 2.3 Current missing-data behavior

`runAction` handles missing source/destination entitlements at execution time:

- missing source entitlement: delete/drop every destination edge in that action
  and finish the action;
- missing destination entitlement: delete/drop only that destination edge and
  continue the rest of the batch.

The new planner should pre-resolve source and destination entitlements for the
current graph and apply the same edge-pruning semantics before evaluation. Any
runtime `NotFound` discovered later is a correctness bug or a concurrent data
change and should fail closed rather than silently producing partial output.

### 2.4 Malformed grants

Current `runAction` skips source grants with a nil principal. The new stream
operators must do the same. A nil-principal grant must not contribute an empty
principal key.

## 3. Correctness model: provenance as a join-semilattice

This section is the correctness backbone. It is deliberately narrower than a
general Datalog/provenance semiring because the current expander records only
the immediate source entitlement for each step.

### 3.1 Contribution value

For a destination entitlement `D` and principal `P`, contributions are combined
into:

```
ContributionSet = map[sourceEntitlementID]isDirect
```

This mirrors `v2.GrantSources.Sources`.

### 3.2 Combine operation

Combining two contribution sets:

```
(A unionDirect B)[source] = A[source] OR B[source]
```

for every source in `keys(A) ∪ keys(B)`, with missing values treated as false.

Properties:

- commutative,
- associative,
- idempotent.

Those properties mean the final contribution set for one `(D, P)` cell is
independent of grouping and ordering. That is the only algebraic guarantee the
new algorithm needs. It does **not** excuse changing which contributions are
generated; topological scheduling and edge filters must still produce the same
contribution set as the current expander.

### 3.3 Edge transform

For source entitlement `S`, destination entitlement `D`, edge `e`, and source
grant `g`:

```
directOnS(g) := len(g.Sources) == 0 || g.Sources[S] != nil

contribution(e, S, g) =
    none                   if g.Principal == nil
    none                   if e.ResourceTypeIDs is set and principal RT not in it
    none                   if e.IsShallow and !directOnS(g)
    { S: directOnS(g) }    otherwise
```

This matches `runAction`:

- `isSourceDirect := len(sgSources) == 0 || sgSources[action.SourceEntitlementID] != nil`
- shallow uses the same direct-on-source predicate.

Only the immediate source entitlement `S` is recorded. Existing source grant
provenance is not copied into the child. Transitive propagation occurs because
the child grant, after finalization, becomes a source grant for its own outgoing
edges.

## 4. Destination merge semantics

This is the most important section. All implementation strategies must call one
shared merge routine with this behavior.

For one destination entitlement `D`, process input in principal order. For each
principal `P`:

- `base` = existing grants already on `D` for `P`.
- `C` = combined contribution set for `P` from all finalized parents.

Rules:

1. If `base` is empty and `C` is non-empty:
   - synthesize one expanded grant:
     - `Id = D + ":" + P.ResourceType + ":" + P.Resource`,
     - `Entitlement = D`,
     - `Principal = deterministic principal resource for P`,
     - `Sources = C`,
     - `Annotations = [GrantImmutable]`.
2. If `base` is non-empty and `C` is non-empty:
   - update every base grant for `P`;
   - keep each base grant's external ID and payload;
   - if the base grant's `Sources` map is empty, first add `{D: true}`;
   - then union in `C`, with direct winning over indirect;
   - emit only changed grants.
3. If `C` is empty:
   - emit nothing.
   - Existing base grants are already persisted and must not be rewritten.

These rules are transcribed from `applyDestGrant` and `newExpandedGrant`. Two
details are load-bearing:

- The `{D: true}` self-source is added only when a contribution lands on an
  existing sourceless base grant. Untouched base grants remain unchanged.
- Existing base grants with no contribution are not passed through the write
  sink. Rewriting them would diverge from current behavior and add Pebble churn.

### 4.1 Deterministic principal payload

For a synthesized grant, the principal identity is the principal key
`(resource_type, resource)`. The full `v2.Resource` payload is taken from a
contributing source grant. If multiple source grants for the same principal key
carry different resource payloads, current behavior is order-dependent through
source/action iteration. The new evaluator must define a deterministic tie-break
and the differential harness must detect whether this diverges from existing
fixtures.

Preferred tie-break for implementation:

1. source node topological order,
2. source entitlement ID lexical order within that node,
3. by-entitlement index order within that source.

If parity tests reveal that current output depends on a different ordering, the
tie-break must be adjusted or the semantic difference must be explicitly
accepted.

## 5. Stream primitives

### 5.1 Logical streams

Logical stream key:

```
PrincipalKey = (resource_type, resource)
```

Logical stream interface:

```go
type PrincipalKey struct {
    ResourceType string
    Resource     string
}

type PrincipalGroup struct {
    Principal PrincipalKey
    Grants    []*v2.Grant // all records for this entitlement+principal
}

type PrincipalGrantStream interface {
    Next(ctx context.Context) (PrincipalGroup, bool, error)
    Close() error
}
```

Streams:

- `BaseStream(D)`: existing grants for destination entitlement `D`, grouped by
  principal.
- `FinalizedStream(S)`: finalized grants for source entitlement `S`, grouped by
  principal.
- `EdgeContributionStream(S -> D)`: `FinalizedStream(S)` filtered by the edge
  transform in Section 3.3, yielding contribution sets instead of grants.

### 5.2 First implementation over existing APIs

The first implementation should use existing store APIs for parity, not maximal
performance:

- `ListGrantsForEntitlement` / `PaginateGrantsByEntitlement` provide sorted
  by-entitlement pages.
- The engine currently materializes each row with a primary `Get`.
- This means Phase 1 validates the merge algorithm and removes current
  destination existence probes, but it does not yet fully exploit Pebble.

This is intentional. A correctness-first implementation keeps the blast radius
small and gives a reliable oracle before introducing lower-level Pebble
streaming changes.

### 5.3 Engine-level stream needed for full benefit

To realize the "range scans, no random materialization probes" goal, add a
Pebble-specific expansion stream after semantic parity is proven.

The current permanent `by_entitlement` index stores the sort key but not the
full grant payload. A full stream therefore still needs the primary record for:

- `Sources` (for shallow/direct behavior),
- principal resource payload for synthesized grants,
- existing grant external ID and payload for dirty base grants.

Options for an optimized stream must be evaluated explicitly:

1. add a projection-bearing temporary expansion keyspace keyed by
   `(entitlement, principal_rt, principal_id, external_id)` with the fields the
   evaluator needs as the value;
2. widen a permanent index value (schema/format impact; not part of Phase 1);
3. accept primary materialization Gets but rely on sorted access and fewer
   destination probes for the first measured version.

Do not claim "no point Gets" until one of these is implemented and profiled.

## 6. Topological evaluation

Build a post-collapse topological plan over graph nodes.

For each node `N` in topological order:

1. all parent nodes are finalized;
2. for each destination entitlement `D` in `N.EntitlementIDs`, build a merge over:
   - `BaseStream(D)`, and
   - every incoming source member stream:

```
for each incoming edge parent node Pn -> N:
    for each source entitlement S in Pn.EntitlementIDs:
        add EdgeContributionStream(S -> D)
```

3. run Section 4 merge;
4. write dirty grants for `D`;
5. mark `D` (and eventually `N`) finalized for downstream reads.

This differs from the current step loop but preserves the key scheduling
property: a source entitlement is only used after every ancestor of its node has
been expanded/finalized.

### 6.1 Depth vs. topological index

Depth is useful for batching and diagnostics, but correctness only requires a
topological order. If levels are used, use longest-path depth so a node is not
reduced before its deepest parent. A Kahn queue with explicit parent counts is
sufficient and avoids depending on the current `graph.Depth` counter.

## 7. Memory bounds

The evaluator must not load an entitlement's full grant set into memory.

For one destination merge, memory is:

```
O(active_inputs * page_size + merge_heap + dirty_write_buffer)
```

not `O(total_grants)`.

High fan-in is the practical limit because each input stream may hold a Pebble
iterator and a page buffer. Set a hard maximum input width `M` (for example
128 or 256, to be measured). If a destination has more than `M` contribution
streams, use a cascade:

1. merge at most `M` inputs into an intermediate run keyed by principal;
2. repeat until all inputs are represented by runs;
3. merge the runs with `BaseStream(D)`.

Intermediate runs must be disk-backed, not heap-backed. They may use a temp
Pebble keyspace. The same Section 4 merge semantics still apply at the final
merge.

## 8. Pebble considerations

### 8.1 What Pebble is good at here

- Prefix range scans over sorted keys.
- Sequential write ingestion.
- Using an LSM keyspace as an external sorter.
- Bulk sorted SST ingest for write-once ranges.

### 8.2 What Pebble is poor at here

- Per-row random primary `Get`s from a secondary index scan.
- Repeated overwrite/delete cycles that create tombstones for primary and index
  keys.
- Too many live iterators, which can pin resources and increase cache pressure.
- Large batches that grow in-memory batch buffers.

### 8.3 Consequences for this design

- Phase 1 should favor correctness and existing APIs, while documenting that
  materialization Gets remain.
- The first optimized stream to try is a **temporary source projection
  keyspace**, built in a separate temp Pebble DB and ingested from an SST. It is
  keyed by `(source_entitlement, principal_rt, principal_id, external_id)` and
  stores the minimum fields needed for source-edge filtering. In the dense
  synthetic benchmarks this was materially better than repeatedly hydrating
  parent streams from the permanent `by_entitlement` index.
- The write path should group output by destination entitlement and principal
  order. This guarantees `by_entitlement` index writes are sorted. It only
  guarantees primary-key sorted writes for synthesized deterministic grant IDs
  (`D:rt:res`); dirty existing base grants may have arbitrary connector
  external IDs and are not necessarily primary-sorted.
- The plan should reduce rewrites to at most once per dirty destination grant.
  It cannot honestly promise "never overwrite" because existing base grants
  with new contributions must be updated.
- SST ingest is attractive only after a precise replacement strategy exists for
  primary keys and all secondary indexes. Existing base grants with arbitrary
  external IDs and stale index cleanup make this non-trivial. Treat SST ingest
  as a later design, not an assumed Phase 2 shortcut.

### 8.4 Temporary source projection keyspace

The source projection keyspace is a scratch index, not a permanent c1z format
change:

```
exp_src | entitlement_id | principal_rt | principal_id | external_id
  -> { direct_on_entitlement }
```

The benchmark projection stores only the directness byte because the synthetic
principals are reconstructible from the key. A production version may also need
principal payload fields if expanded grants must preserve non-ID principal
metadata exactly.

Lifecycle:

1. Build the projection before topological expansion by scanning current grants
   once and writing sorted projection rows into a separate temp Pebble DB.
2. During expansion, whenever dirty grants are written to the real store, append
   matching projection rows so later nodes can read the newly finalized grants.
3. Close and delete the temp DB directory at expansion end. Cleanup is outside
   the permanent c1z and does not create range tombstones in the main DB.

Benchmark signal:

- `64×64×256`: projection pull was ~1.10s / ~641MB vs streaming pull
  ~4.75s / ~3.37GB and spill-SST shuffle ~2.29s / ~1.03GB.
- `128×128×512`: projection pull was ~7.97s / ~4.79GB vs spill-SST shuffle
  ~12.87s / ~7.52GB.
- `256×256×1000`: projection pull completed in ~57.1s / ~36.4GB.

This makes projection-first topological pull the best current dense-region
candidate. The projection size scales with `source_entitlements × principals`,
while full shuffle contribution volume scales with
`source_entitlements × destination_entitlements × principals`.

## 9. Write sink

Initial sink: `StoreExpandedGrants` / `PutExpandedGrantRecords`.

Current properties:

- it preserves `Expansion`, `NeedsExpansion`, and `DiscoveredAt` from the prior
  record;
- it performs one primary `Get` per dirty grant to preserve that side-state and
  delete stale index entries;
- it commits with `pebble.NoSync` for expansion writes;
- it reuses scratch buffers for key/value encoding.

This is acceptable for the first measured implementation. It means the new
algorithm's first win is not "no Gets anywhere"; it is:

- no descendant existence prefetch/probe loop,
- one destination reduction after all parents,
- fewer repeated destination rewrites,
- more sorted `by_entitlement` index writes.

An optimized write sink can be designed after Phase 1 parity and Phase 2
profiling identify the remaining bottleneck.

## 10. External-memory shuffle for both-huge nodes

If real graph telemetry shows nodes with both very large fan-in and very large
fan-out, neither pure pull nor pure source batching is ideal:

- pull re-reads large sources for many children;
- source batching re-touches large destinations for many parents.

For those hotspots, use a disk-backed contribution shuffle only if source
projection pull is still too slow or too memory-heavy:

```
map:
    for each finalized source entitlement S:
        scan FinalizedStream(S)
        for each outgoing edge S-node -> D-node:
            for each destination member D:
                write temp contribution keyed by (dest_node_order, D, principal)
                value includes {source_entitlement_id: S, is_direct}

shuffle:
    Pebble temp key order groups by destination and principal.

reduce:
    for each destination D in order:
        merge grouped temp contributions with BaseStream(D)
        call the same Section 4 merge routine
        write dirty grants once
```

This is an external-memory plan. The temp contribution set can be as large as
the output contribution volume, so it is not the default. Batch writes to a temp
Pebble DB were too expensive in benchmarks. Sorted SST ingestion changed the
result: spill-sort contribution chunks, k-way merge them into an SST, ingest the
SST into the temp DB, then reduce.

Benchmark signal:

- `128×128×512`: spill-SST shuffle was ~15.2s / ~7.5GB, faster than streaming
  pull (~47.9s / ~23.9GB) but slower than source-projection pull
  (~8.0s / ~4.8GB).

Use shuffle as a fallback for regions where projection pull cannot meet memory
or runtime budgets, not as the first dense-region strategy.

## 11. Checkpointing and resumability

The current expander checkpoints through `EntitlementGraph.Actions`,
`PageToken`, `Depth`, and edge `IsExpanded`. A topological evaluator will not
fit that shape cleanly.

Implementation must choose one of these before production use:

1. run the new evaluator as an atomic expansion phase and rely on restart from
   the previous sync checkpoint if interrupted;
2. introduce a new checkpoint schema recording:
   - graph fingerprint/version,
   - finalized node set or topo index,
   - current destination entitlement,
   - last completed principal key,
   - temp-run/shuffle keyspace metadata if cascade/shuffle is active.

Do not reuse `EntitlementGraph.Actions` for the new evaluator unless the mapping
is explicit and tested. Mixed old/new checkpoint resume must be either supported
by a migration or rejected with a clear fallback.

## 12. Validation gates

### 12.1 Differential harness

Run current expander and new evaluator on identical inputs. Diff the resulting
grant set by:

- grant external ID,
- entitlement ID,
- principal resource type and resource ID,
- full `Sources` map and `IsDirect` values,
- annotations,
- `Expansion`,
- `NeedsExpansion`,
- `DiscoveredAt` handling where relevant.

Corpus:

- existing small/medium expansion fixtures;
- pebble/sqlite equivalence fixtures where applicable;
- synthetic generated cases for:
  - linear chains,
  - diamonds / reconvergence,
  - high fan-in,
  - high fan-out,
  - combined high fan-in/fan-out,
  - shallow edges,
  - resource-type filters,
  - collapsed cycles / multi-member nodes,
  - multiple base grants for one principal,
  - nil-principal malformed grants,
  - missing source/destination entitlement behavior.

No optimized path advances without a green parity gate.

### 12.2 Benchmarks

Measure after each phase:

- wall time,
- bytes allocated,
- alloc count,
- Pebble primary Gets,
- iterator count / live iterators,
- bytes written to WAL/SST/temp keyspace,
- compaction CPU and bytes compacted,
- output grants written.

The current output size remains the lower bound. A phase is successful only if
it removes overhead around that lower bound.

## 13. Phased implementation

### Phase 0: spec-backed oracle

- Implement contribution/merge semantics as small Go types and unit tests.
- Build the differential harness.
- No production expander path changes.

Gate: harness can compare current expander output on existing fixtures and
synthetic cases.

### Phase 1: correctness-first pull evaluator

- Implement `PrincipalGrantStream` over existing `ListGrantsForEntitlement`.
- Implement `EdgeContributionStream`.
- Implement bounded k-way merge for manageable fan-in; fail or cascade for
  fan-in above `M`.
- Implement the topological driver.
- Use the existing write sink only in a test/evaluation path until parity is
  proven.

Gate: byte-level parity with the current expander over the validation corpus.

Expected limitation: primary materialization Gets remain because current
`by_entitlement` scans require them.

### Phase 2: measured production candidate

- Wire Phase 1 evaluator behind a feature flag.
- Use `PutExpandedGrantRecords` as the sink.
- Benchmark against current expander on small/medium/whale cases.

Gate: measured win with no parity diffs and acceptable restart/checkpoint
behavior.

### Phase 3: source-projection topological pull

- Build a separate temporary Pebble projection DB using sorted SST ingest.
- Read parent/source streams from the projection instead of from
  `ListGrantsForEntitlement`.
- Maintain the projection when dirty grants are written, so downstream nodes see
  newly finalized grants.
- Keep destination base streams on the canonical store until a separate design
  proves base projection is worthwhile.

Gate: parity plus benchmark win on whale/prod-like dense regions. This is the
current best candidate for high fan-in / dense expansion.

### Phase 4: write-path optimization

Only after Phase 3 profiling:

- If final writes dominate, design sorted-output/SST-ingest mechanics for
  synthetic expanded grants first.
- Existing base grant updates, arbitrary connector external IDs, and stale index
  cleanup make full final-output SST ingest non-trivial.

Gate: separate design review and parity/performance proof.

### Phase 5: disk-backed shuffle for both-huge hotspots

Only if graph telemetry and profiling show projection pull loses or exceeds the
memory budget on both-huge nodes.

- Implement spill-sorted SST temp contribution keyspace.
- Reuse the exact Section 4 merge routine.
- Gate by degree heuristic and benchmark.

Gate: parity plus net win after temp-write overhead.

## 14. Open questions to answer with code or measurements

1. What are real max in-degree and out-degree after SCC collapse?
2. In Phase 1, how much time remains in primary materialization Gets vs. current
   descendant probes?
3. What fraction of dirty output is synthesized deterministic grants vs.
   existing connector grants with arbitrary external IDs?
4. Does Phase 2 reduce compaction bytes/CPU, or do secondary-index writes remain
   the dominant lower bound?
5. Is checkpoint/resume required for the first production rollout, or can the
   feature-flagged evaluator restart from the previous sync checkpoint?
