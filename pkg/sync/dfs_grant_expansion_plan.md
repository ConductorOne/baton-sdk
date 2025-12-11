# DFS Grant Expansion Plan

## Overview

Replace the current BFS (layer-by-layer) grant expansion with a DFS (per-principal) approach that touches each grant exactly once, eliminating the need for a rectification pass.

---

## Data Constraints

- **Graph**: Fits in memory (N = hundreds to thousands of nodes, IDs only)
- **Grants**: Cannot fit in memory (millions total)
- **Entitlement protobufs**: Cannot cache - look up on demand
- **Per-principal grants on graph**: Cannot accumulate - a principal may have millions of grants even on graph nodes. Must process incrementally as we stream.

---

## Initialization (Once Per Expansion)

**Precondition**: Entitlement graph already exists in memory (handled by existing code).

1. **Precompute transitive descendants for each node**
   - For each node in the graph, compute all reachable descendants (transitive closure)
   - Store as: `descendants[entitlement_id] = []entitlement_id`
   - **Cost**: O(N × (N + E)) time, O(N × avg_descendants) memory
   - With N = 1-10K nodes and avg ~100 descendants, this is ~1M entries = ~8-80MB
   - Computed once, used for all principals

2. **Create temp table with graph node IDs in topological order**
   - Schema: `(rowid INTEGER PRIMARY KEY, entitlement_id TEXT UNIQUE)`
   - Insert entitlement IDs in topological order (ancestors first)
   - `rowid` reflects insertion order
   - **Must explicitly ORDER BY `t.rowid`** in queries - SQLite does not guarantee order without ORDER BY

---

## Main Loop: Stream All Grants on Graph Nodes

**Single streaming query**:

```sql
SELECT 
    g.principal_resource_type_id,
    g.principal_resource_id,
    g.entitlement_id,
    g.id,
    g.data
FROM grants g
JOIN temp_expandable_entitlements t ON g.entitlement_id = t.entitlement_id
WHERE g.sync_id = ?
ORDER BY g.principal_resource_type_id, g.principal_resource_id, t.rowid
```

Note: `t.rowid` reflects insertion order = topological order (ancestors before descendants).

Stream through results, processing each grant incrementally as it arrives. Topological ordering ensures ancestors are seen before descendants.

### On New Principal

1. **Flush previous principal** (if any): See "End of Principal" below
2. **Load Resource proto**: Single lookup for the new principal
3. **Reset state**:
   - `hasGrantOn`: map[entitlement_id] → true (membership only)
   - `existingGrantId`: map[entitlement_id] → grant_id (just the int64 ID, not data)

### For Each Grant (Incremental Processing)

As we stream grants in topological order:

```
# For each grant on entitlement E with grant_id:

1. Mark membership: hasGrantOn[E] = true
2. Track existing: existingGrantId[E] = grant_id
```

We do NOT expand during streaming - just collect membership and IDs.

### End of Principal

After seeing all grants for a principal, compute expansions:

```
pendingGrants = {}  # entitlement_id → {sources: [], needsUpdate: bool}

for each E in hasGrantOn:
    # Get ALL transitive descendants (precomputed, O(1) lookup)
    for each D in descendants[E]:
        if D not in pendingGrants:
            pendingGrants[D] = {sources: [], needsUpdate: D in existingGrantId}
        pendingGrants[D].sources.append(E)
```

**Key insight**: `descendants[E]` returns all transitive descendants, precomputed during initialization.

### Batch Fetch for Updates

For grants that need UPDATE (exist but need sources added):

```sql
SELECT id, data FROM grants WHERE id IN (
    SELECT id FROM existingGrantId WHERE entitlement_id IN pendingGrants AND needsUpdate
)
```

One batch query per principal.

### Write Phase

- **INSERT** (new grants): Build Grant proto using:
  - Entitlement proto (lookup by ID)
  - Principal proto (already loaded)
  - Computed sources from pendingGrants
  
- **UPDATE** (existing grants): 
  - Unmarshal fetched data
  - Add sources
  - Re-marshal, write

### Clear State, Next Principal

Discard `hasGrantOn`, `existingGrantId`, `pendingGrants`. Continue streaming.

---

## Memory Profile

| Data | Lifetime | Size |
|------|----------|------|
| Graph structure | Entire sync | O(N + E) IDs (already exists) |
| Precomputed descendants | Entire expansion | O(N × avg_descendants) ≈ O(N²) worst case |
| Temp table | Entire expansion | O(N) IDs (in SQLite) |
| Current principal proto | Per principal | O(1) |
| hasGrantOn | Per principal | O(N) IDs - membership only |
| existingGrantId | Per principal | O(N) int64s - just IDs |
| pendingGrants | Per principal | O(N) - descendants with source lists |

**Global memory**: O(N²) for precomputed descendants (typically much smaller in practice).
**Per-principal memory**: O(N) - bounded by graph size, NOT by grant count.

---

## I/O Summary

| Operation | Count |
|-----------|-------|
| Precompute descendants | O(N) graph traversals (in memory, no I/O) |
| Populate temp table | O(N/chunk) inserts |
| Stream all grants on graph nodes | O(1) query, streams all rows |
| Per principal: load Resource | O(P) queries total |
| Per principal: batch fetch grants for update | O(P) queries (one batch per principal) |
| Per principal: load entitlements for new grants | O(new grants) lookups |
| Per principal: batch write | O(P) batches |

---

## Complexity Comparison

| | BFS (Current) | DFS (Proposed) |
|---|---------------|----------------|
| Queries | O(N) per layer | O(P) total |
| Writes per grant | 1-k (multiple updates) | 1 (exactly once) |
| Rectification | Required | Not needed |
| Parallelizable | Per layer | Per principal |

---

## Open Questions

1. **Batch size for writes**: How many grants per batch?
2. **Error handling**: If one principal fails, continue with others?
3. **Progress tracking**: How to report/resume progress?
4. **Descendants memory**: If N² memory for descendants is too large, consider lazy computation with LRU cache.

