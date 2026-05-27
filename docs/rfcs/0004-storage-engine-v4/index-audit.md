# SQLite → Pebble index parity audit

Comprehensive mapping of every secondary index declared on the
SQLite engine's tables to its Pebble equivalent, with a note for
each gap. Filed as part of resolving the PR-874 review.

## Per-table mapping

### `resources` (SQLite has 3 indexes)

| SQLite | Pebble equivalent | Status |
|---|---|---|
| `(resource_type_id)` | none | **gap** — used by "list resources of type X". Pebble currently does a primary scan + post-filter; with rt-prefixed access uncommon in production we treat this as P3. |
| `(parent_resource_type_id, parent_resource_id)` | `idxResourceByParent` keyed `(sync_id, parent_rt, parent_id, child_rt, child_id)` | covered |
| UNIQUE `(external_id, sync_id)` | primary key `(sync_id, rt_id, res_id)` — uniqueness invariant equivalent | covered |

### `resource_types` (1 index)

| SQLite | Pebble equivalent | Status |
|---|---|---|
| UNIQUE `(external_id, sync_id)` | primary key `(sync_id, external_id)` | covered |

### `entitlements` (2 indexes)

| SQLite | Pebble equivalent | Status |
|---|---|---|
| `(resource_type_id, resource_id)` | `idxEntitlementByResource` keyed `(sync_id, rt_id, res_id, ext_id)` | covered (within-sync) |
| UNIQUE `(external_id, sync_id)` | primary key `(sync_id, external_id)` | covered |

### `grants` (5 + 2 partial)

| SQLite | Pebble equivalent | Status |
|---|---|---|
| `(resource_type_id, resource_id)` | none | **gap** — entitlement-resource lookup on grants. No call site uses this directly today (ListGrants's `request.Resource` filter is by *principal*, served via `idxGrantByPrincipal`); the SQLite index supports JOINs we don't perform in Pebble. P3. |
| `(principal_resource_type_id, principal_resource_id)` | `idxGrantByPrincipal` keyed `(sync_id, princ_rt, princ_id, ext_id)` | covered (within-sync) |
| `(entitlement_id, principal_resource_type_id, principal_resource_id)` | `idxGrantByEntitlement` keyed `(sync_id, ent_id, princ_rt, princ_id, ext_id)` | covered (within-sync; superset) |
| UNIQUE `(external_id, sync_id)` | primary key `(sync_id, external_id)` | covered |
| `(entitlement_id, sync_id, id)` | partially covered by `idxGrantByEntitlement` (sync-prefixed) | **gap** — cross-sync uplift. Discussed with kans in PR thread; design Q open. P1. |
| PARTIAL `(sync_id) WHERE expansion IS NOT NULL` | none | **gap** — used to drive ListWithAnnotationsPage's annotation-only iteration. Current Pebble path iterates all grants and filters; correct but less efficient. P2. |
| PARTIAL `(sync_id) WHERE needs_expansion = 1` | `idxGrantByNeedsExpansion` keyed `(sync_id, ext_id)` — **NEW** | covered — landed in this commit. |

### `assets` (1 index)

| SQLite | Pebble equivalent | Status |
|---|---|---|
| UNIQUE `(external_id, sync_id)` | primary key `(sync_id, external_id)` | covered |

### `sync_runs` (1 index)

| SQLite | Pebble equivalent | Status |
|---|---|---|
| UNIQUE `(sync_id)` | primary key `(sync_id)` | covered |

### `session_store` (1 index)

| SQLite | Pebble equivalent | Status |
|---|---|---|
| UNIQUE `(sync_id, key)` | not a sync-record; out of v3 scope (Pebble doesn't materialise the SQLite-specific session_store) | n/a |

## Summary

Total SQLite secondary indexes (excluding PKs that mirror to Pebble
PKs): 10 (8 indexes + 2 partial on grants).

Now closed by Pebble:
- 6 fully covered by existing indexes
- 1 newly added: `idxGrantByNeedsExpansion` (this commit) — the
  correctness gap that prevented PendingExpansion from finding
  rows on Pebble. Previously PendingExpansionPage returned an
  empty page unconditionally, silently skipping the syncer's
  ExpandGrants phase for any Pebble-backed c1z.

Still open:
- **P1** — `(entitlement_id, sync_id, id)` cross-sync uplift index.
  Design Q sitting with kans in PR thread; the question is
  whether uplift's access pattern is in-fact cross-sync. If yes
  we add `idxGrantByEntitlementCrossSync`; if no the existing
  in-sync index is sufficient.
- **P2** — `(sync_id) WHERE expansion IS NOT NULL` partial.
  Workaround in place (iterate-and-filter). Add only if
  ListWithAnnotations becomes hot.
- **P3** — `(resource_type_id)` on resources, `(resource_type_id,
  resource_id)` on grants. No call sites today; add if a future
  read path needs them.

## What landed in this audit pass

- New constant `idxGrantByNeedsExpansion = 0x05` already declared
  in keys.go is now wired:
  - `encodeGrantByNeedsExpansionIndexKey` / `Prefix`
  - `GrantByNeedsExpansionSyncLowerBound` / `UpperBound`
  - `writeGrantIndexes` populates when r.GetNeedsExpansion() is
    true; `deleteGrantIndexes` always Deletes (no-op when absent)
    so a flip from true→false during overwrite is handled.
- `Engine.IterateGrantsByNeedsExpansion` walks the index +
  materialises primaries.
- `Engine.PaginateGrantsByNeedsExpansion` paginated variant for
  GrantStore.
- `pebbleGrantStore.PendingExpansionPage` and `PendingExpansion`
  rewritten on top of the new index; they return real rows
  instead of empty pages.
- `synccompactor/pebble/bucket_plans.go` adds a
  `grant_by_needs_expansion` bucket so cross-engine compaction
  rebuilds the new keyspace.
- `FileOps.CloneSync` extends its range-copy list to include the
  new keyspace.
- Test coverage: `pending_expansion_test.go` —
  `TestPendingExpansionIndexRoundtrip` (engine level, including
  flag-flip), `TestPebbleGrantStorePendingExpansionPage` (adapter
  level).
