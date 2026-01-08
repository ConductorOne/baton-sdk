# Paired Sync Diffs for c1z

## Overview

Implement diff support in c1z using paired syncs - one sync for upserts (additions/modifications), one for deletions - by adding `linked_sync_id` to `sync_runs` and updating `GenerateSyncDiff` to produce both.

## Existing Functionality

There is existing diff support in `pkg/dotc1z/diff.go`:

```go
func (c *C1File) GenerateSyncDiff(ctx context.Context, baseSyncID string, appliedSyncID string) (string, error)
```

### Current Behavior

- Compares two syncs by `external_id` only
- Creates a new `partial` sync containing **additions only** (items in `appliedSyncID` not in `baseSyncID`)
- Sets `parent_sync_id` to the base sync

### Current Limitations

| Feature | Supported? |
|---------|------------|
| Detect new items (additions) | Yes |
| Detect removed items (deletions) | No |
| Detect modified items (same `external_id`, different data) | No |

### Current Implementation

The `diffTableQuery` function builds a query like:

```sql
INSERT INTO table_name (columns...)
SELECT columns...
FROM table_name
WHERE sync_id = 'applied_sync'
  AND external_id NOT IN (
    SELECT external_id FROM table_name WHERE sync_id = 'base_sync'
  )
```

This captures additions but not deletions or modifications.

## Approach

A diff between two syncs will be expressed as **two partial syncs**:

- One sync containing **upserts** (items in new sync not in base, OR items that changed between syncs)
- One sync containing **deletions** (items in base sync not in new)

Each sync directly references its paired sync via `linked_sync_id`. For safety, the deletions sync uses a **new sync type** (`deletions`) so that old code will fail safely rather than misinterpreting deletions as additions.

## Schema Changes

Add one column and one new sync type to `pkg/dotc1z/sync_runs.go`:

```sql
-- New column
linked_sync_id text not null default ''   -- Points to paired sync (bidirectional)

-- sync_type values (existing + new)
-- 'full'              -- existing
-- 'partial'           -- existing (regular incremental sync)
-- 'resources_only'    -- existing
-- 'partial_upserts'   -- NEW: diff sync for additions/modifications
-- 'partial_deletions' -- NEW: diff sync for deletions
```

Given one sync, find its pair directly:

```sql
SELECT * FROM sync_runs WHERE sync_id = (
    SELECT linked_sync_id FROM sync_runs WHERE sync_id = 'upserts-sync-id'
)
```

### Safety Guarantee

| Sync Type | Old Code Behavior | New Code Behavior |
|-----------|-------------------|-------------------|
| `partial` | Works correctly - regular incremental sync | Works correctly |
| `partial_upserts` | Fails safely - unknown sync type | Processes as diff upserts |
| `partial_deletions` | Fails safely - unknown sync type | Processes as diff deletions |

## Implementation

### 1. Update `sync_runs` table

- Add `linked_sync_id` column to schema
- Add migration for existing databases
- Update `syncRun` struct to include `LinkedSyncID`
- Update all queries that read/write sync runs

### 2. Update `GenerateSyncDiff` in `pkg/dotc1z/diff.go`

Current behavior: Creates one sync with additions only (compares by `external_id`).

New behavior:

- Create **upserts sync** with `sync_type: partial_upserts`
  - Items where `external_id` exists in applied but not in base (additions)
  - Items where `external_id` exists in both but `data` blob differs (modifications)
- Create **deletions sync** with `sync_type: partial_deletions`
  - Items where `external_id` exists in base but not in applied
- Link them bidirectionally via `linked_sync_id`
- Return both sync IDs

```go
func (c *C1File) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (upsertsSyncID, deletionsSyncID string, err error)
```

#### New Query Logic

**Upserts query** (additions + modifications):
```sql
INSERT INTO table_name (columns...)
SELECT columns... FROM table_name AS applied
WHERE applied.sync_id = 'applied_sync'
  AND (
    -- Addition: not in base
    applied.external_id NOT IN (SELECT external_id FROM table_name WHERE sync_id = 'base_sync')
    OR
    -- Modification: in both but data differs
    EXISTS (
      SELECT 1 FROM table_name AS base
      WHERE base.sync_id = 'base_sync'
        AND base.external_id = applied.external_id
        AND base.data != applied.data
    )
  )
```

**Deletions query**:
```sql
INSERT INTO table_name (columns...)
SELECT columns... FROM table_name AS base
WHERE base.sync_id = 'base_sync'
  AND base.external_id NOT IN (SELECT external_id FROM table_name WHERE sync_id = 'applied_sync')
```

### 3. Update `SyncType` constants in `pkg/connectorstore/connectorstore.go`

Add new sync type for deletions:

```go
const (
    SyncTypeFull             SyncType = "full"
    SyncTypePartial          SyncType = "partial"
    SyncTypeResourcesOnly    SyncType = "resources_only"
    SyncTypePartialUpserts   SyncType = "partial_upserts"   // NEW: diff additions/modifications
    SyncTypePartialDeletions SyncType = "partial_deletions" // NEW: diff deletions
    SyncTypeAny              SyncType = ""
)
```

### 4. Expose via reader API (optional)

Update `pb/c1/reader/v2` protos to include `linked_sync_id` in `SyncRun` message if external consumers need to find paired syncs programmatically. The `sync_type` field already exists and will carry the new `deletions` value.

## Data Flow

```
Base Sync (full)          Applied Sync (full)
     │                           │
     └─────────┬─────────────────┘
               │
        GenerateSyncDiff()
               │
      ┌────────┴────────┐
      ▼                 ▼
 Upserts Sync      Deletions Sync
 id: ups-123       id: del-456
 type:             type:
 partial_upserts   partial_deletions
 parent: base      parent: base
 linked: del-456   linked: ups-123
      │                 │
      └────────┬────────┘
               │
        (bidirectional link)
```

## Testing

### Existing Test Coverage

`pkg/dotc1z/diff_test.go` has one test (`TestGenerateSyncDiff`) that:
- Creates a base sync with one resource
- Creates a new sync with the same resource + one new resource
- Generates a diff and verifies only the new resource appears

This test only covers **additions of resources**.

### Required Test Updates

#### 1. Update existing test for new return signature

```go
// Old
diffSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)

// New
upsertsSyncID, deletionsSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
```

#### 2. New test cases to add

| Test Case | Description |
|-----------|-------------|
| `TestGenerateSyncDiff_Additions` | Items added in new sync appear in upserts sync |
| `TestGenerateSyncDiff_Deletions` | Items removed in new sync appear in deletions sync |
| `TestGenerateSyncDiff_Modifications` | Items with same ID but different data appear in upserts sync |
| `TestGenerateSyncDiff_Mixed` | Combination of additions, deletions, and modifications |
| `TestGenerateSyncDiff_NoChanges` | Identical syncs produce empty upserts and deletions syncs |
| `TestGenerateSyncDiff_LinkedSyncID` | Verify bidirectional `linked_sync_id` is set correctly |
| `TestGenerateSyncDiff_SyncTypes` | Verify upserts has `partial` type, deletions has `deletions` type |

#### 3. Entity type coverage

Each test case should cover all entity types:
- Resources
- Resource Types
- Entitlements
- Grants

#### 4. Example test structure

```go
func TestGenerateSyncDiff_Deletions(t *testing.T) {
    ctx := context.Background()
    // Setup base sync with resources A, B, C
    // Setup new sync with resources A, B (C removed)
    // Generate diff
    // Verify:
    //   - upsertsSyncID has no resources (or empty)
    //   - deletionsSyncID has resource C
    //   - linked_sync_id is set correctly on both syncs
    //   - deletions sync has sync_type = "deletions"
}

func TestGenerateSyncDiff_Modifications(t *testing.T) {
    ctx := context.Background()
    // Setup base sync with resource A (displayName: "Original")
    // Setup new sync with resource A (displayName: "Modified")
    // Generate diff
    // Verify:
    //   - upsertsSyncID has resource A with new data
    //   - deletionsSyncID is empty
}

func TestGenerateSyncDiff_Mixed(t *testing.T) {
    ctx := context.Background()
    // Setup base sync with resources A, B, C
    // Setup new sync with resources A (modified), D (added) - B, C removed
    // Generate diff
    // Verify:
    //   - upsertsSyncID has: A (modified), D (added)
    //   - deletionsSyncID has: B, C (deleted)
}
```

## Cleanup / Compaction Behavior

The cleanup logic in `pkg/dotc1z/sync_runs.go` has been updated to handle the new diff sync types:

### Retained After Cleanup

1. **Latest full sync** - The most recent completed full sync is always kept (configurable via `BATON_KEEP_SYNC_COUNT`, default is 1)
2. **Most recent diff pair** - The latest `partial_upserts` and `partial_deletions` syncs are kept
3. **Regular partial syncs** - Only kept if they ended after the earliest-kept full sync started

### Cleanup Logic

```
After cleanup:
├── Latest full sync (the compacted one)
├── Most recent partial_upserts sync
└── Most recent partial_deletions sync
```

Older diff sync pairs are deleted. This ensures that processing can always apply:
1. The base full sync state
2. The latest diff on top of it

### Environment Variables

- `BATON_SKIP_CLEANUP` - Set to `true` to skip cleanup entirely
- `BATON_KEEP_SYNC_COUNT` - Number of full syncs to keep (default: 1)

## Files Modified

| File | Change |
|------|--------|
| `pkg/dotc1z/sync_runs.go` | Add `linked_sync_id` column, migration, struct field, updated cleanup logic |
| `pkg/dotc1z/diff.go` | Generate both upserts and deletions syncs with bidirectional links |
| `pkg/dotc1z/diff_test.go` | Added tests for additions, deletions, modifications, linking, and cleanup |
| `pkg/connectorstore/connectorstore.go` | Add `SyncTypePartialUpserts` and `SyncTypePartialDeletions` constants |

## Implementation Status

✅ **Complete**

All functionality has been implemented and tested:
- [x] New sync types: `partial_upserts`, `partial_deletions`
- [x] `linked_sync_id` field for bidirectional pairing
- [x] Schema migration for existing databases
- [x] `GenerateSyncDiff` returns both upsert and deletion sync IDs
- [x] Upserts sync includes additions AND modifications (byte-level comparison)
- [x] Deletions sync ordered before upserts (so upserts is "latest")
- [x] Cleanup logic correctly handles diff syncs
- [x] Comprehensive test coverage

