package dotc1z

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestSourceCacheWritesRowsAndEntry(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	scopeHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-1"`)
	require.NoError(t, err)

	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build()}.Build()
	require.NoError(t, c1f.SourceCache().PutResourcesWithKey(ctx, key, group))
	ent := v2.Entitlement_builder{Id: "ent-source-cache", Resource: group}.Build()
	require.NoError(t, c1f.SourceCache().PutEntitlementsWithKey(ctx, key, ent))
	grant := v2.Grant_builder{Id: "grant-source-cache", Entitlement: ent, Principal: group}.Build()
	require.NoError(t, c1f.SourceCache().PutGrantsWithKey(ctx, key, grant))
	require.NoError(t, c1f.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, syncID, scopeHash, key, `"etag-1"`, time.Now()))

	for _, tableName := range []string{resources.Name(), entitlements.Name(), grants.Name()} {
		var got string
		require.NoError(t, c1f.db.QueryRowContext(ctx, "select source_cache_key from "+tableName+" where source_cache_key = ? limit 1", key).Scan(&got))
		require.Equal(t, key, got)
	}

	entry, ok, err := c1f.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, syncID, scopeHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, entry.Key)
	require.Equal(t, `"etag-1"`, entry.ETag)
}

func TestSourceCacheReplayRowsAndEntry(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	scopeHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-2"`)
	require.NoError(t, err)

	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-replay"}.Build()}.Build()
	ent := v2.Entitlement_builder{Id: "ent-replay", Resource: group}.Build()
	grant := v2.Grant_builder{Id: "grant-replay", Entitlement: ent, Principal: group}.Build()
	require.NoError(t, c1f.SourceCache().PutResourcesWithKey(ctx, key, group))
	require.NoError(t, c1f.SourceCache().PutEntitlementsWithKey(ctx, key, ent))
	require.NoError(t, c1f.SourceCache().PutGrantsWithKey(ctx, key, grant))
	require.NoError(t, c1f.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, syncID, scopeHash, key, `"etag-2"`, time.Now()))
	require.NoError(t, c1f.EndSync(ctx))

	newSyncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	rows, err := c1f.SourceCache().ReplaySourceCacheRows(ctx, c1f, sourcecache.RowKindGrants, syncID, newSyncID, key, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
	require.NoError(t, c1f.SourceCache().ReplaySourceCacheEntry(ctx, c1f, sourcecache.RowKindGrants, syncID, newSyncID, key, time.Now()))

	var count int
	require.NoError(t, c1f.db.QueryRowContext(ctx, "select count(*) from "+grants.Name()+" where sync_id = ? and source_cache_key = ?", newSyncID, key).Scan(&count))
	require.Equal(t, 1, count)

	entry, ok, err := c1f.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, newSyncID, scopeHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, entry.Key)
}

func TestSourceCacheReplayExternalStoreAfterAttach(t *testing.T) {
	ctx := context.Background()
	source, sourceSyncID := setupSourceCacheReferenceC1Z(ctx, t)
	defer func() { require.NoError(t, source.Close(ctx)) }()
	dest, destSyncID, destCleanup := setupTestC1Z(ctx, t)
	defer destCleanup()

	scopeHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-3"`)
	require.NoError(t, err)

	require.NoError(t, dest.SourceCache().AttachExternalSource(ctx, source))
	defer func() { require.NoError(t, dest.SourceCache().DetachExternalSource(ctx)) }()

	rows, err := dest.SourceCache().ReplaySourceCache(ctx, source, sourcecache.RowKindGrants, sourceSyncID, destSyncID, key, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)

	// Re-running the same replay must remain idempotent (insert-or-replace).
	_, err = dest.SourceCache().ReplaySourceCache(ctx, source, sourcecache.RowKindGrants, sourceSyncID, destSyncID, key, time.Now())
	require.NoError(t, err)

	var grantCount int
	require.NoError(t, dest.db.QueryRowContext(ctx, "select count(*) from "+grants.Name()+" where sync_id = ? and source_cache_key = ?", destSyncID, key).Scan(&grantCount))
	require.Equal(t, 1, grantCount)

	var entryCount int
	require.NoError(t, dest.db.QueryRowContext(ctx, "select count(*) from "+sourceCacheEntries.Name()+" where sync_id = ? and source_cache_key = ?", destSyncID, key).Scan(&entryCount))
	require.Equal(t, 1, entryCount)
}

func TestSourceCacheReplayMissingEntryRollsBack(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source-missing-entry-combined.c1z")
	source, err := NewC1ZFile(ctx, sourcePath)
	require.NoError(t, err)
	sourceSyncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	dest, destSyncID, destCleanup := setupTestC1Z(ctx, t)
	defer destCleanup()

	scopeHash := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-5"`)
	require.NoError(t, err)
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-tx-rb"}.Build()}.Build()
	ent := v2.Entitlement_builder{Id: "ent-tx-rb", Resource: group}.Build()
	grant := v2.Grant_builder{Id: "grant-tx-rb", Entitlement: ent, Principal: group}.Build()
	require.NoError(t, source.SourceCache().PutGrantsWithKey(ctx, key, grant))
	// Intentionally omit PutSourceCacheEntry so the rows are present in the
	// source but the entry isn't. ReplaySourceCache must roll back the
	// already-inserted rows so the destination stays clean.
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))
	source, err = NewC1ZFile(ctx, sourcePath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()

	require.NoError(t, dest.SourceCache().AttachExternalSource(ctx, source))
	defer func() { require.NoError(t, dest.SourceCache().DetachExternalSource(ctx)) }()

	_, err = dest.SourceCache().ReplaySourceCache(ctx, source, sourcecache.RowKindGrants, sourceSyncID, destSyncID, key, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "source cache entry not found")

	var grantCount int
	require.NoError(t, dest.db.QueryRowContext(ctx, "select count(*) from "+grants.Name()+" where sync_id = ? and source_cache_key = ?", destSyncID, key).Scan(&grantCount))
	require.Equal(t, 0, grantCount, "grant rows must be rolled back when the matching cache entry is missing")
}

func TestSourceCacheReplayExternalStoreRequiresAttach(t *testing.T) {
	ctx := context.Background()
	source, sourceSyncID := setupSourceCacheReferenceC1Z(ctx, t)
	defer func() { require.NoError(t, source.Close(ctx)) }()
	dest, destSyncID, destCleanup := setupTestC1Z(ctx, t)
	defer destCleanup()

	scopeHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-3"`)
	require.NoError(t, err)

	_, err = dest.SourceCache().ReplaySourceCacheRows(ctx, source, sourcecache.RowKindGrants, sourceSyncID, destSyncID, key, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "AttachExternalSource has not been called")
}

func TestSourceCacheReplayEntryMissingDoesNotInsert(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source-missing-entry.c1z")
	source, err := NewC1ZFile(ctx, sourcePath)
	require.NoError(t, err)
	sourceSyncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	dest, destSyncID, destCleanup := setupTestC1Z(ctx, t)
	defer destCleanup()

	scopeHash := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-4"`)
	require.NoError(t, err)
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-rollback"}.Build()}.Build()
	ent := v2.Entitlement_builder{Id: "ent-rollback", Resource: group}.Build()
	grant := v2.Grant_builder{Id: "grant-rollback", Entitlement: ent, Principal: group}.Build()
	require.NoError(t, source.SourceCache().PutGrantsWithKey(ctx, key, grant))
	// Intentionally omit PutSourceCacheEntry: the rows table has the grant but
	// the source_cache_entries table does not have a matching metadata row.
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))
	source, err = NewC1ZFile(ctx, sourcePath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()

	require.NoError(t, dest.SourceCache().AttachExternalSource(ctx, source))
	defer func() { require.NoError(t, dest.SourceCache().DetachExternalSource(ctx)) }()

	// ReplaySourceCacheRows copies the grant row(s).
	_, err = dest.SourceCache().ReplaySourceCacheRows(ctx, source, sourcecache.RowKindGrants, sourceSyncID, destSyncID, key, time.Now())
	require.NoError(t, err)
	// ReplaySourceCacheEntry fails because the entry is missing in the source.
	err = dest.SourceCache().ReplaySourceCacheEntry(ctx, source, sourcecache.RowKindGrants, sourceSyncID, destSyncID, key, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "source cache entry not found")
}

func TestSourceCacheLookupMissReturnsFalse(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	scopeHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	entry, ok, err := c1f.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindResources, syncID, scopeHash)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, sourcecache.Entry{}, entry)
}

func TestPutSourceCacheEntryRejectsScopeHashMismatch(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	scopeHashInKey := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherScopeHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	key, err := sourcecache.BuildKey(scopeHashInKey, `"etag"`)
	require.NoError(t, err)

	err = c1f.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, syncID, otherScopeHash, key, `"etag"`, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "scope hash")
}

func TestPutSourceCacheEntryRejectsEtagMismatch(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	scopeHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-in-key"`)
	require.NoError(t, err)

	err = c1f.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, syncID, scopeHash, key, `"different-etag"`, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "etag")
}

func TestPutSourceCacheEntryRejectsInvalidRowKind(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	scopeHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	key, err := sourcecache.BuildKey(scopeHash, `"etag"`)
	require.NoError(t, err)

	err = c1f.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKind("not-a-kind"), syncID, scopeHash, key, `"etag"`, time.Now())
	require.Error(t, err)
}

func TestSourceCacheReplayResourcesAndEntitlements(t *testing.T) {
	// Coverage for non-grant row kinds end-to-end: a previous sync puts
	// resources + entitlements with a key; a fresh sync attaches the source
	// and replays both kinds; the destination ends up with both row sets and
	// the corresponding source_cache_entries metadata.
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source-res-ent.c1z")
	source, err := NewC1ZFile(ctx, sourcePath)
	require.NoError(t, err)
	sourceSyncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resScopeHash := "1111111111111111111111111111111111111111111111111111111111111111"
	resKey, err := sourcecache.BuildKey(resScopeHash, `"res-etag"`)
	require.NoError(t, err)
	entScopeHash := "2222222222222222222222222222222222222222222222222222222222222222"
	entKey, err := sourcecache.BuildKey(entScopeHash, `"ent-etag"`)
	require.NoError(t, err)

	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-multi"}.Build()}.Build()
	ent := v2.Entitlement_builder{Id: "ent-multi", Resource: group}.Build()
	require.NoError(t, source.SourceCache().PutResourcesWithKey(ctx, resKey, group))
	require.NoError(t, source.SourceCache().PutEntitlementsWithKey(ctx, entKey, ent))
	require.NoError(t, source.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindResources, sourceSyncID, resScopeHash, resKey, `"res-etag"`, time.Now()))
	require.NoError(t, source.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindEntitlements, sourceSyncID, entScopeHash, entKey, `"ent-etag"`, time.Now()))
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))

	source, err = NewC1ZFile(ctx, sourcePath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()

	dest, destSyncID, destCleanup := setupTestC1Z(ctx, t)
	defer destCleanup()
	require.NoError(t, dest.SourceCache().AttachExternalSource(ctx, source))
	defer func() { require.NoError(t, dest.SourceCache().DetachExternalSource(ctx)) }()

	resRows, err := dest.SourceCache().ReplaySourceCache(ctx, source, sourcecache.RowKindResources, sourceSyncID, destSyncID, resKey, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 1, resRows)
	entRows, err := dest.SourceCache().ReplaySourceCache(ctx, source, sourcecache.RowKindEntitlements, sourceSyncID, destSyncID, entKey, time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 1, entRows)

	var n int
	require.NoError(t, dest.db.QueryRowContext(ctx, "select count(*) from "+resources.Name()+" where sync_id = ? and source_cache_key = ?", destSyncID, resKey).Scan(&n))
	require.Equal(t, 1, n, "resources row must be replayed")
	require.NoError(t, dest.db.QueryRowContext(ctx, "select count(*) from "+entitlements.Name()+" where sync_id = ? and source_cache_key = ?", destSyncID, entKey).Scan(&n))
	require.Equal(t, 1, n, "entitlements row must be replayed")

	// Per-kind entry rows are in place so a follow-up sync finds them.
	entry, ok, err := dest.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindResources, destSyncID, resScopeHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, resKey, entry.Key)
	entry, ok, err = dest.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindEntitlements, destSyncID, entScopeHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, entKey, entry.Key)
}

// TestLookupHonorsRowKindPartition pins down that the same scope_hash can
// hold a different etag per row_kind. Regression guard: a write under
// RowKindResources must not satisfy a lookup under RowKindGrants.
func TestLookupHonorsRowKindPartition(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	scopeHash := "9999999999999999999999999999999999999999999999999999999999999999"
	resKey, err := sourcecache.BuildKey(scopeHash, `"res"`)
	require.NoError(t, err)
	require.NoError(t, c1f.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindResources, syncID, scopeHash, resKey, `"res"`, time.Now()))

	_, ok, err := c1f.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, syncID, scopeHash)
	require.NoError(t, err)
	require.False(t, ok, "lookup must not cross row_kind boundaries")

	entry, ok, err := c1f.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindResources, syncID, scopeHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, resKey, entry.Key)
}

func setupSourceCacheReferenceC1Z(ctx context.Context, t *testing.T) (*C1File, string) {
	t.Helper()
	sourcePath := filepath.Join(t.TempDir(), "source-cache-ref.c1z")
	source, err := NewC1ZFile(ctx, sourcePath)
	require.NoError(t, err)
	sourceSyncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	scopeHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-3"`)
	require.NoError(t, err)
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-external"}.Build()}.Build()
	ent := v2.Entitlement_builder{Id: "ent-external", Resource: group}.Build()
	grant := v2.Grant_builder{Id: "grant-external", Entitlement: ent, Principal: group}.Build()
	require.NoError(t, source.SourceCache().PutGrantsWithKey(ctx, key, grant))
	require.NoError(t, source.SourceCache().PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, sourceSyncID, scopeHash, key, `"etag-3"`, time.Now()))
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))

	// Re-open read-only so the destination's ATTACH from a separate in-process
	// SQLite connection coexists with the source's own pool — the very
	// property we want to validate. Read-only C1Files no longer take
	// locking_mode=EXCLUSIVE or SetMaxOpenConns(1), so no pragma override is
	// required.
	source, err = NewC1ZFile(ctx, sourcePath, WithReadOnly(true))
	require.NoError(t, err)
	return source, sourceSyncID
}
