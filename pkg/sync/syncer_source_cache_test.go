package sync //nolint:revive,nolintlint // package name matches the rest of the sync package's internal tests

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/stretchr/testify/require"
)

// newSourceCacheSyncer returns a minimal *syncer wired to a fresh c1z file
// and an active sync, suitable for exercising the source-cache helpers.
func newSourceCacheSyncer(t *testing.T) (*syncer, string, func()) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sync.c1z")
	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	s := &syncer{
		syncID:             syncID,
		store:              store,
		sourceCacheEnabled: true,
	}
	return s, syncID, func() { _ = store.Close(ctx) }
}

// TestFinalizeSourceCacheSkipsEntryWhenZeroRows is the regression guard for
// the manifest-before-rows BLOCKER. finalizeSourceCache must NOT write a
// source_cache_entries row when the caller reports rowsWritten == 0, even
// if the SourceCacheEntry annotation was present and validated. Otherwise
// a subsequent sync would lookup a hit for this scope and replay zero rows,
// silently dropping data that's still upstream.
func TestFinalizeSourceCacheSkipsEntryWhenZeroRows(t *testing.T) {
	ctx := context.Background()
	s, syncID, cleanup := newSourceCacheSyncer(t)
	defer cleanup()

	scopeHash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	key, err := sourcecache.BuildKey(scopeHash, `"etag"`)
	require.NoError(t, err)
	annos := annotations.Annotations{}
	annos.Update(&v2.SourceCacheEntry{Key: key})

	prepared, err := s.prepareSourceCache(ctx, sourcecache.RowKindResources, "group", annos)
	require.NoError(t, err)
	require.True(t, prepared.enabled)
	require.Equal(t, key, prepared.key)

	require.NoError(t, s.finalizeSourceCache(ctx, prepared, 0))
	_, ok, err := s.store.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindResources, syncID, scopeHash)
	require.NoError(t, err)
	require.False(t, ok, "manifest entry must not be written when zero rows were persisted for the scope")
}

// TestFinalizeSourceCacheWritesEntryWhenRowsPresent is the positive
// counterpart to TestFinalizeSourceCacheSkipsEntryWhenZeroRows: with
// rowsWritten > 0 the entry IS persisted and the next sync would find it.
func TestFinalizeSourceCacheWritesEntryWhenRowsPresent(t *testing.T) {
	ctx := context.Background()
	s, syncID, cleanup := newSourceCacheSyncer(t)
	defer cleanup()

	scopeHash := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	key, err := sourcecache.BuildKey(scopeHash, `"etag-real"`)
	require.NoError(t, err)
	annos := annotations.Annotations{}
	annos.Update(&v2.SourceCacheEntry{Key: key})

	prepared, err := s.prepareSourceCache(ctx, sourcecache.RowKindGrants, "group", annos)
	require.NoError(t, err)
	require.True(t, prepared.enabled)

	require.NoError(t, s.finalizeSourceCache(ctx, prepared, 3))
	entry, ok, err := s.store.SourceCache().LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, syncID, scopeHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, entry.Key)
	require.Equal(t, `"etag-real"`, entry.ETag)
}

// TestPrepareSourceCacheNoAnnotationDisabled verifies that a response with no
// SourceCacheEntry annotation returns a disabled preparedSourceCache so the
// caller falls back to the non-cached put path.
func TestPrepareSourceCacheNoAnnotationDisabled(t *testing.T) {
	ctx := context.Background()
	s, _, cleanup := newSourceCacheSyncer(t)
	defer cleanup()

	prepared, err := s.prepareSourceCache(ctx, sourcecache.RowKindResources, "group", annotations.Annotations{})
	require.NoError(t, err)
	require.False(t, prepared.enabled)
}

// TestPrepareSourceCacheDisabledOnSyncerSkips covers the (sourceCacheEnabled
// == false) early return. Without it the syncer would call ParseKey on every
// page even when source caching is off.
func TestPrepareSourceCacheDisabledOnSyncerSkips(t *testing.T) {
	ctx := context.Background()
	s, _, cleanup := newSourceCacheSyncer(t)
	defer cleanup()
	s.sourceCacheEnabled = false

	scopeHash := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	key, err := sourcecache.BuildKey(scopeHash, `"etag"`)
	require.NoError(t, err)
	annos := annotations.Annotations{}
	annos.Update(&v2.SourceCacheEntry{Key: key})

	prepared, err := s.prepareSourceCache(ctx, sourcecache.RowKindResources, "group", annos)
	require.NoError(t, err)
	require.False(t, prepared.enabled, "annotation must be ignored when source caching is disabled on the syncer")
}
