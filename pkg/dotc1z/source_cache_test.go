package dotc1z

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

func TestPebbleSourceCacheStoreSurface(t *testing.T) {
	ctx := t.Context()
	store, err := NewStore(ctx, filepath.Join(t.TempDir(), "source-cache.c1z"), WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	sc, ok := store.(SourceCacheStore)
	require.True(t, ok)

	require.NoError(t, sc.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a", "validator-a"))
	entry, found, err := sc.LookupSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "validator-a", entry.CacheValidator)
}
