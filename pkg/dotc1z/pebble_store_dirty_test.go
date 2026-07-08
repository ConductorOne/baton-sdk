package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestPebbleStoreGrantsStoreExpandedGrantsMarksDirty exercises
// the dirty-flag escape that the PR review bot flagged: writes that
// went through Grants().StoreExpandedGrants() were calling
// Adapter.PutGrants directly (bypassing pebbleStore's dirty
// flag), so Close would skip the c1z save. The fix overrides
// Grants() on pebbleStore to wrap StoreExpandedGrants through
// the dirty-marking path.
func TestPebbleStoreGrantsStoreExpandedGrantsMarksDirty(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "dirty.c1z")

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	// The only mutating GrantStore method.
	require.NoError(t, store.Grants().StoreExpandedGrants(ctx,
		mkV2Grant("g1", "ent", "user", "alice"),
	))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	// Close should have saved a c1z to outPath; if the dirty flag
	// stayed false the save would skip and outPath would be empty/missing.
	fi, err := os.Stat(path)
	require.NoError(t, err, "clone stat: %v", err)
	require.NotZero(t, fi.Size(), "c1z size = 0; pebble store didn't flush after StoreExpandedGrants")
}
