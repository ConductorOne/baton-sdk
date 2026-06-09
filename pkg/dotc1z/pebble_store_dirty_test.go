package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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

	store, err := NewStore(ctx, path, WithEngine(EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	// The only mutating GrantStore method.
	if err := store.Grants().StoreExpandedGrants(ctx,
		mkV2Grant("g1", "ent", "user", "alice"),
	); err != nil {
		t.Fatalf("StoreExpandedGrants: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close should have saved a c1z to outPath; if the dirty flag
	// stayed false the save would skip and outPath would be empty/missing.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("clone stat: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatalf("c1z size = 0; pebble store didn't flush after StoreExpandedGrants")
	}
}
