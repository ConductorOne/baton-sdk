package pebble

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestRegisteredStoreGrantsStoreExpandedGrantsMarksDirty exercises
// the dirty-flag escape that the PR review bot flagged: writes that
// went through Grants().StoreExpandedGrants() were calling
// Adapter.PutGrants directly (bypassing registeredStore's dirty
// flag), so Close would skip the c1z save. The fix overrides
// Grants() on registeredStore to wrap StoreExpandedGrants through
// the dirty-marking path.
func TestRegisteredStoreGrantsStoreExpandedGrantsMarksDirty(t *testing.T) {
	ctx := context.Background()
	if err := Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tmp := t.TempDir()
	path := filepath.Join(tmp, "dirty.c1z")

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	c1z := store.(dotc1z.C1ZStore)
	// The only mutating GrantStore method.
	if err := c1z.Grants().StoreExpandedGrants(ctx,
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
		t.Fatalf("c1z size = 0; registered store didn't flush after StoreExpandedGrants")
	}
}

// TestRegisteredStorePutGrantsIfNewerMarksDirty does the same for
// the PutGrantsIfNewer escape (and by induction the other 3
// *IfNewer methods, which share the same override pattern).
func TestRegisteredStorePutGrantsIfNewerMarksDirty(t *testing.T) {
	ctx := context.Background()
	if err := Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tmp := t.TempDir()
	path := filepath.Join(tmp, "dirty-if-newer.c1z")

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	type ifNewer interface {
		PutGrantsIfNewer(ctx context.Context, grants ...*v2.Grant) error
	}
	in, ok := store.(ifNewer)
	if !ok {
		t.Fatalf("registered store missing PutGrantsIfNewer")
	}
	if err := in.PutGrantsIfNewer(ctx, mkV2Grant("g1", "ent", "user", "alice")); err != nil {
		t.Fatalf("PutGrantsIfNewer: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("c1z stat: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatalf("c1z size = 0; registered store didn't flush after PutGrantsIfNewer")
	}
}
