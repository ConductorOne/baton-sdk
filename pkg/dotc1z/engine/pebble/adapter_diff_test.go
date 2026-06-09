package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestGenerateSyncDiffAdditionsOnly walks the additions-only diff
// shape end-to-end: two finished syncs, the second adding one
// grant beyond the first. The diff sync must materialize exactly
// the new grant.
func TestGenerateSyncDiffAdditionsOnly(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "diff.c1z")

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close(ctx)

	baseSync, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync base: %v", err)
	}
	if err := store.PutGrants(ctx,
		mkV2Grant("g1", "ent", "user", "alice"),
		mkV2Grant("g2", "ent", "user", "bob"),
	); err != nil {
		t.Fatalf("PutGrants base: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync base: %v", err)
	}

	appliedSync, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync applied: %v", err)
	}
	if err := store.PutGrants(ctx,
		mkV2Grant("g1", "ent", "user", "alice"),
		mkV2Grant("g2", "ent", "user", "bob"),
		mkV2Grant("g3", "ent", "user", "carol"),
	); err != nil {
		t.Fatalf("PutGrants applied: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync applied: %v", err)
	}

	diffID, err := store.FileOps().GenerateSyncDiff(ctx, baseSync, appliedSync)
	if err != nil {
		t.Fatalf("GenerateSyncDiff: %v", err)
	}
	if diffID == "" {
		t.Fatal("GenerateSyncDiff returned empty diffID")
	}

	if err := store.SetCurrentSync(ctx, diffID); err != nil {
		t.Fatalf("SetCurrentSync diff: %v", err)
	}
	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("ListGrants diff: %v", err)
	}
	if got := len(resp.GetList()); got != 1 {
		t.Fatalf("diff ListGrants = %d, want 1", got)
	}
	if got := resp.GetList()[0].GetId(); got != "g3" {
		t.Fatalf("diff grant id = %q, want g3", got)
	}
}

// TestGenerateSyncDiffRejectsSameSyncIDs validates that
// generating a diff between a sync and itself is an error —
// the empty-result path would silently produce a sync_run
// row with no records, which we'd rather flag.
func TestGenerateSyncDiffRejectsSameSyncIDs(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "diff.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FileOps().GenerateSyncDiff(ctx, syncID, syncID); err == nil {
		t.Fatal("expected error for same base/applied syncIDs")
	}
}
