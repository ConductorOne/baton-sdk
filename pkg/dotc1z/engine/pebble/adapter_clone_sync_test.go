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

// TestCloneSyncRoundtrip writes a small sync to a Pebble-backed
// c1z, calls FileOps().CloneSync to materialize it at a new path,
// then re-opens the cloned file and verifies the grants land
// intact. Covers the basic byte-level copy + envelope-write path.
func TestCloneSyncRoundtrip(t *testing.T) {
	ctx := context.Background()
	if err := Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatalf("NewStore src: %v", err)
	}
	if _, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := src.PutGrants(ctx,
		mkV2Grant("g1", "ent-A", "user", "alice"),
		mkV2Grant("g2", "ent-B", "user", "bob"),
	); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}
	if err := src.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	srcStore, ok := src.(dotc1z.C1ZStore)
	if !ok {
		t.Fatalf("src is %T, want dotc1z.C1ZStore", src)
	}

	clonePath := filepath.Join(tmp, "clone.c1z")
	if err := srcStore.FileOps().CloneSync(ctx, clonePath, ""); err != nil {
		t.Fatalf("CloneSync: %v", err)
	}
	if _, err := os.Stat(clonePath); err != nil {
		t.Fatalf("clone file missing: %v", err)
	}

	if err := srcStore.Close(ctx); err != nil {
		t.Fatalf("close src: %v", err)
	}

	// Re-open the clone and confirm the grants are present.
	cloneStore, err := dotc1z.NewStore(ctx, clonePath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	if err != nil {
		t.Fatalf("NewStore clone: %v", err)
	}
	defer cloneStore.Close(ctx)

	latest, ok := cloneStore.(connectorstore.LatestFinishedSyncIDFetcher)
	if !ok {
		t.Fatalf("clone is %T, want LatestFinishedSyncIDFetcher", cloneStore)
	}
	syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	if err != nil {
		t.Fatalf("LatestFinishedSyncID: %v", err)
	}
	if syncID == "" {
		t.Fatal("clone has no finished sync")
	}
	if err := cloneStore.SetCurrentSync(ctx, syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	resp, err := cloneStore.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("ListGrants: %v", err)
	}
	if got := len(resp.GetList()); got != 2 {
		t.Fatalf("clone ListGrants = %d, want 2", got)
	}
}

// TestCloneSyncRefusesExistingOutPath validates the "must not
// exist" precondition on outPath — clobbering an existing file
// silently is a hazard.
func TestCloneSyncRefusesExistingOutPath(t *testing.T) {
	ctx := context.Background()
	if err := Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}
	if err := src.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")); err != nil {
		t.Fatal(err)
	}
	if err := src.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close(ctx)

	existing := filepath.Join(tmp, "exists.c1z")
	if err := os.WriteFile(existing, []byte("blocker"), 0o600); err != nil {
		t.Fatal(err)
	}
	srcStore := src.(dotc1z.C1ZStore)
	err = srcStore.FileOps().CloneSync(ctx, existing, "")
	if err == nil {
		t.Fatal("CloneSync over existing file: want error, got nil")
	}
}

// TestCloneSyncRefusesUnfinishedSync validates the "sync must be
// ended" precondition. A still-running sync can't be cloned
// because its records may be in flux.
func TestCloneSyncRefusesUnfinishedSync(t *testing.T) {
	ctx := context.Background()
	if err := Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatal(err)
	}
	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close(ctx)

	// No EndSync — sync is still running.
	srcStore := src.(dotc1z.C1ZStore)
	out := filepath.Join(tmp, "clone.c1z")
	if err := srcStore.FileOps().CloneSync(ctx, out, syncID); err == nil {
		t.Fatal("CloneSync of unfinished sync: want error, got nil")
	}
}
