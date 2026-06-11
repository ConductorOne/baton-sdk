package pebble_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestGenerateSyncDiffUnsupported pins the single-sync contract: a v3
// Pebble c1z holds exactly one sync, so GenerateSyncDiff (which needs a
// base + applied sync co-resident in one file) is unsupported and must
// return pebble.ErrDiffUnsupported rather than silently producing a
// bogus or empty diff.
func TestGenerateSyncDiffUnsupported(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "diff.c1z")

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close(ctx)

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}

	if _, err := store.FileOps().GenerateSyncDiff(ctx, syncID, "some-other-sync"); !errors.Is(err, pebble.ErrDiffUnsupported) {
		t.Fatalf("GenerateSyncDiff error = %v, want ErrDiffUnsupported", err)
	}
}
