package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err, "NewStore")
	defer store.Close(ctx)

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")
	require.NoError(t, store.EndSync(ctx), "EndSync")

	_, err = store.FileOps().GenerateSyncDiff(ctx, syncID, "some-other-sync")
	require.ErrorIs(t, err, pebble.ErrDiffUnsupported, "GenerateSyncDiff error = %v, want ErrDiffUnsupported", err)
}
