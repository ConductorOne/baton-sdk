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

// TestPebbleStoreSyncMetaMarksDirty pins the same dirty-flag escape for
// the SyncMeta mutators: a standalone metadata stamp on a REOPENED c1z
// (nothing else set the dirty flag in the session) must survive Close.
// Before pebbleStore overrode SyncMeta(), MarkIngestInvariantsVerified
// wrote to the engine but Close skipped the envelope save and the
// marker vanished with the temp dir.
func TestPebbleStoreSyncMetaMarksDirty(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "meta-dirty.c1z")

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	verification := c1zstore.IngestInvariantVerification{
		Generation: "test-generation",
		Coverage:   []string{"I5"},
		Mode:       c1zstore.IngestInvariantVerificationModeConnector,
	}
	writer, ok := store.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	// The marker is only writable on a sealed sync.
	require.Error(t, writer.MarkIngestInvariantsVerified(ctx, syncID, verification),
		"marking an unfinished sync must be refused")
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	// Reopen CLEAN (no sync started, nothing dirties the store) and stamp
	// only the marker.
	store, err = NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	writer, ok = store.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	require.NoError(t, writer.MarkIngestInvariantsVerified(ctx, syncID, verification))
	require.NoError(t, store.Close(ctx))

	// The stamp-only session must have saved the envelope.
	store, err = NewStore(ctx, path, WithEngine(c1zstore.EnginePebble), WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Equal(t, verification, run.IngestInvariantVerification,
		"a standalone SyncMeta stamp must survive close/reopen")
}
