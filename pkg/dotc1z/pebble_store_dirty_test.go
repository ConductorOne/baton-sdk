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

// TestPebbleStorePageCommitMarksDirty pins the dirty flag for the page
// unit: a sync whose only writes went through BeginPage().Commit must
// still be saved into the c1z at Close.
func TestPebbleStorePageCommitMarksDirty(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dirty-page.c1z")

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	ledger, ok := store.(c1zstore.PageLedgerStore)
	require.True(t, ok, "the pebble store implements the page ledger")
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	w := ledger.BeginPage()
	require.NoError(t, w.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	id := c1zstore.LedgerActionIdentity{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github"}
	require.NoError(t, w.Commit(ctx, id, nil))
	_, found, err := ledger.GetLedgerRow(ctx, id)
	require.NoError(t, err)
	require.True(t, found)

	stats, ok := store.(c1zstore.SyncStatsStore)
	require.True(t, ok, "the pebble store implements the stats side of the ledger")
	require.NoError(t, stats.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	require.NoError(t, store.Close(ctx))
	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.NotZero(t, fi.Size(), "c1z size = 0; pebble store didn't flush after a page commit")

	// The row survives the save/reopen round trip as part of the file.
	reopened, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()
	_, found, err = reopened.(c1zstore.PageLedgerStore).GetLedgerRow(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
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
