package dotc1z

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// runOneSync drives a complete StartNewSync → PutGrants → EndSync
// cycle through the store. Each call produces one finished sync_run
// record with associated grant + index data. Under the single-sync
// contract a fresh StartNewSync first wipes any prior sync, so the
// store always ends holding exactly the sync this call wrote. Returns
// the new sync_id.
func runOneSync(t testing.TB, ctx context.Context, store connectorstore.Writer, label string) string {
	t.Helper()
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync(%s): %v", label, err)
	err = store.PutGrants(ctx,
		mkV2Grant(label+"-g1", "ent", "user", label+"-alice"),
		mkV2Grant(label+"-g2", "ent", "user", label+"-bob"),
	)
	require.NoError(t, err, "PutGrants(%s): %v", label, err)
	err = store.EndSync(ctx)
	require.NoError(t, err, "EndSync(%s): %v", label, err)
	return syncID
}

// listSyncIDs returns sync_run records as IDs in iteration order. Under
// the single-sync contract this is always 0 or 1 entries.
func listSyncIDs(t testing.TB, ctx context.Context, e *pebble.Engine) []string {
	t.Helper()
	var ids []string
	err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		ids = append(ids, r.GetSyncId())
		return true
	})
	require.NoError(t, err, "IterateAllSyncRuns: %v", err)
	return ids
}

// openStoreWithOptions opens a fresh Pebble-backed store at path
// with the supplied C1ZOption set, plus the engine selector.
// The caller is responsible for closing it.
func openStoreWithOptions(t testing.TB, ctx context.Context, path string, opts ...C1ZOption) connectorstore.Writer {
	t.Helper()
	allOpts := append([]C1ZOption{WithEngine(c1zstore.EnginePebble)}, opts...)
	store, err := NewStore(ctx, path, allOpts...)
	require.NoError(t, err, "NewStore: %v", err)
	return store
}

// TestPebbleSingleSyncReplaces pins the single-sync contract: running N
// syncs sequentially leaves exactly one sync_run — the most recent —
// because each StartNewSync wipes the prior sync in place.
func TestPebbleSingleSyncReplaces(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "single.c1z3")
	store := openStoreWithOptions(t, ctx, path)
	defer func() { _ = store.Close(ctx) }()

	const total = 10
	var lastID string
	for i := 0; i < total; i++ {
		lastID = runOneSync(t, ctx, store, "s"+strconv.Itoa(i))
	}

	rs := store.(*pebbleStore)
	ids := listSyncIDs(t, ctx, rs.Engine)
	require.Len(t, ids, 1, "sync_run count = %d, want 1 (single-sync contract); ids=%v", len(ids), ids)
	require.Equal(t, lastID, ids[0], "retained sync_id = %q, want most recent %q", ids[0], lastID)
}

// TestPebbleSecondSyncWipesPriorData confirms a replacement sync leaves
// no trace of the prior one: the old sync's primary records and its
// secondary-index entries are gone (the keyspace carries no sync_id, so
// an un-wiped record would leak under the new sync), the new sync's
// records are present, and exactly one sync_run remains.
func TestPebbleSecondSyncWipesPriorData(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replace.c1z3")
	store := openStoreWithOptions(t, ctx, path)
	defer func() { _ = store.Close(ctx) }()

	runOneSync(t, ctx, store, "old")
	newSyncID := runOneSync(t, ctx, store, "new")

	rs := store.(*pebbleStore)
	ids := listSyncIDs(t, ctx, rs.Engine)
	require.Len(t, ids, 1)
	require.Equal(t, newSyncID, ids[0], "post-replace sync IDs = %v, want [%s]", ids, newSyncID)

	// Old sync's grants must be gone from the primary keyspace.
	for _, ext := range []string{"old-g1", "old-g2"} {
		_, err := rs.GetGrantRecord(ctx, ext)
		require.Error(t, err, "grant %s from the replaced sync still present", ext)
	}

	// Old sync's by-principal index entries must be gone too — a missing
	// wipe range would leak index keys the primary delete caught.
	count := 0
	err := rs.IterateGrantsByPrincipal(ctx, "user", "old-alice", func(*v3.GrantRecord) bool {
		count++
		return true
	})
	require.NoError(t, err, "IterateGrantsByPrincipal: %v", err)
	require.Zero(t, count, "by-principal index still has %d entries from the replaced sync", count)

	// New sync's grants must remain readable.
	_, err = rs.GetGrantRecord(ctx, mkV2GrantID("ent", "user", "new-alice"))
	require.NoError(t, err, "GetGrantRecord on current sync: %v", err)
}

// TestPebbleCleanupIsNoOp confirms Cleanup is inert on the single-sync
// engine: it returns nil, does not mark the store dirty (nothing to
// prune), and leaves the one sync intact. Retention options
// (WithSyncLimit, BATON_SKIP_CLEANUP) no longer drive any behavior.
func TestPebbleCleanupIsNoOp(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup-noop.c1z3")
	store := openStoreWithOptions(t, ctx, path)
	defer func() { _ = store.Close(ctx) }()

	syncID := runOneSync(t, ctx, store, "only")
	rs := store.(*pebbleStore)
	rs.dirty = false

	err := rs.Cleanup(ctx)
	require.NoError(t, err, "Cleanup: %v", err)
	require.False(t, rs.dirty, "Cleanup marked the store dirty; expected a no-op")
	ids := listSyncIDs(t, ctx, rs.Engine)
	require.Len(t, ids, 1)
	require.Equal(t, syncID, ids[0], "post-Cleanup sync IDs = %v, want [%s]", ids, syncID)
}
