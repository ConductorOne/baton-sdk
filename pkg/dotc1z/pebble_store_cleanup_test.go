package dotc1z

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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
	if err != nil {
		t.Fatalf("StartNewSync(%s): %v", label, err)
	}
	if err := store.PutGrants(ctx,
		mkV2Grant(label+"-g1", "ent", "user", label+"-alice"),
		mkV2Grant(label+"-g2", "ent", "user", label+"-bob"),
	); err != nil {
		t.Fatalf("PutGrants(%s): %v", label, err)
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatalf("EndSync(%s): %v", label, err)
	}
	return syncID
}

// listSyncIDs returns sync_run records as IDs in iteration order. Under
// the single-sync contract this is always 0 or 1 entries.
func listSyncIDs(t testing.TB, ctx context.Context, e *pebble.Engine) []string {
	t.Helper()
	var ids []string
	if err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		ids = append(ids, r.GetSyncId())
		return true
	}); err != nil {
		t.Fatalf("IterateAllSyncRuns: %v", err)
	}
	return ids
}

// openStoreWithOptions opens a fresh Pebble-backed store at path
// with the supplied C1ZOption set, plus the engine selector.
// The caller is responsible for closing it.
func openStoreWithOptions(t testing.TB, ctx context.Context, path string, opts ...C1ZOption) connectorstore.Writer {
	t.Helper()
	allOpts := append([]C1ZOption{WithEngine(EnginePebble)}, opts...)
	store, err := NewStore(ctx, path, allOpts...)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
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
	ids := listSyncIDs(t, ctx, rs.engine)
	if len(ids) != 1 {
		t.Fatalf("sync_run count = %d, want 1 (single-sync contract); ids=%v", len(ids), ids)
	}
	if ids[0] != lastID {
		t.Fatalf("retained sync_id = %q, want most recent %q", ids[0], lastID)
	}
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
	ids := listSyncIDs(t, ctx, rs.engine)
	if len(ids) != 1 || ids[0] != newSyncID {
		t.Fatalf("post-replace sync IDs = %v, want [%s]", ids, newSyncID)
	}

	// Old sync's grants must be gone from the primary keyspace.
	for _, ext := range []string{"old-g1", "old-g2"} {
		if _, err := rs.engine.GetGrantRecord(ctx, ext); err == nil {
			t.Errorf("grant %s from the replaced sync still present", ext)
		}
	}

	// Old sync's by-principal index entries must be gone too — a missing
	// wipe range would leak index keys the primary delete caught.
	count := 0
	if err := rs.engine.IterateGrantsByPrincipal(ctx, "user", "old-alice", func(*v3.GrantRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("IterateGrantsByPrincipal: %v", err)
	}
	if count != 0 {
		t.Errorf("by-principal index still has %d entries from the replaced sync", count)
	}

	// New sync's grants must remain readable.
	if _, err := rs.engine.GetGrantRecord(ctx, "new-g1"); err != nil {
		t.Errorf("GetGrantRecord on current sync: %v", err)
	}
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

	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if rs.dirty {
		t.Error("Cleanup marked the store dirty; expected a no-op")
	}
	ids := listSyncIDs(t, ctx, rs.engine)
	if len(ids) != 1 || ids[0] != syncID {
		t.Fatalf("post-Cleanup sync IDs = %v, want [%s]", ids, syncID)
	}
}
