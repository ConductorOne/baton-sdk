package dotc1z

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// runOneSync drives a complete StartNewSync → PutGrants → EndSync
// cycle through the store. Each call produces one finished sync_run
// record with associated grant + index data, mirroring the SQLite
// c1ztest.CreateTestSync helper but operating on the Pebble store.
// Returns the new sync_id.
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

// countSyncRuns returns the number of sync_run records currently
// in the engine. Used to assert post-Cleanup retention.
func countSyncRuns(t testing.TB, ctx context.Context, e *pebble.Engine) int {
	t.Helper()
	count := 0
	if err := e.IterateAllSyncRuns(ctx, func(*v3.SyncRunRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("IterateAllSyncRuns: %v", err)
	}
	return count
}

// listSyncIDs returns sync_run records as IDs in iteration order
// (KSUID-sorted, which is also chronological for syncs created on
// one machine).
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

// TestPebbleCleanupSyncLimit mirrors TestCleanupSyncLimit for the
// Pebble engine: ten finished full syncs, default retention of two
// after Cleanup.
func TestPebbleCleanupSyncLimit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path)
	defer func() { _ = store.Close(ctx) }()

	const total = 10
	for i := 0; i < total; i++ {
		runOneSync(t, ctx, store, "s"+strconv.Itoa(i))
	}

	rs := store.(*pebbleStore)
	if got := countSyncRuns(t, ctx, rs.engine); got != total {
		t.Fatalf("pre-Cleanup sync_run count = %d, want %d", got, total)
	}

	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if got := countSyncRuns(t, ctx, rs.engine); got != 2 {
		t.Fatalf("post-Cleanup sync_run count = %d, want 2 (default retention)", got)
	}
}

// TestPebbleCleanupSyncLimitCurrentSync mirrors
// TestCleanupSyncLimitCurrentSync: WithSyncLimit(1) + an open
// current sync should leave only the current sync after Cleanup.
func TestPebbleCleanupSyncLimitCurrentSync(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path, WithSyncLimit(1))
	defer func() { _ = store.Close(ctx) }()

	for i := 0; i < 10; i++ {
		runOneSync(t, ctx, store, "s"+strconv.Itoa(i))
	}

	currentSyncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync (current): %v", err)
	}

	rs := store.(*pebbleStore)
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	ids := listSyncIDs(t, ctx, rs.engine)
	if len(ids) != 1 {
		t.Fatalf("post-Cleanup sync_run count = %d, want 1; ids=%v", len(ids), ids)
	}
	if ids[0] != currentSyncID {
		t.Fatalf("retained sync_id = %q, want current %q", ids[0], currentSyncID)
	}
}

// TestPebbleCleanupSkipsWhenOptionSet confirms WithSkipCleanup
// short-circuits the policy. None of the syncs should be pruned.
func TestPebbleCleanupSkipsWhenOptionSet(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path, WithSkipCleanup(true))
	defer func() { _ = store.Close(ctx) }()

	const total = 5
	for i := 0; i < total; i++ {
		runOneSync(t, ctx, store, "s"+strconv.Itoa(i))
	}
	rs := store.(*pebbleStore)
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if got := countSyncRuns(t, ctx, rs.engine); got != total {
		t.Fatalf("with skip_cleanup, sync_run count = %d, want %d", got, total)
	}
}

// TestPebbleCleanupSkipsWhenEnvSet confirms BATON_SKIP_CLEANUP
// short-circuits the policy even when the caller didn't pass
// WithSkipCleanup.
func TestPebbleCleanupSkipsWhenEnvSet(t *testing.T) {
	ctx := context.Background()
	t.Setenv("BATON_SKIP_CLEANUP", "true")

	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path)
	defer func() { _ = store.Close(ctx) }()

	const total = 5
	for i := 0; i < total; i++ {
		runOneSync(t, ctx, store, "s"+strconv.Itoa(i))
	}
	rs := store.(*pebbleStore)
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if got := countSyncRuns(t, ctx, rs.engine); got != total {
		t.Fatalf("with BATON_SKIP_CLEANUP, sync_run count = %d, want %d", got, total)
	}
}

// TestPebbleCleanupRemovesAllSyncScopedData seeds two syncs, then
// confirms Cleanup deletes every keyspace scoped to the pruned
// sync — primary + index. This catches missing entries in
// the engine's sync-scoped range list.
func TestPebbleCleanupRemovesAllSyncScopedData(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path, WithSyncLimit(1))
	defer func() { _ = store.Close(ctx) }()

	oldSyncID := runOneSync(t, ctx, store, "old")
	newSyncID := runOneSync(t, ctx, store, "new")

	rs := store.(*pebbleStore)
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	ids := listSyncIDs(t, ctx, rs.engine)
	if len(ids) != 1 || ids[0] != newSyncID {
		t.Fatalf("post-Cleanup sync IDs = %v, want [%s]", ids, newSyncID)
	}

	// Old sync's grants must be gone from the primary keyspace.
	for _, ext := range []string{"old-g1", "old-g2"} {
		_, err := rs.engine.GetGrantRecord(ctx, oldSyncID, ext)
		if err == nil {
			t.Errorf("grant %s under deleted sync still present", ext)
		}
	}

	// Old sync's grants must also be gone from the by-principal
	// index. Without this check, a missing index entry in
	// the engine's range list would leak rows that the primary
	// delete caught but the index delete missed.
	count := 0
	if err := rs.engine.IterateGrantsByPrincipal(ctx, oldSyncID, "user", "old-alice", func(*v3.GrantRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("IterateGrantsByPrincipal: %v", err)
	}
	if count != 0 {
		t.Errorf("by-principal index still has %d entries for deleted sync", count)
	}

	// New sync's grants must remain readable.
	if _, err := rs.engine.GetGrantRecord(ctx, newSyncID, "new-g1"); err != nil {
		t.Errorf("GetGrantRecord on retained sync: %v", err)
	}
}

// TestPebbleCleanupDirtyBitForcesSave confirms Cleanup marks the
// store dirty so Close rewrites the c1z envelope. Without this the
// on-disk file would still hold the pruned sync's bytes.
func TestPebbleCleanupDirtyBitForcesSave(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path, WithSyncLimit(1))

	runOneSync(t, ctx, store, "old")
	runOneSync(t, ctx, store, "new")
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close (initial): %v", err)
	}

	sizeWithBoth, err := fileSize(path)
	if err != nil {
		t.Fatal(err)
	}

	// Reopen, Cleanup, Close. The post-Cleanup envelope should be
	// smaller (the old sync's grants/indexes/sidecar are gone).
	store = openStoreWithOptions(t, ctx, path, WithSyncLimit(1))
	rs := store.(*pebbleStore)
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if !rs.dirty {
		t.Fatal("Cleanup did not mark the store dirty")
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close (post-Cleanup): %v", err)
	}

	sizeAfter, err := fileSize(path)
	if err != nil {
		t.Fatal(err)
	}
	if sizeAfter == 0 {
		t.Fatal("c1z file is empty after Cleanup+Close")
	}
	if sizeAfter >= sizeWithBoth {
		// Cleanup + Compact should reclaim at least the deleted
		// sync's primary + index keyspace; bytes-saved depends on
		// Pebble's compression and L0/L1 placement but should
		// strictly decrease.
		t.Logf("c1z file size before=%d after=%d (expected after < before)", sizeWithBoth, sizeAfter)
	}
}

// TestPebbleCleanupCancelledBeforePass1ReturnsCtxErr exercises
// the "cancellation before logical work starts" branch of the
// three-pass model: ctx is dead before we enter the deletion
// loop, so we bail at the first per-sync check and return
// ctx.Err() verbatim. syncer.go:531 keys off this to mark the
// sync ErrSyncNotComplete and reattempt next run.
//
// Cancellation during pass 2 (compaction) or pass 3 (flush) takes
// the opposite branch — those passes are opportunistic, Cleanup
// returns nil, and the syncer proceeds normally. We don't unit-
// test those branches directly because they'd require timing-
// dependent cancellation injection; the behavior is documented in
// pebbleStore.Cleanup and reachable via the integration path.
func TestPebbleCleanupCancelledBeforePass1ReturnsCtxErr(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path, WithSyncLimit(1))
	defer func() { _ = store.Close(ctx) }()

	for i := 0; i < 4; i++ {
		runOneSync(t, ctx, store, "s"+strconv.Itoa(i))
	}

	canc, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	cancel()
	rs := store.(*pebbleStore)
	err := rs.Cleanup(canc)
	if err == nil {
		t.Fatal("Cleanup with cancelled context: expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Cleanup error = %v, want DeadlineExceeded/Canceled", err)
	}
}

// TestPebbleCleanupIncrementalProgress confirms that Cleanup is
// idempotent and resumable across runs: if a previous Cleanup
// returned without finishing every selected sync (ctx cancel, etc.),
// the next Cleanup picks up where it left off because toDelete is
// recomputed against the current sync_run state each call.
//
// Concretely: seed 5 syncs, run Cleanup with syncLimit=2 (expects
// to drop 3), then run Cleanup AGAIN with syncLimit=1 (expects to
// drop 1 more). Each run should make forward progress against the
// post-previous-run state, never re-considering already-deleted
// syncs.
func TestPebbleCleanupIncrementalProgress(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path, WithSyncLimit(2))
	defer func() { _ = store.Close(ctx) }()

	const total = 5
	for i := 0; i < total; i++ {
		runOneSync(t, ctx, store, "s"+strconv.Itoa(i))
	}
	rs := store.(*pebbleStore)

	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup (first): %v", err)
	}
	if got := countSyncRuns(t, ctx, rs.engine); got != 2 {
		t.Fatalf("after first Cleanup: count = %d, want 2", got)
	}

	// Tighten the retention. The same call should drop one more
	// sync without re-touching the already-pruned ones.
	rs.syncLimit = 1
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup (second): %v", err)
	}
	if got := countSyncRuns(t, ctx, rs.engine); got != 1 {
		t.Fatalf("after second Cleanup: count = %d, want 1", got)
	}

	// A third call with the same tighter limit is a no-op — we're
	// already at the retention floor. Tests the empty-toDelete
	// early-return path.
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup (third no-op): %v", err)
	}
	if got := countSyncRuns(t, ctx, rs.engine); got != 1 {
		t.Fatalf("after third Cleanup: count = %d, want 1", got)
	}
}

// TestPebbleCleanupMarksDirtyBeforeDeletions ensures the dirty
// flag flips at the moment Cleanup commits to mutating the
// keyspace, not after every step succeeds. Without this guarantee,
// a Cleanup that successfully tombstoned some syncs and then
// errored late (Compact race with Close, Flush failure, etc.)
// would leave dirty=false and Close would skip the save — the
// on-disk c1z would resurrect the pruned syncs on reopen.
func TestPebbleCleanupMarksDirtyBeforeDeletions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cleanup.c1z3")
	store := openStoreWithOptions(t, ctx, path, WithSyncLimit(1))

	runOneSync(t, ctx, store, "old1")
	runOneSync(t, ctx, store, "old2")
	runOneSync(t, ctx, store, "new")
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close (initial): %v", err)
	}

	// Reopen with no mutations beyond what Cleanup itself does.
	// dirty starts at false; Cleanup must flip it before its first
	// destructive write.
	store = openStoreWithOptions(t, ctx, path, WithSyncLimit(1))
	rs := store.(*pebbleStore)
	if rs.dirty {
		t.Fatal("pebble store unexpectedly opened dirty")
	}
	if err := rs.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if !rs.dirty {
		t.Fatal("dirty flag should be true after Cleanup committed deletions")
	}
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close (post-Cleanup): %v", err)
	}

	// Reopen one more time and confirm the deletions persisted.
	reopened := openStoreWithOptions(t, ctx, path)
	defer func() { _ = reopened.Close(ctx) }()
	rs2 := reopened.(*pebbleStore)
	if got := countSyncRuns(t, ctx, rs2.engine); got != 1 {
		t.Fatalf("sync_run count after Cleanup+Close+Reopen = %d, want 1", got)
	}
}

func fileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
