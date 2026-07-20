package pebble

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// newTestEngine opens an engine in a fresh temp dir and registers
// cleanup. Returns the engine + the dir for later inspection.
func newTestEngine(t testing.TB, opts ...Option) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	e, err := Open(context.Background(), dbDir, opts...)
	require.NoError(t, err, "Open")
	t.Cleanup(func() {
		_ = e.Close()
	})
	return e, dir
}

// regularFileSizeUnder is the independent oracle for
// CurrentDBSizeBytes: a walk-and-sum over regular files. It MUST stat
// through os.Lstat — the same mechanism production uses — not
// WalkDir's DirEntry.Info: on Windows the two read different size
// views of a file with an open write handle (DirEntry.Info comes from
// FindFirstFile directory metadata; Lstat's GetFileAttributesEx fast
// path lags it), and pebble's active WAL is exactly such a file, so a
// mixed-mechanism comparison diverges by however much WAL the sync
// appended (caught by Windows CI). Same-mechanism sides make the
// comparison OS-independent while still independently checking the
// recursion, the regular-file filter, and the summing.
func regularFileSizeUnder(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	require.NoError(t, err, "walk %s", dir)
	return total
}

// makeGrant returns a fully-populated GrantRecord under the given sync.
// The entitlement ref carries the SDK-shaped raw id ("app:github:"+entID) so
// fixtures match what connectors actually store and bare-id lookups resolve.
func makeGrant(syncID, externalID, entID, principalID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  canonicalTestEntID(entID),
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     principalID,
		}.Build(),
	}.Build()
}

// testGrantByIdentity fetches a grant by its structural refs — the exact
// path for fixtures whose stored external id is connector-custom (not the
// concat shape) and therefore not addressable by bare id string.
func testGrantByIdentity(ctx context.Context, e *Engine, entID, principalRT, principalID string) (*v3.GrantRecord, error) {
	id := grantIdentity{
		entitlement:     entitlementIdentityFromParts("app", "github", canonicalTestEntID(entID)),
		principalTypeID: principalRT,
		principalID:     principalID,
	}
	return getGrantByIdentity(ctx, e.db, id)
}

func TestEngineOpenClose(t *testing.T) {
	e, _ := newTestEngine(t)
	require.NotNil(t, e.db, "db is nil after Open")
}

func TestEngineCurrentDBSizeBytes(t *testing.T) {
	ctx := context.Background()
	e, rootDir := newTestEngine(t)
	dbDir := filepath.Join(rootDir, "db")

	initial, err := e.CurrentDBSizeBytes()
	require.NoError(t, err, "CurrentDBSizeBytes initial")
	require.Greater(t, initial, int64(0), "CurrentDBSizeBytes initial")
	want := regularFileSizeUnder(t, dbDir)
	require.Equal(t, want, initial, "CurrentDBSizeBytes initial")

	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))
	for i := 0; i < 50; i++ {
		err := e.PutGrantRecord(ctx, makeGrant(syncID, ksuid.New().String(), "github-read", ksuid.New().String()))
		require.NoError(t, err, "PutGrantRecord")
	}

	after, err := e.CurrentDBSizeBytes()
	require.NoError(t, err, "CurrentDBSizeBytes after writes")
	want = regularFileSizeUnder(t, dbDir)
	require.Equal(t, want, after, "CurrentDBSizeBytes after writes")
	require.GreaterOrEqual(t, after, initial, "CurrentDBSizeBytes after writes")
}

func TestPutGetGrantRecord(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))

	// Custom stored external id: addressable only by identity, not by the
	// concat id string (which no longer equals the stored public id).
	r := makeGrant(syncID, "grant-1", "github-read", "alice")
	require.NoError(t, e.PutGrantRecord(ctx, r), "Put")
	got, err := testGrantByIdentity(ctx, e, "github-read", "user", "alice")
	require.NoError(t, err, "get by identity")
	require.Equal(t, "grant-1", got.GetExternalId(), "external_id")

	// SDK-shaped external id (the concat): addressable by bare id string.
	sdkID := canonicalTestGrantID("github-write", "user", "alice")
	r2 := makeGrant(syncID, sdkID, "github-write", "alice")
	require.NoError(t, e.PutGrantRecord(ctx, r2), "Put sdk-shaped")
	got2, err := e.GetGrantRecord(ctx, sdkID)
	require.NoError(t, err, "Get")
	require.Equal(t, sdkID, got2.GetExternalId(), "external_id")
	require.Equal(t, canonicalTestEntID("github-write"), got2.GetEntitlement().GetEntitlementId(), "entitlement")
	require.Equal(t, "alice", got2.GetPrincipal().GetResourceId(), "principal")
}

func TestIterateGrantsBySync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))

	const n = 100
	for i := 0; i < n; i++ {
		r := makeGrant(syncID,
			ksuid.New().String(),
			"github-read",
			ksuid.New().String(),
		)
		require.NoError(t, e.PutGrantRecord(ctx, r))
	}

	count := 0
	err := e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, n, count, "got %d grants, want %d", count, n)
}

func TestIterateByEntitlement(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))

	// 5 grants on entitlement A, 3 on entitlement B.
	for i := 0; i < 5; i++ {
		r := makeGrant(syncID, ksuid.New().String(), "ent-A", ksuid.New().String())
		require.NoError(t, e.PutGrantRecord(ctx, r))
	}
	for i := 0; i < 3; i++ {
		r := makeGrant(syncID, ksuid.New().String(), "ent-B", ksuid.New().String())
		require.NoError(t, e.PutGrantRecord(ctx, r))
	}

	var a, b int
	err := e.IterateGrantsByEntitlement(ctx, canonicalTestEntID("ent-A"), func(r *v3.GrantRecord) bool {
		a++
		return true
	})
	require.NoError(t, err)
	err = e.IterateGrantsByEntitlement(ctx, canonicalTestEntID("ent-B"), func(r *v3.GrantRecord) bool {
		b++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 5, a, "ent-A")
	require.Equal(t, 3, b, "ent-B")
}

func TestIterateByPrincipal(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))

	const alicePrincipal = "alice"
	const bobPrincipal = "bob"

	// 4 grants for alice on different entitlements, 2 for bob.
	for i := 0; i < 4; i++ {
		r := makeGrant(syncID, ksuid.New().String(), ksuid.New().String(), alicePrincipal)
		require.NoError(t, e.PutGrantRecord(ctx, r))
	}
	for i := 0; i < 2; i++ {
		r := makeGrant(syncID, ksuid.New().String(), ksuid.New().String(), bobPrincipal)
		require.NoError(t, e.PutGrantRecord(ctx, r))
	}

	var a, b int
	err := e.IterateGrantsByPrincipal(ctx, "user", alicePrincipal, func(r *v3.GrantRecord) bool {
		a++
		return true
	})
	require.NoError(t, err)
	err = e.IterateGrantsByPrincipal(ctx, "user", bobPrincipal, func(r *v3.GrantRecord) bool {
		b++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 4, a, "alice")
	require.Equal(t, 2, b, "bob")
}

func TestDeleteGrantRecord(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))

	r := makeGrant(syncID, canonicalTestGrantID("ent-X", "user", "user-X"), "ent-X", "user-X")
	require.NoError(t, e.PutGrantRecord(ctx, r))

	// Delete it. Both the primary and the index entries should go.
	require.NoError(t, e.DeleteGrantRecord(ctx, canonicalTestGrantID("ent-X", "user", "user-X")))

	_, err := e.GetGrantRecord(ctx, canonicalTestGrantID("ent-X", "user", "user-X"))
	require.ErrorIs(t, err, pebble.ErrNotFound)

	count := 0
	err = e.IterateGrantsByEntitlement(ctx, canonicalTestEntID("ent-X"), func(*v3.GrantRecord) bool {
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 0, count, "expected 0 entries after delete, got %d", count)
}

func TestCheckpointToReadOnly(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))
	r := makeGrant(syncID, canonicalTestGrantID("e1", "user", "p1"), "e1", "p1")
	require.NoError(t, e.PutGrantRecord(ctx, r))
	require.NoError(t, e.Close())

	dbDir := filepath.Join(dir, "db")
	e, err := Open(ctx, dbDir, WithReadOnly(true))
	require.NoError(t, err, "Open read-only")
	t.Cleanup(func() { _ = e.Close() })

	ckDir := filepath.Join(dir, "checkpoint")
	err = e.CheckpointTo(ctx, ckDir)
	require.NoError(t, err, "CheckpointTo read-only")

	e2, err := Open(ctx, ckDir, WithReadOnly(true))
	require.NoError(t, err, "reopen checkpoint")
	defer e2.Close()
	got, err := e2.GetGrantRecord(ctx, canonicalTestGrantID("e1", "user", "p1"))
	require.NoError(t, err, "checkpoint Get")
	require.Equal(t, canonicalTestEntID("e1"), got.GetEntitlement().GetEntitlementId(), "checkpoint Get returned wrong entitlement")
}

func TestCheckpointTo(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))

	r := makeGrant(syncID, canonicalTestGrantID("e1", "user", "p1"), "e1", "p1")
	require.NoError(t, e.PutGrantRecord(ctx, r))

	ckDir := filepath.Join(dir, "checkpoint")
	require.NoError(t, e.CheckpointTo(ctx, ckDir), "CheckpointTo")

	// Open the checkpoint as a fresh engine and verify the grant is there.
	e2, err := Open(ctx, ckDir, WithReadOnly(true))
	require.NoError(t, err, "reopen checkpoint")
	defer e2.Close()
	got, err := e2.GetGrantRecord(ctx, canonicalTestGrantID("e1", "user", "p1"))
	require.NoError(t, err)

	require.Equal(t, canonicalTestEntID("e1"), got.GetEntitlement().GetEntitlementId(), "checkpoint Get returned wrong entitlement")

	r2 := makeGrant(syncID, "g2", "e2", "p2")
	err = e.PutGrantRecord(ctx, r2)
	require.NoError(t, err, "expected Put after CheckpointTo to succeed")
}

func TestSaveDoesNotCloseOnError(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	syncID := ksuid.New().String()
	err := e.bindCurrentSync(syncID)
	require.NoError(t, err)

	err = e.Save(ctx, filepath.Join(dir, "out.c1z3"))
	require.Error(t, err, "expected Save error")

	r := makeGrant(syncID, "after-save", "e1", "p1")
	err = e.PutGrantRecord(ctx, r)
	require.NoError(t, err, "expected Put after failed Save to succeed")
}

func TestConcurrentGrantOverwriteIndexes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	err := e.bindCurrentSync(syncID)
	require.NoError(t, err)

	const writes = 64
	start := make(chan struct{})
	errs := make(chan error, writes)
	var wg sync.WaitGroup
	for i := range writes {
		wg.Add(1)
		entID := "old"
		if i == writes-1 {
			entID = "final"
		}
		go func(entID string) {
			defer wg.Done()
			<-start
			errs <- e.PutGrantRecord(ctx, makeGrant(syncID, "same-grant", entID, "principal"))
		}(entID)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	counts := map[string]int{"old": 0, "final": 0}
	for entID := range counts {
		err := e.IterateGrantsByEntitlement(ctx, canonicalTestEntID(entID), func(*v3.GrantRecord) bool {
			counts[entID]++
			return true
		})
		require.NoError(t, err)
	}
	require.Equal(t, 1, counts["old"], "same structured identity collapses")
	require.Equal(t, 1, counts["final"], "final entitlement")
}

func TestEmptySyncIDFallsBackToCurrent(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))

	// Put with explicit sync id...
	sdkID := canonicalTestGrantID("e1", "user", "p1")
	r := makeGrant(syncID, sdkID, "e1", "p1")
	require.NoError(t, e.PutGrantRecord(ctx, r))
	// ...and Get with empty sync id (should use the engine's current).
	got, err := e.GetGrantRecord(ctx, sdkID)
	require.NoError(t, err, "Get with empty syncID")
	require.Equal(t, sdkID, got.GetExternalId(), "unexpected: %v", got)
}

func TestWriteWithNoCurrentSyncFails(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	// No StartNewSync/SetCurrentSync: nothing is bound, so a record
	// write must be refused rather than landing without a sync run.
	// (Reads do not gate on the binding — an empty store simply
	// returns not-found.)
	err := e.PutGrantRecord(ctx, makeGrant("", "anything", "e1", "p1"))
	require.ErrorIs(t, err, ErrNoCurrentSync, "expected ErrNoCurrentSync, got %v", err)
}
