package pebble

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// newTestEngine opens an engine in a fresh temp dir and registers
// cleanup. Returns the engine + the dir for later inspection.
func newTestEngine(t testing.TB, opts ...Option) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	e, err := Open(context.Background(), dbDir, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = e.Close()
	})
	return e, dir
}

func regularFileSizeUnder(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	}); err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return total
}

// makeGrant returns a fully-populated GrantRecord under the given sync.
func makeGrant(syncID, externalID, entID, principalID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  entID,
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     principalID,
		}.Build(),
	}.Build()
}

func TestEngineOpenClose(t *testing.T) {
	e, _ := newTestEngine(t)
	if e.db == nil {
		t.Fatal("db is nil after Open")
	}
}

func TestEngineCurrentDBSizeBytes(t *testing.T) {
	ctx := context.Background()
	e, rootDir := newTestEngine(t)
	dbDir := filepath.Join(rootDir, "db")

	initial, err := e.CurrentDBSizeBytes()
	if err != nil {
		t.Fatalf("CurrentDBSizeBytes initial: %v", err)
	}
	if initial <= 0 {
		t.Fatalf("CurrentDBSizeBytes initial = %d, want > 0", initial)
	}
	if want := regularFileSizeUnder(t, dbDir); initial != want {
		t.Fatalf("CurrentDBSizeBytes initial = %d, want regular file sum %d", initial, want)
	}

	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := e.PutGrantRecord(ctx, makeGrant(syncID, ksuid.New().String(), "github-read", ksuid.New().String())); err != nil {
			t.Fatalf("PutGrantRecord: %v", err)
		}
	}

	after, err := e.CurrentDBSizeBytes()
	if err != nil {
		t.Fatalf("CurrentDBSizeBytes after writes: %v", err)
	}
	if want := regularFileSizeUnder(t, dbDir); after != want {
		t.Fatalf("CurrentDBSizeBytes after writes = %d, want regular file sum %d", after, want)
	}
	if after < initial {
		t.Fatalf("CurrentDBSizeBytes after writes = %d, want >= initial %d", after, initial)
	}
}

func TestPutGetGrantRecord(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	r := makeGrant(syncID, "grant-1", "github-read", "alice")
	if err := e.PutGrantRecord(ctx, r); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := e.GetGrantRecord(ctx, "grant-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetExternalId() != "grant-1" {
		t.Errorf("external_id: got %q", got.GetExternalId())
	}
	if got.GetEntitlement().GetEntitlementId() != "github-read" {
		t.Errorf("entitlement: got %q", got.GetEntitlement().GetEntitlementId())
	}
	if got.GetPrincipal().GetResourceId() != "alice" {
		t.Errorf("principal: got %q", got.GetPrincipal().GetResourceId())
	}
}

func TestIterateGrantsBySync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	const n = 100
	for i := 0; i < n; i++ {
		r := makeGrant(syncID,
			ksuid.New().String(),
			"github-read",
			ksuid.New().String(),
		)
		if err := e.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	err := e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Errorf("got %d grants, want %d", count, n)
	}
}

func TestIterateByEntitlement(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	// 5 grants on entitlement A, 3 on entitlement B.
	for i := 0; i < 5; i++ {
		r := makeGrant(syncID, ksuid.New().String(), "ent-A", ksuid.New().String())
		if err := e.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		r := makeGrant(syncID, ksuid.New().String(), "ent-B", ksuid.New().String())
		if err := e.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	var a, b int
	if err := e.IterateGrantsByEntitlement(ctx, "ent-A", func(r *v3.GrantRecord) bool {
		a++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.IterateGrantsByEntitlement(ctx, "ent-B", func(r *v3.GrantRecord) bool {
		b++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if a != 5 || b != 3 {
		t.Errorf("ent-A=%d (want 5), ent-B=%d (want 3)", a, b)
	}
}

func TestIterateByPrincipal(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	const alicePrincipal = "alice"
	const bobPrincipal = "bob"

	// 4 grants for alice on different entitlements, 2 for bob.
	for i := 0; i < 4; i++ {
		r := makeGrant(syncID, ksuid.New().String(), ksuid.New().String(), alicePrincipal)
		if err := e.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		r := makeGrant(syncID, ksuid.New().String(), ksuid.New().String(), bobPrincipal)
		if err := e.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	var a, b int
	if err := e.IterateGrantsByPrincipal(ctx, "user", alicePrincipal, func(r *v3.GrantRecord) bool {
		a++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.IterateGrantsByPrincipal(ctx, "user", bobPrincipal, func(r *v3.GrantRecord) bool {
		b++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if a != 4 || b != 2 {
		t.Errorf("alice=%d (want 4), bob=%d (want 2)", a, b)
	}
}

func TestDeleteGrantRecord(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	r := makeGrant(syncID, "to-delete", "ent-X", "user-X")
	if err := e.PutGrantRecord(ctx, r); err != nil {
		t.Fatal(err)
	}

	// Delete it. Both the primary and the index entries should go.
	if err := e.DeleteGrantRecord(ctx, "to-delete"); err != nil {
		t.Fatal(err)
	}

	if _, err := e.GetGrantRecord(ctx, "to-delete"); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	count := 0
	if err := e.IterateGrantsByEntitlement(ctx, "ent-X", func(*v3.GrantRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries after delete, got %d", count)
	}
}

func TestQuiesceBlocksWrites(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	if err := e.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	// Subsequent writes refused.
	r := makeGrant(syncID, "g", "e", "p")
	err := e.PutGrantRecord(ctx, r)
	if !errors.Is(err, ErrEngineQuiesced) {
		t.Errorf("expected ErrEngineQuiesced, got %v", err)
	}
	// Idempotent.
	if err := e.Quiesce(ctx); err != nil {
		t.Errorf("Quiesce should be idempotent, got %v", err)
	}
}

func TestCheckpointTo(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	r := makeGrant(syncID, "g1", "e1", "p1")
	if err := e.PutGrantRecord(ctx, r); err != nil {
		t.Fatal(err)
	}

	ckDir := filepath.Join(dir, "checkpoint")
	if err := e.CheckpointTo(ctx, ckDir); err != nil {
		t.Fatalf("CheckpointTo: %v", err)
	}

	// Open the checkpoint as a fresh engine and verify the grant is there.
	e2, err := Open(ctx, ckDir, WithReadOnly(true))
	if err != nil {
		t.Fatalf("reopen checkpoint: %v", err)
	}
	defer e2.Close()
	got, err := e2.GetGrantRecord(ctx, "g1")
	if err != nil {
		t.Fatalf("checkpoint Get: %v", err)
	}
	if got.GetEntitlement().GetEntitlementId() != "e1" {
		t.Errorf("checkpoint Get returned wrong entitlement: %v", got)
	}

	r2 := makeGrant(syncID, "g2", "e2", "p2")
	if err := e.PutGrantRecord(ctx, r2); err != nil {
		t.Fatalf("Put after CheckpointTo: %v", err)
	}
}

func TestSaveDoesNotQuiesceOnError(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	if err := e.Save(ctx, filepath.Join(dir, "out.c1z3")); err == nil {
		t.Fatal("expected Save error")
	}

	r := makeGrant(syncID, "after-save", "e1", "p1")
	if err := e.PutGrantRecord(ctx, r); err != nil {
		t.Fatalf("Put after failed Save: %v", err)
	}
}

func TestConcurrentGrantOverwriteIndexes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	const writes = 64
	start := make(chan struct{})
	errs := make(chan error, writes)
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
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
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := e.GetGrantRecord(ctx, "same-grant")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{"old": 0, "final": 0}
	for entID := range counts {
		entID := entID
		if err := e.IterateGrantsByEntitlement(ctx, entID, func(*v3.GrantRecord) bool {
			counts[entID]++
			return true
		}); err != nil {
			t.Fatal(err)
		}
	}
	if counts[got.GetEntitlement().GetEntitlementId()] != 1 {
		t.Fatalf("current entitlement index count = %d, want 1", counts[got.GetEntitlement().GetEntitlementId()])
	}
	for entID, count := range counts {
		if entID != got.GetEntitlement().GetEntitlementId() && count != 0 {
			t.Fatalf("stale entitlement %q index count = %d, want 0", entID, count)
		}
	}
}

func TestEmptySyncIDFallsBackToCurrent(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	// Put with explicit sync id...
	r := makeGrant(syncID, "g1", "e1", "p1")
	if err := e.PutGrantRecord(ctx, r); err != nil {
		t.Fatal(err)
	}
	// ...and Get with empty sync id (should use the engine's current).
	got, err := e.GetGrantRecord(ctx, "g1")
	if err != nil {
		t.Fatalf("Get with empty syncID: %v", err)
	}
	if got.GetExternalId() != "g1" {
		t.Errorf("unexpected: %v", got)
	}
}

func TestWriteWithNoCurrentSyncFails(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	// No StartNewSync/SetCurrentSync: nothing is bound, so a record
	// write must be refused rather than landing without a sync run.
	// (Reads do not gate on the binding — an empty store simply
	// returns not-found.)
	err := e.PutGrantRecord(ctx, makeGrant("", "anything", "e1", "p1"))
	if !errors.Is(err, ErrNoCurrentSync) {
		t.Errorf("expected ErrNoCurrentSync, got %v", err)
	}
}
