//go:build batonsdkv2

package pebble

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// newTestEngine opens an engine in a fresh temp dir and registers
// cleanup. Returns the engine + the dir for later inspection.
func newTestEngine(t *testing.T, opts ...Option) (*Engine, string) {
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

// makeGrant returns a fully-populated GrantRecord under the given sync.
func makeGrant(syncID, externalID, entID, principalID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		SyncId:     syncID,
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

	got, err := e.GetGrantRecord(ctx, syncID, "grant-1")
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
	err := e.IterateGrantsBySync(ctx, syncID, func(r *v3.GrantRecord) bool {
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
	if err := e.IterateGrantsByEntitlement(ctx, syncID, "ent-A", func(r *v3.GrantRecord) bool {
		a++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.IterateGrantsByEntitlement(ctx, syncID, "ent-B", func(r *v3.GrantRecord) bool {
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
	if err := e.IterateGrantsByPrincipal(ctx, syncID, "user", alicePrincipal, func(r *v3.GrantRecord) bool {
		a++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.IterateGrantsByPrincipal(ctx, syncID, "user", bobPrincipal, func(r *v3.GrantRecord) bool {
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
	if err := e.DeleteGrantRecord(ctx, syncID, "to-delete"); err != nil {
		t.Fatal(err)
	}

	if _, err := e.GetGrantRecord(ctx, syncID, "to-delete"); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	count := 0
	if err := e.IterateGrantsByEntitlement(ctx, syncID, "ent-X", func(*v3.GrantRecord) bool {
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
	got, err := e2.GetGrantRecord(ctx, syncID, "g1")
	if err != nil {
		t.Fatalf("checkpoint Get: %v", err)
	}
	if got.GetEntitlement().GetEntitlementId() != "e1" {
		t.Errorf("checkpoint Get returned wrong entitlement: %v", got)
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
	got, err := e.GetGrantRecord(ctx, "", "g1")
	if err != nil {
		t.Fatalf("Get with empty syncID: %v", err)
	}
	if got.GetExternalId() != "g1" {
		t.Errorf("unexpected: %v", got)
	}
}

func TestEmptySyncIDWithNoCurrentFails(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	// No SetCurrentSync call.
	_, err := e.GetGrantRecord(ctx, "", "anything")
	if !errors.Is(err, ErrNoCurrentSync) {
		t.Errorf("expected ErrNoCurrentSync, got %v", err)
	}
}
