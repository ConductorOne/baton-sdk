package pebble

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// TestApplyIndexMigrationsBackfillsNeedsExpansion simulates the
// "old engine wrote the c1z before idxGrantByNeedsExpansion
// existed" case. We populate the engine with primary GrantRecords
// that carry NeedsExpansion=true but WITHOUT the corresponding
// index entries (mimicking pre-migration state), then close and
// re-open. The on-Open migration must backfill the index so
// IterateGrantsByNeedsExpansion now finds the records.
func TestApplyIndexMigrationsBackfillsNeedsExpansion(t *testing.T) {
	// Skipped: the indexMigrations registry is intentionally empty
	// (no existing Pebble c1z data to backfill — nothing uses Pebble
	// in prod). The grant_needs_expansion backfill it exercises is
	// not registered, so on-Open migration is a no-op. Re-enable when
	// a migration is added back to the registry.
	t.Skip("indexMigrations registry intentionally empty: no existing Pebble data to backfill")

	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "engine")

	e, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open initial: %v", err)
	}
	syncID := ksuid.New().String()
	if err := e.MarkFreshSync(syncID); err != nil {
		t.Fatalf("MarkFreshSync: %v", err)
	}
	if err := e.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId:    syncID,
		Type:      v3.SyncType_SYNC_TYPE_FULL,
		StartedAt: timestamppb.Now(),
		EndedAt:   timestamppb.Now(),
	}.Build()); err != nil {
		t.Fatalf("PutSyncRunRecord: %v", err)
	}
	// Write grants with NeedsExpansion=true via the normal path —
	// then surgically delete the new index keys to simulate a c1z
	// written by an older engine that didn't know about
	// idxGrantByNeedsExpansion.
	rec := v3.GrantRecord_builder{
		ExternalId: "g-pending",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "admins",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: true,
	}.Build()
	if err := e.PutGrantRecord(ctx, rec); err != nil {
		t.Fatalf("PutGrantRecord: %v", err)
	}

	// Surgically delete the index key + bump back the applied
	// version so the next Open sees an old c1z that needs
	// migration.
	syncIDBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		t.Fatalf("EncodeSyncID: %v", err)
	}
	idxKey := encodeGrantByNeedsExpansionIndexKey(syncIDBytes, "g-pending")
	if err := e.db.Delete(idxKey, pebble.Sync); err != nil {
		t.Fatalf("delete idx key: %v", err)
	}
	if err := e.db.Delete(encodeIndexAppliedKey("grant_needs_expansion"), pebble.Sync); err != nil {
		t.Fatalf("delete applied-version key: %v", err)
	}
	// Sanity: pre-migration walk finds nothing.
	pre := 0
	if err := e.IterateGrantsByNeedsExpansion(ctx, syncID, func(*v3.GrantRecord) bool {
		pre++
		return true
	}); err != nil {
		t.Fatalf("pre-migration iterate: %v", err)
	}
	if pre != 0 {
		t.Fatalf("pre-migration IterateGrantsByNeedsExpansion: got %d, want 0", pre)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-Open — the migration framework should detect the missing
	// applied-version key and run backfillGrantNeedsExpansion.
	e2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open re: %v", err)
	}
	defer e2.Close()
	post := 0
	seen := []string{}
	if err := e2.IterateGrantsByNeedsExpansion(ctx, syncID, func(r *v3.GrantRecord) bool {
		post++
		seen = append(seen, r.GetExternalId())
		return true
	}); err != nil {
		t.Fatalf("post-migration iterate: %v", err)
	}
	if post != 1 || seen[0] != "g-pending" {
		t.Fatalf("post-migration IterateGrantsByNeedsExpansion: got %d (%v), want 1 [g-pending]", post, seen)
	}

	// Migration version must now be stamped.
	v, err := e2.readAppliedIndexVersion("grant_needs_expansion")
	if err != nil {
		t.Fatalf("readAppliedIndexVersion: %v", err)
	}
	if v != 1 {
		t.Fatalf("readAppliedIndexVersion(grant_needs_expansion) = %d, want 1", v)
	}
}

// TestApplyIndexMigrationsIsIdempotent verifies that re-running
// migrations after they've already been applied is a no-op.
func TestApplyIndexMigrationsIsIdempotent(t *testing.T) {
	// Skipped: the indexMigrations registry is intentionally empty
	// (no existing Pebble data to backfill), so no applied-version is
	// ever stamped. Re-enable when a migration is added back.
	t.Skip("indexMigrations registry intentionally empty: no existing Pebble data to backfill")

	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "engine")
	e, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	v1, err := e.readAppliedIndexVersion("grant_needs_expansion")
	if err != nil {
		t.Fatalf("readAppliedIndexVersion 1: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("after first Open: applied version = %d, want 1", v1)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	// Second Open should leave the applied version untouched.
	e2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	v2, err := e2.readAppliedIndexVersion("grant_needs_expansion")
	if err != nil {
		t.Fatalf("readAppliedIndexVersion 2: %v", err)
	}
	if v2 != 1 {
		t.Fatalf("after second Open: applied version = %d, want 1", v2)
	}
	_ = e2.Close()
}
