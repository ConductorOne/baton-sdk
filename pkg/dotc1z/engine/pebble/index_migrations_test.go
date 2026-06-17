package pebble

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
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
	require.NoErrorf(t, err, "Open initial")
	syncID := ksuid.New().String()
	require.NoErrorf(t, e.MarkFreshSync(syncID), "MarkFreshSync")
	require.NoErrorf(t, e.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId:    syncID,
		Type:      v3.SyncType_SYNC_TYPE_FULL,
		StartedAt: timestamppb.Now(),
		EndedAt:   timestamppb.Now(),
	}.Build()), "PutSyncRunRecord")
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
	require.NoErrorf(t, e.PutGrantRecord(ctx, rec), "PutGrantRecord")

	// Surgically delete the index key + bump back the applied
	// version so the next Open sees an old c1z that needs
	// migration.
	idxKey := encodeGrantByNeedsExpansionIndexKey("g-pending")
	require.NoErrorf(t, e.db.Delete(idxKey, pebble.Sync), "delete idx key")
	require.NoErrorf(t, e.db.Delete(encodeIndexAppliedKey("grant_needs_expansion"), pebble.Sync), "delete applied-version key")
	// Sanity: pre-migration walk finds nothing.
	pre := 0
	require.NoErrorf(t, e.IterateGrantsByNeedsExpansion(ctx, func(*v3.GrantRecord) bool {
		pre++
		return true
	}), "pre-migration iterate")
	require.Zero(t, pre, "pre-migration IterateGrantsByNeedsExpansion")
	require.NoErrorf(t, e.Close(), "Close")

	// Re-Open — the migration framework should detect the missing
	// applied-version key and run backfillGrantNeedsExpansion.
	e2, err := Open(ctx, dir)
	require.NoErrorf(t, err, "Open re")
	defer e2.Close()
	post := 0
	seen := []string{}
	require.NoErrorf(t, e2.IterateGrantsByNeedsExpansion(ctx, func(r *v3.GrantRecord) bool {
		post++
		seen = append(seen, r.GetExternalId())
		return true
	}), "post-migration iterate")
	require.Equal(t, 1, post, "post-migration IterateGrantsByNeedsExpansion count")
	require.Equal(t, []string{"g-pending"}, seen, "post-migration IterateGrantsByNeedsExpansion")

	// Migration version must now be stamped.
	v, err := e2.readAppliedIndexVersion("grant_needs_expansion")
	require.NoErrorf(t, err, "readAppliedIndexVersion")
	require.Equal(t, 1, v, "readAppliedIndexVersion(grant_needs_expansion)")
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
	require.NoErrorf(t, err, "Open 1")
	v1, err := e.readAppliedIndexVersion("grant_needs_expansion")
	require.NoErrorf(t, err, "readAppliedIndexVersion 1")
	require.Equal(t, 1, v1, "after first Open: applied version")
	require.NoErrorf(t, e.Close(), "Close 1")
	// Second Open should leave the applied version untouched.
	e2, err := Open(ctx, dir)
	require.NoErrorf(t, err, "Open 2")
	v2, err := e2.readAppliedIndexVersion("grant_needs_expansion")
	require.NoErrorf(t, err, "readAppliedIndexVersion 2")
	require.Equal(t, 1, v2, "after second Open: applied version")
	_ = e2.Close()
}
