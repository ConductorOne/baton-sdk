package pebble

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

type entStatsSpec struct {
	extID string
	rt    string
	rid   string
}

// writeEntitlementRTSource writes a Pebble c1z with both group and user
// resource types/resources and the given entitlements, so callers can
// vary an entitlement's resource type across sources.
func writeEntitlementRTSource(t *testing.T, ctx context.Context, path string, ents []entStatsSpec) kwaySourceFixture {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store is not pebble: %T", w)
	syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	now := timestamppb.New(time.Unix(1, 0).UTC())
	require.NoError(t, eng.PutResourceTypeRecords(ctx,
		v3.ResourceTypeRecord_builder{ExternalId: "user", DisplayName: "User", DiscoveredAt: now}.Build(),
		v3.ResourceTypeRecord_builder{ExternalId: "group", DisplayName: "Group", DiscoveredAt: now}.Build(),
	))
	require.NoError(t, eng.PutResourceRecords(ctx,
		v3.ResourceRecord_builder{ResourceTypeId: "group", ResourceId: "engineering", DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "alice", DiscoveredAt: now}.Build(),
	))
	recs := make([]*v3.EntitlementRecord, 0, len(ents))
	for _, e := range ents {
		recs = append(recs, v3.EntitlementRecord_builder{
			ExternalId:   e.extID,
			Resource:     v3.ResourceRef_builder{ResourceTypeId: e.rt, ResourceId: e.rid}.Build(),
			DiscoveredAt: now,
		}.Build())
	}
	require.NoError(t, eng.PutEntitlementRecords(ctx, recs...))
	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
	return kwaySourceFixture{path: path, syncID: syncID}
}

// TestMergeStatsEntitlementResourceTypeChange exercises the overlay
// replacement path where the newest source re-emits a shared
// entitlement under a DIFFERENT resource type than an older source.
// The merge keeps the newest value, so the entitlement must regroup
// from the old resource type to the new one. Correctness is pinned by
// asserting the absolute grouping and by the accumulated-vs-recompute
// parity that the regroup branch must satisfy.
func TestMergeStatsEntitlementResourceTypeChange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// newest: "shared" lives on a user resource; oldest: "shared" lived
	// on a group resource. "admin" only exists in the oldest source.
	newest := writeEntitlementRTSource(t, ctx, filepath.Join(dir, "newest.c1z"), []entStatsSpec{
		{extID: "shared", rt: "user", rid: "alice"},
	})
	oldest := writeEntitlementRTSource(t, ctx, filepath.Join(dir, "oldest.c1z"), []entStatsSpec{
		{extID: "shared", rt: "group", rid: "engineering"},
		{extID: "admin", rt: "group", rid: "engineering"},
	})
	sources := []SourceFile{
		{Path: newest.path, SyncID: newest.syncID},
		{Path: oldest.path, SyncID: oldest.syncID},
	}

	dest, _ := newEngine(t, "ent-rt-dest")
	destSyncID := ksuid.New().String()
	got, err := MergeFilesIntoOverlay(ctx, dest, sources, destSyncID, t.TempDir())
	require.NoError(t, err, "MergeFilesIntoOverlay")
	require.NotNil(t, got)

	// Entitlement identity includes the owning resource, so the shared external id
	// on user/alice and group/engineering are distinct logical entitlements.
	require.Equal(t, int64(3), got.GetEntitlements(), "entitlement total")
	require.Equal(t, map[string]int64{"user": 1, "group": 2}, got.GetEntitlementsByResourceType())

	// Parity: the merge-time accumulation (which ran the regroup branch)
	// must match a full post-merge recompute that scans final values.
	require.NoError(t, dest.PersistComputedSyncStats(ctx, destSyncID, got))
	stored, err := enginepkg.ReadSyncStatsRecord(ctx, dest, destSyncID)
	require.NoError(t, err)
	require.NoError(t, dest.PersistSyncStats(ctx, destSyncID))
	recomputed, err := enginepkg.ReadSyncStatsRecord(ctx, dest, destSyncID)
	require.NoError(t, err)
	requireSyncStatsEqual(t, recomputed, stored, "accumulated vs recompute")
}
