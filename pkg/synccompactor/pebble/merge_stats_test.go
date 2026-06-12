package pebble

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

type statsGrantSpec struct {
	id          string
	entRT       string
	entRID      string
	entID       string
	principalID string
}

// writeStatsSource writes a Pebble c1z with two resource types, three
// resources (engineering group + alice/bob users), member/admin
// entitlements, and the given grants. The fixed records are identical
// across sources so cross-source dedupe is exercised by every bucket.
func writeStatsSource(t *testing.T, ctx context.Context, path string, grants []statsGrantSpec) kwaySourceFixture {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	store := w
	eng, ok := enginepkg.AsEngine(w)
	if !ok {
		t.Fatalf("store is not pebble: %T", w)
	}
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	now := timestamppb.New(time.Unix(1, 0).UTC())
	if err := eng.PutResourceTypeRecords(ctx,
		v3.ResourceTypeRecord_builder{ExternalId: "user", DisplayName: "User", DiscoveredAt: now}.Build(),
		v3.ResourceTypeRecord_builder{ExternalId: "group", DisplayName: "Group", DiscoveredAt: now}.Build(),
	); err != nil {
		t.Fatal(err)
	}
	if err := eng.PutResourceRecords(ctx,
		v3.ResourceRecord_builder{ResourceTypeId: "group", ResourceId: "engineering", DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "alice", DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "bob", DiscoveredAt: now}.Build(),
	); err != nil {
		t.Fatal(err)
	}
	if err := eng.PutEntitlementRecords(ctx,
		v3.EntitlementRecord_builder{
			ExternalId:   "member",
			Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build(),
			DiscoveredAt: now,
		}.Build(),
		v3.EntitlementRecord_builder{
			ExternalId:   "admin",
			Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build(),
			DiscoveredAt: now,
		}.Build(),
	); err != nil {
		t.Fatal(err)
	}
	for _, g := range grants {
		rec := v3.GrantRecord_builder{
			ExternalId: g.id,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: g.entRT,
				ResourceId:     g.entRID,
				EntitlementId:  g.entID,
			}.Build(),
			Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: g.principalID}.Build(),
			DiscoveredAt: now,
		}.Build()
		if err := eng.PutGrantRecords(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return kwaySourceFixture{path: path, syncID: syncID}
}

func requireSyncStatsEqual(t *testing.T, want, got *v3.SyncStatsRecord, label string) {
	t.Helper()
	if got.GetResourceTypes() != want.GetResourceTypes() {
		t.Errorf("%s: resource_types=%d, want %d", label, got.GetResourceTypes(), want.GetResourceTypes())
	}
	if got.GetResources() != want.GetResources() {
		t.Errorf("%s: resources=%d, want %d", label, got.GetResources(), want.GetResources())
	}
	if got.GetEntitlements() != want.GetEntitlements() {
		t.Errorf("%s: entitlements=%d, want %d", label, got.GetEntitlements(), want.GetEntitlements())
	}
	if got.GetGrants() != want.GetGrants() {
		t.Errorf("%s: grants=%d, want %d", label, got.GetGrants(), want.GetGrants())
	}
	if got.GetAssets() != want.GetAssets() {
		t.Errorf("%s: assets=%d, want %d", label, got.GetAssets(), want.GetAssets())
	}
	requireCountMapEqual(t, want.GetResourcesByResourceType(), got.GetResourcesByResourceType(), label+": resources_by_resource_type")
	requireCountMapEqual(t, want.GetGrantsByEntitlementResourceType(), got.GetGrantsByEntitlementResourceType(), label+": grants_by_entitlement_resource_type")
}

// requireCountMapEqual treats nil and empty maps as equal — empty
// proto maps round-trip through marshal/unmarshal as nil.
func requireCountMapEqual(t *testing.T, want, got map[string]int64, label string) {
	t.Helper()
	if len(want) != len(got) {
		t.Errorf("%s: got %v, want %v", label, got, want)
		return
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s[%q]=%d, want %d", label, k, got[k], v)
		}
	}
}

// TestMergeStatsSidecarMatchesRecompute pins that the stats record
// accumulated at merge time equals what a full post-merge recompute
// (PersistSyncStats) produces, for every merge strategy. The fixtures
// include overlapping keys across sources (dedupe must not double
// count) and a grant whose entitlement ref has no resource — that
// grant must still count, grouped under the empty resource type, to
// stay in parity with the SQLite GROUP BY semantics.
func TestMergeStatsSidecarMatchesRecompute(t *testing.T) {
	ctx := context.Background()

	mkThreeSources := func(t *testing.T) []SourceFile {
		dir := t.TempDir()
		newest := writeStatsSource(t, ctx, filepath.Join(dir, "src0.c1z"), []statsGrantSpec{
			{id: "shared", entRT: "group", entRID: "engineering", entID: "member", principalID: "alice"},
			{id: "only0", entRT: "group", entRID: "engineering", entID: "admin", principalID: "bob"},
			{id: "noent", entID: "ghost", principalID: "alice"},
		})
		mid := writeStatsSource(t, ctx, filepath.Join(dir, "src1.c1z"), []statsGrantSpec{
			{id: "shared", entRT: "group", entRID: "engineering", entID: "member", principalID: "bob"},
			{id: "only1", entRT: "group", entRID: "engineering", entID: "member", principalID: "bob"},
		})
		oldest := writeStatsSource(t, ctx, filepath.Join(dir, "src2.c1z"), []statsGrantSpec{
			{id: "only2", entRT: "group", entRID: "engineering", entID: "member", principalID: "alice"},
		})
		return []SourceFile{
			{Path: newest.path, SyncID: newest.syncID},
			{Path: mid.path, SyncID: mid.syncID},
			{Path: oldest.path, SyncID: oldest.syncID},
		}
	}

	// Two sources where the newest has no grants, so the grants bucket
	// hits the overlay whole-source SST fast path on the oldest source.
	mkWholeSourceSources := func(t *testing.T) []SourceFile {
		dir := t.TempDir()
		newest := writeStatsSource(t, ctx, filepath.Join(dir, "src0.c1z"), nil)
		oldest := writeStatsSource(t, ctx, filepath.Join(dir, "src1.c1z"), []statsGrantSpec{
			{id: "base-a", entRT: "group", entRID: "engineering", entID: "member", principalID: "alice"},
			{id: "base-b", entRT: "group", entRID: "engineering", entID: "admin", principalID: "bob"},
		})
		return []SourceFile{
			{Path: newest.path, SyncID: newest.syncID},
			{Path: oldest.path, SyncID: oldest.syncID},
		}
	}

	threeSourceWant := v3.SyncStatsRecord_builder{
		ResourceTypes:                   2,
		Resources:                       3,
		Entitlements:                    2,
		Grants:                          5,
		ResourcesByResourceType:         map[string]int64{"group": 1, "user": 2},
		GrantsByEntitlementResourceType: map[string]int64{"group": 4, "": 1},
	}.Build()

	cases := []struct {
		name    string
		sources func(*testing.T) []SourceFile
		merge   func(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string) (*v3.SyncStatsRecord, error)
		want    *v3.SyncStatsRecord
	}{
		{
			name:    "overlay",
			sources: mkThreeSources,
			merge: func(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string) (*v3.SyncStatsRecord, error) {
				return MergeFilesIntoOverlay(ctx, dest, sources, destSyncID, tmpDir)
			},
			want: threeSourceWant,
		},
		{
			name:    "overlay whole-source fast path",
			sources: mkWholeSourceSources,
			merge: func(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string) (*v3.SyncStatsRecord, error) {
				return MergeFilesIntoOverlay(ctx, dest, sources, destSyncID, tmpDir)
			},
		},
		{
			name:    "kway direct",
			sources: mkThreeSources,
			merge: func(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string) (*v3.SyncStatsRecord, error) {
				return MergeFilesInto(ctx, dest, sources, destSyncID, tmpDir)
			},
			want: threeSourceWant,
		},
		{
			name:    "kway multi-round",
			sources: mkThreeSources,
			merge: func(ctx context.Context, dest *enginepkg.Engine, sources []SourceFile, destSyncID string, tmpDir string) (*v3.SyncStatsRecord, error) {
				return MergeFilesInto(ctx, dest, sources, destSyncID, tmpDir, WithFanIn(2))
			},
			want: threeSourceWant,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dest, _ := newEngine(t, "stats-dest")
			destSyncID := ksuid.New().String()
			got, err := tc.merge(ctx, dest, tc.sources(t), destSyncID, t.TempDir())
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			if got == nil {
				t.Fatal("merge returned nil stats")
			}
			if tc.want != nil {
				requireSyncStatsEqual(t, tc.want, got, "accumulated vs expected")
			}

			// Persist the accumulated record, read it back, then
			// overwrite with a full recompute and compare: the
			// merge-time accumulation must match the scan exactly.
			if err := dest.PersistComputedSyncStats(ctx, destSyncID, got); err != nil {
				t.Fatalf("PersistComputedSyncStats: %v", err)
			}
			stored, err := enginepkg.ReadSyncStatsRecord(ctx, dest, destSyncID)
			if err != nil {
				t.Fatalf("ReadSyncStatsRecord (accumulated): %v", err)
			}
			if stored == nil {
				t.Fatal("accumulated sidecar missing after persist")
			}
			if err := dest.PersistSyncStats(ctx, destSyncID); err != nil {
				t.Fatalf("PersistSyncStats: %v", err)
			}
			recomputed, err := enginepkg.ReadSyncStatsRecord(ctx, dest, destSyncID)
			if err != nil {
				t.Fatalf("ReadSyncStatsRecord (recomputed): %v", err)
			}
			requireSyncStatsEqual(t, recomputed, stored, "accumulated vs recompute")
		})
	}
}
