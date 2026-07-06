package pebble

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sort"
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

type kwayGrantSpec struct {
	id          string
	principalID string
	entitlement string
	discovered  time.Time
	needsExpand bool
}

type kwaySourceFixture struct {
	path   string
	syncID string
}

func writeKWaySource(t *testing.T, ctx context.Context, path string, grants []kwayGrantSpec, withAsset bool) kwaySourceFixture {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	store := w
	eng, ok := enginepkg.AsEngine(w)
	require.True(t, ok, "store is not pebble: %T", w)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	now := timestamppb.New(time.Unix(1, 0).UTC())
	require.NoError(t, eng.PutResourceTypeRecords(ctx,
		v3.ResourceTypeRecord_builder{ExternalId: "user", DisplayName: "User", DiscoveredAt: now}.Build(),
		v3.ResourceTypeRecord_builder{ExternalId: "group", DisplayName: "Group", DiscoveredAt: now}.Build(),
	))
	parent := v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build()
	require.NoError(t, eng.PutResourceRecords(ctx,
		v3.ResourceRecord_builder{ResourceTypeId: "group", ResourceId: "engineering", DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "alice", Parent: parent, DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "bob", Parent: parent, DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "carol", Parent: parent, DiscoveredAt: now}.Build(),
	))
	entRecords := make([]*v3.EntitlementRecord, 0, len(grants))
	seenEntitlements := map[string]bool{}
	for _, g := range grants {
		if seenEntitlements[g.entitlement] {
			continue
		}
		seenEntitlements[g.entitlement] = true
		entRecords = append(entRecords, v3.EntitlementRecord_builder{
			ExternalId:   g.entitlement,
			Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build(),
			DiscoveredAt: timestamppb.New(g.discovered),
		}.Build())
	}
	if len(entRecords) > 0 {
		require.NoError(t, eng.PutEntitlementRecords(ctx, entRecords...))
	}
	grantRecords := make([]*v3.GrantRecord, 0, len(grants))
	for _, g := range grants {
		grantRecords = append(grantRecords, v3.GrantRecord_builder{
			ExternalId: g.id,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "group",
				ResourceId:     "engineering",
				EntitlementId:  g.entitlement,
			}.Build(),
			Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: g.principalID}.Build(),
			NeedsExpansion: g.needsExpand,
			DiscoveredAt:   timestamppb.New(g.discovered),
		}.Build())
	}
	if len(grantRecords) > 0 {
		require.NoError(t, eng.PutGrantRecords(ctx, grantRecords...))
	}
	if withAsset {
		require.NoError(t, eng.PutAssetRecord(ctx, v3.AssetRecord_builder{SyncId: syncID, ExternalId: "asset-1", ContentType: "text/plain", Data: []byte("asset")}.Build()))
	}
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return kwaySourceFixture{path: path, syncID: syncID}
}

func mergeKWayFixtures(t *testing.T, ctx context.Context, fixtures []kwaySourceFixture, fanIn int) (*enginepkg.Engine, string) {
	t.Helper()
	dest, _ := newEngine(t, "kway-dest")
	destSyncID := ksuid.New().String()
	sources := make([]SourceFile, 0, len(fixtures))
	for _, f := range fixtures {
		sources = append(sources, SourceFile{Path: f.path, SyncID: f.syncID})
	}
	_, err := MergeFilesInto(ctx, dest, sources, destSyncID, t.TempDir(), WithFanIn(fanIn))
	require.NoError(t, err, "MergeFilesInto")
	return dest, destSyncID
}

func TestMergeFilesIntoKWayNewerWinsTieIndexesAndDropsAssets(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()
	tie := time.Unix(3000, 0).UTC()
	src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), []kwayGrantSpec{
		{id: "shared", principalID: "alice", entitlement: "member", discovered: older},
		{id: "tie", principalID: "alice", entitlement: "member", discovered: tie},
		{id: "only-src1", principalID: "alice", entitlement: "admin", discovered: older, needsExpand: true},
	}, true)
	src2 := writeKWaySource(t, ctx, filepath.Join(dir, "src2.c1z"), []kwayGrantSpec{
		{id: "shared", principalID: "bob", entitlement: "member", discovered: newer},
		{id: "tie", principalID: "bob", entitlement: "member", discovered: tie},
		{id: "only-src2", principalID: "carol", entitlement: "member", discovered: newer},
	}, true)

	dest, _ := mergeKWayFixtures(t, ctx, []kwaySourceFixture{src1, src2}, 50)

	grants := map[string]*v3.GrantRecord{}
	require.NoError(t, dest.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		grants[g.GetExternalId()] = g
		return true
	}))
	require.Equal(t, 3, len(grants), "merged grant count")
	require.NotContains(t, grants, "shared", "same structured identity is superseded by tie")
	require.Equal(t, "bob", grants["tie"].GetPrincipal().GetResourceId(), "retained external_id map observes later structured identity")
	var byEntitlement []string
	require.NoError(t, dest.IterateGrantsByEntitlement(ctx, "member", func(g *v3.GrantRecord) bool {
		byEntitlement = append(byEntitlement, g.GetExternalId())
		return true
	}))
	sort.Strings(byEntitlement)
	require.Equal(t, fmtSprint([]string{"only-src2", "tie", "tie"}), fmtSprint(byEntitlement), "by_entitlement index")
	var byPrincipal []string
	require.NoError(t, dest.IterateGrantsByPrincipal(ctx, "user", "alice", func(g *v3.GrantRecord) bool {
		byPrincipal = append(byPrincipal, g.GetExternalId())
		return true
	}))
	sort.Strings(byPrincipal)
	require.Equal(t, fmtSprint([]string{"only-src1", "tie"}), fmtSprint(byPrincipal), "by_principal index")
	var needsExpansion []string
	require.NoError(t, dest.IterateGrantsByNeedsExpansion(ctx, func(g *v3.GrantRecord) bool {
		needsExpansion = append(needsExpansion, g.GetExternalId())
		return true
	}))
	require.Equal(t, fmtSprint([]string{"only-src1"}), fmtSprint(needsExpansion), "needs_expansion index")
	var children []string
	require.NoError(t, dest.IterateResourcesByParent(ctx, "group", "engineering", func(r *v3.ResourceRecord) bool {
		children = append(children, r.GetResourceId())
		return true
	}))
	sort.Strings(children)
	require.Equal(t, fmtSprint([]string{"alice", "bob", "carol"}), fmtSprint(children), "resource_by_parent index")
	assetCount := 0
	require.NoError(t, dest.IterateAssets(ctx, func(*v3.AssetRecord) bool {
		assetCount++
		return true
	}))
	require.Equal(t, 0, assetCount, "asset count")
}

func TestMergeFilesIntoKWayRecursiveFanInMatchesSingleRound(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var fixtures []kwaySourceFixture
	for i := 0; i < 7; i++ {
		fixtures = append(fixtures, writeKWaySource(t, ctx, filepath.Join(dir, fmt.Sprintf("src-%d.c1z", i)), []kwayGrantSpec{
			{id: "shared", principalID: fmt.Sprintf("user-%d", i), entitlement: "member", discovered: time.Unix(int64(1000+i), 0).UTC()},
			{id: fmt.Sprintf("only-%d", i), principalID: "alice", entitlement: "member", discovered: time.Unix(int64(1000+i), 0).UTC()},
		}, false))
	}
	singleDest, singleSync := mergeKWayFixtures(t, ctx, fixtures, 50)
	recursiveDest, recursiveSync := mergeKWayFixtures(t, ctx, fixtures, 3)

	single := grantPrincipalMap(t, ctx, singleDest, singleSync)
	recursive := grantPrincipalMap(t, ctx, recursiveDest, recursiveSync)
	require.Equal(t, fmtSprint(single), fmtSprint(recursive), "recursive fan-in result")
	require.Equal(t, "user-6", recursive["shared"], "recursive shared winner")
}

func TestReadRunRecordIntoRejectsPartialHeader(t *testing.T) {
	var hdr [runHeaderSize]byte
	ok, err := readRunRecordInto(bytes.NewReader([]byte{1, 2, 3}), &runRecord{}, &hdr)
	require.Error(t, err, "readRunRecordInto partial header error = nil")
	require.False(t, ok, "readRunRecordInto partial header ok = true")
}

func TestReadLengthPrefixedBytesRejectsPartialHeader(t *testing.T) {
	var dst []byte
	var lenBuf [4]byte
	ok, err := readLengthPrefixedBytes(bytes.NewReader([]byte{1, 2}), &dst, &lenBuf)
	require.Error(t, err, "readLengthPrefixedBytes partial header error = nil")
	require.False(t, ok, "readLengthPrefixedBytes partial header ok = true")
}

func grantPrincipalMap(t *testing.T, ctx context.Context, e *enginepkg.Engine, syncID string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, e.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		out[g.GetExternalId()] = g.GetPrincipal().GetResourceId()
		return true
	}))
	return out
}

func fmtSprint(v any) string {
	return fmt.Sprint(v)
}
