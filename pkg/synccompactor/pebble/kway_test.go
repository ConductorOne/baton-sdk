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
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
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
	parent := v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build()
	if err := eng.PutResourceRecords(ctx,
		v3.ResourceRecord_builder{ResourceTypeId: "group", ResourceId: "engineering", DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "alice", Parent: parent, DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "bob", Parent: parent, DiscoveredAt: now}.Build(),
		v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "carol", Parent: parent, DiscoveredAt: now}.Build(),
	); err != nil {
		t.Fatal(err)
	}
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
		if err := eng.PutEntitlementRecords(ctx, entRecords...); err != nil {
			t.Fatal(err)
		}
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
		if err := eng.PutGrantRecords(ctx, grantRecords...); err != nil {
			t.Fatal(err)
		}
	}
	if withAsset {
		if err := eng.PutAssetRecord(ctx, v3.AssetRecord_builder{SyncId: syncID, ExternalId: "asset-1", ContentType: "text/plain", Data: []byte("asset")}.Build()); err != nil {
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

func mergeKWayFixtures(t *testing.T, ctx context.Context, fixtures []kwaySourceFixture, fanIn int) (*enginepkg.Engine, string) {
	t.Helper()
	dest, _ := newEngine(t, "kway-dest")
	destSyncID := ksuid.New().String()
	sources := make([]SourceFile, 0, len(fixtures))
	for _, f := range fixtures {
		sources = append(sources, SourceFile{Path: f.path, SyncID: f.syncID})
	}
	if _, err := MergeFilesInto(ctx, dest, sources, destSyncID, t.TempDir(), WithFanIn(fanIn)); err != nil {
		t.Fatalf("MergeFilesInto: %v", err)
	}
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
	if err := dest.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		grants[g.GetExternalId()] = g
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(grants) != 4 {
		t.Fatalf("merged grant count = %d, want 4", len(grants))
	}
	if got := grants["shared"].GetPrincipal().GetResourceId(); got != "bob" {
		t.Fatalf("newer shared grant principal = %q, want bob", got)
	}
	if got := grants["tie"].GetPrincipal().GetResourceId(); got != "alice" {
		t.Fatalf("tie grant principal = %q, want alice from earlier-applied source", got)
	}
	var byEntitlement []string
	if err := dest.IterateGrantsByEntitlement(ctx, "member", func(g *v3.GrantRecord) bool {
		byEntitlement = append(byEntitlement, g.GetExternalId())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(byEntitlement)
	if got, want := byEntitlement, []string{"only-src2", "shared", "tie"}; fmtSprint(got) != fmtSprint(want) {
		t.Fatalf("by_entitlement index = %v, want %v", got, want)
	}
	var byPrincipal []string
	if err := dest.IterateGrantsByPrincipal(ctx, "user", "alice", func(g *v3.GrantRecord) bool {
		byPrincipal = append(byPrincipal, g.GetExternalId())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(byPrincipal)
	if got, want := byPrincipal, []string{"only-src1", "tie"}; fmtSprint(got) != fmtSprint(want) {
		t.Fatalf("by_principal index = %v, want %v", got, want)
	}
	var needsExpansion []string
	if err := dest.IterateGrantsByNeedsExpansion(ctx, func(g *v3.GrantRecord) bool {
		needsExpansion = append(needsExpansion, g.GetExternalId())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if got, want := needsExpansion, []string{"only-src1"}; fmtSprint(got) != fmtSprint(want) {
		t.Fatalf("needs_expansion index = %v, want %v", got, want)
	}
	var children []string
	if err := dest.IterateResourcesByParent(ctx, "group", "engineering", func(r *v3.ResourceRecord) bool {
		children = append(children, r.GetResourceId())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(children)
	if got, want := children, []string{"alice", "bob", "carol"}; fmtSprint(got) != fmtSprint(want) {
		t.Fatalf("resource_by_parent index = %v, want %v", got, want)
	}
	assetCount := 0
	if err := dest.IterateAssets(ctx, func(*v3.AssetRecord) bool {
		assetCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if assetCount != 0 {
		t.Fatalf("asset count = %d, want 0", assetCount)
	}
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
	if fmtSprint(single) != fmtSprint(recursive) {
		t.Fatalf("recursive fan-in result = %v, want %v", recursive, single)
	}
	if got := recursive["shared"]; got != "user-6" {
		t.Fatalf("recursive shared winner = %q, want user-6", got)
	}
}

func TestReadRunRecordIntoRejectsPartialHeader(t *testing.T) {
	var hdr [runHeaderSize]byte
	ok, err := readRunRecordInto(bytes.NewReader([]byte{1, 2, 3}), &runRecord{}, &hdr)
	if err == nil {
		t.Fatal("readRunRecordInto partial header error = nil")
	}
	if ok {
		t.Fatal("readRunRecordInto partial header ok = true")
	}
}

func TestReadLengthPrefixedBytesRejectsPartialHeader(t *testing.T) {
	var dst []byte
	var lenBuf [4]byte
	ok, err := readLengthPrefixedBytes(bytes.NewReader([]byte{1, 2}), &dst, &lenBuf)
	if err == nil {
		t.Fatal("readLengthPrefixedBytes partial header error = nil")
	}
	if ok {
		t.Fatal("readLengthPrefixedBytes partial header ok = true")
	}
}

func grantPrincipalMap(t *testing.T, ctx context.Context, e *enginepkg.Engine, syncID string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := e.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		out[g.GetExternalId()] = g.GetPrincipal().GetResourceId()
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func fmtSprint(v any) string {
	return fmt.Sprint(v)
}
