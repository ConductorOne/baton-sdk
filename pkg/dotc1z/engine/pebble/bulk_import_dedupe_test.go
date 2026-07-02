package pebble

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// startBulkImport sets up an engine with a fresh sync and an open bulk
// import, mirroring the sqlite→pebble conversion entry sequence.
func startBulkImport(t *testing.T, ctx context.Context) (*Adapter, *BulkSyncImport) {
	t.Helper()
	e, _ := newTestEngine(t)
	store := NewAdapter(e)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync")
	b, err := e.StartBulkSyncImport(ctx, syncID, t.TempDir())
	require.NoError(t, err, "StartBulkSyncImport")
	return store, b
}

// TestBulkImportEntitlementIdentityOrderDivergesFromExternalID pins the fix
// for the AddEntitlements ordering bug: the converter feeds entitlements in
// external-id string order, which does NOT match structural-identity tuple
// order for (a) prefix-pair resource ids ("dev" sorts after "dev-1" as a
// string but before it as a tuple) and (b) sdk/custom kind mixes (the tuple
// injects the kind component; the string does not). Both shapes previously
// aborted the import with ErrBulkImportOutOfOrder.
func TestBulkImportEntitlementIdentityOrderDivergesFromExternalID(t *testing.T) {
	ctx := context.Background()
	store, b := startBulkImport(t, ctx)
	defer b.Abort()

	require.NoError(t, b.AddResourceTypes(ctx, v2.ResourceType_builder{Id: "app"}.Build(), v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, b.AddResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "app", Resource: "dev"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "app", Resource: "dev-1"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
	))
	// External-id sorted order (the converter's ORDER BY external_id):
	//   "app:dev-1:x" < "app:dev:x"      ('-' 0x2D < ':' 0x3A)
	//   "group:g1:member" < "opaque-ent"
	// Identity-tuple order flips both pairs: (app,dev,…) < (app,dev-1,…),
	// and the opaque flag sorts "opaque-ent" before the stripped "member".
	require.NoError(t, b.AddEntitlements(ctx,
		mkV2Entitlement("app:dev-1:x", "app", "dev-1"),
		mkV2Entitlement("app:dev:x", "app", "dev"),
		mkV2Entitlement("group:g1:member", "group", "g1"),
		mkV2Entitlement("opaque-ent", "group", "g1"),
	), "AddEntitlements must accept identity keys out of tuple order")

	shard, err := b.NewGrantShard()
	require.NoError(t, err)
	shard.Close()
	require.NoError(t, b.Finish(ctx), "Finish")

	resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	got := make([]string, 0, len(resp.GetList()))
	for _, ent := range resp.GetList() {
		got = append(got, ent.GetId())
	}
	sort.Strings(got)
	require.Equal(t, []string{"app:dev-1:x", "app:dev:x", "group:g1:member", "opaque-ent"}, got)

	stats := b.ComputedStats()
	require.Equal(t, int64(4), stats.GetEntitlements())
	require.Equal(t, map[string]int64{"app": 2, "group": 2}, stats.GetEntitlementsByResourceType())
}

// TestBulkImportMergesDuplicateIdentityGrants pins the merge-and-warn
// behavior for legacy rows with distinct external ids that fold to one
// structural identity: the conversion must merge them (like the in-place
// id-index migration does) instead of aborting with a duplicate-key error,
// index reads must stay consistent with the merged primary, and stats must
// reflect the post-merge row count.
func TestBulkImportMergesDuplicateIdentityGrants(t *testing.T) {
	ctx := context.Background()
	store, b := startBulkImport(t, ctx)
	defer b.Abort()

	require.NoError(t, b.AddResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build(), v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, b.AddResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build(),
	))
	ent := mkV2Entitlement("group:g1:member", "group", "g1")
	require.NoError(t, b.AddEntitlements(ctx, ent))

	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	mkGrant := func(externalID, sourceEnt string) *v2.Grant {
		return v2.Grant_builder{
			Id:          externalID,
			Entitlement: ent,
			Principal:   principal,
			Sources: v2.GrantSources_builder{
				Sources: map[string]*v2.GrantSources_GrantSource{
					sourceEnt: {},
				},
			}.Build(),
		}.Build()
	}

	shard, err := b.NewGrantShard()
	require.NoError(t, err)
	// Distinct external ids, identical refs → one structural identity.
	require.NoError(t, shard.AddGrants(ctx, mkGrant("legacy-grant-a", "src-ent-a"), mkGrant("legacy-grant-b", "src-ent-b")))
	shard.Close()
	require.NoError(t, b.Finish(ctx), "Finish must merge duplicate identities, not abort")

	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "duplicate-identity rows must fold to one grant")
	merged := resp.GetList()[0]
	sources := merged.GetSources().GetSources()
	require.Contains(t, sources, "src-ent-a", "merged grant must union sources")
	require.Contains(t, sources, "src-ent-b", "merged grant must union sources")

	// The by_principal index must resolve to the merged primary (one row,
	// no dangling second entry).
	byPrincipal, err := store.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		PrincipalId: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, byPrincipal.GetList(), 1)

	stats := b.ComputedStats()
	require.Equal(t, int64(1), stats.GetGrants(), "stats must count post-merge rows")
	require.Equal(t, map[string]int64{"group": 1}, stats.GetGrantsByEntitlementResourceType())
}

// TestBulkImportPreservesDistinctRawEntitlementIDs pins the no-grammar
// contract: ids that a grammar-based parser would fold (an escape-looking
// byte sequence vs the "unescaped" string) are distinct raw external ids,
// hence distinct structural identities — both rows survive conversion with
// their bytes untouched.
func TestBulkImportPreservesDistinctRawEntitlementIDs(t *testing.T) {
	ctx := context.Background()
	store, b := startBulkImport(t, ctx)
	defer b.Abort()

	require.NoError(t, b.AddResourceTypes(ctx, v2.ResourceType_builder{Id: "app"}.Build()))
	require.NoError(t, b.AddResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "app", Resource: "dev"}.Build()}.Build(),
	))
	require.NoError(t, b.AddEntitlements(ctx,
		mkV2Entitlement("app:dev:a:b", "app", "dev"),
		mkV2Entitlement(`app:dev:a\:b`, "app", "dev"),
	))

	shard, err := b.NewGrantShard()
	require.NoError(t, err)
	shard.Close()
	require.NoError(t, b.Finish(ctx), "Finish")

	resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 2, "distinct raw ids must stay distinct")
	got := []string{resp.GetList()[0].GetId(), resp.GetList()[1].GetId()}
	sort.Strings(got)
	require.Equal(t, []string{"app:dev:a:b", `app:dev:a\:b`}, got)

	stats := b.ComputedStats()
	require.Equal(t, int64(2), stats.GetEntitlements())
	require.Equal(t, map[string]int64{"app": 2}, stats.GetEntitlementsByResourceType())
}
