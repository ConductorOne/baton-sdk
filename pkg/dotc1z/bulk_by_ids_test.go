package dotc1z

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestBulkByIdsRoundtrip exercises the three new bulk-by-ID Reader
// methods: ListResourcesByIds, ListEntitlementsByIds,
// GetResourceTypes. All three must (a) return only the rows that
// match the requested ids and (b) silently omit ids that have no
// row in the active sync (caller-detects-partial-miss contract).
func TestBulkByIdsRoundtrip(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test.c1z")
	c1z, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer func() { _ = c1z.Close(ctx) }()

	_, err = c1z.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	userType := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupType := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	appType := v2.ResourceType_builder{Id: "app", DisplayName: "App"}.Build()
	require.NoError(t, c1z.PutResourceTypes(ctx, userType, groupType, appType))

	mkResource := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	users := []*v2.Resource{mkResource("user", "u1"), mkResource("user", "u2"), mkResource("user", "u3")}
	groups := []*v2.Resource{mkResource("group", "g1"), mkResource("group", "g2")}
	require.NoError(t, c1z.PutResources(ctx, users...))
	require.NoError(t, c1z.PutResources(ctx, groups...))

	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "ent-A", Resource: mkResource("app", "github")}.Build(),
		v2.Entitlement_builder{Id: "ent-B", Resource: mkResource("app", "github")}.Build(),
		v2.Entitlement_builder{Id: "ent-C", Resource: mkResource("app", "github")}.Build(),
	}
	require.NoError(t, c1z.PutEntitlements(ctx, ents...))

	require.NoError(t, c1z.EndSync(ctx))

	t.Run("GetResourceTypes returns hits and skips misses", func(t *testing.T) {
		resp, err := c1z.GetResourceTypes(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypesRequest_builder{
			ResourceTypeIds: []string{"user", "missing", "group"},
		}.Build())
		require.NoError(t, err)
		got := map[string]bool{}
		for _, rt := range resp.GetList() {
			got[rt.GetId()] = true
		}
		require.Equal(t, map[string]bool{"user": true, "group": true}, got)
	})

	t.Run("ListEntitlementsByIds", func(t *testing.T) {
		resp, err := c1z.ListEntitlementsByIds(ctx, reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest_builder{
			EntitlementIds: []string{"ent-A", "ent-C", "ent-zzz"},
		}.Build())
		require.NoError(t, err)
		got := map[string]bool{}
		for _, e := range resp.GetList() {
			got[e.GetId()] = true
		}
		require.Equal(t, map[string]bool{"ent-A": true, "ent-C": true}, got)
	})

	t.Run("ListResourcesByIds", func(t *testing.T) {
		ids := []*v2.ResourceId{
			v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
			v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(),
			v2.ResourceId_builder{ResourceType: "user", Resource: "u-missing"}.Build(),
			v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}
		resp, err := c1z.ListResourcesByIds(ctx, reader_v2.ResourcesReaderServiceListResourcesByIdsRequest_builder{
			ResourceIds: ids,
		}.Build())
		require.NoError(t, err)
		got := map[string]bool{}
		for _, r := range resp.GetList() {
			got[r.GetId().GetResourceType()+":"+r.GetId().GetResource()] = true
		}
		require.Equal(t, map[string]bool{
			"user:u1":  true,
			"user:u2":  true,
			"group:g1": true,
		}, got)
	})

	t.Run("empty id list returns empty list (no error)", func(t *testing.T) {
		respE, err := c1z.ListEntitlementsByIds(ctx, reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Empty(t, respE.GetList())

		respR, err := c1z.ListResourcesByIds(ctx, reader_v2.ResourcesReaderServiceListResourcesByIdsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Empty(t, respR.GetList())

		respRT, err := c1z.GetResourceTypes(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypesRequest_builder{}.Build())
		require.NoError(t, err)
		require.Empty(t, respRT.GetList())
	})
}

// TestBulkByIdsChunksOverFiveHundred exercises the inClauseChunkSize
// boundary — a single request whose id count exceeds the chunk size.
// All ids should round-trip without missing rows across the chunk
// stitch.
func TestBulkByIdsChunksOverFiveHundred(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "chunk.c1z")
	c1z, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer func() { _ = c1z.Close(ctx) }()

	_, err = c1z.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, c1z.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))

	const total = 1200 // > 2x inClauseChunkSize
	rs := make([]*v2.Resource, total)
	ids := make([]*v2.ResourceId, total)
	for i := 0; i < total; i++ {
		id := "u" + strconv.Itoa(i)
		rs[i] = v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: id}.Build(),
		}.Build()
		ids[i] = v2.ResourceId_builder{ResourceType: "user", Resource: id}.Build()
	}
	require.NoError(t, c1z.PutResources(ctx, rs...))
	require.NoError(t, c1z.EndSync(ctx))

	resp, err := c1z.ListResourcesByIds(ctx, reader_v2.ResourcesReaderServiceListResourcesByIdsRequest_builder{
		ResourceIds: ids,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), total)
}
