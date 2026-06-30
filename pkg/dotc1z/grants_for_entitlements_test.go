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

// TestListGrantsForEntitlementsRoundtrip exercises the new
// batched-K-entitlement RPC against the SQLite backend. Covers:
//   - basic aggregation across multiple entitlements
//   - principal_resource_type_ids filter
//   - pagination crossing entitlement boundaries
//   - checksum mismatch → cursor reset to start
func TestListGrantsForEntitlementsRoundtrip(t *testing.T) {
	ctx := t.Context()
	c1z, err := newC1ZFile(ctx, filepath.Join(t.TempDir(), "lgfe.c1z"))
	require.NoError(t, err)
	defer func() { _ = c1z.Close(ctx) }()

	_, err = c1z.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, c1z.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "app"}.Build(),
	))
	appRes := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "gh"}.Build(),
	}.Build()
	require.NoError(t, c1z.PutResources(ctx, appRes))

	entA := v2.Entitlement_builder{Id: "ent-A", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "A"}.Build()
	entB := v2.Entitlement_builder{Id: "ent-B", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "B"}.Build()
	entC := v2.Entitlement_builder{Id: "ent-C", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "C"}.Build()
	require.NoError(t, c1z.PutEntitlements(ctx, entA, entB, entC))

	mkGrant := func(id, entID, princRT, princID string) *v2.Grant {
		return v2.Grant_builder{
			Id:          id,
			Entitlement: v2.Entitlement_builder{Id: entID, Resource: appRes}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: princRT, Resource: princID}.Build(),
			}.Build(),
		}.Build()
	}

	// 5 user grants on A, 3 group grants on B, 7 user grants on C.
	grants := []*v2.Grant{}
	for i := 0; i < 5; i++ {
		grants = append(grants, mkGrant("a-u-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)))
	}
	for i := 0; i < 3; i++ {
		grants = append(grants, mkGrant("b-g-"+strconv.Itoa(i), "ent-B", "group", "g"+strconv.Itoa(i)))
	}
	for i := 0; i < 7; i++ {
		grants = append(grants, mkGrant("c-u-"+strconv.Itoa(i), "ent-C", "user", "uc"+strconv.Itoa(i)))
	}
	require.NoError(t, c1z.PutGrants(ctx, grants...))
	require.NoError(t, c1z.EndSync(ctx))

	t.Run("aggregate all", func(t *testing.T) {
		resp, err := c1z.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB, entC},
			PageSize:     100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 15)
		// Counts by ent_id.
		got := map[string]int{}
		for _, g := range resp.GetList() {
			got[g.GetEntitlement().GetId()]++
		}
		require.Equal(t, map[string]int{"ent-A": 5, "ent-B": 3, "ent-C": 7}, got)
	})

	t.Run("pagination crosses entitlement boundary", func(t *testing.T) {
		seen := map[string]bool{}
		token := ""
		for i := 0; i < 10; i++ {
			resp, err := c1z.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
				Entitlements: []*v2.Entitlement{entA, entB, entC},
				PageSize:     4,
				PageToken:    token,
			}.Build())
			require.NoError(t, err)
			for _, g := range resp.GetList() {
				require.False(t, seen[g.GetId()], "duplicate %s", g.GetId())
				seen[g.GetId()] = true
			}
			token = resp.GetNextPageToken()
			if token == "" {
				break
			}
		}
		require.Len(t, seen, 15)
	})

	t.Run("checksum mismatch resets", func(t *testing.T) {
		resp1, err := c1z.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB, entC},
			PageSize:     2,
		}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, resp1.GetNextPageToken())

		// Drop entC; cursor's checksum no longer matches.
		resp2, err := c1z.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB},
			PageSize:     2,
			PageToken:    resp1.GetNextPageToken(),
		}.Build())
		require.NoError(t, err)
		// Restart-from-zero must yield the first 2 grants again,
		// not skip ahead based on the stale cursor.
		require.Len(t, resp2.GetList(), 2)
		require.Equal(t, "ent-A", resp2.GetList()[0].GetEntitlement().GetId())
	})

	t.Run("empty entitlement list", func(t *testing.T) {
		resp, err := c1z.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			PageSize: 100,
		}.Build())
		require.NoError(t, err)
		require.Empty(t, resp.GetList())
		require.Empty(t, resp.GetNextPageToken())
	})
}
