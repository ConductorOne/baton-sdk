package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// A file's record values need not match their keys. Each list RPC below has
// a row whose value names an id sorting before every key, placed last on the
// first page. Paging must still serve every row exactly once and stop.
func TestSecurity_PageTokensAdvancePastHostileRows(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	parent := v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "g"}.Build()
	rb := e.db.NewRecordBatch()
	for _, ids := range [][2]string{{"r1", "r1"}, {"r2", "r2"}, {"r2x", "r0"}, {"r3", "r3"}} {
		val, err := marshalRecord(v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: ids[1], Parent: parent}.Build())
		require.NoError(t, err)
		require.NoError(t, rb.StageResourcePut(encodeResourceKey("user", ids[0]), val, nil, "user", ids[0]))
	}
	require.NoError(t, rb.Commit(nil))
	require.NoError(t, rb.Close())

	ent := v2.Entitlement_builder{
		Id:       canonicalTestEntID("ent"),
		Resource: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build()}.Build(),
	}.Build()
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("", "ent", "user", "u1"), mkV2Grant("", "ent", "user", "u2"), mkV2Grant("", "ent", "user", "u3")))
	honest, err := e.GetGrantRecord(ctx, canonicalTestGrantID("ent", "user", "u2"))
	require.NoError(t, err)
	id, err := grantIdentityFromRecord(honest)
	require.NoError(t, err)
	id.principalID = "u2x"
	honest.GetPrincipal().SetResourceId("u0")
	val, err := marshalRecord(honest)
	require.NoError(t, err)
	require.NoError(t, e.db.UnsafeForTesting().Set(encodeGrantIdentityKey(id), val, nil))

	resources := func(req *v2.ResourcesServiceListResourcesRequest) func(string) ([]string, string, error) {
		return func(token string) ([]string, string, error) {
			req.SetPageToken(token)
			resp, err := a.ListResources(ctx, req)
			var ids []string
			for _, r := range resp.GetList() {
				ids = append(ids, r.GetId().GetResource())
			}
			return ids, resp.GetNextPageToken(), err
		}
	}
	principals := func(grants []*v2.Grant) []string {
		var ids []string
		for _, g := range grants {
			ids = append(ids, g.GetPrincipal().GetId().GetResource())
		}
		return ids
	}
	grantsForEntitlement := func(rts ...string) func(string) ([]string, string, error) {
		return func(token string) ([]string, string, error) {
			resp, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: ent, PrincipalResourceTypeIds: rts, PageSize: 3, PageToken: token,
			}.Build())
			return principals(resp.GetList()), resp.GetNextPageToken(), err
		}
	}
	resourceIDs := []string{"r0", "r1", "r2", "r3"}
	principalIDs := []string{"u0", "u1", "u2", "u3"}
	for name, tc := range map[string]struct {
		page func(token string) ([]string, string, error)
		want []string
	}{
		"ListResources": {resources(v2.ResourcesServiceListResourcesRequest_builder{PageSize: 3}.Build()), resourceIDs},
		"ListResources by type": {
			resources(v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: "user", PageSize: 3}.Build()), resourceIDs,
		},
		"ListResources by parent": {
			resources(v2.ResourcesServiceListResourcesRequest_builder{
				ParentResourceId: v2.ResourceId_builder{ResourceType: "group", Resource: "g"}.Build(), PageSize: 3,
			}.Build()),
			resourceIDs,
		},
		"ListResources by parent and type": {
			resources(v2.ResourcesServiceListResourcesRequest_builder{
				ParentResourceId: v2.ResourceId_builder{ResourceType: "group", Resource: "g"}.Build(), ResourceTypeId: "user", PageSize: 3,
			}.Build()),
			resourceIDs,
		},
		"ListGrantsForEntitlement":                   {grantsForEntitlement(), principalIDs},
		"ListGrantsForEntitlement by principal type": {grantsForEntitlement("user"), principalIDs},
		"ListGrantsForEntitlements": {
			func(token string) ([]string, string, error) {
				resp, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
					Entitlements: []*v2.Entitlement{ent}, PageSize: 3, PageToken: token,
				}.Build())
				return principals(resp.GetList()), resp.GetNextPageToken(), err
			},
			principalIDs,
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.ElementsMatch(t, tc.want, pageAll(t, ctx, tc.page))
		})
	}
}

func pageAll(t *testing.T, ctx context.Context, page func(token string) ([]string, string, error)) []string {
	t.Helper()
	var all []string
	token := ""
	for range 10 {
		require.NoError(t, ctx.Err())
		ids, next, err := page(token)
		require.NoError(t, err)
		all = append(all, ids...)
		if next == "" {
			return all
		}
		token = next
	}
	require.FailNow(t, fmt.Sprintf("paging did not stop; served %v", all))
	return nil
}
