package pebble

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestListResourcesTypeFilterPagination pages type-filtered listings, with
// and without a parent, over children of three types under one parent. "us"
// is a name prefix of "user", so a scan prefix missing its trailing
// separator would mix the two.
func TestListResourcesTypeFilterPagination(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync")

	parent := v2.ResourceId_builder{ResourceType: "group", Resource: "p"}.Build()
	const total = 8
	want := map[string][]string{}
	var all []*v2.Resource
	for i := range total {
		for _, rt := range []string{"user", "us", "group"} {
			id := rt + "-" + strconv.Itoa(i)
			want[rt] = append(want[rt], id)
			all = append(all, v2.Resource_builder{
				Id:               v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
				ParentResourceId: parent,
			}.Build())
		}
	}
	require.NoErrorf(t, a.PutResources(ctx, all...), "PutResources")

	for _, byParent := range []bool{false, true} {
		for _, rt := range []string{"user", "us"} {
			var got []string
			pageToken := ""
			for pages := 1; ; pages++ {
				require.LessOrEqual(t, pages, 10, "ListResources(%q, byParent=%v) did not terminate", rt, byParent)
				req := v2.ResourcesServiceListResourcesRequest_builder{
					ResourceTypeId: rt,
					PageSize:       3,
					PageToken:      pageToken,
				}.Build()
				if byParent {
					req.SetParentResourceId(parent)
				}
				resp, err := a.ListResources(ctx, req)
				require.NoErrorf(t, err, "ListResources")
				for _, r := range resp.GetList() {
					got = append(got, r.GetId().GetResource())
				}
				pageToken = resp.GetNextPageToken()
				if pageToken == "" {
					break
				}
			}
			require.ElementsMatch(t, want[rt], got, "ListResources(%q, byParent=%v)", rt, byParent)
		}
	}
}

// TestListGrantsForEntitlementPostFilterDoesNotSkip pages
// ListGrantsForEntitlement filtered by principal resource type over
// grants on ent-A whose principals interleave user/group; the page-3
// iteration must see every user-principal grant.
func TestListGrantsForEntitlementPostFilterDoesNotSkip(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync")
	const total = 8
	grants := make([]*v2.Grant, 0, 2*total)
	for i := 0; i < total; i++ {
		grants = append(grants,
			mkV2Grant("u-grant-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)),
			mkV2Grant("g-grant-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)),
		)
	}
	require.NoErrorf(t, a.PutGrants(ctx, grants...), "PutGrants")

	seen := make(map[string]bool, total)
	pageToken := ""
	pages := 0
	for {
		pages++
		require.LessOrEqual(t, pages, 10, "ListGrantsForEntitlement did not terminate after %d pages", pages)
		resp, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: v2.Entitlement_builder{
				Id: canonicalTestEntID("ent-A"),
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
				}.Build(),
			}.Build(),
			PrincipalResourceTypeIds: []string{"user"},
			PageSize:                 3,
			PageToken:                pageToken,
		}.Build())
		require.NoErrorf(t, err, "ListGrantsForEntitlement")
		for _, g := range resp.GetList() {
			require.Equal(t, "user", g.GetPrincipal().GetId().GetResourceType(), "got non-user principal: %s", g.GetPrincipal().GetId().GetResourceType())
			seen[g.GetId()] = true
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, total, len(seen), "post-filter ListGrantsForEntitlement missed records: got %d (%v), want %d", len(seen), seen, total)
}

// TestListGrantsForResourceTypePostFilterDoesNotSkip walks the
// same regression for the rtFilter variant on ListGrantsForResourceType.
func TestListGrantsForResourceTypePostFilterDoesNotSkip(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync")
	const total = 8
	grants := make([]*v2.Grant, 0, 2*total)
	for i := 0; i < total; i++ {
		grants = append(grants,
			mkV2Grant("u-grant-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)),
			mkV2Grant("g-grant-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)),
		)
	}
	require.NoErrorf(t, a.PutGrants(ctx, grants...), "PutGrants")

	seen := make(map[string]bool, total)
	pageToken := ""
	pages := 0
	for {
		pages++
		require.LessOrEqual(t, pages, 10, "ListGrantsForResourceType did not terminate after %d pages", pages)
		resp, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
			ResourceTypeId: "user",
			PageSize:       3,
			PageToken:      pageToken,
		}.Build())
		require.NoErrorf(t, err, "ListGrantsForResourceType")
		for _, g := range resp.GetList() {
			require.Equal(t, "user", g.GetPrincipal().GetId().GetResourceType(), "got non-user principal: %s", g.GetPrincipal().GetId().GetResourceType())
			seen[g.GetId()] = true
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, total, len(seen), "post-filter ListGrantsForResourceType missed records: got %d (%v), want %d", len(seen), seen, total)
}
