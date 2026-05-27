package dotc1z

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestListResourcesTraitFilter exercises the new trait filter
// (RFC §B2). resource_types is small so the implementation
// pre-resolves matching IDs from v1_resource_types and filters
// resources by IN (...). Tests cover:
//   - trait=USER returns only user-trait resources
//   - trait + explicit resource_type_id intersect
//   - trait with no matching RT returns empty
func TestListResourcesTraitFilter(t *testing.T) {
	ctx := t.Context()
	c1z, err := NewC1ZFile(ctx, filepath.Join(t.TempDir(), "trait.c1z"))
	require.NoError(t, err)
	defer func() { _ = c1z.Close(ctx) }()

	_, err = c1z.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Two user-trait RTs (employee, contractor), one group-trait
	// RT, one app-trait RT. ListResources(trait=USER) should
	// return only employee+contractor resources.
	require.NoError(t, c1z.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "employee", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "contractor", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "team", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "github", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP}}.Build(),
	))
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	require.NoError(t, c1z.PutResources(ctx,
		mkRes("employee", "e1"), mkRes("employee", "e2"),
		mkRes("contractor", "c1"),
		mkRes("team", "t1"), mkRes("team", "t2"), mkRes("team", "t3"),
		mkRes("github", "gh"),
	))
	require.NoError(t, c1z.EndSync(ctx))

	t.Run("trait=USER returns 3", func(t *testing.T) {
		resp, err := c1z.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			Trait:    v2.ResourceType_TRAIT_USER,
			PageSize: 100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 3)
		for _, r := range resp.GetList() {
			rt := r.GetId().GetResourceType()
			require.True(t, rt == "employee" || rt == "contractor", "got rt %q", rt)
		}
	})

	t.Run("trait + explicit RT intersects", func(t *testing.T) {
		resp, err := c1z.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			Trait:          v2.ResourceType_TRAIT_USER,
			ResourceTypeId: "employee",
			PageSize:       100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 2)
	})

	t.Run("trait + RT outside intersection returns empty", func(t *testing.T) {
		resp, err := c1z.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			Trait:          v2.ResourceType_TRAIT_USER,
			ResourceTypeId: "team", // group, not user
			PageSize:       100,
		}.Build())
		require.NoError(t, err)
		require.Empty(t, resp.GetList())
	})

	t.Run("trait=ROLE with no matching RT returns empty", func(t *testing.T) {
		resp, err := c1z.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			Trait:    v2.ResourceType_TRAIT_ROLE,
			PageSize: 100,
		}.Build())
		require.NoError(t, err)
		require.Empty(t, resp.GetList())
	})
}
