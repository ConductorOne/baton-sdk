package dotc1z

import (
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func generateResources(count int, resourceType *v2.ResourceType) []*v2.Resource {
	if count < 1 {
		return nil
	}

	response := make([]*v2.Resource, count)

	for i := range count {
		id := ksuid.New().String()

		newResource, err := resource.NewResource(id, resourceType, id)
		if err != nil {
			panic(err)
		}

		response[i] = newResource
	}

	return response
}

func generateResourcesWithParent(count int, resourceType *v2.ResourceType, parentResourceId *v2.ResourceId) []*v2.Resource {
	if count < 1 {
		return nil
	}

	response := make([]*v2.Resource, count)

	for i := range count {
		id := ksuid.New().String()

		newResource, err := resource.NewResource(id, resourceType, id, resource.WithParentResourceID(parentResourceId))
		if err != nil {
			panic(err)
		}

		response[i] = newResource
	}

	return response
}

func TestPutResources(t *testing.T) {
	ctx := t.Context()

	tempDir := filepath.Join(t.TempDir(), "test.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	syncId, err := c1zFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceType := v2.ResourceType_builder{
		Id:          "test_resource_type",
		DisplayName: "Test Resource Type",
	}.Build()
	err = c1zFile.PutResourceTypes(ctx, resourceType)
	require.NoError(t, err)

	err = c1zFile.PutResources(
		ctx,
		generateResources(10_000, resourceType)...,
	)
	require.NoError(t, err)

	err = c1zFile.EndSync(ctx)
	require.NoError(t, err)

	stats, err := c1zFile.Stats(ctx, connectorstore.SyncTypeFull, syncId)
	require.NoError(t, err)

	require.Equal(t, int64(10_000), stats[resourceType.GetId()])

	err = c1zFile.Close(ctx)
	require.NoError(t, err)
}

func TestListResources(t *testing.T) {
	ctx := t.Context()

	tempDir := filepath.Join(t.TempDir(), "test.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer func() {
		err := c1zFile.Close(ctx)
		require.NoError(t, err)
	}()

	_, err = c1zFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Create two resource types
	userType := v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
	}.Build()
	groupType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
	}.Build()

	err = c1zFile.PutResourceTypes(ctx, userType, groupType)
	require.NoError(t, err)

	// Create resources
	users := generateResources(5, userType)
	groups := generateResources(3, groupType)

	err = c1zFile.PutResources(ctx, users...)
	require.NoError(t, err)

	err = c1zFile.PutResources(ctx, groups...)
	require.NoError(t, err)

	err = c1zFile.EndSync(ctx)
	require.NoError(t, err)

	t.Run("list all resources", func(t *testing.T) {
		resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 8) // 5 users + 3 groups
	})

	t.Run("list resources by type", func(t *testing.T) {
		resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "user",
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 5)
		for _, r := range resp.GetList() {
			require.Equal(t, "user", r.GetId().GetResourceType())
		}

		resp, err = c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "group",
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 3)
		for _, r := range resp.GetList() {
			require.Equal(t, "group", r.GetId().GetResourceType())
		}
	})

	t.Run("list resources with pagination", func(t *testing.T) {
		var allResources []*v2.Resource
		pageToken := ""

		for {
			resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
				PageSize:  2,
				PageToken: pageToken,
			}.Build())
			require.NoError(t, err)
			allResources = append(allResources, resp.GetList()...)

			if resp.GetNextPageToken() == "" {
				break
			}
			pageToken = resp.GetNextPageToken()
		}

		require.Len(t, allResources, 8)
	})
}

func TestListResourcesWithParentFilter(t *testing.T) {
	ctx := t.Context()

	tempDir := filepath.Join(t.TempDir(), "test.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer func() {
		err := c1zFile.Close(ctx)
		require.NoError(t, err)
	}()

	_, err = c1zFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Create resource types
	orgType := v2.ResourceType_builder{
		Id:          "organization",
		DisplayName: "Organization",
	}.Build()
	teamType := v2.ResourceType_builder{
		Id:          "team",
		DisplayName: "Team",
	}.Build()
	userType := v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
	}.Build()

	err = c1zFile.PutResourceTypes(ctx, orgType, teamType, userType)
	require.NoError(t, err)

	// Create parent organization resources
	org1, err := resource.NewResource("Org 1", orgType, "org-1")
	require.NoError(t, err)
	org2, err := resource.NewResource("Org 2", orgType, "org-2")
	require.NoError(t, err)

	err = c1zFile.PutResources(ctx, org1, org2)
	require.NoError(t, err)

	// Create teams under org1
	teamsUnderOrg1 := generateResourcesWithParent(3, teamType, org1.GetId())
	err = c1zFile.PutResources(ctx, teamsUnderOrg1...)
	require.NoError(t, err)

	// Create teams under org2
	teamsUnderOrg2 := generateResourcesWithParent(2, teamType, org2.GetId())
	err = c1zFile.PutResources(ctx, teamsUnderOrg2...)
	require.NoError(t, err)

	// Create users without parent
	usersNoParent := generateResources(4, userType)
	err = c1zFile.PutResources(ctx, usersNoParent...)
	require.NoError(t, err)

	// Create users under org1
	usersUnderOrg1 := generateResourcesWithParent(2, userType, org1.GetId())
	err = c1zFile.PutResources(ctx, usersUnderOrg1...)
	require.NoError(t, err)

	err = c1zFile.EndSync(ctx)
	require.NoError(t, err)

	t.Run("list all resources", func(t *testing.T) {
		resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
		require.NoError(t, err)
		// 2 orgs + 3 teams (org1) + 2 teams (org2) + 4 users (no parent) + 2 users (org1) = 13
		require.Len(t, resp.GetList(), 13)
	})

	t.Run("list resources by parent resource id", func(t *testing.T) {
		// List resources under org1
		resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ParentResourceId: org1.GetId(),
		}.Build())
		require.NoError(t, err)
		// 3 teams + 2 users under org1 = 5
		require.Len(t, resp.GetList(), 5)
		for _, r := range resp.GetList() {
			require.NotNil(t, r.GetParentResourceId())
			require.Equal(t, org1.GetId().GetResource(), r.GetParentResourceId().GetResource())
			require.Equal(t, org1.GetId().GetResourceType(), r.GetParentResourceId().GetResourceType())
		}

		// List resources under org2
		resp, err = c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ParentResourceId: org2.GetId(),
		}.Build())
		require.NoError(t, err)
		// 2 teams under org2
		require.Len(t, resp.GetList(), 2)
		for _, r := range resp.GetList() {
			require.NotNil(t, r.GetParentResourceId())
			require.Equal(t, org2.GetId().GetResource(), r.GetParentResourceId().GetResource())
			require.Equal(t, org2.GetId().GetResourceType(), r.GetParentResourceId().GetResourceType())
		}
	})

	t.Run("list resources by parent resource id and resource type", func(t *testing.T) {
		// List only teams under org1
		resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId:   "team",
			ParentResourceId: org1.GetId(),
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 3)
		for _, r := range resp.GetList() {
			require.Equal(t, "team", r.GetId().GetResourceType())
			require.NotNil(t, r.GetParentResourceId())
			require.Equal(t, org1.GetId().GetResource(), r.GetParentResourceId().GetResource())
		}

		// List only users under org1
		resp, err = c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId:   "user",
			ParentResourceId: org1.GetId(),
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 2)
		for _, r := range resp.GetList() {
			require.Equal(t, "user", r.GetId().GetResourceType())
			require.NotNil(t, r.GetParentResourceId())
			require.Equal(t, org1.GetId().GetResource(), r.GetParentResourceId().GetResource())
		}
	})

	t.Run("list resources by non-existent parent returns empty", func(t *testing.T) {
		nonExistentParent := v2.ResourceId_builder{
			ResourceType: "organization",
			Resource:     "org-does-not-exist",
		}.Build()

		resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ParentResourceId: nonExistentParent,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 0)
	})

	t.Run("list resources with parent filter and pagination", func(t *testing.T) {
		var allResources []*v2.Resource
		pageToken := ""

		for {
			resp, err := c1zFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
				ParentResourceId: org1.GetId(),
				PageSize:         2,
				PageToken:        pageToken,
			}.Build())
			require.NoError(t, err)
			allResources = append(allResources, resp.GetList()...)

			if resp.GetNextPageToken() == "" {
				break
			}
			pageToken = resp.GetNextPageToken()
		}

		require.Len(t, allResources, 5) // 3 teams + 2 users under org1
	})
}

func BenchmarkPutResources(b *testing.B) {
	cases := []struct {
		name           string
		resourcesCount int
	}{
		{"500", 500},
		{"1k", 1_000},
		{"10k", 10_000},
		{"50k", 50_000},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			ctx := b.Context()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				tempDir := filepath.Join(b.TempDir(), "test.c1z")

				c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
				require.NoError(b, err)

				_, err = c1zFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(b, err)

				resourceType := v2.ResourceType_builder{
					Id:          "test_resource_type",
					DisplayName: "Test Resource Type",
				}.Build()
				err = c1zFile.PutResourceTypes(ctx, resourceType)
				require.NoError(b, err)

				generatedData := generateResources(c.resourcesCount, resourceType)

				b.StartTimer()

				err = c1zFile.PutResources(
					ctx,
					generatedData...,
				)
				require.NoError(b, err)

				b.StopTimer()

				err = c1zFile.EndSync(ctx)
				require.NoError(b, err)

				err = c1zFile.Close(ctx)
				require.NoError(b, err)
			}
		})
	}
}
