package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

func TestPutGrants(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		DisplayName: "Test User",
	}

	grant := &v2.Grant{
		Id:          "test-grant",
		Entitlement: entitlement,
		Principal:   principal,
	}

	// Put the grant
	err = engine.PutGrants(ctx, grant)
	require.NoError(t, err)

	// Verify the grant was stored
	key := engine.keyEncoder.EncodeGrantKey(syncID, grant.GetId())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	// Decode and verify
	storedGrant := &v2.Grant{}
	envelope, err := engine.valueCodec.DecodeValue(value, storedGrant)
	require.NoError(t, err)
	require.NotNil(t, envelope)
	require.Equal(t, grant.GetId(), storedGrant.GetId())
	require.Equal(t, grant.GetEntitlement().GetId(), storedGrant.GetEntitlement().GetId())
	require.Equal(t, grant.GetPrincipal().GetId().GetResource(), storedGrant.GetPrincipal().GetId().GetResource())
}

func TestPutGrantsIfNewer(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		DisplayName: "Test User",
	}

	grant1 := &v2.Grant{
		Id:          "test-grant",
		Entitlement: entitlement,
		Principal:   principal,
	}

	grant2 := &v2.Grant{
		Id:          "test-grant",
		Entitlement: entitlement,
		Principal:   principal,
	}

	// Put the first grant
	err = engine.PutGrantsIfNewer(ctx, grant1)
	require.NoError(t, err)

	// Put the second grant (should update since it's newer)
	err = engine.PutGrantsIfNewer(ctx, grant2)
	require.NoError(t, err)

	// Verify the grant was updated
	key := engine.keyEncoder.EncodeGrantKey(syncID, grant1.GetId())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	// Decode and verify
	storedGrant := &v2.Grant{}
	envelope, err := engine.valueCodec.DecodeValue(value, storedGrant)
	require.NoError(t, err)
	require.NotNil(t, envelope)
	require.Equal(t, grant2.GetId(), storedGrant.GetId())
}

func TestDeleteGrant(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		DisplayName: "Test User",
	}

	grant := &v2.Grant{
		Id:          "test-grant",
		Entitlement: entitlement,
		Principal:   principal,
	}

	// Put the grant
	err = engine.PutGrants(ctx, grant)
	require.NoError(t, err)

	// Verify the grant exists
	key := engine.keyEncoder.EncodeGrantKey(syncID, grant.GetId())
	_, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	closer.Close()

	// Delete the grant
	err = engine.DeleteGrant(ctx, grant.GetId())
	require.NoError(t, err)

	// Verify the grant was deleted
	_, closer, err = engine.db.Get(key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestListGrants(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	principal1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user-1",
		},
		DisplayName: "Test User 1",
	}

	principal2 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user-2",
		},
		DisplayName: "Test User 2",
	}

	grant1 := &v2.Grant{
		Id:          "test-grant-1",
		Entitlement: entitlement,
		Principal:   principal1,
	}

	grant2 := &v2.Grant{
		Id:          "test-grant-2",
		Entitlement: entitlement,
		Principal:   principal2,
	}

	// Put the grants
	err = engine.PutGrants(ctx, grant1, grant2)
	require.NoError(t, err)

	// Test listing all grants
	req := &v2.GrantsServiceListGrantsRequest{
		PageSize: 10,
	}

	resp, err := engine.ListGrants(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 2)

	// Verify grant IDs
	grantIDs := make(map[string]bool)
	for _, grant := range resp.List {
		grantIDs[grant.GetId()] = true
	}
	require.True(t, grantIDs["test-grant-1"])
	require.True(t, grantIDs["test-grant-2"])
}

func TestListGrantsFilteredByResource(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource-1",
		},
		DisplayName: "Test Resource 1",
	}

	resource2 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource-2",
		},
		DisplayName: "Test Resource 2",
	}

	entitlement1 := &v2.Entitlement{
		Id:          "test-entitlement-1",
		DisplayName: "Test Entitlement 1",
		Resource:    resource1,
	}

	entitlement2 := &v2.Entitlement{
		Id:          "test-entitlement-2",
		DisplayName: "Test Entitlement 2",
		Resource:    resource2,
	}

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		DisplayName: "Test User",
	}

	grant1 := &v2.Grant{
		Id:          "test-grant-1",
		Entitlement: entitlement1,
		Principal:   principal,
	}

	grant2 := &v2.Grant{
		Id:          "test-grant-2",
		Entitlement: entitlement2,
		Principal:   principal,
	}

	// Put the grants
	err = engine.PutGrants(ctx, grant1, grant2)
	require.NoError(t, err)

	// Test listing grants filtered by resource1
	req := &v2.GrantsServiceListGrantsRequest{
		Resource: resource1,
		PageSize: 10,
	}

	resp, err := engine.ListGrants(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 1)
	require.Equal(t, "test-grant-1", resp.List[0].GetId())
}

func TestListGrantsForEntitlement(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement1 := &v2.Entitlement{
		Id:          "test-entitlement-1",
		DisplayName: "Test Entitlement 1",
		Resource:    resource,
	}

	entitlement2 := &v2.Entitlement{
		Id:          "test-entitlement-2",
		DisplayName: "Test Entitlement 2",
		Resource:    resource,
	}

	principal1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user-1",
		},
		DisplayName: "Test User 1",
	}

	principal2 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user-2",
		},
		DisplayName: "Test User 2",
	}

	grant1 := &v2.Grant{
		Id:          "test-grant-1",
		Entitlement: entitlement1,
		Principal:   principal1,
	}

	grant2 := &v2.Grant{
		Id:          "test-grant-2",
		Entitlement: entitlement1,
		Principal:   principal2,
	}

	grant3 := &v2.Grant{
		Id:          "test-grant-3",
		Entitlement: entitlement2,
		Principal:   principal1,
	}

	// Put the grants
	err = engine.PutGrants(ctx, grant1, grant2, grant3)
	require.NoError(t, err)

	// Test listing grants for entitlement1
	req := &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		Entitlement: entitlement1,
		PageSize:    10,
	}

	resp, err := engine.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 2)

	// Verify grant IDs
	grantIDs := make(map[string]bool)
	for _, grant := range resp.List {
		grantIDs[grant.GetId()] = true
	}
	require.True(t, grantIDs["test-grant-1"])
	require.True(t, grantIDs["test-grant-2"])
	require.False(t, grantIDs["test-grant-3"])
}

func TestListGrantsForEntitlementWithPrincipal(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	principal1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user-1",
		},
		DisplayName: "Test User 1",
	}

	principal2 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user-2",
		},
		DisplayName: "Test User 2",
	}

	grant1 := &v2.Grant{
		Id:          "test-grant-1",
		Entitlement: entitlement,
		Principal:   principal1,
	}

	grant2 := &v2.Grant{
		Id:          "test-grant-2",
		Entitlement: entitlement,
		Principal:   principal2,
	}

	// Put the grants
	err = engine.PutGrants(ctx, grant1, grant2)
	require.NoError(t, err)

	// Test listing grants for entitlement and principal1
	req := &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		Entitlement: entitlement,
		PrincipalId: principal1.GetId(),
		PageSize:    10,
	}

	resp, err := engine.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 1)
	require.Equal(t, "test-grant-1", resp.List[0].GetId())
}

func TestListGrantsForResourceType(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource-1",
		},
		DisplayName: "Test Resource 1",
	}

	resource2 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource-2",
		},
		DisplayName: "Test Resource 2",
	}

	resource3 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "other-resource-type",
			Resource:     "other-resource",
		},
		DisplayName: "Other Resource",
	}

	entitlement1 := &v2.Entitlement{
		Id:          "test-entitlement-1",
		DisplayName: "Test Entitlement 1",
		Resource:    resource1,
	}

	entitlement2 := &v2.Entitlement{
		Id:          "test-entitlement-2",
		DisplayName: "Test Entitlement 2",
		Resource:    resource2,
	}

	entitlement3 := &v2.Entitlement{
		Id:          "test-entitlement-3",
		DisplayName: "Test Entitlement 3",
		Resource:    resource3,
	}

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		DisplayName: "Test User",
	}

	grant1 := &v2.Grant{
		Id:          "test-grant-1",
		Entitlement: entitlement1,
		Principal:   principal,
	}

	grant2 := &v2.Grant{
		Id:          "test-grant-2",
		Entitlement: entitlement2,
		Principal:   principal,
	}

	grant3 := &v2.Grant{
		Id:          "test-grant-3",
		Entitlement: entitlement3,
		Principal:   principal,
	}

	// Put the grants
	err = engine.PutGrants(ctx, grant1, grant2, grant3)
	require.NoError(t, err)

	// Test listing grants for test-resource-type
	req := &reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest{
		ResourceTypeId: "test-resource-type",
		PageSize:       10,
	}

	resp, err := engine.ListGrantsForResourceType(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 2)

	// Verify grant IDs
	grantIDs := make(map[string]bool)
	for _, grant := range resp.List {
		grantIDs[grant.GetId()] = true
	}
	require.True(t, grantIDs["test-grant-1"])
	require.True(t, grantIDs["test-grant-2"])
	require.False(t, grantIDs["test-grant-3"])
}

func TestGrantIndexes(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		DisplayName: "Test User",
	}

	grant := &v2.Grant{
		Id:          "test-grant",
		Entitlement: entitlement,
		Principal:   principal,
	}

	// Put the grant
	err = engine.PutGrants(ctx, grant)
	require.NoError(t, err)

	// Verify grants-by-resource index
	resourceIndexKey := engine.keyEncoder.EncodeGrantsByResourceIndexKey(
		syncID,
		resource.GetId().GetResourceType(),
		resource.GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err := engine.db.Get(resourceIndexKey)
	require.NoError(t, err)
	closer.Close()

	// Verify grants-by-principal index
	principalIndexKey := engine.keyEncoder.EncodeGrantsByPrincipalIndexKey(
		syncID,
		principal.GetId().GetResourceType(),
		principal.GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err = engine.db.Get(principalIndexKey)
	require.NoError(t, err)
	closer.Close()

	// Verify grants-by-entitlement index
	entitlementIndexKey := engine.keyEncoder.EncodeGrantsByEntitlementIndexKey(
		syncID,
		entitlement.GetId(),
		principal.GetId().GetResourceType(),
		principal.GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err = engine.db.Get(entitlementIndexKey)
	require.NoError(t, err)
	closer.Close()
}

func TestGrantPagination(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	// Create multiple grants
	var grants []*v2.Grant
	for i := 0; i < 5; i++ {
		principal := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     fmt.Sprintf("test-user-%d", i),
			},
			DisplayName: fmt.Sprintf("Test User %d", i),
		}

		grant := &v2.Grant{
			Id:          fmt.Sprintf("test-grant-%d", i),
			Entitlement: entitlement,
			Principal:   principal,
		}
		grants = append(grants, grant)
	}

	// Put all grants
	err = engine.PutGrants(ctx, grants...)
	require.NoError(t, err)

	// Test pagination with page size 2
	req := &v2.GrantsServiceListGrantsRequest{
		PageSize: 2,
	}

	// First page
	resp, err := engine.ListGrants(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 2)
	require.NotEmpty(t, resp.NextPageToken)

	// Second page
	req.PageToken = resp.NextPageToken
	resp, err = engine.ListGrants(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 2)
	require.NotEmpty(t, resp.NextPageToken)

	// Third page (last page)
	req.PageToken = resp.NextPageToken
	resp, err = engine.ListGrants(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 1)
	require.Empty(t, resp.NextPageToken)
}

func TestGrantValidation(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test missing grant ID
	grant := &v2.Grant{
		Id: "",
	}
	err = engine.PutGrants(ctx, grant)
	require.Error(t, err)
	require.Contains(t, err.Error(), "grant missing ID")

	// Test missing entitlement
	grant = &v2.Grant{
		Id:          "test-grant",
		Entitlement: nil,
	}
	err = engine.PutGrants(ctx, grant)
	require.Error(t, err)
	require.Contains(t, err.Error(), "grant missing entitlement")

	// Test missing principal
	grant = &v2.Grant{
		Id: "test-grant",
		Entitlement: &v2.Entitlement{
			Id: "test-entitlement",
		},
		Principal: nil,
	}
	err = engine.PutGrants(ctx, grant)
	require.Error(t, err)
	require.Contains(t, err.Error(), "grant missing principal")
}

func TestDeleteNonExistentGrant(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Try to delete a non-existent grant
	err = engine.DeleteGrant(ctx, "non-existent-grant")
	require.Error(t, err)
	require.Contains(t, err.Error(), "grant not found")
}
