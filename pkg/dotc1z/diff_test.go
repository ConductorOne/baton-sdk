package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestGenerateSyncDiff(t *testing.T) {
	ctx := context.Background()

	// Create paths for our test files
	baseFilePath := filepath.Join(c1zTests.workingDir, "base.c1z")
	newFilePath := filepath.Join(c1zTests.workingDir, "new.c1z")

	// Create the base C1Z file
	baseFile, err := NewC1ZFile(ctx, baseFilePath)
	require.NoError(t, err)

	// Start a sync in the base file
	baseSyncID, newSync, err := baseFile.StartSync(ctx)
	require.NoError(t, err)
	require.True(t, newSync)
	require.NotEmpty(t, baseSyncID)

	// Add a resource type to the base file
	resourceTypeID := testResourceType
	err = baseFile.PutResourceTypes(ctx, &v2.ResourceType{Id: resourceTypeID})
	require.NoError(t, err)

	// Add a resource to the base file
	baseResourceID := "base-resource"
	err = baseFile.PutResources(ctx, &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     baseResourceID,
		},
	})
	require.NoError(t, err)

	// End the sync in the base file
	err = baseFile.EndSync(ctx)
	require.NoError(t, err)

	// Close the base file
	err = baseFile.Close()
	require.NoError(t, err)

	// Create the new C1Z file
	newFile, err := NewC1ZFile(ctx, newFilePath)
	require.NoError(t, err)

	// Start a sync in the new file
	newSyncID, newSync, err := newFile.StartSync(ctx)
	require.NoError(t, err)
	require.True(t, newSync)
	require.NotEmpty(t, newSyncID)

	// Add the same resource type to the new file
	err = newFile.PutResourceTypes(ctx, &v2.ResourceType{Id: resourceTypeID})
	require.NoError(t, err)

	// Add the same resource to the new file
	err = newFile.PutResources(ctx, &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     baseResourceID,
		},
	})
	require.NoError(t, err)

	// Add a new resource to the new file that wasn't in the base file
	newResourceID := "new-resource"
	err = newFile.PutResources(ctx, &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     newResourceID,
		},
	})
	require.NoError(t, err)

	// End the sync in the new file
	err = newFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate a diff between the two files
	diffSyncID, err := newFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, diffSyncID)

	// Set the view sync ID to the diff sync ID
	err = newFile.ViewSync(ctx, diffSyncID)
	require.NoError(t, err)

	// Verify that the diff contains only the new resource
	resourcesResp, err := newFile.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: resourceTypeID,
	})
	require.NoError(t, err)
	require.Len(t, resourcesResp.List, 1)
	require.Equal(t, newResourceID, resourcesResp.List[0].Id.Resource)

	// Close the new file
	err = newFile.Close()
	require.NoError(t, err)
}
