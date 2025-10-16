package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestGenerateSyncDiff(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "file.c1z")

	// Create the base C1Z file
	syncFile, err := NewC1ZFile(ctx, filePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close()

	// Start a sync in the base file
	baseSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, baseSyncID)

	// Add a resource type to the base file
	resourceTypeID := testResourceType
	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	// Add a resource to the base file
	baseResourceID := "base-resource"
	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     baseResourceID,
		}.Build(),
	}.Build())
	require.NoError(t, err)

	// End the sync in the base file
	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Start a sync in the new file
	newSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, newSyncID)

	// Add the same resource type to the new file
	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	// Add the same resource to the new file
	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     baseResourceID,
		}.Build(),
	}.Build())
	require.NoError(t, err)

	// Add a new resource to the new file that wasn't in the base file
	newResourceID := "new-resource"
	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     newResourceID,
		}.Build(),
	}.Build())
	require.NoError(t, err)

	// End the sync in the new file
	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate a diff between the two files
	diffSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, diffSyncID)

	// Set the view sync ID to the diff sync ID
	err = syncFile.ViewSync(ctx, diffSyncID)
	require.NoError(t, err)

	// Verify that the diff contains only the new resource
	resourcesResp, err := syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, newResourceID, resourcesResp.GetList()[0].GetId().GetResource())

	// Close the new file
	err = syncFile.Close()
	require.NoError(t, err)
}
