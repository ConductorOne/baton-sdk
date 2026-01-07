package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestGenerateSyncDiff_Additions(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "diff_additions.c1z")

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
	upsertsSyncID, deletionsSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, upsertsSyncID)
	require.NotEmpty(t, deletionsSyncID)

	// Verify upserts sync contains only the new resource
	err = syncFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, newResourceID, resourcesResp.GetList()[0].GetId().GetResource())

	// Verify deletions sync is empty (nothing was deleted)
	err = syncFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)
}

func TestGenerateSyncDiff_Deletions(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "diff_deletions.c1z")

	syncFile, err := NewC1ZFile(ctx, filePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close()

	resourceTypeID := testResourceType

	// Base sync with resources A, B, C
	baseSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	for _, id := range []string{"resource-a", "resource-b", "resource-c"} {
		err = syncFile.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     id,
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// New sync with only resources A, B (C deleted)
	newSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	for _, id := range []string{"resource-a", "resource-b"} {
		err = syncFile.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     id,
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate diff
	upsertsSyncID, deletionsSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)

	// Verify upserts sync is empty (nothing added)
	err = syncFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	// Verify deletions sync contains resource-c
	err = syncFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-c", resourcesResp.GetList()[0].GetId().GetResource())
}

func TestGenerateSyncDiff_Modifications(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "diff_modifications.c1z")

	syncFile, err := NewC1ZFile(ctx, filePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close()

	resourceTypeID := testResourceType

	// Base sync with resource A (displayName: "Original")
	baseSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-a",
		}.Build(),
		DisplayName: "Original",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// New sync with resource A (displayName: "Modified")
	newSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-a",
		}.Build(),
		DisplayName: "Modified",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate diff
	upsertsSyncID, deletionsSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)

	// Verify upserts sync contains the modified resource
	err = syncFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-a", resourcesResp.GetList()[0].GetId().GetResource())
	require.Equal(t, "Modified", resourcesResp.GetList()[0].GetDisplayName())

	// Verify deletions sync is empty
	err = syncFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)
}

func TestGenerateSyncDiff_LinkedSyncID(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "diff_linked.c1z")

	syncFile, err := NewC1ZFile(ctx, filePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close()

	resourceTypeID := testResourceType

	// Create base sync
	baseSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Create new sync
	newSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate diff
	upsertsSyncID, deletionsSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)

	// Verify linked_sync_id is set correctly on both syncs
	upsertsSync, err := syncFile.getSync(ctx, upsertsSyncID)
	require.NoError(t, err)
	require.Equal(t, deletionsSyncID, upsertsSync.LinkedSyncID)
	require.Equal(t, connectorstore.SyncTypePartialUpserts, upsertsSync.Type)

	deletionsSync, err := syncFile.getSync(ctx, deletionsSyncID)
	require.NoError(t, err)
	require.Equal(t, upsertsSyncID, deletionsSync.LinkedSyncID)
	require.Equal(t, connectorstore.SyncTypePartialDeletions, deletionsSync.Type)

	// Verify both have the same parent_sync_id (base sync)
	require.Equal(t, baseSyncID, upsertsSync.ParentSyncID)
	require.Equal(t, baseSyncID, deletionsSync.ParentSyncID)
}

func TestGenerateSyncDiff_Mixed(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "diff_mixed.c1z")

	syncFile, err := NewC1ZFile(ctx, filePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close()

	resourceTypeID := testResourceType

	// Base sync with resources A, B, C
	baseSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-a",
		}.Build(),
		DisplayName: "A Original",
	}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-b",
		}.Build(),
		DisplayName: "B",
	}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-c",
		}.Build(),
		DisplayName: "C",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// New sync: A modified, B unchanged, C deleted, D added
	newSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-a",
		}.Build(),
		DisplayName: "A Modified",
	}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-b",
		}.Build(),
		DisplayName: "B",
	}.Build())
	require.NoError(t, err)

	// resource-c not added (deleted)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-d",
		}.Build(),
		DisplayName: "D",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate diff
	upsertsSyncID, deletionsSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)

	// Verify upserts contains A (modified) and D (added)
	err = syncFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 2)

	resourceIDs := make(map[string]string)
	for _, r := range resourcesResp.GetList() {
		resourceIDs[r.GetId().GetResource()] = r.GetDisplayName()
	}
	require.Equal(t, "A Modified", resourceIDs["resource-a"])
	require.Equal(t, "D", resourceIDs["resource-d"])

	// Verify deletions contains C
	err = syncFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-c", resourcesResp.GetList()[0].GetId().GetResource())
}

func TestGenerateSyncDiff_NoChanges(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "diff_nochanges.c1z")

	syncFile, err := NewC1ZFile(ctx, filePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close()

	resourceTypeID := testResourceType

	// Base sync with resource A
	baseSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-a",
		}.Build(),
		DisplayName: "A",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// New sync with identical resource A
	newSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-a",
		}.Build(),
		DisplayName: "A",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate diff
	upsertsSyncID, deletionsSyncID, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, newSyncID)
	require.NoError(t, err)

	// Verify both syncs are empty
	err = syncFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	err = syncFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = syncFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)
}
