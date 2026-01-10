package dotc1z

import (
	"context"
	"fmt"
	"os"
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

func TestCleanup_DiffSyncs(t *testing.T) {
	ctx := context.Background()

	dbPath := filepath.Join(c1zTests.workingDir, "diff_cleanup.c1z")
	defer os.Remove(dbPath)

	syncFile, err := NewC1ZFile(ctx, dbPath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close()

	resourceTypeID := testResourceType

	// Create the initial full sync with some resources
	baseSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = syncFile.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     fmt.Sprintf("resource_%d", i),
			}.Build(),
			DisplayName: fmt.Sprintf("Resource %d", i),
		}.Build())
		require.NoError(t, err)
	}

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Create a second sync with different resources
	secondSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource_0",
		}.Build(),
		DisplayName: "Resource 0",
	}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource_3",
		}.Build(),
		DisplayName: "Resource 3 (new)",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate first diff
	upsertsSync1, deletionsSync1, err := syncFile.GenerateSyncDiff(ctx, baseSyncID, secondSyncID)
	require.NoError(t, err)

	// Create a third sync
	thirdSyncID, err := syncFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = syncFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = syncFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource_0",
		}.Build(),
		DisplayName: "Resource 0",
	}.Build())
	require.NoError(t, err)

	err = syncFile.EndSync(ctx)
	require.NoError(t, err)

	// Generate second diff
	upsertsSync2, deletionsSync2, err := syncFile.GenerateSyncDiff(ctx, secondSyncID, thirdSyncID)
	require.NoError(t, err)

	// Verify we have 3 full syncs and 4 diff syncs before cleanup
	runs, _, err := syncFile.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)

	var fullCount, diffCount int
	for _, r := range runs {
		switch r.Type {
		case connectorstore.SyncTypeFull:
			fullCount++
		case connectorstore.SyncTypePartialUpserts, connectorstore.SyncTypePartialDeletions:
			diffCount++
		}
	}
	require.Equal(t, 3, fullCount, "Expected 3 full syncs before cleanup")
	require.Equal(t, 4, diffCount, "Expected 4 diff syncs before cleanup")

	// Run cleanup
	err = syncFile.Cleanup(ctx)
	require.NoError(t, err)

	// Verify we have 1 full sync and 2 diff syncs after cleanup
	runs, _, err = syncFile.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)

	var fullSyncs, upsertSyncs, deletionSyncs []string
	for _, r := range runs {
		switch r.Type {
		case connectorstore.SyncTypeFull:
			fullSyncs = append(fullSyncs, r.ID)
		case connectorstore.SyncTypePartialUpserts:
			upsertSyncs = append(upsertSyncs, r.ID)
		case connectorstore.SyncTypePartialDeletions:
			deletionSyncs = append(deletionSyncs, r.ID)
		}
	}

	require.Len(t, fullSyncs, 1, "Expected 1 full sync after cleanup")
	require.Equal(t, thirdSyncID, fullSyncs[0], "Expected the latest full sync to be kept")

	require.Len(t, upsertSyncs, 1, "Expected 1 upserts sync after cleanup")
	require.Equal(t, upsertsSync2, upsertSyncs[0], "Expected the latest upserts sync to be kept")

	require.Len(t, deletionSyncs, 1, "Expected 1 deletions sync after cleanup")
	require.Equal(t, deletionsSync2, deletionSyncs[0], "Expected the latest deletions sync to be kept")

	// Verify the old diff syncs were deleted
	_, err = syncFile.getSync(ctx, upsertsSync1)
	require.Error(t, err, "Old upserts sync should be deleted")

	_, err = syncFile.getSync(ctx, deletionsSync1)
	require.Error(t, err, "Old deletions sync should be deleted")
}

func TestGenerateSyncDiffFromFile_Additions(t *testing.T) {
	ctx := context.Background()

	basePath := filepath.Join(c1zTests.workingDir, "diff_from_file_base.c1z")
	appliedPath := filepath.Join(c1zTests.workingDir, "diff_from_file_applied.c1z")
	defer os.Remove(basePath)
	defer os.Remove(appliedPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	// Create the base file with one resource
	baseFile, err := NewC1ZFile(ctx, basePath, opts...)
	require.NoError(t, err)

	baseSyncID, err := baseFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = baseFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = baseFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = baseFile.EndSync(ctx)
	require.NoError(t, err)

	// Create the applied file with base resource + a new resource
	appliedFile, err := NewC1ZFile(ctx, appliedPath, opts...)
	require.NoError(t, err)

	appliedSyncID, err := appliedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = appliedFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = appliedFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = appliedFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-B",
		}.Build(),
		DisplayName: "Resource B (new)",
	}.Build())
	require.NoError(t, err)

	err = appliedFile.EndSync(ctx)
	require.NoError(t, err)

	// Attach the applied file to the base file
	attached, err := baseFile.AttachFile(appliedFile, "attached")
	require.NoError(t, err)

	// Generate diff from file
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, baseSyncID, appliedSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, upsertsSyncID)
	require.NotEmpty(t, deletionsSyncID)

	// Detach
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts sync has only the new resource
	err = baseFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := baseFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-B", resourcesResp.GetList()[0].GetId().GetResource())

	// Verify deletions sync is empty
	err = baseFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = baseFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	// Verify sync types
	syncs, _, err := baseFile.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)

	var foundUpserts, foundDeletions bool
	for _, s := range syncs {
		if s.ID == upsertsSyncID {
			require.Equal(t, connectorstore.SyncTypePartialUpserts, s.Type)
			foundUpserts = true
		}
		if s.ID == deletionsSyncID {
			require.Equal(t, connectorstore.SyncTypePartialDeletions, s.Type)
			foundDeletions = true
		}
	}
	require.True(t, foundUpserts, "Expected to find upserts sync")
	require.True(t, foundDeletions, "Expected to find deletions sync")

	baseFile.Close()
	appliedFile.Close()
}

func TestGenerateSyncDiffFromFile_Deletions(t *testing.T) {
	ctx := context.Background()

	basePath := filepath.Join(c1zTests.workingDir, "diff_from_file_del_base.c1z")
	appliedPath := filepath.Join(c1zTests.workingDir, "diff_from_file_del_applied.c1z")
	defer os.Remove(basePath)
	defer os.Remove(appliedPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	// Create the base file with two resources
	baseFile, err := NewC1ZFile(ctx, basePath, opts...)
	require.NoError(t, err)

	baseSyncID, err := baseFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = baseFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = baseFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = baseFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-B",
		}.Build(),
		DisplayName: "Resource B",
	}.Build())
	require.NoError(t, err)

	err = baseFile.EndSync(ctx)
	require.NoError(t, err)

	// Create the applied file with only one resource (B is deleted)
	appliedFile, err := NewC1ZFile(ctx, appliedPath, opts...)
	require.NoError(t, err)

	appliedSyncID, err := appliedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = appliedFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = appliedFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = appliedFile.EndSync(ctx)
	require.NoError(t, err)

	// Attach and generate diff
	attached, err := baseFile.AttachFile(appliedFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, baseSyncID, appliedSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts sync is empty
	err = baseFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := baseFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	// Verify deletions sync has the deleted resource
	err = baseFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = baseFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-B", resourcesResp.GetList()[0].GetId().GetResource())

	baseFile.Close()
	appliedFile.Close()
}

func TestGenerateSyncDiffFromFile_Modifications(t *testing.T) {
	ctx := context.Background()

	basePath := filepath.Join(c1zTests.workingDir, "diff_from_file_mod_base.c1z")
	appliedPath := filepath.Join(c1zTests.workingDir, "diff_from_file_mod_applied.c1z")
	defer os.Remove(basePath)
	defer os.Remove(appliedPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	// Create the base file with a resource
	baseFile, err := NewC1ZFile(ctx, basePath, opts...)
	require.NoError(t, err)

	baseSyncID, err := baseFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = baseFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = baseFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Original Name",
	}.Build())
	require.NoError(t, err)

	err = baseFile.EndSync(ctx)
	require.NoError(t, err)

	// Create the applied file with the same resource but modified
	appliedFile, err := NewC1ZFile(ctx, appliedPath, opts...)
	require.NoError(t, err)

	appliedSyncID, err := appliedFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = appliedFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = appliedFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Modified Name",
	}.Build())
	require.NoError(t, err)

	err = appliedFile.EndSync(ctx)
	require.NoError(t, err)

	// Attach and generate diff
	attached, err := baseFile.AttachFile(appliedFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, baseSyncID, appliedSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts sync has the modified resource
	err = baseFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := baseFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-A", resourcesResp.GetList()[0].GetId().GetResource())
	require.Equal(t, "Modified Name", resourcesResp.GetList()[0].GetDisplayName())

	// Verify deletions sync is empty
	err = baseFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = baseFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	baseFile.Close()
	appliedFile.Close()
}
