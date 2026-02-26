package dotc1z

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestGenerateSyncDiff(t *testing.T) {
	ctx := context.Background()

	filePath := filepath.Join(c1zTests.workingDir, "diff_additions.c1z")

	// Create the base C1Z file
	syncFile, err := NewC1ZFile(ctx, filePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer syncFile.Close(ctx)

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
	err = syncFile.Close(ctx)
	require.NoError(t, err)

	// Verify no extra items
	require.Len(t, resourcesResp.GetList(), 1)
}

func TestGenerateSyncDiffFromFile_Additions(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	// Create the OLD file with one resource.
	// We're attaching this later, so don't use an exclusive lock.
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create the NEW file with old resource + a new resource
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-B",
		}.Build(),
		DisplayName: "Resource B (new)",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Attach the OLD file to the NEW file (main=NEW, attached=OLD)
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	// Generate diff from file
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, upsertsSyncID)
	require.NotEmpty(t, deletionsSyncID)

	// Detach
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts sync has only the new resource (in newFile which is main)
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-B", resourcesResp.GetList()[0].GetId().GetResource())

	// Verify deletions sync is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	// Verify sync types and linked_sync_id
	syncs, _, err := newFile.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)

	var foundUpserts, foundDeletions bool
	for _, s := range syncs {
		if s.ID == upsertsSyncID {
			require.Equal(t, connectorstore.SyncTypePartialUpserts, s.Type)
			require.Equal(t, deletionsSyncID, s.LinkedSyncID, "upserts sync should link to deletions sync")
			foundUpserts = true
		}
		if s.ID == deletionsSyncID {
			require.Equal(t, connectorstore.SyncTypePartialDeletions, s.Type)
			require.Equal(t, upsertsSyncID, s.LinkedSyncID, "deletions sync should link to upserts sync")
			foundDeletions = true
		}
	}
	require.True(t, foundUpserts, "Expected to find upserts sync")
	require.True(t, foundDeletions, "Expected to find deletions sync")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_Deletions(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_del_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_del_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	// Create the OLD file with two resources
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-B",
		}.Build(),
		DisplayName: "Resource B",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create the NEW file with only one resource (B is deleted)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Attach OLD to NEW (main=NEW, attached=OLD)
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts sync is empty (in newFile which is main)
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	// Verify deletions sync has the deleted resource
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-B", resourcesResp.GetList()[0].GetId().GetResource())

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_Modifications(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_mod_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_mod_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	// Create the OLD file with a resource
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Original Name",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create the NEW file with the same resource but modified
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Modified Name",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Attach OLD to NEW (main=NEW, attached=OLD)
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts sync has the modified resource (in newFile which is main)
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-A", resourcesResp.GetList()[0].GetId().GetResource())
	require.Equal(t, "Modified Name", resourcesResp.GetList()[0].GetDisplayName())

	// Verify deletions sync is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0)

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_MixedChanges(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_mixed_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_mixed_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with resources A, B, C
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	for _, id := range []string{"resource-A", "resource-B", "resource-C"} {
		err = oldFile.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     id,
			}.Build(),
			DisplayName: "Original " + id,
		}.Build())
		require.NoError(t, err)
	}

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file:
	// - A: modified (DisplayName changed)
	// - B: unchanged
	// - C: deleted (not present)
	// - D: added (new)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	// A - modified
	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Modified resource-A",
	}.Build())
	require.NoError(t, err)

	// B - unchanged
	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-B",
		}.Build(),
		DisplayName: "Original resource-B",
	}.Build())
	require.NoError(t, err)

	// C - deleted (not added)

	// D - added
	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-D",
		}.Build(),
		DisplayName: "New resource-D",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts: A (modified) and D (added)
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 2)

	upsertIDs := make(map[string]string)
	for _, r := range resourcesResp.GetList() {
		upsertIDs[r.GetId().GetResource()] = r.GetDisplayName()
	}
	require.Equal(t, "Modified resource-A", upsertIDs["resource-A"])
	require.Equal(t, "New resource-D", upsertIDs["resource-D"])
	_, hasB := upsertIDs["resource-B"]
	require.False(t, hasB, "unchanged resource-B should NOT be in upserts")

	// Verify deletions: C
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 1)
	require.Equal(t, "resource-C", resourcesResp.GetList()[0].GetId().GetResource())

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_NoChanges(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_nochange_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_nochange_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with resource A
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with identical resource A
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts is empty
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0, "upserts should not include resources when no changes")

	// Resource types are always included in upserts.
	rtResp, err := newFile.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 1, "upserts should include resource types")

	// Verify deletions is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0, "deletions should be empty when no changes")

	rtResp, err = newFile.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 0, "deletions should not include resource types when no changes")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_EntitlementsOnly(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_ent_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_ent_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with entitlement A
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-A",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Entitlement A",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with entitlement A + new entitlement B
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-A",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Entitlement A",
	}.Build())
	require.NoError(t, err)

	err = newFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-B",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Entitlement B (new)",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts has entitlement B
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	entsResp, err := newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entsResp.GetList(), 1)
	require.Equal(t, "entitlement-B", entsResp.GetList()[0].GetId())

	// Verify deletions is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	entsResp, err = newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entsResp.GetList(), 0)

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_GrantsOnly(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_grant_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_grant_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with grant A
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutGrants(ctx, v2.Grant_builder{
		Id: "grant-A",
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "principal-1",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeID,
					Resource:     "resource-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with grant A (unchanged) + grant B (new)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutGrants(ctx, v2.Grant_builder{
		Id: "grant-A",
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "principal-1",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeID,
					Resource:     "resource-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = newFile.PutGrants(ctx, v2.Grant_builder{
		Id: "grant-B",
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "principal-2",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeID,
					Resource:     "resource-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts has grant B
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	grantsResp, err := newFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1)
	require.Equal(t, "grant-B", grantsResp.GetList()[0].GetId())

	// Verify deletions is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	grantsResp, err = newFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 0)

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_EmptyBase(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_emptybase_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_emptybase_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with empty sync (no resources)
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	// No resources added - empty sync

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with resources A and B
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-B",
		}.Build(),
		DisplayName: "Resource B",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts has both resources (all are new)
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 2)

	// Verify specific resources
	upsertIDs := make(map[string]bool)
	for _, r := range resourcesResp.GetList() {
		upsertIDs[r.GetId().GetResource()] = true
	}
	require.True(t, upsertIDs["resource-A"], "resource-A should be in upserts")
	require.True(t, upsertIDs["resource-B"], "resource-B should be in upserts")

	// Verify deletions is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0, "deletions should be empty when base is empty")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_EmptyNew(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_emptynew_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_emptynew_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with resources A and B
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-A",
		}.Build(),
		DisplayName: "Resource A",
	}.Build())
	require.NoError(t, err)

	err = oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     "resource-B",
		}.Build(),
		DisplayName: "Resource B",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with empty sync (no resources)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	// No resources added - empty sync

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts is empty
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	resourcesResp, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0, "upserts should be empty when new is empty")

	// Verify deletions has both resources (all are deleted)
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 2)

	// Verify specific resources
	deletionIDs := make(map[string]bool)
	for _, r := range resourcesResp.GetList() {
		deletionIDs[r.GetId().GetResource()] = true
	}
	require.True(t, deletionIDs["resource-A"], "resource-A should be in deletions")
	require.True(t, deletionIDs["resource-B"], "resource-B should be in deletions")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_EntitlementsDeletions(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_ent_del_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_ent_del_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with entitlements A and B
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-A",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Entitlement A",
	}.Build())
	require.NoError(t, err)

	err = oldFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-B",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Entitlement B",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with only entitlement A (B is deleted)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-A",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Entitlement A",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts is empty
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	entsResp, err := newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entsResp.GetList(), 0, "upserts should be empty")

	// Verify deletions has entitlement B
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	entsResp, err = newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entsResp.GetList(), 1)
	require.Equal(t, "entitlement-B", entsResp.GetList()[0].GetId())

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_EntitlementsModifications(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_ent_mod_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_ent_mod_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with entitlement A
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-A",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Original Entitlement A",
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with modified entitlement A
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "entitlement-A",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "resource-1",
			}.Build(),
		}.Build(),
		DisplayName: "Modified Entitlement A",
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts has the modified entitlement
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	entsResp, err := newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entsResp.GetList(), 1)
	require.Equal(t, "entitlement-A", entsResp.GetList()[0].GetId())
	require.Equal(t, "Modified Entitlement A", entsResp.GetList()[0].GetDisplayName())

	// Verify deletions is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	entsResp, err = newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entsResp.GetList(), 0, "deletions should be empty")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_GrantsDeletions(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_from_file_grant_del_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_from_file_grant_del_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	resourceTypeID := testResourceType

	// Create OLD file with grants A and B
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = oldFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = oldFile.PutGrants(ctx, v2.Grant_builder{
		Id: "grant-A",
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "principal-1",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeID,
					Resource:     "resource-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = oldFile.PutGrants(ctx, v2.Grant_builder{
		Id: "grant-B",
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "principal-2",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeID,
					Resource:     "resource-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	// Create NEW file with only grant A (B is deleted)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = newFile.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	err = newFile.PutGrants(ctx, v2.Grant_builder{
		Id: "grant-A",
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeID,
				Resource:     "principal-1",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeID,
					Resource:     "resource-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = newFile.EndSync(ctx)
	require.NoError(t, err)
	err = newFile.SetSupportsDiff(ctx, newSyncID)
	require.NoError(t, err)

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts is empty
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	grantsResp, err := newFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 0, "upserts should be empty")

	// Verify deletions has grant B
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	grantsResp, err = newFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1)
	require.Equal(t, "grant-B", grantsResp.GetList()[0].GetId())

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_MissingExpansionMarker(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_missing_marker_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_missing_marker_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	// Create the OLD file with an expandable grant (but WITHOUT setting expansion marker)
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	err = oldFile.PutResourceTypes(ctx, groupRT, userRT)
	require.NoError(t, err)

	g1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "G1",
	}.Build()
	g2 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(),
		DisplayName: "G2",
	}.Build()
	err = oldFile.PutResources(ctx, g1, g2)
	require.NoError(t, err)

	e1 := v2.Entitlement_builder{
		Id:          "group:g1:member",
		Resource:    g1,
		Slug:        "member",
		DisplayName: "member",
	}.Build()
	e2 := v2.Entitlement_builder{
		Id:          "group:g2:member",
		Resource:    g2,
		Slug:        "member",
		DisplayName: "member",
	}.Build()
	err = oldFile.PutEntitlements(ctx, e1, e2)
	require.NoError(t, err)

	// Create an expandable grant (g1 -> e2 with expansion annotation)
	expandableGrant := v2.Grant_builder{
		Id:          "grant:g1:e2",
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	err = oldFile.PutGrants(ctx, expandableGrant)
	require.NoError(t, err)

	// Simulate an old sync that predates supports_diff by explicitly clearing it.
	// (New code defaults supports_diff=1 for newly created syncs.)
	_, err = oldFile.db.ExecContext(ctx, "UPDATE "+syncRuns.Name()+" SET supports_diff=0 WHERE sync_id = ?", oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)

	// Create the NEW file (minimal, just needs to exist)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	err = newFile.PutResourceTypes(ctx, groupRT, userRT)
	require.NoError(t, err)
	err = newFile.PutResources(ctx, g1, g2)
	require.NoError(t, err)
	err = newFile.PutEntitlements(ctx, e1, e2)
	require.NoError(t, err)
	err = newFile.EndSync(ctx)
	require.NoError(t, err)

	// Attach and try to generate diff - should fail
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	_, _, err = attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrOldSyncMissingExpansionMarker), "expected ErrOldSyncMissingExpansionMarker, got: %v", err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

func TestGenerateSyncDiffFromFile_WithExpansionMarker(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_with_marker_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_with_marker_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	// Create the OLD file with an expandable grant AND set expansion marker
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	err = oldFile.PutResourceTypes(ctx, groupRT, userRT)
	require.NoError(t, err)

	g1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "G1",
	}.Build()
	g2 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(),
		DisplayName: "G2",
	}.Build()
	err = oldFile.PutResources(ctx, g1, g2)
	require.NoError(t, err)

	e1 := v2.Entitlement_builder{
		Id:          "group:g1:member",
		Resource:    g1,
		Slug:        "member",
		DisplayName: "member",
	}.Build()
	e2 := v2.Entitlement_builder{
		Id:          "group:g2:member",
		Resource:    g2,
		Slug:        "member",
		DisplayName: "member",
	}.Build()
	err = oldFile.PutEntitlements(ctx, e1, e2)
	require.NoError(t, err)

	// Create an expandable grant (g1 -> e2 with expansion annotation)
	expandableGrant := v2.Grant_builder{
		Id:          "grant:g1:e2",
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	err = oldFile.PutGrants(ctx, expandableGrant)
	require.NoError(t, err)

	// Set the expansion marker - simulates sync expanded with new code
	err = oldFile.SetSupportsDiff(ctx, oldSyncID)
	require.NoError(t, err)

	err = oldFile.EndSync(ctx)
	require.NoError(t, err)

	// Create the NEW file (minimal, just needs to exist)
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	err = newFile.PutResourceTypes(ctx, groupRT, userRT)
	require.NoError(t, err)
	err = newFile.PutResources(ctx, g1, g2)
	require.NoError(t, err)
	err = newFile.PutEntitlements(ctx, e1, e2)
	require.NoError(t, err)
	err = newFile.EndSync(ctx)
	require.NoError(t, err)

	// Attach and generate diff - should succeed
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, upsertsSyncID)
	require.NotEmpty(t, deletionsSyncID)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestGenerateSyncDiffFromFile_ExpansionOnlyChange verifies that a grant whose
// data blob is identical but whose expansion column changed is detected as a
// modification in the cross-file diff.
func TestGenerateSyncDiffFromFile_ExpansionOnlyChange(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_expansion_only_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_expansion_only_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()

	e1 := v2.Entitlement_builder{Id: "group:g1:member", Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: "group:g2:member", Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	grantID := "grant:g1:e2"

	// OLD: grant with expansion shallow=false
	grantOld := v2.Grant_builder{
		Id:          grantID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// NEW: same grant but expansion shallow=true (expansion column differs)
	grantNew := v2.Grant_builder{
		Id:          grantID,
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         true,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Create OLD file
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2))
	require.NoError(t, oldFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, oldFile.PutGrants(ctx, grantOld))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	// Create NEW file
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2))
	require.NoError(t, newFile.PutEntitlements(ctx, e1, e2))
	require.NoError(t, newFile.PutGrants(ctx, grantNew))
	require.NoError(t, newFile.EndSync(ctx))

	// Generate diff
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, _, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	// Verify added edges via cross-DB query (NEW version: shallow=true).
	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.Len(t, addedDefs, 1, "should have one added expandable grant")
	require.True(t, addedDefs[0].Shallow, "added grant should have NEW expansion (shallow=true)")

	// Verify removed edges via cross-DB query (OLD version: shallow=false).
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.Len(t, removedDefs, 1, "should have one removed expandable grant")
	require.False(t, removedDefs[0].Shallow, "removed grant should have OLD expansion (shallow=false)")

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Also verify the upserts sync in main has the NEW version.
	upsertsRows, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:   connectorstore.GrantListModeExpansion,
		SyncID: upsertsSyncID,
	})
	require.NoError(t, err)
	require.Len(t, upsertsRows.Rows, 1, "upserts should contain the modified grant")
	require.Equal(t, grantID, upsertsRows.Rows[0].Expansion.GrantExternalID)
	require.True(t, upsertsRows.Rows[0].Expansion.Shallow, "upserts should contain NEW expansion version (shallow=true)")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestComputeExpandableGrants_StableExternalIDRetarget verifies that moving an expandable grant
// to a different entitlement with the same external_id is surfaced as remove+add edge delta.
func TestComputeExpandableGrants_StableExternalIDRetarget(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_expandable_retarget_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_expandable_retarget_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()

	g1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "G1",
	}.Build()
	g2 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(),
		DisplayName: "G2",
	}.Build()
	g3 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g3"}.Build(),
		DisplayName: "G3",
	}.Build()

	eSrc := v2.Entitlement_builder{Id: "group:g1:member", Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	eOld := v2.Entitlement_builder{Id: "group:g2:member", Resource: g2, Slug: "member", DisplayName: "member"}.Build()
	eNew := v2.Entitlement_builder{Id: "group:g3:member", Resource: g3, Slug: "member", DisplayName: "member"}.Build()

	grantID := "grant:stable-retarget"
	expandable := annotations.New(v2.GrantExpandable_builder{
		EntitlementIds:  []string{eSrc.GetId()},
		Shallow:         false,
		ResourceTypeIds: []string{"user"},
	}.Build())

	grantOld := v2.Grant_builder{
		Id:          grantID,
		Entitlement: eOld,
		Principal:   g1,
		Annotations: expandable,
	}.Build()
	grantNew := v2.Grant_builder{
		Id:          grantID,
		Entitlement: eNew,
		Principal:   g1,
		Annotations: expandable,
	}.Build()

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, g3))
	require.NoError(t, oldFile.PutEntitlements(ctx, eSrc, eOld, eNew))
	require.NoError(t, oldFile.PutGrants(ctx, grantOld))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, g3))
	require.NoError(t, newFile.PutEntitlements(ctx, eSrc, eOld, eNew))
	require.NoError(t, newFile.PutGrants(ctx, grantNew))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.Len(t, addedDefs, 1, "retarget should add one edge definition for the NEW destination")
	require.Equal(t, grantID, addedDefs[0].GrantExternalID)
	require.Equal(t, eNew.GetId(), addedDefs[0].TargetEntitlementID)

	require.Len(t, removedDefs, 1, "retarget should remove one edge definition for the OLD destination")
	require.Equal(t, grantID, removedDefs[0].GrantExternalID)
	require.Equal(t, eOld.GetId(), removedDefs[0].TargetEntitlementID)

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestGenerateSyncDiffFromFile_GrantDataModification verifies that modifying a grant's
// data blob (not expansion) emits the NEW version in upserts and the OLD version in deletions.
func TestGenerateSyncDiffFromFile_GrantDataModification(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_grant_data_mod_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_grant_data_mod_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	rt := v2.ResourceType_builder{Id: "group"}.Build()
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	grantOld := v2.Grant_builder{
		Id: "grant-mod", Entitlement: ent, Principal: u1,
	}.Build()
	grantNew := v2.Grant_builder{
		Id: "grant-mod", Entitlement: ent, Principal: u1,
		Annotations: annotations.New(&v2.GrantImmutable{}),
	}.Build()

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, rt))
	require.NoError(t, oldFile.PutResources(ctx, g1, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, ent))
	require.NoError(t, oldFile.PutGrants(ctx, grantOld))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, rt))
	require.NoError(t, newFile.PutResources(ctx, g1, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, ent))
	require.NoError(t, newFile.PutGrants(ctx, grantNew))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, _, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	// Verify changed grant entitlement IDs via cross-DB query.
	changedIDs, err := attached.ComputeChangedGrantEntitlementIDs(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.Contains(t, changedIDs, "ent1", "modified grant's entitlement should appear in changed IDs")

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	upserts, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayload, SyncID: upsertsSyncID,
	})
	require.NoError(t, err)
	require.Len(t, upserts.Rows, 1, "upserts should contain the modified grant")
	require.Equal(t, "grant-mod", upserts.Rows[0].Grant.GetId())

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestGenerateSyncDiffFromFile_GrantMixedChanges tests add + modify + delete + unchanged grants in one diff.
func TestGenerateSyncDiffFromFile_GrantMixedChanges(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_grant_mixed_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_grant_mixed_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	rt := v2.ResourceType_builder{Id: "group"}.Build()
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build()}.Build()
	u3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u3"}.Build()}.Build()
	u4 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u4"}.Build()}.Build()
	ent := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	grantA := v2.Grant_builder{Id: "grant-A", Entitlement: ent, Principal: u1}.Build()
	grantBOld := v2.Grant_builder{Id: "grant-B", Entitlement: ent, Principal: u2}.Build()
	grantBNew := v2.Grant_builder{
		Id: "grant-B", Entitlement: ent, Principal: u2,
		Annotations: annotations.New(&v2.GrantImmutable{}),
	}.Build()
	grantC := v2.Grant_builder{Id: "grant-C", Entitlement: ent, Principal: u3}.Build()
	grantD := v2.Grant_builder{Id: "grant-D", Entitlement: ent, Principal: u4}.Build()

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, rt))
	require.NoError(t, oldFile.PutResources(ctx, g1, u1, u2, u3, u4))
	require.NoError(t, oldFile.PutEntitlements(ctx, ent))
	require.NoError(t, oldFile.PutGrants(ctx, grantA, grantBOld, grantC))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, rt))
	require.NoError(t, newFile.PutResources(ctx, g1, u1, u2, u3, u4))
	require.NoError(t, newFile.PutEntitlements(ctx, ent))
	require.NoError(t, newFile.PutGrants(ctx, grantA, grantBNew, grantD))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	// Verify changed grants via cross-DB query (should include B's entitlement and C's entitlement).
	changedIDs, err := attached.ComputeChangedGrantEntitlementIDs(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.Contains(t, changedIDs, "ent1")

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Upserts: grant-B (modified NEW) and grant-D (added). NOT grant-A (unchanged).
	upserts, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayload, SyncID: upsertsSyncID,
	})
	require.NoError(t, err)
	upsertIDs := map[string]bool{}
	for _, r := range upserts.Rows {
		upsertIDs[r.Grant.GetId()] = true
	}
	require.Len(t, upsertIDs, 2)
	require.True(t, upsertIDs["grant-B"], "modified grant should be in upserts")
	require.True(t, upsertIDs["grant-D"], "added grant should be in upserts")
	require.False(t, upsertIDs["grant-A"], "unchanged grant should NOT be in upserts")

	// Deletions: only grant-C (purely deleted). Modified grants' OLD rows are no longer copied here.
	deletions, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayload, SyncID: deletionsSyncID,
	})
	require.NoError(t, err)
	deletionIDs := map[string]bool{}
	for _, r := range deletions.Rows {
		deletionIDs[r.Grant.GetId()] = true
	}
	require.Len(t, deletionIDs, 1)
	require.True(t, deletionIDs["grant-C"], "deleted grant should be in deletions")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestGenerateSyncDiffFromFile_NeedsExpansionPreserved verifies that the needs_expansion
// column value is preserved in diff output rows.
func TestGenerateSyncDiffFromFile_NeedsExpansionPreserved(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_needs_exp_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_needs_exp_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	rt := v2.ResourceType_builder{Id: "group"}.Build()
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "group:g1:member", Resource: g1}.Build()
	ent2 := v2.Entitlement_builder{Id: "group:g2:member", Resource: g2}.Build()

	expandableGrant := v2.Grant_builder{
		Id: "grant-expandable", Entitlement: ent2, Principal: g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{ent1.GetId()}, Shallow: false, ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, rt))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, rt))
	require.NoError(t, newFile.PutResources(ctx, g1, g2))
	require.NoError(t, newFile.PutEntitlements(ctx, ent1, ent2))
	require.NoError(t, newFile.PutGrants(ctx, expandableGrant))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, _, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	upserts, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:   connectorstore.GrantListModeExpansion,
		SyncID: upsertsSyncID,
	})
	require.NoError(t, err)
	require.Len(t, upserts.Rows, 1)
	require.True(t, upserts.Rows[0].Expansion.NeedsExpansion, "needs_expansion should be preserved as true in upserts")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestGenerateSyncDiffFromFile_ModifiedResourceNoOldRowInDeletions verifies that
// diffModifiedFromAttachedTx only runs for grants. Modified resources and entitlements
// should appear in upserts but their OLD versions should NOT appear in deletions.
func TestGenerateSyncDiffFromFile_ModifiedResourceNoOldRowInDeletions(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_mod_res_no_old_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_mod_res_no_old_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	rt := v2.ResourceType_builder{Id: "group"}.Build()

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, rt))
	require.NoError(t, oldFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Old Name",
	}.Build()))
	require.NoError(t, oldFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "ent-old", Resource: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
		DisplayName: "Old Ent",
	}.Build()))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, rt))
	require.NoError(t, newFile.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "New Name",
	}.Build()))
	require.NoError(t, newFile.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: "ent-old", Resource: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
		DisplayName: "New Ent",
	}.Build()))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, newFile.ViewSync(ctx, upsertsSyncID))
	resources, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: "group"}.Build())
	require.NoError(t, err)
	require.Len(t, resources.GetList(), 1)
	require.Equal(t, "New Name", resources.GetList()[0].GetDisplayName())

	ents, err := newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, ents.GetList(), 1)
	require.Equal(t, "New Ent", ents.GetList()[0].GetDisplayName())

	require.NoError(t, newFile.ViewSync(ctx, deletionsSyncID))
	delResources, err := newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: "group"}.Build())
	require.NoError(t, err)
	require.Len(t, delResources.GetList(), 0, "modified resources should NOT have OLD rows in deletions")

	delEnts, err := newFile.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, delEnts.GetList(), 0, "modified entitlements should NOT have OLD rows in deletions")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestGenerateSyncDiffFromFile_ResourceTypesAlwaysCopied verifies that resource types
// are always fully copied into the upserts sync even when unchanged, and never appear in deletions.
func TestGenerateSyncDiffFromFile_ResourceTypesAlwaysCopied(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_rt_copy_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_rt_copy_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	rtA := v2.ResourceType_builder{Id: "typeA", DisplayName: "Type A"}.Build()
	rtB := v2.ResourceType_builder{Id: "typeB", DisplayName: "Type B"}.Build()

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, rtA, rtB))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, rtA))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	require.NoError(t, newFile.ViewSync(ctx, upsertsSyncID))
	rts, err := newFile.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	rtIDs := map[string]bool{}
	for _, r := range rts.GetList() {
		rtIDs[r.GetId()] = true
	}
	require.True(t, rtIDs["typeA"], "unchanged resource type should still be in upserts (full copy)")

	require.NoError(t, newFile.ViewSync(ctx, deletionsSyncID))
	delRts, err := newFile.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, delRts.GetList(), 0, "resource types should never appear in deletions")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestGenerateSyncDiffFromFile_ExpansionNullEdgeCases tests NULL expansion transitions:
// NULL->non-NULL (grant becomes expandable), non-NULL->NULL (stops being expandable),
// and NULL->NULL (should NOT be detected as a modification).
func TestGenerateSyncDiffFromFile_ExpansionNullEdgeCases(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_exp_null_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_exp_null_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	rt := v2.ResourceType_builder{Id: "group"}.Build()
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "group:g1:member", Resource: g1}.Build()
	ent2 := v2.Entitlement_builder{Id: "group:g2:member", Resource: g2}.Build()

	expandAnno := annotations.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{ent1.GetId()}, Shallow: false, ResourceTypeIds: []string{"user"},
	}.Build())

	grantBecomeExpandableOld := v2.Grant_builder{Id: "grant-becomes-expandable", Entitlement: ent2, Principal: g1}.Build()
	grantBecomeExpandableNew := v2.Grant_builder{Id: "grant-becomes-expandable", Entitlement: ent2, Principal: g1, Annotations: expandAnno}.Build()

	grantStopsExpandableOld := v2.Grant_builder{Id: "grant-stops-expandable", Entitlement: ent2, Principal: g1, Annotations: expandAnno}.Build()
	grantStopsExpandableNew := v2.Grant_builder{Id: "grant-stops-expandable", Entitlement: ent2, Principal: g1}.Build()

	grantAlwaysPlain := v2.Grant_builder{Id: "grant-always-plain", Entitlement: ent2, Principal: u1}.Build()

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, rt))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, ent1, ent2))
	require.NoError(t, oldFile.PutGrants(ctx, grantBecomeExpandableOld, grantStopsExpandableOld, grantAlwaysPlain))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldFile.EndSync(ctx))

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, rt))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, ent1, ent2))
	require.NoError(t, newFile.PutGrants(ctx, grantBecomeExpandableNew, grantStopsExpandableNew, grantAlwaysPlain))
	require.NoError(t, newFile.EndSync(ctx))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, _, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	// Verify via cross-DB queries: added and removed expandable grants.
	addedDefs, err := attached.ComputeAddedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	addedByID := map[string]bool{}
	for _, d := range addedDefs {
		addedByID[d.GrantExternalID] = true
	}
	require.True(t, addedByID["grant-becomes-expandable"], "NULL->non-NULL should appear in added expandable grants")
	require.False(t, addedByID["grant-stops-expandable"], "non-NULL->NULL should NOT appear in added expandable grants")
	require.False(t, addedByID["grant-always-plain"], "NULL->NULL should NOT appear in added expandable grants")

	removedDefs, err := attached.ComputeRemovedExpandableGrants(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	removedByID := map[string]bool{}
	for _, d := range removedDefs {
		removedByID[d.GrantExternalID] = true
	}
	require.False(t, removedByID["grant-becomes-expandable"], "NULL->non-NULL should NOT appear in removed expandable grants")
	require.True(t, removedByID["grant-stops-expandable"], "non-NULL->NULL should appear in removed expandable grants")
	require.False(t, removedByID["grant-always-plain"], "NULL->NULL should NOT appear in removed expandable grants")

	// Verify changed grant entitlement IDs includes both modified grants.
	changedIDs, err := attached.ComputeChangedGrantEntitlementIDs(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	require.Contains(t, changedIDs, ent2.GetId(), "changed IDs should include entitlement of modified grants")

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Verify upserts in main still have both modified grants with correct expansion state.
	upserts, err := newFile.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayloadWithExpansion, SyncID: upsertsSyncID,
	})
	require.NoError(t, err)
	upsertIDs := map[string]bool{}
	for _, r := range upserts.Rows {
		upsertIDs[r.Grant.GetId()] = true
		if r.Grant.GetId() == "grant-becomes-expandable" {
			require.NotNil(t, r.Expansion, "NULL->non-NULL: upserts should have expansion metadata")
		}
		if r.Grant.GetId() == "grant-stops-expandable" {
			require.Nil(t, r.Expansion, "non-NULL->NULL: upserts should have no expansion metadata")
		}
	}
	require.True(t, upsertIDs["grant-becomes-expandable"], "NULL->non-NULL grant should be in upserts")
	require.True(t, upsertIDs["grant-stops-expandable"], "non-NULL->NULL grant should be in upserts")
	require.False(t, upsertIDs["grant-always-plain"], "NULL->NULL unchanged grant should NOT be in upserts")

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}
