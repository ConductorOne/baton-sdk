package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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
	require.Len(t, resourcesResp.GetList(), 0, "upserts should be empty when no changes")

	// Verify deletions is empty
	err = newFile.ViewSync(ctx, deletionsSyncID)
	require.NoError(t, err)

	resourcesResp, err = newFile.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourcesResp.GetList(), 0, "deletions should be empty when no changes")

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
