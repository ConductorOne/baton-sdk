package attached

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func putTestData(ctx context.Context, t *testing.T, db *dotc1z.C1File) {
	err := db.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build())
	require.NoError(t, err)
	err = db.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build())
	require.NoError(t, err)
	err = db.PutResources(ctx, v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "user1"}.Build(), DisplayName: "User1"}.Build())
	require.NoError(t, err)
	err = db.PutResources(ctx, v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "user2"}.Build(), DisplayName: "User2"}.Build())
	require.NoError(t, err)
	err = db.PutResources(ctx, v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "group1"}.Build(), DisplayName: "Group1"}.Build())
	require.NoError(t, err)
	err = db.PutEntitlements(ctx, v2.Entitlement_builder{Id: "group1:member", DisplayName: "Member"}.Build())
	require.NoError(t, err)
	err = db.PutGrants(ctx, v2.Grant_builder{
		Id:          "group1:member:user1",
		Principal:   v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "user1"}.Build()}.Build(),
		Entitlement: v2.Entitlement_builder{Id: "group1:member", DisplayName: "Member"}.Build(),
	}.Build())
	require.NoError(t, err)
	err = db.PutGrants(ctx, v2.Grant_builder{
		Id: "group1:member:user2", Principal: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "user2"}.Build()}.Build(),
		Entitlement: v2.Entitlement_builder{Id: "group1:member", DisplayName: "Member"}.Build(),
	}.Build())
	require.NoError(t, err)
}

func verifyTestData(ctx context.Context, t *testing.T, db *dotc1z.C1File) {
	usersResp, err := db.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "user",
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, usersResp)
	users := usersResp.GetList()
	require.Equal(t, 2, len(users))
	require.Equal(t, "user1", users[0].GetId().GetResource())
	require.Equal(t, "user2", users[1].GetId().GetResource())
}

func TestAttachedCompactor(t *testing.T) {
	ctx := t.Context()

	// Create temporary files for base, applied, and dest databases
	tmpDir := t.TempDir()
	baseFile := filepath.Join(tmpDir, "base.c1z")
	appliedFile := filepath.Join(tmpDir, "applied.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithTmpDir(tmpDir),
	}

	// Create base database with some test data
	baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
	require.NoError(t, err)
	defer baseDB.Close(ctx)

	// Start sync and add some base data
	_, err = baseDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = baseDB.EndSync(ctx)
	require.NoError(t, err)

	// Create applied database with some test data
	appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
	require.NoError(t, err)

	// Start sync and add some applied data
	_, err = appliedDB.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)

	putTestData(ctx, t, appliedDB)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)
	err = appliedDB.Close(ctx)
	require.NoError(t, err)
	readOnlyOpts := append(slices.Clone(opts), dotc1z.WithReadOnly(true))
	appliedDB, err = dotc1z.NewC1ZFile(ctx, appliedFile, readOnlyOpts...)
	require.NoError(t, err)
	defer func() {
		err := appliedDB.Close(ctx)
		require.NoError(t, err)
	}()

	compactor := NewAttachedCompactor(baseDB, appliedDB)
	err = compactor.Compact(ctx)
	require.NoError(t, err)

	baseSync, err := baseDB.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypeAny),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, baseSync)
	require.Equal(t, connectorstore.SyncTypeFull, connectorstore.SyncType(baseSync.GetSync().GetSyncType()))

	verifyTestData(ctx, t, baseDB)
}

func TestAttachedCompactorMixedSyncTypes(t *testing.T) {
	ctx := t.Context()

	// Create temporary files for base, applied, and dest databases
	tmpDir := t.TempDir()
	baseFile := filepath.Join(tmpDir, "base.c1z")
	appliedFile := filepath.Join(tmpDir, "applied.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithTmpDir(tmpDir),
	}

	// Create base database with a full sync
	baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
	require.NoError(t, err)
	defer baseDB.Close(ctx)

	// Start a full sync and add some base data
	baseSyncID, err := baseDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, baseSyncID)

	err = baseDB.EndSync(ctx)
	require.NoError(t, err)

	// Create applied database with an incremental sync
	appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
	require.NoError(t, err)

	// Start an incremental sync and add some applied data
	appliedSyncID, err := appliedDB.StartNewSync(ctx, connectorstore.SyncTypePartial, baseSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, appliedSyncID)

	putTestData(ctx, t, appliedDB)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)
	err = appliedDB.Close(ctx)
	require.NoError(t, err)

	// Reopen the applied database in read only mode.
	readOnlyOpts := append(slices.Clone(opts), dotc1z.WithReadOnly(true))
	appliedDB, err = dotc1z.NewC1ZFile(ctx, appliedFile, readOnlyOpts...)
	require.NoError(t, err)
	defer func() {
		err := appliedDB.Close(ctx)
		require.NoError(t, err)
	}()

	compactor := NewAttachedCompactor(baseDB, appliedDB)
	err = compactor.Compact(ctx)
	require.NoError(t, err)

	verifyTestData(ctx, t, baseDB)
}

func TestAttachedCompactorUsesLatestAppliedSyncOfAnyType(t *testing.T) {
	ctx := t.Context()

	// Create temporary files for base, applied, and dest databases
	tmpDir := t.TempDir()
	baseFile := filepath.Join(tmpDir, "base.c1z")
	appliedFile := filepath.Join(tmpDir, "applied.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithTmpDir(tmpDir),
	}

	// Create base database with a full sync
	baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
	require.NoError(t, err)
	defer baseDB.Close(ctx)

	baseSyncID, err := baseDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, baseSyncID)

	err = baseDB.EndSync(ctx)
	require.NoError(t, err)

	// Create applied database with multiple syncs of different types
	appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
	require.NoError(t, err)

	// First sync: full
	firstAppliedSyncID, err := appliedDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, firstAppliedSyncID)

	putTestData(ctx, t, appliedDB)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)

	// Second sync: incremental (this should be the one selected)
	secondAppliedSyncID, err := appliedDB.StartNewSync(ctx, connectorstore.SyncTypePartial, firstAppliedSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, secondAppliedSyncID)

	putTestData(ctx, t, appliedDB)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)
	err = appliedDB.Close(ctx)
	require.NoError(t, err)

	readOnlyOpts := append(slices.Clone(opts), dotc1z.WithReadOnly(true))
	appliedDB, err = dotc1z.NewC1ZFile(ctx, appliedFile, readOnlyOpts...)
	require.NoError(t, err)
	defer func() {
		err := appliedDB.Close(ctx)
		require.NoError(t, err)
	}()
	compactor := NewAttachedCompactor(baseDB, appliedDB)
	err = compactor.Compact(ctx)
	require.NoError(t, err)

	verifyTestData(ctx, t, baseDB)

	// Verify that compaction completed without errors
	// This test verifies that the latest sync (incremental) was used from applied
	// even though there was an earlier full sync
}

func TestAttachedCompactorDoesNotOperateOnDiffSyncTypes(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithTmpDir(tmpDir),
	}

	// Base DB with a full sync
	baseDB, err := dotc1z.NewC1ZFile(ctx, filepath.Join(tmpDir, "base.c1z"), opts...)
	require.NoError(t, err)
	defer baseDB.Close(ctx)

	_, err = baseDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, baseDB.EndSync(ctx))

	// Old DB (used to generate diff syncs)
	oldOpts := append(slices.Clone(opts), dotc1z.WithPragma("locking_mode", "normal"))
	oldDB, err := dotc1z.NewC1ZFile(ctx, filepath.Join(tmpDir, "old.c1z"), oldOpts...)
	require.NoError(t, err)
	defer oldDB.Close(ctx)

	oldSyncID, err := oldDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldDB.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(t, oldDB.EndSync(ctx))
	require.NoError(t, oldDB.SetSupportsDiff(ctx, oldSyncID))

	// Applied DB: create a full sync, then generate diff syncs, then delete the full sync.
	appliedFile := filepath.Join(tmpDir, "applied.c1z")
	appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
	require.NoError(t, err)

	appliedFullSyncID, err := appliedDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, appliedDB.EndSync(ctx))
	require.NoError(t, appliedDB.SetSupportsDiff(ctx, appliedFullSyncID))

	attached, err := appliedDB.AttachFile(oldDB, "attached")
	require.NoError(t, err)
	_, _, err = attached.GenerateSyncDiffFromFile(ctx, oldSyncID, appliedFullSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// Remove the only compactable sync from appliedDB; only diff syncs remain.
	require.NoError(t, appliedDB.DeleteSyncRun(ctx, appliedFullSyncID))

	err = appliedDB.Close(ctx)
	require.NoError(t, err)

	readOnlyOpts := append(slices.Clone(opts), dotc1z.WithReadOnly(true))
	appliedDB, err = dotc1z.NewC1ZFile(ctx, appliedFile, readOnlyOpts...)
	require.NoError(t, err)
	defer func() {
		err := appliedDB.Close(ctx)
		require.NoError(t, err)
	}()
	compactor := NewAttachedCompactor(baseDB, appliedDB)
	err = compactor.Compact(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no finished compactable sync found in applied")
}
