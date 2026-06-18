package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// A diff c1z produced by GenerateSyncDiffFromFile holds a full sync
// plus a partial_upserts / partial_deletions pair that reference each
// other via linked_sync_id and carry the OTHER file's base sync as
// their parent. Sanitize must reproduce that whole graph: same sync
// types, the pair still mutually linked, supports_diff carried over,
// the external parent reference transformed rather than dropped, and
// the rows inside the diff syncs sanitized like any other rows.
func TestSanitizeDiffSyncGraph(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	oldPath := filepath.Join(tmp, "old.c1z")
	newPath := filepath.Join(tmp, "new.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("diffsync")

	rt := v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build()
	resA := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		DisplayName: "Alice",
	}.Build()
	resB := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
		DisplayName: "Bob",
	}.Build()

	// OLD file: alice only. Attached later, so no exclusive lock.
	oldFile, err := dotc1z.NewC1ZFile(ctx, oldPath, dotc1z.WithPragma("locking_mode", "normal"))
	require.NoError(t, err)
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, rt))
	require.NoError(t, oldFile.PutResources(ctx, resA))
	require.NoError(t, oldFile.EndSync(ctx))
	require.NoError(t, oldFile.SetSupportsDiff(ctx, oldSyncID))

	// NEW file: alice + bob — bob is the upsert.
	newFile, err := dotc1z.NewC1ZFile(ctx, newPath)
	require.NoError(t, err)
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, rt))
	require.NoError(t, newFile.PutResources(ctx, resA, resB))
	require.NoError(t, newFile.EndSync(ctx))
	require.NoError(t, newFile.SetSupportsDiff(ctx, newSyncID))

	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)
	_, err = attached.DetachFile("attached")
	require.NoError(t, err)
	require.NoError(t, oldFile.Close(ctx))
	require.NoError(t, newFile.Close(ctx))

	srcRO := mustOpen(t, ctx, newPath, true)
	defer srcRO.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, srcRO, dst, Options{Secret: secret}))
	require.NoError(t, dst.Close(ctx))

	// Map src sync ids to dst sync ids by walking dst runs in
	// insertion order — listAllSyncs processes src runs id-asc, so
	// order is preserved.
	srcRO2 := mustOpen(t, ctx, newPath, true)
	defer srcRO2.Close(ctx)
	srcRuns, _, err := srcRO2.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, srcRuns, 3)

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)
	dstRuns, _, err := dstRO.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, dstRuns, 3, "every source sync including the diff pair must be reproduced")

	srcToDst := map[string]string{}
	for i, sr := range srcRuns {
		require.Equal(t, sr.Type, dstRuns[i].Type,
			"sync %d type must be preserved (src %s)", i, sr.Type)
		srcToDst[sr.ID] = dstRuns[i].ID
	}

	var dstUpserts, dstDeletions *dotc1z.SyncRun
	for _, dr := range dstRuns {
		switch dr.Type {
		case connectorstore.SyncTypePartialUpserts:
			dstUpserts = dr
		case connectorstore.SyncTypePartialDeletions:
			dstDeletions = dr
		case connectorstore.SyncTypeFull:
			require.True(t, dr.SupportsDiff, "full sync's supports_diff marker must carry over")
		default:
			require.Failf(t, "unexpected sync type in sanitized output", "%s", dr.Type)
		}
	}
	require.NotNil(t, dstUpserts)
	require.NotNil(t, dstDeletions)

	// The pair's bidirectional linkage must be rebuilt against the
	// DESTINATION ids.
	require.Equal(t, dstDeletions.ID, dstUpserts.LinkedSyncID, "upserts must link to deletions")
	require.Equal(t, dstUpserts.ID, dstDeletions.LinkedSyncID, "deletions must link to upserts")
	require.Equal(t, srcToDst[upsertsSyncID], dstUpserts.ID)
	require.Equal(t, srcToDst[deletionsSyncID], dstDeletions.ID)

	// The pair's parent is the OLD file's base sync — external to this
	// c1z. The reference is transformed, not dropped and not leaked.
	wantParent := SanitizeID(secret, oldSyncID)
	require.Equal(t, wantParent, dstUpserts.ParentSyncID, "external parent reference must be transformed")
	require.Equal(t, wantParent, dstDeletions.ParentSyncID)
	require.NotEqual(t, oldSyncID, dstUpserts.ParentSyncID, "raw external sync id must not survive")

	// Rows inside the upserts sync are sanitized like any others.
	require.NoError(t, dstRO.ViewSync(ctx, dstUpserts.ID))
	resp, err := dstRO.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "the upserts sync carries exactly the added resource")
	got := resp.GetList()[0]
	require.Equal(t, SanitizeID(secret, "bob"), got.GetId().GetResource(),
		"diff-sync rows must be sanitized under the same transform")
	require.NotEqual(t, "Bob", got.GetDisplayName())
}
