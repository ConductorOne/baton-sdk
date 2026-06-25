package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// writeSnapSync writes one sync (resource types, resources, an entitlement, and
// nGrants grants) into f. When end is true the sync is EndSync'd; when false it
// is left as the current, un-ended sync. Returns the sync id.
func writeSnapSync(t *testing.T, ctx context.Context, f *C1File, prefix string, nGrants int, end bool) string {
	t.Helper()
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "role", DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
	))

	role := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "role", Resource: prefix + "admin"}.Build(), DisplayName: "Admin"}.Build()
	resources := []*v2.Resource{role}
	for i := 0; i < nGrants; i++ {
		resources = append(resources, v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: prefix + "u" + strconv.Itoa(i)}.Build(),
			DisplayName: "User " + strconv.Itoa(i),
		}.Build())
	}
	require.NoError(t, f.PutResources(ctx, resources...))

	ent := v2.Entitlement_builder{Id: prefix + "ent-admin", Resource: role, DisplayName: "admin", Slug: "admin"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, ent))

	var grants []*v2.Grant
	for i := 0; i < nGrants; i++ {
		grants = append(grants, v2.Grant_builder{
			Id:          prefix + "grant-" + strconv.Itoa(i),
			Entitlement: ent,
			Principal:   resources[i+1],
		}.Build())
	}
	require.NoError(t, f.PutGrants(ctx, grants...))

	if end {
		require.NoError(t, f.EndSync(ctx))
	}
	return syncID
}

func listAllSyncRuns(t *testing.T, ctx context.Context, f *C1File) []*SyncRun {
	t.Helper()
	var out []*SyncRun
	pageToken := ""
	for {
		runs, next, err := f.ListSyncRuns(ctx, pageToken, 1000)
		require.NoError(t, err)
		out = append(out, runs...)
		if next == "" {
			return out
		}
		pageToken = next
	}
}

// TestSnapshotIncludesUnendedSyncs covers Test 2: the snapshot copies every
// sync_runs row, including an in-progress (un-ended) sync, and the un-ended row
// still has EndedAt == nil after reopen.
func TestSnapshotIncludesUnendedSyncs(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dstPath := filepath.Join(tmp, "dst.c1z")
	snapPath := filepath.Join(tmp, "snap.c1z")

	f, err := newC1ZFile(ctx, dstPath)
	require.NoError(t, err)
	endedID := writeSnapSync(t, ctx, f, "a-", 3, true)    // ended
	unendedID := writeSnapSync(t, ctx, f, "b-", 2, false) // in-progress

	require.NoError(t, f.SnapshotTo(ctx, snapPath))
	require.NoError(t, f.Close(ctx))

	snap, err := newC1ZFile(ctx, snapPath, WithReadOnly(true))
	require.NoError(t, err)
	defer snap.Close(ctx)

	runs := listAllSyncRuns(t, ctx, snap)
	byID := map[string]*SyncRun{}
	for _, r := range runs {
		byID[r.ID] = r
	}
	require.Len(t, runs, 2, "snapshot must contain both the ended and the un-ended sync")
	require.Contains(t, byID, endedID)
	require.Contains(t, byID, unendedID)
	require.NotNil(t, byID[endedID].EndedAt, "ended sync must keep its EndedAt")
	require.Nil(t, byID[unendedID].EndedAt, "un-ended sync must remain un-ended in the snapshot")
}

// TestSnapshotAtomicityOnFailure covers Test 4: a compress-side failure (outPath
// in a non-existent directory) leaves no file at outPath, and the live handle
// stays fully usable afterward.
func TestSnapshotAtomicityOnFailure(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dstPath := filepath.Join(tmp, "dst.c1z")
	badOut := filepath.Join(tmp, "missing-dir", "snap.c1z") // parent dir does not exist

	f, err := newC1ZFile(ctx, dstPath)
	require.NoError(t, err)
	writeSnapSync(t, ctx, f, "a-", 2, true)

	err = f.SnapshotTo(ctx, badOut)
	require.Error(t, err, "snapshot into a non-existent directory must fail")

	_, statErr := os.Stat(badOut)
	require.True(t, os.IsNotExist(statErr), "no partial file may be left at outPath after a failed snapshot")

	// Live handle is unchanged: keep writing and close cleanly.
	writeSnapSync(t, ctx, f, "b-", 1, true)
	require.NoError(t, f.Close(ctx))

	reopened, err := newC1ZFile(ctx, dstPath, WithReadOnly(true))
	require.NoError(t, err)
	defer reopened.Close(ctx)
	require.Len(t, listAllSyncRuns(t, ctx, reopened), 2, "both syncs written around the failed snapshot must be present")
}

// TestSnapshotLiveHandleStillWritable covers Test 5: after a snapshot on an
// active destination, the live handle continues to accept writes and closes to
// a valid, complete file; the snapshot itself is a valid point-in-time view.
func TestSnapshotLiveHandleStillWritable(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dstPath := filepath.Join(tmp, "dst.c1z")
	snapPath := filepath.Join(tmp, "snap.c1z")

	f, err := newC1ZFile(ctx, dstPath)
	require.NoError(t, err)
	writeSnapSync(t, ctx, f, "a-", 2, true)

	require.NoError(t, f.SnapshotTo(ctx, snapPath))

	// Keep writing on the same live handle after the snapshot.
	writeSnapSync(t, ctx, f, "b-", 2, true)
	require.NoError(t, f.Close(ctx))

	// Live file has both syncs.
	live, err := newC1ZFile(ctx, dstPath, WithReadOnly(true))
	require.NoError(t, err)
	require.Len(t, listAllSyncRuns(t, ctx, live), 2)
	require.NoError(t, live.Close(ctx))

	// Snapshot is a valid point-in-time view: only the first sync existed when
	// it was taken.
	snap, err := newC1ZFile(ctx, snapPath, WithReadOnly(true))
	require.NoError(t, err)
	require.Len(t, listAllSyncRuns(t, ctx, snap), 1)
	require.NoError(t, snap.Close(ctx))
}

// TestCloneSyncRejectsUnendedSync covers Test 6: the shared-helper refactor must
// preserve CloneSync's precondition that the target sync is ended. This pins the
// codes.FailedPrecondition rejection so the refactor cannot silently drop it.
func TestCloneSyncRejectsUnendedSync(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "src.db")
	outPath := filepath.Join(tmp, "clone.c1z")

	f, err := NewC1File(ctx, dbPath, WithC1FTmpDir(tmp))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.rawDb.Close()) }()

	syncID := writeSnapSync(t, ctx, f, "a-", 2, false) // un-ended

	err = f.CloneSync(ctx, outPath, syncID)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "CloneSync must return a gRPC status error for an un-ended sync")
	require.Equal(t, codes.FailedPrecondition, st.Code())

	_, statErr := os.Stat(outPath)
	require.True(t, os.IsNotExist(statErr), "no clone file may be written when the precondition fails")
}

// TestCloneSyncOutPathCheckedBeforeUnendedPrecondition pins the precondition
// ordering: when CloneSync is given BOTH an already-existing outPath AND an
// un-ended sync, it must report the output-path error first — not the
// FailedPrecondition (not-ended) error. The cloneCopy refactor must preserve
// this original observable order.
func TestCloneSyncOutPathCheckedBeforeUnendedPrecondition(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "src.db")
	outPath := filepath.Join(tmp, "clone.c1z")
	require.NoError(t, os.WriteFile(outPath, []byte("occupied"), 0o600))

	f, err := NewC1File(ctx, dbPath, WithC1FTmpDir(tmp))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.rawDb.Close()) }()

	syncID := writeSnapSync(t, ctx, f, "a-", 2, false) // un-ended

	err = f.CloneSync(ctx, outPath, syncID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output path")
	require.Contains(t, err.Error(), "must not exist")
	st, ok := status.FromError(err)
	require.False(t, ok && st.Code() == codes.FailedPrecondition,
		"outPath-exists must be reported before the un-ended precondition")
}

// TestSnapshotGuards covers Test 7: outPath-must-not-exist, the Pebble-engine
// guard, and the read-only guard each return an error and write no file.
func TestSnapshotGuards(t *testing.T) {
	ctx := context.Background()

	t.Run("outPath exists", func(t *testing.T) {
		tmp := t.TempDir()
		dstPath := filepath.Join(tmp, "dst.c1z")
		snapPath := filepath.Join(tmp, "snap.c1z")
		require.NoError(t, os.WriteFile(snapPath, []byte("occupied"), 0o600))

		f, err := newC1ZFile(ctx, dstPath)
		require.NoError(t, err)
		defer f.Close(ctx)
		writeSnapSync(t, ctx, f, "a-", 1, true)

		require.Error(t, f.SnapshotTo(ctx, snapPath), "snapshot must refuse an existing outPath")
	})

	t.Run("pebble engine rejected", func(t *testing.T) {
		tmp := t.TempDir()
		dbPath := filepath.Join(tmp, "src.db")
		snapPath := filepath.Join(tmp, "snap.c1z")

		f, err := NewC1File(ctx, dbPath, WithC1FTmpDir(tmp))
		require.NoError(t, err)
		defer func() { require.NoError(t, f.rawDb.Close()) }()
		f.engine = EnginePebble

		require.Error(t, f.SnapshotTo(ctx, snapPath), "snapshot must reject the pebble engine")
		_, statErr := os.Stat(snapPath)
		require.True(t, os.IsNotExist(statErr))
	})

	t.Run("read-only handle rejected", func(t *testing.T) {
		tmp := t.TempDir()
		dbPath := filepath.Join(tmp, "src.db")
		snapPath := filepath.Join(tmp, "snap.c1z")

		f, err := NewC1File(ctx, dbPath, WithC1FTmpDir(tmp))
		require.NoError(t, err)
		defer func() { require.NoError(t, f.rawDb.Close()) }()
		f.readOnly = true

		require.Error(t, f.SnapshotTo(ctx, snapPath), "snapshot must reject a read-only handle")
		_, statErr := os.Stat(snapPath)
		require.True(t, os.IsNotExist(statErr))
	})
}
