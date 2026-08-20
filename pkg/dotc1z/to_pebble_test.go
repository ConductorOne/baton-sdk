package dotc1z_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// TestToPebbleRoundTrip seeds a SQLite .c1z with a finished full sync
// (resource types, resources, entitlements, grants, and an asset), converts it
// to a v3/Pebble .c1z via ToPebble, and asserts the converted store reads back
// the same data.
func TestToPebbleRoundTrip(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")

	src, err := dotc1z.NewC1ZFile(ctx, srcPath, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))

	const userCount = 25
	users := make([]*v2.Resource, userCount)
	for i := 0; i < userCount; i++ {
		users[i] = v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u" + strconv.Itoa(i)}.Build(),
		}.Build()
	}
	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	require.NoError(t, src.PutResources(ctx, append(users, group)...))

	ent := v2.Entitlement_builder{Id: "ent1", Resource: group}.Build()
	require.NoError(t, src.PutEntitlements(ctx, ent))

	grants := make([]*v2.Grant, userCount)
	for i := 0; i < userCount; i++ {
		grants[i] = v2.Grant_builder{
			Id:          "grant-" + strconv.Itoa(i),
			Entitlement: ent,
			Principal:   users[i],
		}.Build()
	}
	require.NoError(t, src.PutGrants(ctx, grants...))

	assetData := []byte("hello-asset-bytes")
	require.NoError(t, src.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "text/plain", assetData))

	wantVerification := c1zstore.IngestInvariantVerification{
		Generation: "test-generation",
		Coverage:   []string{"I5"},
		Mode:       c1zstore.IngestInvariantVerificationModeConnector,
	}
	verificationWriter, ok := src.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	// Production ordering: the marker is only writable on a sealed sync.
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, verificationWriter.MarkIngestInvariantsVerified(ctx, syncID, wantVerification))

	// Convert the finished sync into a new Pebble .c1z.
	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)
	require.Equal(t, syncID, stats.SourceSyncID)
	// The converted file describes the same snapshot, so it preserves the
	// source sync's id rather than minting a new one.
	require.Equal(t, syncID, stats.DestSyncID)
	require.Equal(t, int64(2), stats.ResourceTypes.Rows)
	require.Equal(t, int64(userCount+1), stats.Resources.Rows)
	require.Equal(t, int64(1), stats.Entitlements.Rows)
	require.Equal(t, int64(userCount), stats.Grants.Rows)
	require.Equal(t, int64(1), stats.Assets.Rows)
	require.Equal(t, int64(len(assetData)), stats.AssetBytes)

	// Open the converted Pebble store and verify the data round-tripped.
	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()
	dstRun, err := dst.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, dstRun)
	require.Equal(t, wantVerification, dstRun.IngestInvariantVerification)
	require.NoError(t, dst.SetCurrentSync(ctx, stats.DestSyncID))

	rtResp, err := dst.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 2)

	resCount := countResources(ctx, t, dst)
	require.Equal(t, userCount+1, resCount)

	entResp, err := dst.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entResp.GetList(), 1)

	grantCount := countGrants(ctx, t, dst)
	require.Equal(t, userCount, grantCount)

	// Verify the fast grant copy path populated Pebble's secondary indexes, not
	// just the primary grant keyspace.
	resourceFiltered, err := dst.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: group,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourceFiltered.GetList(), userCount)

	byEntitlement, err := dst.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: "ent1"}.Build(),
		PageSize:    1000,
	}.Build())
	require.NoError(t, err)
	require.Len(t, byEntitlement.GetList(), userCount)

	byPrincipalRT, err := dst.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "user",
		PageSize:       1000,
	}.Build())
	require.NoError(t, err)
	require.Len(t, byPrincipalRT.GetList(), userCount)

	contentType, r, err := dst.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{
		Asset: v2.AssetRef_builder{Id: "asset-1"}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "text/plain", contentType)
	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, assetData, gotData)
}

// TestToPebbleErrors exercises ToPebble's guard clauses: output path must not
// exist and the sync must exist. An unfinished source sync with an explicit
// syncID is NOT an error — ToPebble converts it and leaves the destination
// unfinished with the source sync_token.
func TestToPebbleErrors(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "source.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	// A finished full sync to use for the "output exists" case.
	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	t.Run("output path exists", func(t *testing.T) {
		outPath := filepath.Join(dir, "exists.c1z")
		require.NoError(t, os.WriteFile(outPath, []byte("x"), 0600))
		_, err := src.ToPebble(ctx, outPath, syncID)
		require.Error(t, err)
	})

	t.Run("sync not found", func(t *testing.T) {
		_, err := src.ToPebble(ctx, filepath.Join(dir, "not-found.c1z"), "nonexistent-sync-id")
		require.Error(t, err)
	})

	t.Run("unfinished sync converts and stays unfinished", func(t *testing.T) {
		unfinished, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
		require.NoError(t, src.CheckpointSync(ctx, "resume-token"))
		// Deliberately do NOT EndSync: the source sync is in-progress.
		srcSync, err := src.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: unfinished}.Build())
		require.NoError(t, err)
		require.NotNil(t, srcSync.GetSync().GetStartedAt())

		outPath := filepath.Join(dir, "unfinished.c1z")
		stats, err := src.ToPebble(ctx, outPath, unfinished)
		require.NoError(t, err)
		require.Equal(t, unfinished, stats.SourceSyncID)
		require.Equal(t, unfinished, stats.DestSyncID)

		dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
		require.NoError(t, err)
		defer func() { require.NoError(t, dst.Close(ctx)) }()
		latest, err := dst.SyncMeta().LatestFullSync(ctx)
		require.NoError(t, err)
		require.Nil(t, latest, "unfinished source must not be sealed as finished")
		got, err := dst.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: unfinished}.Build())
		require.NoError(t, err)
		require.NotNil(t, got.GetSync())
		require.Nil(t, got.GetSync().GetEndedAt())
		require.Equal(t, "resume-token", got.GetSync().GetSyncToken())
		// Compared as wall clocks, not instants. sync_runs stores local time
		// with no zone and the sqlite driver hands that back labelled UTC,
		// while the converted Pebble record holds the true instant, so the two
		// sides only line up once the destination is rendered back to local.
		const wallClock = "2006-01-02 15:04:05.999999999"
		require.Equal(t,
			srcSync.GetSync().GetStartedAt().AsTime().UTC().Format(wallClock),
			got.GetSync().GetStartedAt().AsTime().In(time.Local).Format(wallClock),
			"destination started_at must preserve the source's started_at")
	})
}

// TestToPebbleNoSyncsEmptySyncID converts a never-synced SQLite c1z when
// syncID is "". The destination is a valid empty Pebble c1z with no syncs.
func TestToPebbleNoSyncsEmptySyncID(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "empty.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Empty(t, stats.SourceSyncID)
	require.Empty(t, stats.DestSyncID)
	require.Zero(t, stats.ResourceTypes.Rows)
	require.Zero(t, stats.Resources.Rows)
	require.Zero(t, stats.Entitlements.Rows)
	require.Zero(t, stats.Grants.Rows)
	require.Zero(t, stats.Assets.Rows)

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	latest, err := dst.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.Nil(t, latest)
	require.Equal(t, string(c1zstore.EnginePebble), dst.Metadata().Engine)
}

// TestToPebbleUnfinishedOnlyEmptySyncID converts an unfinished full sync when
// syncID is "". The destination stays unfinished under the same id.
func TestToPebbleUnfinishedOnlyEmptySyncID(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "unfinished-only.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.CheckpointSync(ctx, "resume-token"))

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Equal(t, syncID, stats.SourceSyncID)

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()
	latest, err := dst.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.Nil(t, latest)
	got, err := dst.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: syncID}.Build())
	require.NoError(t, err)
	require.NotNil(t, got.GetSync())
	require.Nil(t, got.GetSync().GetEndedAt())
	require.Equal(t, "resume-token", got.GetSync().GetSyncToken())
}

// TestToPebbleEmptySyncIDPicksNewestConvertible pins the default
// ConvertResolveBehaviorNewest: syncID "" converts the most recently started
// convertible sync, even when it is a partial and an older finished full exists.
func TestToPebbleEmptySyncIDPicksNewestConvertible(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "mixed.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	partialID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Equal(t, partialID, stats.SourceSyncID)
}

// TestToPebblePartialOnlyEmptySyncID converts a finished-partial-only source
// when syncID is "".
func TestToPebblePartialOnlyEmptySyncID(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "partial-only.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Equal(t, syncID, stats.SourceSyncID)

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()
	latest, err := dst.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.Equal(t, syncID, latest.ID)
	require.Equal(t, connectorstore.SyncTypePartial, latest.Type)
}

// TestToPebbleResourcesOnlyEmptySyncID converts a finished-resources_only
// source when syncID is "".
func TestToPebbleResourcesOnlyEmptySyncID(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "resources-only.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeResourcesOnly, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Equal(t, syncID, stats.SourceSyncID)
}

// TestToPebblePreservesSyncLineage pins the remaining sync_runs columns that
// describe the sync's relationship to other syncs: parent_sync_id and
// supports_diff.
//
// The parent is a cross-file reference by nature — a partial's parent full
// sync lives in another c1z file — so the fact that the destination holds one
// sync is not a reason to drop it. Zeroing it makes a converted partial read
// as a standalone snapshot (losing which full it applies to); dropping
// supports_diff turns RollbackExpansion into ErrSyncNotExpanded. The
// sanitizer's c1z-to-c1z copy already carries the marker
// (pkg/c1zsanitize/sanitize.go), and the Pebble record has fields for both.
func TestToPebblePreservesSyncLineage(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	// A base full sync, then a partial that descends from it.
	baseID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	partialID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, baseID)
	require.NoError(t, err)
	require.NoError(t, src.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.SetSupportsDiff(ctx, partialID))

	srcRun, err := src.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: partialID}.Build())
	require.NoError(t, err)
	require.Equal(t, baseID, srcRun.GetSync().GetParentSyncId(), "source must have the lineage under test")

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, partialID)
	require.NoError(t, err)

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)
	rec, err := eng.GetSyncRunRecord(ctx, partialID)
	require.NoError(t, err)
	require.Equal(t, baseID, rec.GetParentSyncId(), "converted sync must keep its parent sync id")
	require.True(t, rec.GetSupportsDiff(), "converted sync must keep supports_diff")

	// The same lineage has to be visible through the read APIs consumers
	// use, not just the raw record.
	gotRun, err := dst.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: partialID}.Build())
	require.NoError(t, err)
	require.Equal(t, baseID, gotRun.GetSync().GetParentSyncId())

	latest, err := dst.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.Equal(t, baseID, latest.ParentSyncID)
	require.True(t, latest.SupportsDiff)
}

// TestToPebbleConvertedUnfinishedSyncResumes is the end-to-end check on the
// reason ToPebble preserves unfinished state at all: a converted in-progress
// sync must actually be resumable, not merely look unfinished.
//
// It drives the call the syncer makes when an activity window expires and a
// later one picks the sync back up — StartOrResumeSync(ctx, syncType, "") with
// an empty syncID (pkg/sync/syncer.go startOrResumeSync) — against the
// converted file, then finishes the sync and asserts the records written before
// and after conversion are both in the sealed snapshot.
func TestToPebbleConvertedUnfinishedSyncResumes(t *testing.T) {
	ctx := context.Background()
	const fsmCursor = "fsm-page-cursor-42"

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	// Activity window 1, on SQLite: sync some resources, stash session
	// state, checkpoint an FSM cursor, and stop without EndSync.
	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	require.NoError(t, src.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "before-convert"}.Build(),
	}.Build()))
	sid := sessions.WithSyncID(syncID)
	require.NoError(t, src.SessionSet(ctx, "cursor", []byte("page-2"), sid))
	require.NoError(t, src.CheckpointSync(ctx, fsmCursor))

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)

	// Activity window 2, now on Pebble: the converted file is what the
	// connector reopens, and the sync has to continue where it left off.
	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)

	resumedID, startedNew, err := dst.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, startedNew, "converted unfinished sync must resume, not be replaced by a fresh sync")
	require.Equal(t, syncID, resumedID, "must resume under the source sync_id")

	step, err := dst.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, fsmCursor, step, "resumed sync must retain the checkpointed FSM cursor")

	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)
	sessionValue, found, err := eng.SessionGet(ctx, "cursor", sid)
	require.NoError(t, err)
	require.True(t, found, "resumed sync must see the session state it checkpointed against")
	require.Equal(t, []byte("page-2"), sessionValue)

	// Finish the work the interrupted sync had left.
	require.NoError(t, dst.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "after-resume"}.Build(),
	}.Build()))
	require.NoError(t, dst.EndSync(ctx))
	require.NoError(t, dst.Close(ctx))

	// The completed sync is one sealed snapshot holding both halves of the
	// work: resuming must not have wiped what conversion imported.
	reopened, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()

	finished, err := reopened.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, finished, "the resumed sync must end up finished")
	require.Equal(t, syncID, finished.ID)
	require.Equal(t, 2, countResources(ctx, t, reopened),
		"sealed snapshot must hold the pre-conversion and post-resume resources")
}

// TestToPebbleSelectsNewestPartial pins the documented consequence of
// ConvertResolveBehaviorNewest on a file whose most recent sync is a
// partial (e.g. a targeted sync): "" resolves to the partial, not the
// full sync it was derived from.
func TestToPebbleSelectsNewestPartial(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "partialed.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	baseID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	partialID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, baseID)
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
	))
	require.NoError(t, src.EndSync(ctx))

	stats, err := src.ToPebble(ctx, filepath.Join(dir, "out.c1z"), "")
	require.NoError(t, err)
	require.Equal(t, partialID, stats.SourceSyncID,
		"the newest-wins default selects the most recent partial")
}

// TestToPebbleCopiesSessionState pins that conversion carries the converted
// sync's connector_sessions rows. ToPebble leaves an unfinished source
// resumable (ended_at cleared, sync_token preserved); a connector that
// resumes against an empty session store would silently redo or skip work.
// Rows belonging to other syncs must not come along.
func TestToPebbleCopiesSessionState(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "sessions.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))

	sid := sessions.WithSyncID(syncID)
	require.NoError(t, src.SessionSet(ctx, "cursor", []byte("page-2"), sid))
	require.NoError(t, src.SessionSetMany(ctx, map[string][]byte{
		"seen/u1": []byte("1"),
		"seen/u2": []byte("2"),
	}, sid))
	// Session rows keyed to a different sync must stay behind.
	require.NoError(t, src.SessionSet(ctx, "cursor", []byte("other"), sessions.WithSyncID("other-sync")))
	require.NoError(t, src.CheckpointSync(ctx, "resume-token"))
	// Deliberately unfinished: this is the resume case sessions matter for.

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)
	require.Equal(t, int64(3), stats.Sessions.Rows)

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()
	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)

	got, found, err := eng.SessionGet(ctx, "cursor", sid)
	require.NoError(t, err)
	require.True(t, found, "converted sync must keep its session state")
	require.Equal(t, []byte("page-2"), got)

	many, _, err := eng.SessionGetMany(ctx, []string{"seen/u1", "seen/u2"}, sid)
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"seen/u1": []byte("1"), "seen/u2": []byte("2")}, many)

	_, found, err = eng.SessionGet(ctx, "cursor", sessions.WithSyncID("other-sync"))
	require.NoError(t, err)
	require.False(t, found, "session rows of syncs that were not converted must not be copied")
}

// TestToPebbleLeavesFinishedSyncSessionsBehind is the other half of the session
// rule: the copy exists for the resume case, and a finished sync has nothing to
// resume. Its session rows are connector scratch state that the lifecycle
// already deletes at Cleanup so it does not ship in a saved c1z (a source that
// still holds them crashed before that, or the clear failed), so conversion
// must not carry them into the new artifact.
func TestToPebbleLeavesFinishedSyncSessionsBehind(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "sessions-finished.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))

	sid := sessions.WithSyncID(syncID)
	require.NoError(t, src.SessionSet(ctx, "cursor", []byte("page-2"), sid))
	// Cleanup never ran, so the rows outlive the sync they belonged to.
	require.NoError(t, src.EndSync(ctx))

	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)
	require.Zero(t, stats.Sessions.Rows, "a finished sync's session rows must not be copied")

	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()
	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)

	// The record data still has to be there: this is about scratch state only.
	require.NoError(t, dst.SetCurrentSync(ctx, stats.DestSyncID))
	rtResp, err := dst.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 1)

	_, found, err := eng.SessionGet(ctx, "cursor", sid)
	require.NoError(t, err)
	require.False(t, found, "the converted artifact must not carry the finished sync's scratch state")
}

// TestToPebbleFullFinishedResolve covers ConvertResolveBehaviorFullFinished,
// the branch baton to-pebble ships on: it pins the finished full sync over a
// newer finished partial, requires the full sync to be finished, and still
// writes an empty c1z for a never-synced source.
func TestToPebbleFullFinishedResolve(t *testing.T) {
	ctx := context.Background()

	t.Run("prefers finished full over newer finished partial", func(t *testing.T) {
		dir := t.TempDir()
		src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "mixed.c1z"), dotc1z.WithTmpDir(dir))
		require.NoError(t, err)
		defer func() { require.NoError(t, src.Close(ctx)) }()

		fullID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
		require.NoError(t, src.EndSync(ctx))

		_, err = src.StartNewSync(ctx, connectorstore.SyncTypePartial, fullID)
		require.NoError(t, err)
		require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
		require.NoError(t, src.EndSync(ctx))

		stats, err := src.ToPebble(ctx, filepath.Join(dir, "out.c1z"), "",
			dotc1z.WithConvertResolveBehavior(dotc1z.ConvertResolveBehaviorFullFinished))
		require.NoError(t, err)
		require.Equal(t, fullID, stats.SourceSyncID)
	})

	t.Run("errors when the only full sync is unfinished", func(t *testing.T) {
		dir := t.TempDir()
		src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "unfinished.c1z"), dotc1z.WithTmpDir(dir))
		require.NoError(t, err)
		defer func() { require.NoError(t, src.Close(ctx)) }()

		_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))

		outPath := filepath.Join(dir, "out.c1z")
		_, err = src.ToPebble(ctx, outPath, "",
			dotc1z.WithConvertResolveBehavior(dotc1z.ConvertResolveBehaviorFullFinished))
		require.Equal(t, codes.NotFound, status.Code(err))
		_, statErr := os.Stat(outPath)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("no sync runs still yields an empty c1z", func(t *testing.T) {
		dir := t.TempDir()
		src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "empty.c1z"), dotc1z.WithTmpDir(dir))
		require.NoError(t, err)
		defer func() { require.NoError(t, src.Close(ctx)) }()

		outPath := filepath.Join(dir, "out.c1z")
		stats, err := src.ToPebble(ctx, outPath, "",
			dotc1z.WithConvertResolveBehavior(dotc1z.ConvertResolveBehaviorFullFinished))
		require.NoError(t, err)
		require.Empty(t, stats.SourceSyncID)

		dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
		require.NoError(t, err)
		defer func() { require.NoError(t, dst.Close(ctx)) }()
		latest, err := dst.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
		require.NoError(t, err)
		require.Nil(t, latest)
	})
}

// TestToPebbleUnknownResolveBehaviorErrors pins that an unrecognized
// ConvertResolveBehavior fails loudly instead of silently falling back to
// Newest, and that the verdict does not depend on what the source holds: the
// never-synced case must reject the option too rather than short-circuiting
// into an empty conversion.
func TestToPebbleUnknownResolveBehaviorErrors(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		seed func(t *testing.T, src *dotc1z.C1File)
	}{
		{
			name: "source has a sync run",
			seed: func(t *testing.T, src *dotc1z.C1File) {
				_, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.NoError(t, src.EndSync(ctx))
			},
		},
		{
			name: "source has no sync runs",
			seed: func(t *testing.T, src *dotc1z.C1File) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), dotc1z.WithTmpDir(dir))
			require.NoError(t, err)
			defer func() { require.NoError(t, src.Close(ctx)) }()
			tc.seed(t, src)

			outPath := filepath.Join(dir, "out.c1z")
			_, err = src.ToPebble(ctx, outPath, "", dotc1z.WithConvertResolveBehavior("not-a-real-behavior"))
			require.ErrorContains(t, err, "unknown convert resolve behavior")
			_, statErr := os.Stat(outPath)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

// TestToPebbleZeroValueResolveBehaviorIsDefault pins that an explicitly-passed
// zero value means "unset" (Newest) rather than an unknown behavior, so a
// struct-literal caller does not get an error.
func TestToPebbleZeroValueResolveBehaviorIsDefault(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	stats, err := src.ToPebble(ctx, filepath.Join(dir, "out.c1z"), "", dotc1z.WithConvertResolveBehavior(""))
	require.NoError(t, err)
	require.Equal(t, syncID, stats.SourceSyncID)
}

func countResources(ctx context.Context, t *testing.T, store connectorstore.Reader) int {
	t.Helper()
	total := 0
	pageToken := ""
	for {
		resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		total += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return total
		}
	}
}

func countGrants(ctx context.Context, t *testing.T, store connectorstore.Reader) int {
	t.Helper()
	total := 0
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		total += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return total
		}
	}
}
