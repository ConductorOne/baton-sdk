package sqlite_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sqlitereader "github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func TestResolveSyncID(t *testing.T) {
	ctx := context.Background()
	multi, err := sqlitereader.Open(ctx, filepath.Join("testdata", "multi_sync.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, multi.Close(ctx)) }()
	runs, err := multi.SyncRuns(ctx)
	require.NoError(t, err)
	require.Len(t, runs, 3)
	fullID := runs[0].ID
	partialID := runs[1].ID
	got, err := multi.ResolveSyncID(ctx, sqlitereader.ResolveNewest)
	require.NoError(t, err)
	require.Equal(t, partialID, got)
	got, err = multi.ResolveSyncID(ctx, sqlitereader.ResolveFullFinished)
	require.NoError(t, err)
	require.Equal(t, fullID, got)

	unfinished, err := sqlitereader.Open(ctx, filepath.Join("testdata", "unfinished_partial.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, unfinished.Close(ctx)) }()
	_, err = unfinished.ResolveSyncID(ctx, sqlitereader.ResolveFullFinished)
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = unfinished.ResolveSyncID(ctx, "unknown")
	require.ErrorContains(t, err, "unknown resolve behavior")

	empty, err := sqlitereader.Open(ctx, filepath.Join("testdata", "empty.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, empty.Close(ctx)) }()
	_, err = empty.ResolveSyncID(ctx, sqlitereader.ResolveNewest)
	require.ErrorIs(t, err, sqlitereader.ErrNoSyncRuns)

	resourcesOnlyPath := filepath.Join(t.TempDir(), "resources-only.db")
	copyFixture(t, filepath.Join("testdata", "bare.db"), resourcesOnlyPath)
	db, err := sql.Open("sqlite", resourcesOnlyPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE v1_sync_runs SET sync_type = ?", connectorstore.SyncTypeResourcesOnly)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	resourcesOnly, err := sqlitereader.Open(ctx, resourcesOnlyPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, resourcesOnly.Close(ctx)) }()
	resourcesOnlyRuns, err := resourcesOnly.SyncRuns(ctx)
	require.NoError(t, err)
	require.Len(t, resourcesOnlyRuns, 1)
	got, err = resourcesOnly.ResolveSyncID(ctx, sqlitereader.ResolveNewest)
	require.NoError(t, err)
	require.Equal(t, resourcesOnlyRuns[0].ID, got)
}

func TestUnknownLegacySyncTypesListButDoNotAutoResolve(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.db")
	copyFixture(t, filepath.Join("testdata", "bare.db"), path)
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE v1_sync_runs SET sync_type = 'partial_upserts', parent_sync_id = 'external-base'")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	source, err := sqlitereader.Open(ctx, path)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()
	runs, err := source.SyncRuns(ctx)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, connectorstore.SyncType("partial_upserts"), runs[0].Type)
	require.Equal(t, "external-base", runs[0].ParentSyncID)
	output := filepath.Join(t.TempDir(), "out.c1z")
	_, err = source.ToPebble(ctx, output, "")
	require.ErrorContains(t, err, "no convertible sync found")
	require.NoFileExists(t, output)
	_, err = source.ToPebble(ctx, output, runs[0].ID)
	require.NoError(t, err)
	require.FileExists(t, output)
}

func TestUnfinishedConversionPreservesResumeState(t *testing.T) {
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, filepath.Join("testdata", "unfinished_partial.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	runs, err := source.SyncRuns(ctx)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	sourceRun := runs[0]
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	stats, err := source.ToPebble(ctx, outPath, sourceRun.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.Sessions.Rows)
	require.NoError(t, source.Close(ctx))

	dest, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	resumedID, started, err := dest.StartOrResumeSync(ctx, connectorstore.SyncTypePartial, sourceRun.ID)
	require.NoError(t, err)
	require.False(t, started)
	require.Equal(t, sourceRun.ID, resumedID)
	require.Equal(t, "resume-token", sourceRun.SyncToken)
	step, err := dest.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, "resume-token", step)
	engine, ok := pebble.AsEngine(dest)
	require.True(t, ok)
	value, found, err := engine.SessionGet(ctx, "cursor", sessions.WithSyncID(sourceRun.ID))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("page-2"), value)
	require.NoError(t, dest.Close(ctx))
}

func TestFinishedConversionLeavesSessionsBehind(t *testing.T) {
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, filepath.Join("testdata", "finished_full.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	stats, err := source.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Zero(t, stats.Sessions.Rows)
	require.NoError(t, source.Close(ctx))
}

func TestSlimGrantsAreHydrated(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join("testdata", "slim_grants.c1z")
	source, err := sqlitereader.Open(ctx, sourcePath, sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	stats, err := source.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.Equal(t, int64(3), stats.Grants.Rows)
	require.NoError(t, source.Close(ctx))

	dest, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, dest.Close(ctx)) }()
	require.NoError(t, dest.SetCurrentSync(ctx, stats.DestSyncID))
	response, err := dest.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, response.GetList(), 3)
	for _, grant := range response.GetList() {
		require.NotNil(t, grant.GetEntitlement())
		require.NotNil(t, grant.GetPrincipal())
	}

	reference, err := dotc1z.NewC1ZFile(ctx, sourcePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	referencePath := filepath.Join(t.TempDir(), "reference.c1z")
	referenceStats, err := reference.ToPebble(ctx, referencePath, "")
	require.NoError(t, err)
	require.Equal(t, int64(3), referenceStats.Grants.Rows)
	require.NoError(t, reference.SetCurrentSync(ctx, referenceStats.SourceSyncID))
	sourceGrants, err := reference.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, sourceGrants.GetList(), 3)
	sourceByID := make(map[string]*v2.Grant, len(sourceGrants.GetList()))
	for _, grant := range sourceGrants.GetList() {
		sourceByID[grant.GetId()] = grant
	}
	for _, grant := range response.GetList() {
		sourceGrant, found := sourceByID[grant.GetId()]
		require.True(t, found)
		require.True(t, proto.Equal(sourceGrant.GetEntitlement(), grant.GetEntitlement()))
		require.True(t, proto.Equal(sourceGrant.GetPrincipal(), grant.GetPrincipal()))
	}
	require.NoError(t, reference.Close(ctx))
	referenceOutput, err := dotc1z.NewStore(ctx, referencePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, referenceOutput.SetCurrentSync(ctx, referenceStats.DestSyncID))
	referenceGrants, err := referenceOutput.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, referenceGrants.GetList(), 1)
	require.NoError(t, referenceOutput.Close(ctx))
}

func TestToPebbleOpenMatchesToPebbleAndAllowsFurtherWrites(t *testing.T) {
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, filepath.Join("testdata", "finished_full.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()

	closedPath := filepath.Join(t.TempDir(), "closed.c1z")
	closedStats, err := source.ToPebble(ctx, closedPath, "")
	require.NoError(t, err)
	openPath := filepath.Join(t.TempDir(), "open.c1z")
	store, openStats, err := source.ToPebbleOpen(ctx, openPath, "")
	require.NoError(t, err)
	require.Equal(t, closedStats.DestSyncID, openStats.DestSyncID)
	require.NoError(t, store.Close(ctx))
	require.Equal(t,
		digestConvertedFile(t, closedPath, closedStats.DestSyncID),
		digestConvertedFile(t, openPath, openStats.DestSyncID),
	)

	mutablePath := filepath.Join(t.TempDir(), "mutable.c1z")
	mutable, stats, err := source.ToPebbleOpen(ctx, mutablePath, "")
	require.NoError(t, err)
	require.NoError(t, mutable.SetCurrentSync(ctx, stats.DestSyncID))
	mutableEngine, ok := pebble.AsEngine(mutable)
	require.True(t, ok)
	sourceRecord, err := mutableEngine.GetSyncRunRecord(ctx, stats.DestSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, sourceRecord.GetIngestInvariantGeneration())
	verificationWriter, ok := mutable.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	require.NoError(t, verificationWriter.ClearIngestInvariantVerification(ctx, stats.DestSyncID))
	require.NoError(t, mutable.PutGrants(ctx, v2.Grant_builder{
		Id: "post-conversion",
		Entitlement: v2.Entitlement_builder{
			Id: "member",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "post"}.Build(),
		}.Build(),
	}.Build()))
	require.NoError(t, mutable.EndSync(ctx))
	resealedRecord, err := mutableEngine.GetSyncRunRecord(ctx, stats.DestSyncID)
	require.NoError(t, err)
	resealedRecord.SetStartedAt(sourceRecord.GetStartedAt())
	resealedRecord.SetEndedAt(sourceRecord.GetEndedAt())
	require.NoError(t, mutableEngine.PutSyncRunRecord(ctx, resealedRecord))
	require.NoError(t, mutable.Close(ctx))
	reopened, err := dotc1z.NewStore(ctx, mutablePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()
	require.NoError(t, reopened.SetCurrentSync(ctx, stats.DestSyncID))
	grants, err := reopened.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 3)
	reopenedEngine, ok := pebble.AsEngine(reopened)
	require.True(t, ok)
	record, err := reopenedEngine.GetSyncRunRecord(ctx, stats.DestSyncID)
	require.NoError(t, err)
	require.True(t, proto.Equal(sourceRecord.GetStartedAt(), record.GetStartedAt()))
	require.True(t, proto.Equal(sourceRecord.GetEndedAt(), record.GetEndedAt()))
	require.Empty(t, record.GetIngestInvariantGeneration())
	require.Empty(t, record.GetIngestInvariantCoverage())
	require.Empty(t, record.GetIngestInvariantMode())
	persistedStats, err := pebble.ReadSyncStatsRecord(ctx, reopenedEngine, stats.DestSyncID)
	require.NoError(t, err)
	require.Equal(t, int64(3), persistedStats.GetGrants())
	digestReader, ok := reopened.(c1zstore.GrantGenerationDigestReader)
	require.True(t, ok)
	digest, found, err := digestReader.GrantGenerationDigest(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(3), digest.Count)
}

func TestToPebbleOpenSurvivesSourceClose(t *testing.T) {
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, filepath.Join("testdata", "finished_full.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	store, stats, err := source.ToPebbleOpen(ctx, outPath, "")
	require.NoError(t, err)
	require.NoError(t, source.Close(ctx))
	require.NoError(t, store.Close(ctx))

	reopened, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()
	require.NoError(t, reopened.SetCurrentSync(ctx, stats.DestSyncID))
	grants, err := reopened.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 2)
}

func TestOpenRejectsUnsupportedOrIncompleteSchemaWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		statement string
		want      string
	}{
		{
			name:      "trigger",
			statement: "CREATE TRIGGER hostile AFTER INSERT ON v1_assets BEGIN DELETE FROM v1_assets; END",
			want:      "rejected trigger",
		},
		{
			name:      "view",
			statement: "CREATE VIEW hostile AS SELECT * FROM v1_assets",
			want:      "rejected view",
		},
		{
			name:      "required column",
			statement: "ALTER TABLE v1_assets DROP COLUMN content_type",
			want:      `required column "content_type" is missing from table "v1_assets"`,
		},
		{
			name:      "query base column",
			statement: "DROP INDEX idx_assets_external_sync_v1; ALTER TABLE v1_assets DROP COLUMN sync_id",
			want:      `required column "sync_id" is missing from table "v1_assets"`,
		},
		{
			name: "sync run row id",
			statement: `ALTER TABLE v1_sync_runs RENAME TO old_sync_runs;
				CREATE TABLE v1_sync_runs (
					sync_id TEXT, started_at DATETIME, ended_at DATETIME, sync_token TEXT
				)`,
			want: `required column "id" is missing from table "v1_sync_runs"`,
		},
		{
			name: "virtual table",
			statement: `DROP TABLE v1_grants;
				CREATE VIRTUAL TABLE v1_grants USING rtree(
					id, min_x, max_x,
					+sync_id, +external_id, +data, +discovered_at, +entitlement_id,
					+resource_type_id, +resource_id, +principal_resource_type_id, +principal_resource_id
				)`,
			want: `rejected table "v1_grants" with type "virtual"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.db")
			copyFixture(t, filepath.Join("testdata", "bare.db"), path)
			db, err := sql.Open("sqlite", path)
			require.NoError(t, err)
			_, err = db.ExecContext(context.Background(), tc.statement)
			require.NoError(t, err)
			require.NoError(t, db.Close())
			before := fileSHA256(t, path)
			_, err = sqlitereader.Open(context.Background(), path)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, before, fileSHA256(t, path))
		})
	}
}

func TestOpenHonorsDecoderLimits(t *testing.T) {
	_, err := sqlitereader.Open(
		context.Background(),
		filepath.Join("testdata", "finished_full.c1z"),
		sqlitereader.WithTmpDir(t.TempDir()),
		sqlitereader.WithDecoderOptions(dotc1z.WithDecoderMaxDecodedSize(16)),
	)
	require.Error(t, err)
}

func TestConversionErrorsRemoveDestination(t *testing.T) {
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, filepath.Join("testdata", "finished_full.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()

	existing := filepath.Join(t.TempDir(), "existing.c1z")
	require.NoError(t, os.WriteFile(existing, []byte("present"), 0o600))
	_, err = source.ToPebble(ctx, existing, "")
	require.ErrorContains(t, err, "must not exist")
	require.Equal(t, []byte("present"), mustReadFile(t, existing))

	missingSyncOutput := filepath.Join(t.TempDir(), "missing.c1z")
	_, err = source.ToPebble(ctx, missingSyncOutput, "missing")
	require.Error(t, err)
	require.NoFileExists(t, missingSyncOutput)

	unknownBehaviorOutput := filepath.Join(t.TempDir(), "unknown-behavior.c1z")
	_, err = source.ToPebble(ctx, unknownBehaviorOutput, "", sqlitereader.WithResolveBehavior("unknown"))
	require.ErrorContains(t, err, "unknown convert resolve behavior")
	require.NoFileExists(t, unknownBehaviorOutput)

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	failedOutput := filepath.Join(t.TempDir(), "failed.c1z")
	_, err = source.ToPebble(ctx, failedOutput, "", sqlitereader.WithConvertTmpDir(blocker))
	require.Error(t, err)
	require.NoFileExists(t, failedOutput)

	malformedPath := filepath.Join(t.TempDir(), "malformed.db")
	copyFixture(t, filepath.Join("testdata", "bare.db"), malformedPath)
	db, err := sql.Open("sqlite", malformedPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE v1_resource_types SET data = x'ff' WHERE id = (SELECT MIN(id) FROM v1_resource_types)")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	malformed, err := sqlitereader.Open(ctx, malformedPath)
	require.NoError(t, err)
	postOpenFailure := filepath.Join(t.TempDir(), "post-open-failure.c1z")
	_, err = malformed.ToPebble(ctx, postOpenFailure, "")
	require.Error(t, err)
	require.NoFileExists(t, postOpenFailure)
	require.NoError(t, malformed.Close(ctx))
}

func TestParallelAndSerialConversionMatch(t *testing.T) {
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, filepath.Join("testdata", "edge_keys.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()
	parallelPath := filepath.Join(t.TempDir(), "parallel.c1z")
	parallel, err := source.ToPebble(ctx, parallelPath, "")
	require.NoError(t, err)
	serialPath := filepath.Join(t.TempDir(), "serial.c1z")
	serial, err := source.ToPebble(
		ctx,
		serialPath,
		"",
		sqlitereader.WithParallelism(1),
		sqlitereader.WithBatchSize(1),
		sqlitereader.WithResolveBehavior(""),
	)
	require.NoError(t, err)
	require.Equal(t, convertedKeyspace(t, parallelPath), convertedKeyspace(t, serialPath))
	require.True(t, protoEqualSyncRecords(t, parallelPath, serialPath, parallel.DestSyncID, serial.DestSyncID))
}

func TestRowIDSpanOverflowUsesSingleGrantRange(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.db")
	copyFixture(t, filepath.Join("testdata", "bare.db"), path)
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	rows, err := db.QueryContext(ctx, "SELECT external_id FROM v1_grants ORDER BY id")
	require.NoError(t, err)
	var externalIDs []string
	for rows.Next() {
		var externalID string
		require.NoError(t, rows.Scan(&externalID))
		externalIDs = append(externalIDs, externalID)
	}
	require.NoError(t, rows.Close())
	require.GreaterOrEqual(t, len(externalIDs), 2)
	_, err = db.ExecContext(ctx, "UPDATE v1_grants SET id = ? WHERE external_id = ?", int64(math.MinInt64), externalIDs[0])
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE v1_grants SET id = ? WHERE external_id = ?", int64(math.MaxInt64), externalIDs[1])
	require.NoError(t, err)
	require.NoError(t, db.Close())

	source, err := sqlitereader.Open(ctx, path)
	require.NoError(t, err)
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	stats, err := source.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.Grants.Rows, int64(2))
	require.NoError(t, source.Close(ctx))
}

func TestBareSQLiteMatchesFramedFixture(t *testing.T) {
	ctx := context.Background()
	convert := func(name string) string {
		t.Helper()
		source, err := sqlitereader.Open(ctx, filepath.Join("testdata", name), sqlitereader.WithTmpDir(t.TempDir()))
		require.NoError(t, err)
		output := filepath.Join(t.TempDir(), name+".c1z")
		stats, err := source.ToPebble(ctx, output, "")
		require.NoError(t, err)
		require.NoError(t, source.Close(ctx))
		return digestConvertedFile(t, output, stats.DestSyncID)
	}
	require.Equal(t, convert("finished_full.c1z"), convert("bare.db"))
}

func protoEqualSyncRecords(t testing.TB, left, right, leftID, rightID string) bool {
	t.Helper()
	return syncRecord(t, left, leftID).String() == syncRecord(t, right, rightID).String()
}

func copyFixture(t testing.TB, source, dest string) {
	t.Helper()
	require.NoError(t, os.WriteFile(dest, mustReadFile(t, source), 0o600))
}

func mustReadFile(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func fileSHA256(t testing.TB, path string) [sha256.Size]byte {
	t.Helper()
	return sha256.Sum256(mustReadFile(t, path))
}
