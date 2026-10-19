//go:build fixturegen

package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sqlitereader "github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	_ "modernc.org/sqlite"
)

func TestGenerateFixtures(t *testing.T) {
	ctx := context.Background()
	testdata := "testdata"
	require.NoError(t, os.MkdirAll(testdata, 0o755))

	finishedRaw := filepath.Join(t.TempDir(), "finished.db")
	finished, finishedSyncID := newRawFixture(t, finishedRaw)
	seedFinished(t, finished, finishedSyncID, false)
	closeRawFixture(t, finished)
	frameSQLite(t, finishedRaw, filepath.Join(testdata, "finished_full.c1z"))
	copyFile(t, finishedRaw, filepath.Join(testdata, "bare.db"))

	unfinishedRaw := filepath.Join(t.TempDir(), "unfinished.db")
	unfinished, unfinishedSyncID := newRawFixtureWithType(t, unfinishedRaw, connectorstore.SyncTypePartial, "parent-sync")
	seedRecords(t, unfinished, false)
	require.NoError(t, unfinished.CheckpointSync(ctx, "resume-token"))
	require.NoError(t, unfinished.SessionSet(ctx, "cursor", []byte("page-2"), sessions.WithSyncID(unfinishedSyncID)))
	require.NoError(t, unfinished.SessionSet(ctx, "state", []byte("ready"), sessions.WithSyncID(unfinishedSyncID)))
	closeRawFixture(t, unfinished)
	frameSQLite(t, unfinishedRaw, filepath.Join(testdata, "unfinished_partial.c1z"))

	multiRaw := filepath.Join(t.TempDir(), "multi.db")
	multi := newRawFile(t, multiRaw)
	fullID, err := multi.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, multi.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "full"}.Build()))
	require.NoError(t, multi.EndSync(ctx))
	partialID, err := multi.StartNewSync(ctx, connectorstore.SyncTypePartial, fullID)
	require.NoError(t, err)
	require.NoError(t, multi.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "partial"}.Build()))
	require.NoError(t, multi.EndSync(ctx))
	staleID, err := multi.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, multi.CheckpointSync(ctx, "stale"))
	closeRawFixture(t, multi)
	updateSQLite(t, multiRaw,
		"UPDATE v1_sync_runs SET started_at = ? WHERE sync_id = ?",
		time.Now().Add(-30*24*time.Hour).Format("2006-01-02 15:04:05.999999999"), staleID)
	require.NotEmpty(t, partialID)
	frameSQLite(t, multiRaw, filepath.Join(testdata, "multi_sync.c1z"))

	emptyRaw := filepath.Join(t.TempDir(), "empty.db")
	empty := newRawFile(t, emptyRaw)
	emptySyncID, err := empty.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, empty.EndSync(ctx))
	require.NoError(t, empty.DeleteSyncRun(ctx, emptySyncID))
	closeRawFixture(t, empty)
	frameSQLite(t, emptyRaw, filepath.Join(testdata, "empty.c1z"))

	duplicateRaw := filepath.Join(t.TempDir(), "duplicate.db")
	duplicate, duplicateSyncID := newRawFixture(t, duplicateRaw)
	group, user, entitlement := seedIdentityRecords(t, duplicate)
	require.NoError(t, duplicate.PutGrants(ctx,
		v2.Grant_builder{Id: "duplicate-a", Entitlement: entitlement, Principal: user}.Build(),
		v2.Grant_builder{Id: "duplicate-b", Entitlement: entitlement, Principal: user}.Build(),
	))
	require.NotNil(t, group)
	require.NoError(t, duplicate.EndSync(ctx))
	closeRawFixture(t, duplicate)
	require.NotEmpty(t, duplicateSyncID)
	frameSQLite(t, duplicateRaw, filepath.Join(testdata, "duplicate_identity_grants.c1z"))

	oldRaw := filepath.Join(t.TempDir(), "old.db")
	old, oldSyncID := newRawFixture(t, oldRaw)
	seedFinished(t, old, oldSyncID, false)
	closeRawFixture(t, old)
	makeOldSchema(t, oldRaw)
	frameSQLite(t, oldRaw, filepath.Join(testdata, "old_schema.c1z"))

	edgeRaw := filepath.Join(t.TempDir(), "edge.db")
	edge, _ := newRawFixture(t, edgeRaw)
	seedEdgeKeys(t, edge)
	require.NoError(t, edge.EndSync(ctx))
	closeRawFixture(t, edge)
	frameSQLite(t, edgeRaw, filepath.Join(testdata, "edge_keys.c1z"))

	slimRaw := filepath.Join(t.TempDir(), "slim.db")
	slim, slimSyncID := newRawFixtureOptions(t, slimRaw, dotc1z.WithV2GrantsWriter(true))
	seedFinished(t, slim, slimSyncID, true)
	closeRawFixture(t, slim)
	frameSQLite(t, slimRaw, filepath.Join(testdata, "slim_grants.c1z"))

	for _, fixture := range fixtureNames {
		generateGolden(t, filepath.Join(testdata, fixture), fixture == "slim_grants.c1z")
	}
}

func TestGenerateGoldens(t *testing.T) {
	for _, fixture := range fixtureNames {
		generateGolden(t, filepath.Join("testdata", fixture), fixture == "slim_grants.c1z")
	}
}

func newRawFixture(t *testing.T, path string) (*dotc1z.C1File, string) {
	return newRawFixtureOptions(t, path)
}

func newRawFile(t *testing.T, path string) *dotc1z.C1File {
	t.Helper()
	outputPath := path + ".c1z"
	file, err := dotc1z.NewC1ZFile(context.Background(), outputPath, dotc1z.WithTmpDir(filepath.Dir(path)))
	require.NoError(t, err)
	rawFixturePaths[file] = struct {
		raw    string
		output string
	}{raw: path, output: outputPath}
	return file
}

func newRawFixtureOptions(t *testing.T, path string, opts ...dotc1z.C1ZOption) (*dotc1z.C1File, string) {
	t.Helper()
	return newRawFixtureWithTypeOptions(t, path, connectorstore.SyncTypeFull, "", opts...)
}

func newRawFixtureWithType(t *testing.T, path string, syncType connectorstore.SyncType, parent string) (*dotc1z.C1File, string) {
	t.Helper()
	return newRawFixtureWithTypeOptions(t, path, syncType, parent)
}

func newRawFixtureWithTypeOptions(
	t *testing.T,
	path string,
	syncType connectorstore.SyncType,
	parent string,
	opts ...dotc1z.C1ZOption,
) (*dotc1z.C1File, string) {
	t.Helper()
	ctx := context.Background()
	outputPath := path + ".c1z"
	opts = append(opts, dotc1z.WithTmpDir(filepath.Dir(path)))
	file, err := dotc1z.NewC1ZFile(ctx, outputPath, opts...)
	require.NoError(t, err)
	rawFixturePaths[file] = struct {
		raw    string
		output string
	}{raw: path, output: outputPath}
	syncID, err := file.StartNewSync(ctx, syncType, parent)
	require.NoError(t, err)
	return file, syncID
}

func seedFinished(t *testing.T, file *dotc1z.C1File, syncID string, includeUnsafeGrant bool) {
	t.Helper()
	ctx := context.Background()
	seedRecords(t, file, includeUnsafeGrant)
	require.NoError(t, file.PutAsset(ctx, v2.AssetRef_builder{Id: "logo"}.Build(), "image/png", []byte("png")))
	require.NoError(t, file.PutAsset(ctx, v2.AssetRef_builder{Id: "readme"}.Build(), "text/plain", []byte("fixture")))
	require.NoError(t, file.EndSync(ctx))
	require.NoError(t, file.SetSupportsDiff(ctx, syncID))
	writer, ok := file.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	require.NoError(t, writer.MarkIngestInvariantsVerified(ctx, syncID, c1zstore.IngestInvariantVerification{
		Generation: "fixture-generation",
		Coverage:   []string{"I1", "I5"},
		Mode:       c1zstore.IngestInvariantVerificationModeConnector,
	}))
}

func seedRecords(t *testing.T, file *dotc1z.C1File, includeUnsafeGrant bool) {
	t.Helper()
	ctx := context.Background()
	group, user, entitlement := seedIdentityRecords(t, file)
	secondUser := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(),
		ParentResourceId: group.GetId(),
	}.Build()
	thirdUser := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "user", Resource: "u3"}.Build(),
		ParentResourceId: group.GetId(),
	}.Build()
	secondEntitlement := v2.Entitlement_builder{Id: "member-2", Resource: group}.Build()
	thirdEntitlement := v2.Entitlement_builder{Id: "member-3", Resource: group}.Build()
	require.NoError(t, file.PutResources(ctx, secondUser, thirdUser))
	require.NoError(t, file.PutEntitlements(ctx, secondEntitlement, thirdEntitlement))
	expandable, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{"member-3"},
		Shallow:        true,
	}.Build())
	require.NoError(t, err)
	grants := []*v2.Grant{
		v2.Grant_builder{Id: "grant-basic", Entitlement: entitlement, Principal: user}.Build(),
		v2.Grant_builder{Id: "grant-expandable", Entitlement: secondEntitlement, Principal: secondUser, Annotations: []*anypb.Any{expandable}}.Build(),
	}
	if includeUnsafeGrant {
		insert, err := anypb.New(&v2.InsertResourceGrants{})
		require.NoError(t, err)
		grants = append(grants, v2.Grant_builder{
			Id:          "grant-full",
			Entitlement: thirdEntitlement,
			Principal:   thirdUser,
			Annotations: []*anypb.Any{insert},
		}.Build())
	}
	require.NoError(t, file.PutGrants(ctx, grants...))
}

func seedIdentityRecords(t *testing.T, file *dotc1z.C1File) (*v2.Resource, *v2.Resource, *v2.Entitlement) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, file.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
	))
	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	user := v2.Resource_builder{
		Id:               v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		ParentResourceId: group.GetId(),
	}.Build()
	require.NoError(t, file.PutResources(ctx, group, user))
	entitlement := v2.Entitlement_builder{
		Id:       "member",
		Resource: group,
		Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	require.NoError(t, file.PutEntitlements(ctx, entitlement))
	return group, user, entitlement
}

func seedEdgeKeys(t *testing.T, file *dotc1z.C1File) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, file.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "grp"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	))
	resources := []*v2.Resource{
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "a"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "ab"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u:colon:id"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "Ωmega"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
	}
	require.NoError(t, file.PutResources(ctx, resources...))
	entitlement := v2.Entitlement_builder{
		Id: "ent:a",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "missing"}.Build(),
		}.Build(),
	}.Build()
	require.NoError(t, file.PutEntitlements(ctx, entitlement))
	expandable, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{"missing"},
		Shallow:        true,
	}.Build())
	require.NoError(t, err)
	require.NoError(t, file.PutGrants(ctx,
		v2.Grant_builder{
			Id:          "grant:Ω",
			Entitlement: entitlement,
			Principal:   resources[3],
			Annotations: []*anypb.Any{expandable},
		}.Build(),
	))
}

func makeOldSchema(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	rows, err := db.Query("SELECT id, data, expansion FROM v1_grants WHERE expansion IS NOT NULL")
	require.NoError(t, err)
	type update struct {
		id   int64
		data []byte
	}
	var updates []update
	for rows.Next() {
		var id int64
		var data, expansion []byte
		require.NoError(t, rows.Scan(&id, &data, &expansion))
		grant := &v2.Grant{}
		require.NoError(t, proto.Unmarshal(data, grant))
		expandable := &v2.GrantExpandable{}
		require.NoError(t, proto.Unmarshal(expansion, expandable))
		grantAnnotations := annotations.Annotations(grant.GetAnnotations())
		grantAnnotations.Update(expandable)
		grant.SetAnnotations(grantAnnotations)
		data, err = proto.Marshal(grant)
		require.NoError(t, err)
		updates = append(updates, update{id: id, data: data})
	}
	require.NoError(t, rows.Close())
	for _, item := range updates {
		_, err := db.Exec("UPDATE v1_grants SET data = ? WHERE id = ?", item.data, item.id)
		require.NoError(t, err)
	}
	for _, statement := range []string{
		"DROP INDEX IF EXISTS idx_grants_sync_expansion_v1",
		"DROP INDEX IF EXISTS idx_grants_sync_needs_expansion_v1",
		"ALTER TABLE v1_grants DROP COLUMN expansion",
		"ALTER TABLE v1_grants DROP COLUMN needs_expansion",
		"ALTER TABLE v1_sync_runs DROP COLUMN supports_diff",
		"ALTER TABLE v1_sync_runs DROP COLUMN grants_backfilled",
		"ALTER TABLE v1_sync_runs DROP COLUMN stats",
		"ALTER TABLE v1_sync_runs DROP COLUMN linked_sync_id",
		"ALTER TABLE v1_sync_runs DROP COLUMN ingest_invariant_generation",
		"ALTER TABLE v1_sync_runs DROP COLUMN ingest_invariant_coverage",
		"ALTER TABLE v1_sync_runs DROP COLUMN ingest_invariant_mode",
		"DROP TABLE v1_connector_sessions",
	} {
		_, err := db.Exec(statement)
		require.NoError(t, err, statement)
	}
}

func updateSQLite(t *testing.T, path, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.Exec(query, args...)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func closeRawFixture(t *testing.T, file *dotc1z.C1File) {
	t.Helper()
	paths, ok := rawFixturePaths[file]
	require.True(t, ok)
	require.NoError(t, file.Close(context.Background()))
	decodeC1Z(t, paths.output, paths.raw)
	require.NoError(t, os.Remove(paths.output))
	delete(rawFixturePaths, file)
}

var rawFixturePaths = make(map[*dotc1z.C1File]struct {
	raw    string
	output string
})

func decodeC1Z(t *testing.T, sourcePath, rawPath string) {
	t.Helper()
	source, err := os.Open(sourcePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close()) }()
	decoder, err := dotc1z.NewC1ZFileDecoder(source)
	require.NoError(t, err)
	defer func() { require.NoError(t, decoder.Close()) }()
	output, err := os.Create(rawPath)
	require.NoError(t, err)
	_, err = io.Copy(output, decoder)
	require.NoError(t, err)
	require.NoError(t, output.Close())
}

func frameSQLite(t *testing.T, rawPath, outputPath string) {
	t.Helper()
	input, err := os.Open(rawPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, input.Close()) }()
	output, err := os.Create(outputPath)
	require.NoError(t, err)
	_, err = output.Write(dotc1z.C1ZFileHeader)
	require.NoError(t, err)
	encoder, err := zstd.NewWriter(output, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	_, err = io.Copy(encoder, input)
	require.NoError(t, err)
	require.NoError(t, encoder.Close())
	require.NoError(t, output.Close())
}

func copyFile(t *testing.T, sourcePath, outputPath string) {
	t.Helper()
	data, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(outputPath, data, 0o600))
}

func generateGolden(t *testing.T, sourcePath string, useNewConverter bool) {
	t.Helper()
	ctx := context.Background()
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.c1z")
	var syncID string
	if useNewConverter {
		source, err := sqlitereader.Open(ctx, sourcePath, sqlitereader.WithTmpDir(tmpDir))
		require.NoError(t, err)
		stats, err := source.ToPebble(ctx, outputPath, "")
		require.NoError(t, err)
		syncID = stats.DestSyncID
		require.NoError(t, source.Close(ctx))
	} else if filepath.Ext(sourcePath) == ".db" {
		sourceDir := filepath.Join(tmpDir, "source")
		require.NoError(t, os.Mkdir(sourceDir, 0o700))
		sourceCopy := filepath.Join(sourceDir, "source.db")
		copyFile(t, sourcePath, sourceCopy)
		source, err := dotc1z.NewC1File(ctx, sourceCopy, dotc1z.WithC1FReadOnly(true))
		require.NoError(t, err)
		stats, err := source.ToPebble(ctx, outputPath, "")
		require.NoError(t, err)
		syncID = stats.DestSyncID
		require.NoError(t, source.Close(ctx))
	} else {
		source, err := dotc1z.NewC1ZFile(ctx, sourcePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
		require.NoError(t, err)
		stats, err := source.ToPebble(ctx, outputPath, "")
		require.NoError(t, err)
		syncID = stats.DestSyncID
		require.NoError(t, source.Close(ctx))
	}
	digest := digestConvertedFile(t, outputPath, syncID)
	goldenPath := sourcePath + ".golden"
	require.NoError(t, os.WriteFile(goldenPath, []byte(fmt.Sprintf("%s\n", digest)), 0o600))
}
