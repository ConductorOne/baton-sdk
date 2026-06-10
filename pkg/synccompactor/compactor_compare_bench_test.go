package synccompactor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
)

func BenchmarkCompactorSQLiteVsPebbleKWay(b *testing.B) {
	ctx := context.Background()

	for _, sourceCount := range []int{10, 50, 500} {
		sourceCount := sourceCount
		b.Run(fmt.Sprintf("syncs=%d", sourceCount), func(b *testing.B) {
			benchmarkCompactorSQLiteVsPebbleKWayCase(b, ctx, sourceCount, compactorCompareSize{
				resources:    250,
				entitlements: 500,
				grants:       2500,
			})
		})
	}
}

func BenchmarkCompactorSQLiteVsPebbleSkewed(b *testing.B) {
	ctx := context.Background()

	for _, sourceCount := range []int{10, 50, 500} {
		sourceCount := sourceCount
		b.Run(fmt.Sprintf("syncs=%d", sourceCount), func(b *testing.B) {
			benchmarkCompactorSQLiteVsPebbleSkewedCase(b, ctx, sourceCount)
		})
	}
}

type compactorCompareSize struct {
	resources    int
	entitlements int
	grants       int
}

type compactorCompareFixtureManifest struct {
	SQLite []*CompactableSync `json:"sqlite"`
	Pebble []*CompactableSync `json:"pebble"`
}

func benchmarkCompactorSQLiteVsPebbleKWayCase(b *testing.B, ctx context.Context, sourceCount int, size compactorCompareSize) {
	if inputs, ok := loadCompareFixtureManifest(b, sourceCount, "same"); ok {
		benchmarkCompactorInputs(b, ctx, sourceCount, size, inputs.SQLite, inputs.Pebble)
		return
	}
	fixtureDir := compareFixtureBuildDir(b, sourceCount, "same")
	sqliteInputs := buildCompareSQLiteInputs(b, ctx, filepath.Join(fixtureDir, "sqlite"), sourceCount, size)
	pebbleInputs := convertCompareInputsToPebble(b, ctx, filepath.Join(fixtureDir, "pebble"), sqliteInputs)
	writeCompareFixtureManifestIfRequested(b, sourceCount, "same", sqliteInputs, pebbleInputs)
	benchmarkCompactorInputs(b, ctx, sourceCount, size, sqliteInputs, pebbleInputs)
}

func benchmarkCompactorSQLiteVsPebbleSkewedCase(b *testing.B, ctx context.Context, sourceCount int) {
	if inputs, ok := loadCompareFixtureManifest(b, sourceCount, "skewed"); ok {
		benchmarkCompactorInputs(b, ctx, sourceCount, compactorCompareSize{
			resources:    250,
			entitlements: 500,
			grants:       2500,
		}, inputs.SQLite, inputs.Pebble)
		return
	}
	fixtureDir := compareFixtureBuildDir(b, sourceCount, "skewed")
	sqliteInputs := buildCompareSkewedSQLiteInputs(b, ctx, filepath.Join(fixtureDir, "sqlite"), sourceCount)
	pebbleInputs := convertCompareInputsToPebble(b, ctx, filepath.Join(fixtureDir, "pebble"), sqliteInputs)
	writeCompareFixtureManifestIfRequested(b, sourceCount, "skewed", sqliteInputs, pebbleInputs)
	benchmarkCompactorInputs(b, ctx, sourceCount, compactorCompareSize{
		resources:    250,
		entitlements: 500,
		grants:       2500,
	}, sqliteInputs, pebbleInputs)
}

func compareFixtureManifestPath(root string, sourceCount int, shape string) string {
	return filepath.Join(root, shape, fmt.Sprintf("syncs-%d", sourceCount), "manifest.json")
}

func compareFixtureBuildDir(b *testing.B, sourceCount int, shape string) string {
	b.Helper()
	root := os.Getenv("BATON_WRITE_COMPACTION_COMPARE_FIXTURES_DIR")
	if root == "" {
		return b.TempDir()
	}
	dir := filepath.Join(root, shape, fmt.Sprintf("syncs-%d", sourceCount))
	// #nosec G703 - benchmark fixture root is an explicit developer-provided env var.
	require.NoError(b, os.RemoveAll(dir))
	// #nosec G703 - benchmark fixture root is an explicit developer-provided env var.
	require.NoError(b, os.MkdirAll(dir, 0o755))
	return dir
}

func loadCompareFixtureManifest(b *testing.B, sourceCount int, shape string) (compactorCompareFixtureManifest, bool) {
	b.Helper()
	root := os.Getenv("BATON_COMPACTION_COMPARE_FIXTURES_DIR")
	if root == "" {
		return compactorCompareFixtureManifest{}, false
	}
	path := compareFixtureManifestPath(root, sourceCount, shape)
	data, err := os.ReadFile(path)
	require.NoError(b, err)
	var manifest compactorCompareFixtureManifest
	require.NoError(b, json.Unmarshal(data, &manifest))
	return manifest, true
}

func writeCompareFixtureManifestIfRequested(b *testing.B, sourceCount int, shape string, sqliteInputs []*CompactableSync, pebbleInputs []*CompactableSync) {
	b.Helper()
	root := os.Getenv("BATON_WRITE_COMPACTION_COMPARE_FIXTURES_DIR")
	if root == "" {
		return
	}
	path := compareFixtureManifestPath(root, sourceCount, shape)
	require.NoError(b, os.MkdirAll(filepath.Dir(path), 0o755))
	data, err := json.MarshalIndent(compactorCompareFixtureManifest{SQLite: sqliteInputs, Pebble: pebbleInputs}, "", "  ")
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(path, data, 0o600))
	b.Skipf("wrote compactor compare fixtures to %s", path)
}

func benchmarkCompactorInputs(
	b *testing.B,
	ctx context.Context,
	sourceCount int,
	size compactorCompareSize,
	sqliteInputs []*CompactableSync,
	pebbleInputs []*CompactableSync,
) {
	for _, tc := range []struct {
		name       string
		inputs     []*CompactableSync
		engine     dotc1z.Engine
		pebbleMode PebbleCompactorMode
	}{
		{name: "sqlite", inputs: sqliteInputs},
		{name: "pebble_kway", inputs: pebbleInputs, engine: dotc1z.EnginePebble, pebbleMode: PebbleCompactorModeKWay},
		{name: "pebble_overlay", inputs: pebbleInputs, engine: dotc1z.EnginePebble, pebbleMode: PebbleCompactorModeOverlay},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(sourceCount), "sources/op")
			b.ReportMetric(float64(size.resources), "resources_per_source")
			b.ReportMetric(float64(size.entitlements), "entitlements_per_source")
			b.ReportMetric(float64(size.grants), "grants_per_source")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				outputDir := b.TempDir()
				tmpDir := b.TempDir()
				opts := []Option{WithTmpDir(tmpDir), WithSkipGrantExpansion()}
				if tc.engine != "" {
					opts = append(opts, WithEngine(tc.engine), WithPebbleCompactorMode(tc.pebbleMode))
				}
				c, cleanup, err := NewCompactor(ctx, outputDir, tc.inputs, opts...)
				require.NoError(b, err)
				out, err := c.Compact(ctx)
				require.NoError(b, err)
				require.NotNil(b, out)
				b.StopTimer()
				require.NoError(b, cleanup())
				b.StartTimer()
			}
		})
	}
}

func buildCompareSkewedSQLiteInputs(b *testing.B, ctx context.Context, dir string, sourceCount int) []*CompactableSync {
	b.Helper()
	require.NoError(b, os.MkdirAll(dir, 0o755))
	out := make([]*CompactableSync, 0, sourceCount)
	for sourceIdx := 0; sourceIdx < sourceCount; sourceIdx++ {
		syncType := connectorstore.SyncTypePartial
		size := compactorCompareSize{
			resources:    1,
			entitlements: 2,
			grants:       5,
		}
		if sourceIdx == 0 {
			syncType = connectorstore.SyncTypeFull
			size = compactorCompareSize{
				resources:    250,
				entitlements: 500,
				grants:       2500,
			}
		}
		path := filepath.Join(dir, fmt.Sprintf("source-%03d.c1z", sourceIdx))
		syncID := writeCompareSQLiteSource(b, ctx, path, sourceIdx, syncType, size)
		out = append(out, &CompactableSync{FilePath: path, SyncID: syncID})
	}
	return out
}

func buildCompareSQLiteInputs(b *testing.B, ctx context.Context, dir string, sourceCount int, size compactorCompareSize) []*CompactableSync {
	b.Helper()
	require.NoError(b, os.MkdirAll(dir, 0o755))
	out := make([]*CompactableSync, 0, sourceCount)
	for sourceIdx := 0; sourceIdx < sourceCount; sourceIdx++ {
		syncType := connectorstore.SyncTypePartial
		if sourceIdx == 0 {
			syncType = connectorstore.SyncTypeFull
		}
		path := filepath.Join(dir, fmt.Sprintf("source-%03d.c1z", sourceIdx))
		syncID := writeCompareSQLiteSource(b, ctx, path, sourceIdx, syncType, size)
		out = append(out, &CompactableSync{FilePath: path, SyncID: syncID})
	}
	return out
}

func writeCompareSQLiteSource(
	b *testing.B,
	ctx context.Context,
	path string,
	sourceIdx int,
	syncType connectorstore.SyncType,
	size compactorCompareSize,
) string {
	b.Helper()
	store, err := dotc1z.NewC1ZFile(ctx, path,
		dotc1z.WithPragma("journal_mode", "OFF"),
		dotc1z.WithPragma("synchronous", "OFF"),
		dotc1z.WithEncoderConcurrency(0),
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
	)
	require.NoError(b, err)
	syncID, err := store.StartNewSync(ctx, syncType, "")
	require.NoError(b, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(b, store.PutResourceTypes(ctx, userRT, groupRT))

	userCount := max(1, size.resources/2)
	groupCount := max(1, size.resources-userCount)
	users := make([]*v2.Resource, 0, userCount)
	groups := make([]*v2.Resource, 0, groupCount)
	resources := make([]*v2.Resource, 0, size.resources)
	for i := 0; i < userCount; i++ {
		user := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: compareObjectID(sourceIdx, i, size.resources)}.Build(),
			DisplayName: fmt.Sprintf("User %d", i),
		}.Build()
		users = append(users, user)
		resources = append(resources, user)
	}
	for i := 0; i < groupCount; i++ {
		group := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "group", Resource: compareObjectID(sourceIdx, i, size.resources)}.Build(),
			DisplayName: fmt.Sprintf("Group %d", i),
		}.Build()
		groups = append(groups, group)
		resources = append(resources, group)
	}
	require.NoError(b, store.PutResources(ctx, resources...))

	entitlements := make([]*v2.Entitlement, 0, size.entitlements)
	for i := 0; i < size.entitlements; i++ {
		entitlement := v2.Entitlement_builder{
			Id:       compareObjectID(sourceIdx, i, size.entitlements),
			Resource: groups[i%len(groups)],
			Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build()
		entitlements = append(entitlements, entitlement)
	}
	require.NoError(b, store.PutEntitlements(ctx, entitlements...))

	grants := make([]*v2.Grant, 0, 10000)
	flush := func() {
		require.NoError(b, store.PutGrants(ctx, grants...))
		grants = grants[:0]
	}
	for grantIdx := 0; grantIdx < size.grants; grantIdx++ {
		grants = append(grants, v2.Grant_builder{
			Id:          compareObjectID(sourceIdx, grantIdx, size.grants),
			Principal:   users[grantIdx%len(users)],
			Entitlement: entitlements[grantIdx%len(entitlements)],
		}.Build())
		if len(grants) >= cap(grants) {
			flush()
		}
	}
	if len(grants) > 0 {
		flush()
	}
	require.NoError(b, store.EndSync(ctx))
	require.NoError(b, store.Close(ctx))
	return syncID
}

func compareObjectID(sourceIdx int, i int, total int) string {
	if i < max(1, total/5) {
		return fmt.Sprintf("shared-%d", i)
	}
	return fmt.Sprintf("source-%03d-%d", sourceIdx, i)
}

func convertCompareInputsToPebble(b *testing.B, ctx context.Context, dir string, sqliteInputs []*CompactableSync) []*CompactableSync {
	b.Helper()
	require.NoError(b, os.MkdirAll(dir, 0o755))
	out := make([]*CompactableSync, 0, len(sqliteInputs))
	for i, input := range sqliteInputs {
		src, err := dotc1z.NewC1ZFile(ctx, input.FilePath,
			dotc1z.WithReadOnly(true),
			dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
			dotc1z.WithTmpDir(dir),
		)
		require.NoError(b, err)
		outPath := filepath.Join(dir, fmt.Sprintf("source-%03d.pebble.c1z", i))
		stats, err := src.ToPebble(ctx, outPath, input.SyncID, dotc1z.WithConvertTmpDir(dir))
		require.NoError(b, err)
		require.NoError(b, src.Close(ctx))
		normalizePebbleCompareFixture(b, ctx, dir, outPath, stats.DestSyncID)
		out = append(out, &CompactableSync{FilePath: outPath, SyncID: stats.DestSyncID})
	}
	return out
}

func normalizePebbleCompareFixture(b *testing.B, ctx context.Context, tmpDir string, path string, syncID string) {
	b.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(tmpDir))
	require.NoError(b, err)
	require.NoError(b, pebbleengine.NormalizeForFixtureSave(ctx, w, syncID))
	require.NoError(b, w.Close(ctx))
}
