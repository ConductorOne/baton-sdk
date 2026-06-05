package synccompactor

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const experimentalRunFileBufferSize = 1 << 20

type experimentalIndexMode string

const (
	experimentalIndexModeNone                experimentalIndexMode = "none"
	experimentalIndexModeAll                 experimentalIndexMode = "all"
	experimentalIndexModeNeedsExpansion      experimentalIndexMode = "needs_expansion"
	experimentalIndexModeGrantPrincipal      experimentalIndexMode = "grant_principal"
	experimentalIndexModeGrantEntitlement    experimentalIndexMode = "grant_entitlement"
	experimentalIndexModeResourceEntitlement experimentalIndexMode = "resource_entitlement"
)

func experimentalIndexModeForBench() experimentalIndexMode {
	switch experimentalIndexMode(os.Getenv("BATON_EXPERIMENTAL_KWAY_INDEXES")) {
	case experimentalIndexModeNone:
		return experimentalIndexModeNone
	case experimentalIndexModeNeedsExpansion:
		return experimentalIndexModeNeedsExpansion
	case experimentalIndexModeGrantPrincipal:
		return experimentalIndexModeGrantPrincipal
	case experimentalIndexModeGrantEntitlement:
		return experimentalIndexModeGrantEntitlement
	case experimentalIndexModeResourceEntitlement:
		return experimentalIndexModeResourceEntitlement
	default:
		return experimentalIndexModeAll
	}
}

// BenchmarkData represents the scale of data to generate for benchmarks.
type BenchmarkData struct {
	ResourceTypes int
	Resources     int
	Entitlements  int
	Grants        int
}

// Small dataset for quick benchmarks.
var SmallDataset = BenchmarkData{
	ResourceTypes: 5,
	Resources:     100,
	Entitlements:  250,
	Grants:        1000,
}

// Medium dataset for realistic scenarios.
var MediumDataset = BenchmarkData{
	ResourceTypes: 10,
	Resources:     1000,
	Entitlements:  2500,
	Grants:        10000,
}

// Large dataset for stress testing.
var LargeDataset = BenchmarkData{
	ResourceTypes: 20,
	Resources:     10000,
	Entitlements:  25000,
	Grants:        100000,
}

// generateTestData creates test databases with the specified amount of data.
func generateTestData(ctx context.Context, t *testing.B, tmpDir string, dataset BenchmarkData) []*CompactableSync {
	baseFile := filepath.Join(tmpDir, "base.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "OFF"), // Speed up inserts. This is a test so it's OK to have unsafe writes.
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
		dotc1z.WithEncoderConcurrency(0),
		dotc1z.WithTmpDir(tmpDir),
	}

	// Create base sync file
	baseSync, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
	require.NoError(t, err)

	baseSyncID, err := baseSync.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Generate resource types
	resourceTypeIDs := make([]string, dataset.ResourceTypes)
	for i := 0; i < dataset.ResourceTypes; i++ {
		rtId := fmt.Sprintf("resource-type-%d", i)
		resourceTypeIDs[i] = rtId
		err = baseSync.PutResourceTypes(ctx, v2.ResourceType_builder{
			Id:          rtId,
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		}.Build())
		require.NoError(t, err)
	}

	// Generate resources for base sync (about 60% of total)
	baseResourceCount := int(float64(dataset.Resources) * 0.6)
	baseResources := make([]*v2.Resource, baseResourceCount)
	for i := range baseResourceCount {
		resource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceTypeIDs[i%len(resourceTypeIDs)],
				Resource:     fmt.Sprintf("base-resource-%d", i),
			}.Build(),
			DisplayName: fmt.Sprintf("Base Resource %d", i),
		}.Build()
		baseResources[i] = resource
		err = baseSync.PutResources(ctx, resource)
		require.NoError(t, err)
	}

	// Generate entitlements for base sync
	baseEntitlementCount := int(float64(dataset.Entitlements) * 0.6)
	baseEntitlements := make([]*v2.Entitlement, baseEntitlementCount)
	for i := range baseEntitlementCount {
		entitlement := v2.Entitlement_builder{
			Id:          fmt.Sprintf("base-entitlement-%d", i),
			DisplayName: fmt.Sprintf("Base Entitlement %d", i),
			Resource:    baseResources[i%len(baseResources)],
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build()
		baseEntitlements[i] = entitlement
		err = baseSync.PutEntitlements(ctx, entitlement)
		require.NoError(t, err)
	}

	// Generate grants for base sync
	baseGrantCount := int(float64(dataset.Grants) * 0.6)
	for i := range baseGrantCount {
		grant := v2.Grant_builder{
			Id:          fmt.Sprintf("base-grant-%d", i),
			Principal:   baseResources[i%len(baseResources)],
			Entitlement: baseEntitlements[i%len(baseEntitlements)],
		}.Build()
		err = baseSync.PutGrants(ctx, grant)
		require.NoError(t, err)
	}

	err = baseSync.EndSync(ctx)
	require.NoError(t, err)
	err = baseSync.Close(ctx)
	require.NoError(t, err)

	compactableSyncs := []*CompactableSync{
		{FilePath: baseFile, SyncID: baseSyncID},
	}

	// Create 10 applied syncs
	for i := range 10 {
		// Create applied sync file
		appliedFile := filepath.Join(tmpDir, fmt.Sprintf("applied-%d.c1z", i))
		appliedSync, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
		require.NoError(t, err)

		appliedSyncID, err := appliedSync.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
		require.NoError(t, err)

		// Reuse same resource types
		for _, rtId := range resourceTypeIDs {
			err = appliedSync.PutResourceTypes(ctx, v2.ResourceType_builder{
				Id:          rtId,
				DisplayName: fmt.Sprintf("Resource Type %s", rtId),
			}.Build())
			require.NoError(t, err)
		}

		// Generate resources for applied sync (remaining 40% + some overlapping)
		appliedResourceCount := dataset.Resources - baseResourceCount + int(float64(baseResourceCount)*0.2) // 20% overlap
		appliedResources := make([]*v2.Resource, 0, appliedResourceCount)

		// Add some overlapping resources from base
		overlapCount := int(float64(baseResourceCount) * 0.2)
		for i := range overlapCount {
			resource := v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeIDs[i%len(resourceTypeIDs)],
					Resource:     fmt.Sprintf("base-resource-%d", i), // Same ID as base
				}.Build(),
				DisplayName: fmt.Sprintf("Updated Base Resource %d", i), // Different display name
			}.Build()
			appliedResources = append(appliedResources, resource)
			err = appliedSync.PutResources(ctx, resource)
			require.NoError(t, err)
		}

		// Add new resources only in applied
		newResourceCount := appliedResourceCount - overlapCount
		for i := range newResourceCount {
			resource := v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceTypeIDs[i%len(resourceTypeIDs)],
					Resource:     fmt.Sprintf("applied-resource-%d", i),
				}.Build(),
				DisplayName: fmt.Sprintf("Applied Resource %d", i),
			}.Build()
			appliedResources = append(appliedResources, resource)
			err = appliedSync.PutResources(ctx, resource)
			require.NoError(t, err)
		}

		// Generate entitlements for applied sync
		appliedEntitlementCount := dataset.Entitlements - baseEntitlementCount
		appliedEntitlements := make([]*v2.Entitlement, appliedEntitlementCount)
		for i := range appliedEntitlementCount {
			entitlement := v2.Entitlement_builder{
				Id:          fmt.Sprintf("applied-entitlement-%d", i),
				DisplayName: fmt.Sprintf("Applied Entitlement %d", i),
				Resource:    appliedResources[i%len(appliedResources)],
				Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
			}.Build()
			appliedEntitlements[i] = entitlement
			err = appliedSync.PutEntitlements(ctx, entitlement)
			require.NoError(t, err)
		}

		// Generate grants for applied sync
		appliedGrantCount := dataset.Grants - baseGrantCount
		for i := range appliedGrantCount {
			grant := v2.Grant_builder{
				Id:          fmt.Sprintf("applied-grant-%d", i),
				Principal:   appliedResources[i%len(appliedResources)],
				Entitlement: appliedEntitlements[i%len(appliedEntitlements)],
			}.Build()
			err = appliedSync.PutGrants(ctx, grant)
			require.NoError(t, err)
		}

		err = appliedSync.EndSync(ctx)
		require.NoError(t, err)
		err = appliedSync.Close(ctx)
		require.NoError(t, err)

		compactableSyncs = append(compactableSyncs, &CompactableSync{FilePath: appliedFile, SyncID: appliedSyncID})
	}

	return compactableSyncs
}

// benchmarkAttachedCompactor runs a benchmark using the attached compactor.
func benchmarkAttachedCompactor(b *testing.B, dataset BenchmarkData) {
	ctx := b.Context()

	for b.Loop() {
		b.StopTimer()

		// Create temporary directories
		tmpDir, err := os.MkdirTemp("", "benchmark-attached")
		require.NoError(b, err)
		defer os.RemoveAll(tmpDir)

		outputDir, err := os.MkdirTemp("", "benchmark-output")
		require.NoError(b, err)
		defer os.RemoveAll(outputDir)

		// Generate test data
		compactableSyncs := generateTestData(ctx, b, tmpDir, dataset)

		compactor, cleanup, err := NewCompactor(ctx, outputDir,
			compactableSyncs,
			WithTmpDir(tmpDir),
			WithCompactorType(CompactorTypeAttached),
		)
		require.NoError(b, err)

		b.StartTimer()
		compactedSync, err := compactor.Compact(ctx)
		require.NoError(b, err)
		require.NotNil(b, compactedSync)

		b.StopTimer()
		err = cleanup()
		require.NoError(b, err)
		b.StartTimer()
	}
}

// Benchmark functions for different data sizes and approaches

func BenchmarkAttachedCompactor_Small(b *testing.B) {
	benchmarkAttachedCompactor(b, SmallDataset)
}

func BenchmarkAttachedCompactor_Medium(b *testing.B) {
	benchmarkAttachedCompactor(b, MediumDataset)
}

func BenchmarkAttachedCompactor_Large(b *testing.B) {
	benchmarkAttachedCompactor(b, LargeDataset)
}

type compactionEngine string

const (
	compactionEngineSQLite     compactionEngine = "sqlite"
	compactionEnginePebble     compactionEngine = "pebble"
	compactionEnginePebbleKWay compactionEngine = "pebble_kway"
)

type compactionBenchSize struct {
	Resources    int
	Entitlements int
	Grants       int
}

type compactionBenchCase struct {
	Name  string
	Syncs int
	Size  compactionBenchSize
}

type compactionBenchFixtureManifest struct {
	SQLite    []*CompactableSync `json:"sqlite"`
	Pebble    []*CompactableSync `json:"pebble"`
	PebbleTar []*CompactableSync `json:"pebble_tar"`
	PebbleDir []*CompactableSync `json:"pebble_dir"`
}

var compactionOverheadPerSyncSize = compactionBenchSize{
	Resources:    250,
	Entitlements: 500,
	Grants:       2500,
}

var compactionOverheadPerSizeSyncs = 6

var compactionSizeCases = []struct {
	Name string
	Size compactionBenchSize
}{
	{
		Name: "small",
		Size: compactionBenchSize{Resources: 100, Entitlements: 250, Grants: 1000},
	},
	{
		Name: "medium",
		Size: compactionBenchSize{Resources: 1000, Entitlements: 2500, Grants: 10000},
	},
	{
		Name: "large",
		Size: compactionBenchSize{Resources: 5000, Entitlements: 12500, Grants: 50000},
	},
}

// BenchmarkCompactorEngineComparison compares the end-to-end compactor path for
// SQLite inputs vs Pebble inputs. The SQLite fixture is generated once per case,
// then converted to Pebble before the timer starts so conversion cost is not
// charged to Pebble compaction.
//
// The benchmark has two sweeps:
//   - by_sync_count: fixed per-sync size, varied number of sync files.
//   - by_sync_size: fixed sync count, varied per-sync record volume.
//
// Example:
//
//	go test ./pkg/synccompactor -run '^$' -bench BenchmarkCompactorEngineComparison -benchtime=1x
func BenchmarkCompactorEngineComparison(b *testing.B) {
	for _, tc := range compactionBenchmarkCases() {
		tc := tc
		prefix := "by_sync_count/"
		if strings.HasPrefix(tc.Name, "size=") {
			prefix = "by_sync_size/"
		}
		b.Run(prefix+tc.Name, func(b *testing.B) {
			benchmarkCompactorEngines(b, tc)
		})
	}
}

func TestGenerateCompactorEngineComparisonFixtures(t *testing.T) {
	fixtureRoot := os.Getenv("BATON_WRITE_COMPACTION_BENCH_FIXTURES_DIR")
	if fixtureRoot == "" {
		t.Skip("set BATON_WRITE_COMPACTION_BENCH_FIXTURES_DIR to generate compaction benchmark fixtures")
	}
	require.NoError(t, pebbleengine.Register())
	ctx := context.Background()
	for _, tc := range compactionBenchmarkCases() {
		caseDir := filepath.Join(fixtureRoot, compactionBenchCaseSlug(tc))
		require.NoError(t, os.RemoveAll(caseDir))
		require.NoError(t, os.MkdirAll(caseDir, 0o755))
		sqliteInputs := generateCompactionBenchSQLiteInputsForTB(t, ctx, filepath.Join(caseDir, "sqlite"), tc)
		pebbleInputs := convertCompactionBenchInputsToPebbleForTB(t, ctx, filepath.Join(caseDir, "pebble"), sqliteInputs)
		pebbleTarInputs := convertCompactionBenchInputsToPebbleTarForTB(t, ctx, filepath.Join(caseDir, "pebble-tar"), sqliteInputs)
		pebbleDirInputs := convertCompactionBenchInputsToPebbleDirsForTB(t, ctx, filepath.Join(caseDir, "pebble-dir"), pebbleInputs)
		manifest := compactionBenchFixtureManifest{SQLite: sqliteInputs, Pebble: pebbleInputs, PebbleTar: pebbleTarInputs, PebbleDir: pebbleDirInputs}
		data, err := json.MarshalIndent(manifest, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(caseDir, "manifest.json"), data, 0o600))
	}
}

func benchmarkCompactorEngines(b *testing.B, tc compactionBenchCase) {
	ctx := context.Background()
	require.NoError(b, pebbleengine.Register())

	sqliteInputs, pebbleInputs := loadOrGenerateCompactionBenchFixtures(b, ctx, tc)

	totalResources := int64(tc.Syncs * tc.Size.Resources)
	totalEntitlements := int64(tc.Syncs * tc.Size.Entitlements)
	totalGrants := int64(tc.Syncs * tc.Size.Grants)
	totalRecords := totalResources + totalEntitlements + totalGrants + int64(tc.Syncs*2) // two resource types per sync

	for _, engine := range []compactionEngine{compactionEngineSQLite, compactionEnginePebble, compactionEnginePebbleKWay} {
		engine := engine
		inputs := sqliteInputs
		if engine == compactionEnginePebble || engine == compactionEnginePebbleKWay {
			inputs = pebbleInputs
		}
		b.Run(string(engine), func(b *testing.B) {
			b.ReportAllocs()

			b.ResetTimer()
			for b.Loop() {
				outputDir, err := os.MkdirTemp("", "compactor-bench-output")
				require.NoError(b, err)
				tmpDir, err := os.MkdirTemp("", "compactor-bench-tmp")
				require.NoError(b, err)

				opts := []Option{
					WithTmpDir(tmpDir),
					WithSkipGrantExpansion(),
				}
				if engine == compactionEnginePebble {
					opts = append(opts, WithEngine(dotc1z.EnginePebble))
				}

				var cleanup func() error
				if engine == compactionEnginePebbleKWay {
					compacted, err := experimentalRecursiveKWayPebbleCompact(ctx, outputDir, tmpDir, inputs, experimentalKWayFanIn())
					require.NoError(b, err)
					require.NotNil(b, compacted)
					cleanup = func() error { return nil }
				} else {
					compactor, c, err := NewCompactor(ctx, outputDir, inputs, opts...)
					require.NoError(b, err)
					cleanup = c
					compacted, err := compactor.Compact(ctx)
					require.NoError(b, err)
					require.NotNil(b, compacted)
				}

				b.StopTimer()
				require.NoError(b, cleanup())
				require.NoError(b, os.RemoveAll(tmpDir))
				require.NoError(b, os.RemoveAll(outputDir))
				b.StartTimer()
			}
			b.ReportMetric(float64(tc.Syncs), "syncs/op")
			b.ReportMetric(float64(tc.Size.Resources), "resources_per_sync")
			b.ReportMetric(float64(tc.Size.Entitlements), "entitlements_per_sync")
			b.ReportMetric(float64(tc.Size.Grants), "grants_per_sync")
			b.ReportMetric(float64(totalRecords), "records/op")
			b.ReportMetric(float64(totalResources), "resources/op")
			b.ReportMetric(float64(totalEntitlements), "entitlements/op")
			b.ReportMetric(float64(totalGrants), "grants/op")
		})
	}
}

func experimentalKWayFanIn() int {
	if raw := os.Getenv("BATON_EXPERIMENTAL_KWAY_FANIN"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 1 {
			return n
		}
	}
	return 50
}

func compactionBenchmarkCases() []compactionBenchCase {
	out := []compactionBenchCase{
		{Name: "syncs=2", Syncs: 2, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=4", Syncs: 4, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=8", Syncs: 8, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=10", Syncs: 10, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=16", Syncs: 16, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=50", Syncs: 50, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=100", Syncs: 100, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=150", Syncs: 150, Size: compactionOverheadPerSyncSize},
		{Name: "syncs=500", Syncs: 500, Size: compactionOverheadPerSyncSize},
	}
	for _, sc := range compactionSizeCases {
		out = append(out, compactionBenchCase{
			Name:  "size=" + sc.Name,
			Syncs: compactionOverheadPerSizeSyncs,
			Size:  sc.Size,
		})
	}
	return out
}

func compactionBenchCaseSlug(tc compactionBenchCase) string {
	return strings.NewReplacer("=", "-", "/", "-", " ", "-").Replace(tc.Name)
}

func loadOrGenerateCompactionBenchFixtures(b *testing.B, ctx context.Context, tc compactionBenchCase) ([]*CompactableSync, []*CompactableSync) {
	b.Helper()
	if fixtureRoot := os.Getenv("BATON_COMPACTION_BENCH_FIXTURES_DIR"); fixtureRoot != "" {
		data, err := os.ReadFile(filepath.Join(fixtureRoot, compactionBenchCaseSlug(tc), "manifest.json"))
		require.NoError(b, err)
		var manifest compactionBenchFixtureManifest
		require.NoError(b, json.Unmarshal(data, &manifest))
		return manifest.SQLite, selectCompactionBenchPebbleInputs(manifest)
	}

	fixtureDir := b.TempDir()
	sqliteInputs := generateCompactionBenchSQLiteInputsForTB(b, ctx, filepath.Join(fixtureDir, "sqlite"), tc)
	pebbleInputs := convertCompactionBenchInputsToPebbleForTB(b, ctx, filepath.Join(fixtureDir, "pebble"), sqliteInputs)
	return sqliteInputs, pebbleInputs
}

func selectCompactionBenchPebbleInputs(manifest compactionBenchFixtureManifest) []*CompactableSync {
	switch os.Getenv("BATON_COMPACTION_BENCH_INPUT_FLAVOR") {
	case "pebble_tar":
		return manifest.PebbleTar
	case "pebble_dir":
		return manifest.PebbleDir
	default:
		return manifest.Pebble
	}
}

func generateCompactionBenchSQLiteInputsForTB(tb testing.TB, ctx context.Context, dir string, tc compactionBenchCase) []*CompactableSync {
	tb.Helper()
	require.NoError(tb, os.MkdirAll(dir, 0o755))

	out := make([]*CompactableSync, 0, tc.Syncs)
	for syncIdx := 0; syncIdx < tc.Syncs; syncIdx++ {
		syncType := connectorstore.SyncTypePartial
		if syncIdx == 0 {
			syncType = connectorstore.SyncTypeFull
		}
		path := filepath.Join(dir, fmt.Sprintf("sync-%02d.c1z", syncIdx))
		syncID := writeCompactionBenchSQLiteSync(tb, ctx, path, syncIdx, syncType, tc.Size)
		out = append(out, &CompactableSync{FilePath: path, SyncID: syncID})
	}
	return out
}

func writeCompactionBenchSQLiteSync(
	tb testing.TB,
	ctx context.Context,
	path string,
	syncIdx int,
	syncType connectorstore.SyncType,
	size compactionBenchSize,
) string {
	tb.Helper()
	store, err := dotc1z.NewC1ZFile(
		ctx,
		path,
		dotc1z.WithPragma("journal_mode", "OFF"),
		dotc1z.WithPragma("synchronous", "OFF"),
		dotc1z.WithEncoderConcurrency(0),
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
	)
	require.NoError(tb, err)

	syncID, err := store.StartNewSync(ctx, syncType, "")
	require.NoError(tb, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(tb, store.PutResourceTypes(ctx, userRT, groupRT))

	resources := make([]*v2.Resource, 0, size.Resources)
	users := make([]*v2.Resource, 0, max(1, size.Resources/2))
	groups := make([]*v2.Resource, 0, max(1, size.Resources-size.Resources/2))
	userCount := max(1, size.Resources/2)
	groupCount := max(1, size.Resources-userCount)
	for i := 0; i < userCount; i++ {
		r := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     compactionBenchObjectID(syncIdx, i, size.Resources),
			}.Build(),
			DisplayName: fmt.Sprintf("User %02d/%d", syncIdx, i),
		}.Build()
		users = append(users, r)
		resources = append(resources, r)
	}
	for i := 0; i < groupCount; i++ {
		r := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "group",
				Resource:     compactionBenchObjectID(syncIdx, i, size.Resources),
			}.Build(),
			DisplayName: fmt.Sprintf("Group %02d/%d", syncIdx, i),
		}.Build()
		groups = append(groups, r)
		resources = append(resources, r)
	}
	require.NoError(tb, store.PutResources(ctx, resources...))

	entitlements := make([]*v2.Entitlement, 0, size.Entitlements)
	for i := 0; i < size.Entitlements; i++ {
		ent := v2.Entitlement_builder{
			Id:          compactionBenchObjectID(syncIdx, i, size.Entitlements),
			DisplayName: fmt.Sprintf("Entitlement %02d/%d", syncIdx, i),
			Resource:    groups[i%len(groups)],
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build()
		entitlements = append(entitlements, ent)
	}
	require.NoError(tb, store.PutEntitlements(ctx, entitlements...))

	grants := make([]*v2.Grant, 0, size.Grants)
	const grantBatchSize = 10000
	for i := 0; i < size.Grants; i++ {
		grant := v2.Grant_builder{
			Id:          compactionBenchObjectID(syncIdx, i, size.Grants),
			Principal:   users[i%len(users)],
			Entitlement: entitlements[i%len(entitlements)],
		}.Build()
		grants = append(grants, grant)
		if len(grants) >= grantBatchSize {
			require.NoError(tb, store.PutGrants(ctx, grants...))
			grants = grants[:0]
		}
	}
	if len(grants) > 0 {
		require.NoError(tb, store.PutGrants(ctx, grants...))
	}

	require.NoError(tb, store.EndSync(ctx))
	require.NoError(tb, store.Close(ctx))
	return syncID
}

func compactionBenchObjectID(syncIdx int, i int, total int) string {
	// Make 20% of records overlap across syncs to exercise newer-wins dedupe,
	// while the rest are unique so compaction also measures union growth.
	overlap := max(1, total/5)
	if i < overlap {
		return fmt.Sprintf("shared-%d", i)
	}
	return fmt.Sprintf("sync-%02d-%d", syncIdx, i)
}

func convertCompactionBenchInputsToPebbleForTB(
	tb testing.TB,
	ctx context.Context,
	dir string,
	sqliteInputs []*CompactableSync,
) []*CompactableSync {
	return convertCompactionBenchInputsToPebbleWithOptionsForTB(tb, ctx, dir, sqliteInputs)
}

func convertCompactionBenchInputsToPebbleTarForTB(
	tb testing.TB,
	ctx context.Context,
	dir string,
	sqliteInputs []*CompactableSync,
) []*CompactableSync {
	return convertCompactionBenchInputsToPebbleWithOptionsForTB(tb, ctx, dir, sqliteInputs, dotc1z.WithPayloadEncoding(dotc1z.PayloadEncodingTar))
}

func convertCompactionBenchInputsToPebbleWithOptionsForTB(
	tb testing.TB,
	ctx context.Context,
	dir string,
	sqliteInputs []*CompactableSync,
	c1zOpts ...dotc1z.C1ZOption,
) []*CompactableSync {
	tb.Helper()
	require.NoError(tb, os.MkdirAll(dir, 0o755))

	out := make([]*CompactableSync, 0, len(sqliteInputs))
	for i, input := range sqliteInputs {
		src, err := dotc1z.NewC1ZFile(
			ctx,
			input.FilePath,
			dotc1z.WithReadOnly(true),
			dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
			dotc1z.WithTmpDir(dir),
		)
		require.NoError(tb, err)

		outPath := filepath.Join(dir, fmt.Sprintf("sync-%02d.pebble.c1z", i))
		convertOpts := []dotc1z.ConvertOption{dotc1z.WithConvertTmpDir(dir)}
		if len(c1zOpts) > 0 {
			convertOpts = append(convertOpts, dotc1z.WithConvertC1ZOptions(c1zOpts...))
		}
		stats, err := src.ToPebble(ctx, outPath, input.SyncID, convertOpts...)
		require.NoError(tb, err)
		require.NoError(tb, src.Close(ctx))
		out = append(out, &CompactableSync{FilePath: outPath, SyncID: stats.DestSyncID})
	}
	return out
}

func convertCompactionBenchInputsToPebbleDirsForTB(
	tb testing.TB,
	ctx context.Context,
	dir string,
	pebbleInputs []*CompactableSync,
) []*CompactableSync {
	tb.Helper()
	require.NoError(tb, os.MkdirAll(dir, 0o755))
	out := make([]*CompactableSync, 0, len(pebbleInputs))
	for i, input := range pebbleInputs {
		w, err := dotc1z.NewStore(ctx, input.FilePath, dotc1z.WithTmpDir(dir))
		require.NoError(tb, err)
		eng, ok := pebbleengine.AsEngine(w)
		require.True(tb, ok)
		outDir := filepath.Join(dir, fmt.Sprintf("sync-%02d.pebbledb", i))
		require.NoError(tb, eng.CheckpointTo(ctx, outDir))
		require.NoError(tb, w.Close(ctx))
		out = append(out, &CompactableSync{FilePath: outDir, SyncID: input.SyncID})
	}
	return out
}

func experimentalOpenPebbleInput(ctx context.Context, tmpDir string, inputPath string) (*pebbleengine.Engine, func() error, error) {
	if st, err := os.Stat(inputPath); err == nil && st.IsDir() {
		eng, err := pebbleengine.Open(ctx, inputPath, pebbleengine.WithReadOnly(true))
		if err != nil {
			return nil, nil, err
		}
		return eng, eng.Close, nil
	}
	w, err := dotc1z.NewStore(ctx, inputPath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
	if err != nil {
		return nil, nil, err
	}
	eng, ok := pebbleengine.AsEngine(w)
	if !ok {
		_ = w.Close(ctx)
		return nil, nil, fmt.Errorf("input is not pebble: %s", inputPath)
	}
	return eng, func() error { return w.Close(ctx) }, nil
}

const experimentalKWayBatchSize = 100000

func experimentalRecursiveKWayPebbleCompact(
	ctx context.Context,
	outputDir string,
	tmpDir string,
	inputs []*CompactableSync,
	fanIn int,
) (*CompactableSync, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("kway: no inputs")
	}
	if fanIn < 2 {
		fanIn = 2
	}
	return experimentalRecursiveRunFilePebbleCompact(ctx, outputDir, tmpDir, inputs, fanIn)
}

func experimentalRecursiveRunFilePebbleCompact(
	ctx context.Context,
	outputDir string,
	tmpDir string,
	inputs []*CompactableSync,
	fanIn int,
) (*CompactableSync, error) {
	finalSyncID := ksuid.New().String()
	finalSyncBytes, err := codec.EncodeSyncID(finalSyncID)
	if err != nil {
		return nil, err
	}
	indexMode := experimentalIndexModeForBench()
	if len(inputs) <= fanIn {
		return experimentalDirectPebbleSourcesToFinal(ctx, outputDir, tmpDir, inputs, finalSyncID, finalSyncBytes, indexMode)
	}
	roundInputs := make([]experimentalRunInput, 0, (len(inputs)+fanIn-1)/fanIn)
	for start := 0; start < len(inputs); start += fanIn {
		end := min(start+fanIn, len(inputs))
		run, err := experimentalBuildChunkRunInputFromPebbleSources(ctx, tmpDir, inputs[start:end], start, fmt.Sprintf("initial-%04d", len(roundInputs)), finalSyncID, finalSyncBytes, indexMode)
		if err != nil {
			experimentalRemoveRunInputs(roundInputs)
			return nil, err
		}
		roundInputs = append(roundInputs, run)
	}
	defer experimentalRemoveRunInputs(roundInputs)

	round := 1
	for len(roundInputs) > fanIn {
		next := make([]experimentalRunInput, 0, (len(roundInputs)+fanIn-1)/fanIn)
		for start := 0; start < len(roundInputs); start += fanIn {
			end := min(start+fanIn, len(roundInputs))
			merged, err := experimentalMergeRunInputGroup(ctx, tmpDir, roundInputs[start:end], fmt.Sprintf("%02d-%04d", round, len(next)))
			if err != nil {
				experimentalRemoveRunInputs(next)
				return nil, err
			}
			next = append(next, merged)
		}
		if round > 0 {
			experimentalRemoveRunInputs(roundInputs)
		}
		roundInputs = next
		round++
	}

	outPath := filepath.Join(outputDir, fmt.Sprintf("kway-runfile-final-%02d.c1z", round))
	out, err := experimentalMaterializeRunInputsToPebble(ctx, outPath, tmpDir, roundInputs, finalSyncID, indexMode)
	if round > 0 {
		experimentalRemoveRunInputs(roundInputs)
	}
	return out, err
}

func experimentalBuildChunkRunInputFromPebbleSources(
	ctx context.Context,
	tmpDir string,
	inputs []*CompactableSync,
	baseRank int,
	name string,
	finalSyncID string,
	finalSyncBytes []byte,
	indexMode experimentalIndexMode,
) (experimentalRunInput, error) {
	sources := make([]experimentalKWaySource, 0, len(inputs))
	closers := make([]func() error, 0, len(inputs))
	defer func() {
		for _, closeFn := range closers {
			_ = closeFn()
		}
	}()
	for i, input := range inputs {
		eng, closeFn, err := experimentalOpenPebbleInput(ctx, tmpDir, input.FilePath)
		if err != nil {
			return experimentalRunInput{}, err
		}
		closers = append(closers, closeFn)
		sources = append(sources, experimentalKWaySource{
			rank:   baseRank + i,
			syncID: input.SyncID,
			engine: eng,
		})
	}

	path := filepath.Join(tmpDir, "runfile-"+name+".bin")
	f, err := os.Create(path)
	if err != nil {
		return experimentalRunInput{}, err
	}
	cw := &experimentalCountingWriter{w: bufio.NewWriterSize(f, experimentalRunFileBufferSize)}
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	run := experimentalRunInput{path: path}
	if err := experimentalAppendPebbleSourceBucketRunSection(ctx, cw, sources, experimentalResourceTypeBucket(), finalSyncID, finalSyncBytes, indexMode, &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalAppendPebbleSourceBucketRunSection(ctx, cw, sources, experimentalResourceBucket(), finalSyncID, finalSyncBytes, indexMode, &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalAppendPebbleSourceBucketRunSection(ctx, cw, sources, experimentalEntitlementBucket(), finalSyncID, finalSyncBytes, indexMode, &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalAppendPebbleSourceBucketRunSection(ctx, cw, sources, experimentalGrantBucket(), finalSyncID, finalSyncBytes, indexMode, &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := cw.Flush(); err != nil {
		return experimentalRunInput{}, err
	}
	if err := f.Close(); err != nil {
		return experimentalRunInput{}, err
	}
	success = true
	return run, nil
}

func experimentalDirectPebbleSourcesToFinal(
	ctx context.Context,
	outputDir string,
	tmpDir string,
	inputs []*CompactableSync,
	finalSyncID string,
	finalSyncBytes []byte,
	indexMode experimentalIndexMode,
) (*CompactableSync, error) {
	sources := make([]experimentalKWaySource, 0, len(inputs))
	closers := make([]func() error, 0, len(inputs))
	defer func() {
		for _, closeFn := range closers {
			_ = closeFn()
		}
	}()
	for i, input := range inputs {
		eng, closeFn, err := experimentalOpenPebbleInput(ctx, tmpDir, input.FilePath)
		if err != nil {
			return nil, err
		}
		closers = append(closers, closeFn)
		sources = append(sources, experimentalKWaySource{rank: i, syncID: input.SyncID, engine: eng})
	}

	outPath := filepath.Join(outputDir, "kway-direct-final.c1z")
	if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	w, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(tmpDir))
	if err != nil {
		return nil, err
	}
	store, ok := w.(dotc1z.C1ZStore)
	if !ok {
		_ = w.Close(ctx)
		return nil, fmt.Errorf("direct kway: destination does not implement C1ZStore: %T", w)
	}
	dest, ok := pebbleengine.AsEngine(w)
	if !ok {
		_ = w.Close(ctx)
		return nil, fmt.Errorf("direct kway: destination is not pebble")
	}
	startedSyncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	_ = startedSyncID

	if err := experimentalDirectPebbleSourceBucketToFinal(ctx, dest, tmpDir, sources, experimentalResourceTypeBucket(), finalSyncID, finalSyncBytes, indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := experimentalDirectPebbleSourceBucketToFinal(ctx, dest, tmpDir, sources, experimentalResourceBucket(), finalSyncID, finalSyncBytes, indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := experimentalDirectPebbleSourceBucketToFinal(ctx, dest, tmpDir, sources, experimentalEntitlementBucket(), finalSyncID, finalSyncBytes, indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := experimentalDirectPebbleSourceBucketToFinal(ctx, dest, tmpDir, sources, experimentalGrantBucket(), finalSyncID, finalSyncBytes, indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := store.EndSync(ctx); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := store.Close(ctx); err != nil {
		return nil, err
	}
	return &CompactableSync{FilePath: outPath, SyncID: finalSyncID}, nil
}

type experimentalRunInput struct {
	path     string
	sections [experimentalRunBucketCount]experimentalRunSection
}

type experimentalRunSection struct {
	offset int64
	length int64
}

func experimentalRemoveRunInputs(inputs []experimentalRunInput) {
	for _, input := range inputs {
		_ = os.Remove(input.path)
	}
}

func experimentalBuildInitialRunInputs(ctx context.Context, tmpDir string, inputs []*CompactableSync) ([]experimentalRunInput, error) {
	out := make([]experimentalRunInput, 0, len(inputs))
	for sourceRank, input := range inputs {
		w, err := dotc1z.NewStore(ctx, input.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
		if err != nil {
			experimentalRemoveRunInputs(out)
			return nil, err
		}
		eng, ok := pebbleengine.AsEngine(w)
		if !ok {
			_ = w.Close(ctx)
			experimentalRemoveRunInputs(out)
			return nil, fmt.Errorf("runfile kway: input is not pebble: %s", input.FilePath)
		}
		run, err := experimentalBuildRunInputFromPebble(ctx, tmpDir, eng, input.SyncID, uint32(sourceRank))
		closeErr := w.Close(ctx)
		if err != nil {
			experimentalRemoveRunInputs(out)
			return nil, err
		}
		if closeErr != nil {
			experimentalRemoveRunInputs(out)
			return nil, closeErr
		}
		out = append(out, run)
	}
	return out, nil
}

func experimentalBuildRunInputFromPebble(ctx context.Context, tmpDir string, eng *pebbleengine.Engine, syncID string, sourceRank uint32) (experimentalRunInput, error) {
	syncBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return experimentalRunInput{}, err
	}
	name := fmt.Sprintf("source-%04d", sourceRank)
	path := filepath.Join(tmpDir, "runfile-"+name+".bin")
	f, err := os.Create(path)
	if err != nil {
		return experimentalRunInput{}, err
	}
	cw := &experimentalCountingWriter{w: bufio.NewWriterSize(f, experimentalRunFileBufferSize)}
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()

	run := experimentalRunInput{path: path}
	if err := experimentalAppendPebbleBucketRunSectionToInput(ctx, cw, eng, syncBytes, sourceRank, experimentalResourceTypeBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalAppendPebbleBucketRunSectionToInput(ctx, cw, eng, syncBytes, sourceRank, experimentalResourceBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalAppendPebbleBucketRunSectionToInput(ctx, cw, eng, syncBytes, sourceRank, experimentalEntitlementBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalAppendPebbleBucketRunSectionToInput(ctx, cw, eng, syncBytes, sourceRank, experimentalGrantBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := cw.Flush(); err != nil {
		return experimentalRunInput{}, err
	}
	if err := f.Close(); err != nil {
		return experimentalRunInput{}, err
	}
	success = true
	return run, nil
}

type experimentalCountingWriter struct {
	w *bufio.Writer
	n int64
}

func (w *experimentalCountingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *experimentalCountingWriter) Flush() error {
	return w.w.Flush()
}

func experimentalAppendPebbleBucketRunSectionToInput[T proto.Message](
	ctx context.Context,
	w *experimentalCountingWriter,
	eng *pebbleengine.Engine,
	syncBytes []byte,
	sourceRank uint32,
	bucket experimentalBucket[T],
	run *experimentalRunInput,
) error {
	start := w.n
	lower, upper := bucket.bounds(syncBytes)
	iter, err := eng.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec := bucket.mk()
		if err := proto.Unmarshal(iter.Value(), rec); err != nil {
			return err
		}
		if err := experimentalWriteRunRecord(w, []byte(bucket.key(rec)), experimentalTimestampNanos(bucket.ts(rec)), sourceRank, iter.Value(), experimentalIndexBlobs{}); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	run.sections[bucket.id] = experimentalRunSection{offset: start, length: w.n - start}
	return nil
}

type experimentalDirectSourceItem struct {
	streamIdx int
	rec       experimentalRunRecord
}

type experimentalDirectSourceHeap []experimentalDirectSourceItem

func (h experimentalDirectSourceHeap) Len() int { return len(h) }
func (h experimentalDirectSourceHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].rec.key, h[j].rec.key)
	if cmp == 0 {
		return h[i].rec.sourceRank < h[j].rec.sourceRank
	}
	return cmp < 0
}
func (h experimentalDirectSourceHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *experimentalDirectSourceHeap) Push(x any)   { *h = append(*h, x.(experimentalDirectSourceItem)) }
func (h *experimentalDirectSourceHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

type experimentalDirectSourceStream[T proto.Message] struct {
	sourceRank uint32
	iter       *pebble.Iterator
	bucket     experimentalBucket[T]
	sourceLo   []byte
	destLo     []byte
	finalSync  string
	finalBytes []byte
	indexMode  experimentalIndexMode
}

func (s *experimentalDirectSourceStream[T]) close() {
	if s.iter != nil {
		_ = s.iter.Close()
	}
}

func (s *experimentalDirectSourceStream[T]) next() (experimentalRunRecord, bool, error) {
	if !s.iter.Valid() {
		if err := s.iter.Error(); err != nil {
			return experimentalRunRecord{}, false, err
		}
		return experimentalRunRecord{}, false, nil
	}
	rec := s.bucket.mk()
	if err := proto.Unmarshal(s.iter.Value(), rec); err != nil {
		return experimentalRunRecord{}, false, err
	}
	s.bucket.setSync(rec, s.finalSync)
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(rec)
	if err != nil {
		return experimentalRunRecord{}, false, err
	}
	key := experimentalRewritePrimaryKeyForDest(s.iter.Key(), s.sourceLo, s.destLo)
	var indexBlobs experimentalIndexBlobs
	if s.indexMode != experimentalIndexModeNone {
		indexBlobs = experimentalEncodeIndexKeysByFamily(experimentalFilterIndexKeys(s.indexMode, experimentalBucketIndexKeys(s.bucket, s.finalBytes, rec)))
	}
	item := experimentalRunRecord{
		key:        key,
		value:      value,
		indexes:    indexBlobs,
		tsNanos:    experimentalTimestampNanos(s.bucket.ts(rec)),
		sourceRank: s.sourceRank,
	}
	s.iter.Next()
	return item, true, nil
}

func experimentalAppendPebbleSourceBucketRunSection[T proto.Message](
	ctx context.Context,
	w *experimentalCountingWriter,
	sources []experimentalKWaySource,
	bucket experimentalBucket[T],
	finalSyncID string,
	finalSyncBytes []byte,
	indexMode experimentalIndexMode,
	run *experimentalRunInput,
) error {
	start := w.n
	streams := make([]*experimentalDirectSourceStream[T], 0, len(sources))
	defer func() {
		for _, stream := range streams {
			stream.close()
		}
	}()
	h := &experimentalDirectSourceHeap{}
	heap.Init(h)
	for i, source := range sources {
		syncBytes, err := codec.EncodeSyncID(source.syncID)
		if err != nil {
			return err
		}
		lower, upper := bucket.bounds(syncBytes)
		destLower, _ := bucket.bounds(finalSyncBytes)
		iter, err := source.engine.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		iter.First()
		stream := &experimentalDirectSourceStream[T]{
			sourceRank: uint32(source.rank),
			iter:       iter,
			bucket:     bucket,
			sourceLo:   lower,
			destLo:     destLower,
			finalSync:  finalSyncID,
			finalBytes: finalSyncBytes,
			indexMode:  indexMode,
		}
		streams = append(streams, stream)
		rec, ok, err := stream.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, experimentalDirectSourceItem{streamIdx: i, rec: rec})
		}
	}

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := heap.Pop(h).(experimentalDirectSourceItem)
		winner := first.rec
		group := []experimentalDirectSourceItem{first}
		for h.Len() > 0 && bytes.Equal((*h)[0].rec.key, first.rec.key) {
			group = append(group, heap.Pop(h).(experimentalDirectSourceItem))
		}
		for _, item := range group[1:] {
			if experimentalRunRecordIsNewer(item.rec, winner) {
				winner = item.rec
			}
		}
		if err := experimentalMaterializeDirectWinner(&winner, bucket, finalSyncID, finalSyncBytes, indexMode); err != nil {
			return err
		}
		if err := experimentalWriteRunRecord(w, winner.key, winner.tsNanos, winner.sourceRank, winner.value, winner.indexes); err != nil {
			return err
		}
		for _, item := range group {
			next, ok, err := streams[item.streamIdx].next()
			if err != nil {
				return err
			}
			if ok {
				heap.Push(h, experimentalDirectSourceItem{streamIdx: item.streamIdx, rec: next})
			}
		}
	}
	run.sections[bucket.id] = experimentalRunSection{offset: start, length: w.n - start}
	return nil
}

func experimentalDirectPebbleSourceBucketToFinal[T proto.Message](
	ctx context.Context,
	dest *pebbleengine.Engine,
	tmpDir string,
	sources []experimentalKWaySource,
	bucket experimentalBucket[T],
	finalSyncID string,
	finalSyncBytes []byte,
	indexMode experimentalIndexMode,
) error {
	primaryPath := filepath.Join(tmpDir, fmt.Sprintf("kway-direct-%s-primary.sst", bucket.name))
	primaryWriter, err := experimentalNewSSTWriter(primaryPath)
	if err != nil {
		return err
	}
	primarySuccess := false
	defer func() {
		if !primarySuccess {
			_ = primaryWriter.Close()
			_ = os.Remove(primaryPath)
		}
	}()

	streams := make([]*experimentalDirectSourceStream[T], 0, len(sources))
	defer func() {
		for _, stream := range streams {
			stream.close()
		}
	}()
	h := &experimentalDirectSourceHeap{}
	heap.Init(h)
	for i, source := range sources {
		syncBytes, err := codec.EncodeSyncID(source.syncID)
		if err != nil {
			return err
		}
		lower, upper := bucket.bounds(syncBytes)
		destLower, _ := bucket.bounds(finalSyncBytes)
		iter, err := source.engine.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		iter.First()
		stream := &experimentalDirectSourceStream[T]{
			sourceRank: uint32(source.rank),
			iter:       iter,
			bucket:     bucket,
			sourceLo:   lower,
			destLo:     destLower,
			finalSync:  finalSyncID,
			finalBytes: finalSyncBytes,
			indexMode:  indexMode,
		}
		streams = append(streams, stream)
		rec, ok, err := stream.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, experimentalDirectSourceItem{streamIdx: i, rec: rec})
		}
	}

	indexWriters, err := experimentalNewIndexWriterSet(tmpDir, bucket.name+"-direct-index", indexMode)
	if err != nil {
		return err
	}
	defer indexWriters.cleanup()
	var lastPrimaryKey []byte
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := heap.Pop(h).(experimentalDirectSourceItem)
		winner := first.rec
		group := []experimentalDirectSourceItem{first}
		for h.Len() > 0 && bytes.Equal((*h)[0].rec.key, first.rec.key) {
			group = append(group, heap.Pop(h).(experimentalDirectSourceItem))
		}
		for _, item := range group[1:] {
			if experimentalRunRecordIsNewer(item.rec, winner) {
				winner = item.rec
			}
		}
		if err := experimentalMaterializeDirectWinner(&winner, bucket, finalSyncID, finalSyncBytes, indexMode); err != nil {
			return err
		}
		if !bytes.Equal(winner.key, lastPrimaryKey) {
			if err := primaryWriter.Set(winner.key, winner.value); err != nil {
				return err
			}
			lastPrimaryKey = append(lastPrimaryKey[:0], winner.key...)
		}
		if indexMode == experimentalIndexModeAll {
			if err := indexWriters.writeBlobs(winner.indexes); err != nil {
				return err
			}
		}
		for _, item := range group {
			next, ok, err := streams[item.streamIdx].next()
			if err != nil {
				return err
			}
			if ok {
				heap.Push(h, experimentalDirectSourceItem{streamIdx: item.streamIdx, rec: next})
			}
		}
	}
	if err := primaryWriter.Close(); err != nil {
		return err
	}
	primarySuccess = true
	defer func() { _ = os.Remove(primaryPath) }()
	if err := dest.DB().Ingest(ctx, []string{primaryPath}); err != nil {
		return fmt.Errorf("ingest direct %s primary: %w", bucket.name, err)
	}
	if indexMode == experimentalIndexModeAll {
		return indexWriters.closeSortAndIngest(ctx, dest)
	}
	return nil
}

const experimentalRunIndexFamilies = 8
const experimentalRunHeaderSize = 20 + (experimentalRunIndexFamilies-1)*4
const experimentalRunBucketCount = 4

const (
	experimentalRunBucketResourceTypes = iota
	experimentalRunBucketResources
	experimentalRunBucketEntitlements
	experimentalRunBucketGrants
)

type experimentalRunRecord struct {
	key        []byte
	value      []byte
	indexes    experimentalIndexBlobs
	tsNanos    int64
	sourceRank uint32
	msg        proto.Message
}

type experimentalIndexBlobs [experimentalRunIndexFamilies][]byte

func experimentalWriteRunRecord(w io.Writer, key []byte, tsNanos int64, sourceRank uint32, value []byte, indexes experimentalIndexBlobs) error {
	var header [experimentalRunHeaderSize]byte
	binary.BigEndian.PutUint64(header[0:8], uint64(tsNanos))
	binary.BigEndian.PutUint32(header[8:12], sourceRank)
	binary.BigEndian.PutUint32(header[12:16], uint32(len(key)))
	binary.BigEndian.PutUint32(header[16:20], uint32(len(value)))
	for i := 1; i < experimentalRunIndexFamilies; i++ {
		off := 20 + (i-1)*4
		binary.BigEndian.PutUint32(header[off:off+4], uint32(len(indexes[i])))
	}
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if _, err := w.Write(value); err != nil {
		return err
	}
	for i := 1; i < experimentalRunIndexFamilies; i++ {
		if len(indexes[i]) == 0 {
			continue
		}
		if _, err := w.Write(indexes[i]); err != nil {
			return err
		}
	}
	return nil
}

func experimentalReadRunRecord(r io.Reader) (experimentalRunRecord, bool, error) {
	var rec experimentalRunRecord
	ok, err := experimentalReadRunRecordInto(r, &rec)
	return rec, ok, err
}

func experimentalReadRunRecordInto(r io.Reader, rec *experimentalRunRecord) (bool, error) {
	var header [experimentalRunHeaderSize]byte
	_, err := io.ReadFull(r, header[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		return false, err
	}
	keyLen := binary.BigEndian.Uint32(header[12:16])
	valueLen := binary.BigEndian.Uint32(header[16:20])
	var indexLens [experimentalRunIndexFamilies]uint32
	for i := 1; i < experimentalRunIndexFamilies; i++ {
		off := 20 + (i-1)*4
		indexLens[i] = binary.BigEndian.Uint32(header[off : off+4])
	}
	rec.tsNanos = int64(binary.BigEndian.Uint64(header[0:8]))
	rec.sourceRank = binary.BigEndian.Uint32(header[8:12])
	if cap(rec.key) < int(keyLen) {
		rec.key = make([]byte, keyLen)
	} else {
		rec.key = rec.key[:keyLen]
	}
	if cap(rec.value) < int(valueLen) {
		rec.value = make([]byte, valueLen)
	} else {
		rec.value = rec.value[:valueLen]
	}
	if _, err := io.ReadFull(r, rec.key); err != nil {
		return false, err
	}
	if _, err := io.ReadFull(r, rec.value); err != nil {
		return false, err
	}
	for i := 1; i < experimentalRunIndexFamilies; i++ {
		indexLen := int(indexLens[i])
		if cap(rec.indexes[i]) < indexLen {
			rec.indexes[i] = make([]byte, indexLen)
		} else {
			rec.indexes[i] = rec.indexes[i][:indexLen]
		}
		if indexLen == 0 {
			continue
		}
		if _, err := io.ReadFull(r, rec.indexes[i]); err != nil {
			return false, err
		}
	}
	return true, nil
}

func experimentalTimestampNanos(ts *timestamppb.Timestamp) int64 {
	if ts == nil {
		return 0
	}
	return ts.AsTime().UnixNano()
}

func experimentalRewritePrimaryKeyForDest(sourceKey []byte, sourceLower []byte, destLower []byte) []byte {
	out := make([]byte, 0, len(destLower)+len(sourceKey)-len(sourceLower))
	out = append(out, destLower...)
	out = append(out, sourceKey[len(sourceLower):]...)
	return out
}

func experimentalBucketIndexKeys[T proto.Message](bucket experimentalBucket[T], syncBytes []byte, rec T) [][]byte {
	if bucket.indexKeys == nil {
		return nil
	}
	return bucket.indexKeys(syncBytes, rec)
}

func experimentalFilterIndexKeys(mode experimentalIndexMode, keys [][]byte) [][]byte {
	if mode == experimentalIndexModeAll {
		return keys
	}
	if len(keys) == 0 || mode == experimentalIndexModeNone {
		return nil
	}
	out := keys[:0]
	for _, key := range keys {
		if experimentalKeepIndexKey(mode, key) {
			out = append(out, key)
		}
	}
	return out
}

func experimentalKeepIndexKey(mode experimentalIndexMode, key []byte) bool {
	// Pebble v3 secondary index keys start with:
	// versionV3 | typeIndex | idx...
	if len(key) < 3 {
		return false
	}
	idx := key[2]
	switch mode {
	case experimentalIndexModeNeedsExpansion:
		return idx == 0x05 // idxGrantByNeedsExpansion
	case experimentalIndexModeGrantPrincipal:
		return idx == 0x04 || idx == 0x06 // by_principal, by_principal_resource_type
	case experimentalIndexModeGrantEntitlement:
		return idx == 0x03 || idx == 0x07 // by_entitlement, by_entitlement_resource
	case experimentalIndexModeResourceEntitlement:
		return idx == 0x01 || idx == 0x02 // resource_by_parent, entitlement_by_resource
	default:
		return false
	}
}

func experimentalEncodeIndexKeys(keys [][]byte) []byte {
	if len(keys) == 0 {
		return nil
	}
	total := 0
	for _, key := range keys {
		total += 4 + len(key)
	}
	out := make([]byte, 0, total)
	var lenBuf [4]byte
	for _, key := range keys {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
		out = append(out, lenBuf[:]...)
		out = append(out, key...)
	}
	return out
}

func experimentalEncodeIndexKeysByFamily(keys [][]byte) experimentalIndexBlobs {
	var out experimentalIndexBlobs
	for _, key := range keys {
		idx, ok := experimentalIndexID(key)
		if !ok || int(idx) >= len(out) {
			continue
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
		out[idx] = append(out[idx], lenBuf[:]...)
		out[idx] = append(out[idx], key...)
	}
	return out
}

func experimentalAppendIndexEntriesFromBlob(entries []experimentalSSTEntry, blob []byte) []experimentalSSTEntry {
	for len(blob) > 0 {
		if len(blob) < 4 {
			return entries
		}
		n := int(binary.BigEndian.Uint32(blob[:4]))
		blob = blob[4:]
		if len(blob) < n {
			return entries
		}
		key := append([]byte(nil), blob[:n]...)
		entries = append(entries, experimentalSSTEntry{key: key})
		blob = blob[n:]
	}
	return entries
}

const experimentalIndexSortChunkKeys = 100000

type experimentalIndexWriterSet struct {
	writers map[byte]*experimentalIndexRunWriter
	all     []*experimentalIndexRunWriter
}

func experimentalNewIndexWriterSet(tmpDir string, name string, mode experimentalIndexMode) (*experimentalIndexWriterSet, error) {
	set := &experimentalIndexWriterSet{writers: map[byte]*experimentalIndexRunWriter{}}
	for _, idx := range experimentalIndexIDsForMode(mode) {
		w, err := experimentalNewIndexRunWriter(tmpDir, fmt.Sprintf("%s-%02x", name, idx))
		if err != nil {
			set.cleanup()
			return nil, err
		}
		set.writers[idx] = w
		set.all = append(set.all, w)
	}
	return set, nil
}

func experimentalIndexIDsForMode(mode experimentalIndexMode) []byte {
	switch mode {
	case experimentalIndexModeAll:
		return []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	case experimentalIndexModeNeedsExpansion:
		return []byte{0x05}
	case experimentalIndexModeGrantPrincipal:
		return []byte{0x04, 0x06}
	case experimentalIndexModeGrantEntitlement:
		return []byte{0x03, 0x07}
	case experimentalIndexModeResourceEntitlement:
		return []byte{0x01, 0x02}
	default:
		return nil
	}
}

func experimentalIndexID(key []byte) (byte, bool) {
	if len(key) < 3 {
		return 0, false
	}
	return key[2], true
}

func (s *experimentalIndexWriterSet) writeBlob(blob []byte) error {
	for len(blob) > 0 {
		if len(blob) < 4 {
			return fmt.Errorf("index writer set: corrupt index blob")
		}
		n := int(binary.BigEndian.Uint32(blob[:4]))
		blob = blob[4:]
		if len(blob) < n {
			return fmt.Errorf("index writer set: truncated index blob")
		}
		idx, ok := experimentalIndexID(blob[:n])
		if ok {
			if w := s.writers[idx]; w != nil {
				if err := w.writeKey(blob[:n]); err != nil {
					return err
				}
			}
		}
		blob = blob[n:]
	}
	return nil
}

func (s *experimentalIndexWriterSet) writeBlobs(blobs experimentalIndexBlobs) error {
	for idx, blob := range blobs {
		if idx == 0 || len(blob) == 0 {
			continue
		}
		w := s.writers[byte(idx)]
		if w == nil {
			continue
		}
		if err := w.writeBlob(blob); err != nil {
			return err
		}
	}
	return nil
}

func (s *experimentalIndexWriterSet) writeKeys(keys [][]byte) error {
	for _, key := range keys {
		idx, ok := experimentalIndexID(key)
		if !ok {
			continue
		}
		if w := s.writers[idx]; w != nil {
			if err := w.writeKey(key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *experimentalIndexWriterSet) closeSortAndIngest(ctx context.Context, dest *pebbleengine.Engine) error {
	for _, w := range s.all {
		if err := w.closeSortAndIngest(ctx, dest); err != nil {
			return err
		}
	}
	return nil
}

func (s *experimentalIndexWriterSet) cleanup() {
	for _, w := range s.all {
		w.cleanup()
	}
}

type experimentalIndexRunWriter struct {
	path   string
	tmpDir string
	name   string
	file   *os.File
	writer *bufio.Writer
	count  int
	closed bool
}

func experimentalNewIndexRunWriter(tmpDir string, name string) (*experimentalIndexRunWriter, error) {
	path := filepath.Join(tmpDir, "index-run-"+name+".bin")
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &experimentalIndexRunWriter{
		path:   path,
		tmpDir: tmpDir,
		name:   name,
		file:   f,
		writer: bufio.NewWriterSize(f, experimentalRunFileBufferSize),
	}, nil
}

func (w *experimentalIndexRunWriter) writeBlob(blob []byte) error {
	for len(blob) > 0 {
		if len(blob) < 4 {
			return fmt.Errorf("index run %s: corrupt index blob", w.name)
		}
		n := int(binary.BigEndian.Uint32(blob[:4]))
		blob = blob[4:]
		if len(blob) < n {
			return fmt.Errorf("index run %s: truncated index blob", w.name)
		}
		if err := experimentalWriteLengthPrefixedBytes(w.writer, blob[:n]); err != nil {
			return err
		}
		w.count++
		blob = blob[n:]
	}
	return nil
}

func (w *experimentalIndexRunWriter) writeKey(key []byte) error {
	if err := experimentalWriteLengthPrefixedBytes(w.writer, key); err != nil {
		return err
	}
	w.count++
	return nil
}

func (w *experimentalIndexRunWriter) writeKeys(keys [][]byte) error {
	for _, key := range keys {
		if err := w.writeKey(key); err != nil {
			return err
		}
	}
	return nil
}

func (w *experimentalIndexRunWriter) close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.writer.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}

func (w *experimentalIndexRunWriter) cleanup() {
	_ = w.close()
	_ = os.Remove(w.path)
}

func (w *experimentalIndexRunWriter) closeSortAndIngest(ctx context.Context, dest *pebbleengine.Engine) error {
	if err := w.close(); err != nil {
		return err
	}
	if w.count == 0 {
		return nil
	}
	chunks, err := experimentalSortIndexRunChunks(ctx, w.tmpDir, w.name, w.path)
	if err != nil {
		return err
	}
	defer func() {
		for _, chunk := range chunks {
			_ = os.Remove(chunk)
		}
	}()
	sstPath := filepath.Join(w.tmpDir, "index-sorted-"+w.name+".sst")
	if err := experimentalMergeSortedIndexChunksToSST(ctx, sstPath, chunks); err != nil {
		return err
	}
	defer func() { _ = os.Remove(sstPath) }()
	if err := dest.DB().Ingest(ctx, []string{sstPath}); err != nil {
		return fmt.Errorf("ingest index %s: %w", w.name, err)
	}
	return nil
}

func experimentalWriteLengthPrefixedBytes(w io.Writer, b []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func experimentalReadLengthPrefixedBytes(r io.Reader, dst *[]byte) (bool, error) {
	var lenBuf [4]byte
	_, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		return false, err
	}
	n := int(binary.BigEndian.Uint32(lenBuf[:]))
	if cap(*dst) < n {
		*dst = make([]byte, n)
	} else {
		*dst = (*dst)[:n]
	}
	if _, err := io.ReadFull(r, *dst); err != nil {
		return false, err
	}
	return true, nil
}

func experimentalSortIndexRunChunks(ctx context.Context, tmpDir string, name string, path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, experimentalRunFileBufferSize)
	var chunks []string
	var scratch []byte
	for {
		keys := make([][]byte, 0, experimentalIndexSortChunkKeys)
		for len(keys) < experimentalIndexSortChunkKeys {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			ok, err := experimentalReadLengthPrefixedBytes(reader, &scratch)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			keys = append(keys, append([]byte(nil), scratch...))
		}
		if len(keys) == 0 {
			break
		}
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
		chunkPath := filepath.Join(tmpDir, fmt.Sprintf("index-run-%s-chunk-%04d.bin", name, len(chunks)))
		if err := experimentalWriteSortedIndexChunk(chunkPath, keys); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunkPath)
		if len(keys) < experimentalIndexSortChunkKeys {
			break
		}
	}
	return chunks, nil
}

func experimentalWriteSortedIndexChunk(path string, keys [][]byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(f, experimentalRunFileBufferSize)
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	var last []byte
	for _, key := range keys {
		if bytes.Equal(key, last) {
			continue
		}
		if err := experimentalWriteLengthPrefixedBytes(writer, key); err != nil {
			return err
		}
		last = key
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	success = true
	return nil
}

type experimentalIndexChunkItem struct {
	chunkIdx int
	key      []byte
}

type experimentalIndexChunkHeap []experimentalIndexChunkItem

func (h experimentalIndexChunkHeap) Len() int { return len(h) }
func (h experimentalIndexChunkHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp == 0 {
		return h[i].chunkIdx < h[j].chunkIdx
	}
	return cmp < 0
}
func (h experimentalIndexChunkHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *experimentalIndexChunkHeap) Push(x any)   { *h = append(*h, x.(experimentalIndexChunkItem)) }
func (h *experimentalIndexChunkHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

func experimentalMergeSortedIndexChunksToSST(ctx context.Context, sstPath string, chunks []string) error {
	readers := make([]*os.File, 0, len(chunks))
	bufReaders := make([]*bufio.Reader, 0, len(chunks))
	readBufs := make([][]byte, len(chunks))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	h := &experimentalIndexChunkHeap{}
	heap.Init(h)
	for i, chunk := range chunks {
		f, err := os.Open(chunk)
		if err != nil {
			return err
		}
		readers = append(readers, f)
		br := bufio.NewReaderSize(f, experimentalRunFileBufferSize)
		bufReaders = append(bufReaders, br)
		ok, err := experimentalReadLengthPrefixedBytes(br, &readBufs[i])
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, experimentalIndexChunkItem{chunkIdx: i, key: readBufs[i]})
		}
	}
	writer, err := experimentalNewSSTWriter(sstPath)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = writer.Close()
			_ = os.Remove(sstPath)
		}
	}()
	var last []byte
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := heap.Pop(h).(experimentalIndexChunkItem)
		if !bytes.Equal(item.key, last) {
			if err := writer.Set(item.key, nil); err != nil {
				return err
			}
			last = append(last[:0], item.key...)
		}
		ok, err := experimentalReadLengthPrefixedBytes(bufReaders[item.chunkIdx], &readBufs[item.chunkIdx])
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, experimentalIndexChunkItem{chunkIdx: item.chunkIdx, key: readBufs[item.chunkIdx]})
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	success = true
	return nil
}

func experimentalMergeRunInputGroup(ctx context.Context, tmpDir string, inputs []experimentalRunInput, name string) (experimentalRunInput, error) {
	outPath := filepath.Join(tmpDir, "runfile-merge-"+name+".bin")
	out, err := os.Create(outPath)
	if err != nil {
		return experimentalRunInput{}, err
	}
	cw := &experimentalCountingWriter{w: bufio.NewWriterSize(out, experimentalRunFileBufferSize)}
	success := false
	defer func() {
		_ = out.Close()
		if !success {
			_ = os.Remove(outPath)
		}
	}()
	run := experimentalRunInput{path: outPath}
	if err := experimentalMergeRunSectionToInput(ctx, cw, inputs, experimentalResourceTypeBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalMergeRunSectionToInput(ctx, cw, inputs, experimentalResourceBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalMergeRunSectionToInput(ctx, cw, inputs, experimentalEntitlementBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := experimentalMergeRunSectionToInput(ctx, cw, inputs, experimentalGrantBucket(), &run); err != nil {
		return experimentalRunInput{}, err
	}
	if err := cw.Flush(); err != nil {
		return experimentalRunInput{}, err
	}
	if err := out.Close(); err != nil {
		return experimentalRunInput{}, err
	}
	success = true
	return run, nil
}

type experimentalRunHeapItem struct {
	readerIdx int
	rec       experimentalRunRecord
}

type experimentalRunRecordHeap []experimentalRunHeapItem

func (h experimentalRunRecordHeap) Len() int { return len(h) }
func (h experimentalRunRecordHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].rec.key, h[j].rec.key)
	if cmp == 0 {
		return h[i].readerIdx < h[j].readerIdx
	}
	return cmp < 0
}
func (h experimentalRunRecordHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *experimentalRunRecordHeap) Push(x any)   { *h = append(*h, x.(experimentalRunHeapItem)) }
func (h *experimentalRunRecordHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

func experimentalMergeRunSectionToInput[T proto.Message](
	ctx context.Context,
	w *experimentalCountingWriter,
	inputs []experimentalRunInput,
	bucket experimentalBucket[T],
	run *experimentalRunInput,
) error {
	start := w.n
	readers := make([]*os.File, 0, len(inputs))
	bufReaders := make([]*bufio.Reader, 0, len(inputs))
	readBufs := make([]experimentalRunRecord, 0, len(inputs))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	h := &experimentalRunRecordHeap{}
	heap.Init(h)
	for i, input := range inputs {
		f, err := os.Open(input.path)
		if err != nil {
			return err
		}
		section := input.sections[bucket.id]
		if _, err := f.Seek(section.offset, io.SeekStart); err != nil {
			_ = f.Close()
			return err
		}
		readers = append(readers, f)
		br := bufio.NewReaderSize(io.LimitReader(f, section.length), experimentalRunFileBufferSize)
		bufReaders = append(bufReaders, br)
		readBufs = append(readBufs, experimentalRunRecord{})
		ok, err := experimentalReadRunRecordInto(br, &readBufs[len(readBufs)-1])
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, experimentalRunHeapItem{readerIdx: i, rec: readBufs[len(readBufs)-1]})
		}
	}

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := heap.Pop(h).(experimentalRunHeapItem)
		winner := first.rec
		group := []experimentalRunHeapItem{first}
		for h.Len() > 0 && bytes.Equal((*h)[0].rec.key, first.rec.key) {
			group = append(group, heap.Pop(h).(experimentalRunHeapItem))
		}
		for _, item := range group[1:] {
			if experimentalRunRecordIsNewer(item.rec, winner) {
				winner = item.rec
			}
		}
		if err := experimentalWriteRunRecord(w, winner.key, winner.tsNanos, winner.sourceRank, winner.value, winner.indexes); err != nil {
			return err
		}
		for _, item := range group {
			ok, err := experimentalReadRunRecordInto(bufReaders[item.readerIdx], &readBufs[item.readerIdx])
			if err != nil {
				return err
			}
			if ok {
				heap.Push(h, experimentalRunHeapItem{readerIdx: item.readerIdx, rec: readBufs[item.readerIdx]})
			}
		}
	}
	run.sections[bucket.id] = experimentalRunSection{offset: start, length: w.n - start}
	return nil
}

func experimentalRunRecordIsNewer(incoming, existing experimentalRunRecord) bool {
	if incoming.tsNanos != existing.tsNanos {
		return incoming.tsNanos > existing.tsNanos
	}
	return incoming.sourceRank < existing.sourceRank
}

func experimentalMaterializeDirectWinner[T proto.Message](
	winner *experimentalRunRecord,
	bucket experimentalBucket[T],
	finalSyncID string,
	finalSyncBytes []byte,
	indexMode experimentalIndexMode,
) error {
	if winner.value != nil {
		return nil
	}
	msg, ok := winner.msg.(T)
	if !ok {
		return fmt.Errorf("direct winner %s: unexpected message type %T", bucket.name, winner.msg)
	}
	bucket.setSync(msg, finalSyncID)
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return err
	}
	winner.value = value
	if indexMode != experimentalIndexModeNone {
		winner.indexes = experimentalEncodeIndexKeysByFamily(experimentalFilterIndexKeys(indexMode, experimentalBucketIndexKeys(bucket, finalSyncBytes, msg)))
	}
	winner.msg = nil
	return nil
}

func experimentalMaterializeRunInputsToPebble(ctx context.Context, outPath string, tmpDir string, inputs []experimentalRunInput, destSyncID string, indexMode experimentalIndexMode) (*CompactableSync, error) {
	merged, err := experimentalMergeRunInputGroup(ctx, tmpDir, inputs, "final")
	if err != nil {
		return nil, err
	}
	defer experimentalRemoveRunInputs([]experimentalRunInput{merged})

	if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	w, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(tmpDir))
	if err != nil {
		return nil, err
	}
	store, ok := w.(dotc1z.C1ZStore)
	if !ok {
		_ = w.Close(ctx)
		return nil, fmt.Errorf("runfile kway: destination does not implement C1ZStore: %T", w)
	}
	eng, ok := pebbleengine.AsEngine(w)
	if !ok {
		_ = w.Close(ctx)
		return nil, fmt.Errorf("runfile kway: destination is not pebble")
	}
	startedSyncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	_ = startedSyncID
	destSyncBytes, err := codec.EncodeSyncID(destSyncID)
	if err != nil {
		_ = store.Close(ctx)
		return nil, err
	}

	if err := experimentalMaterializeRunFileBucket(ctx, eng, tmpDir, destSyncID, destSyncBytes, merged, experimentalResourceTypeBucket(), indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := experimentalMaterializeRunFileBucket(ctx, eng, tmpDir, destSyncID, destSyncBytes, merged, experimentalResourceBucket(), indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := experimentalMaterializeRunFileBucket(ctx, eng, tmpDir, destSyncID, destSyncBytes, merged, experimentalEntitlementBucket(), indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := experimentalMaterializeRunFileBucket(ctx, eng, tmpDir, destSyncID, destSyncBytes, merged, experimentalGrantBucket(), indexMode); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := store.EndSync(ctx); err != nil {
		_ = store.Close(ctx)
		return nil, err
	}
	if err := store.Close(ctx); err != nil {
		return nil, err
	}
	return &CompactableSync{FilePath: outPath, SyncID: destSyncID}, nil
}

func experimentalMaterializeRunFileBucket[T proto.Message](
	ctx context.Context,
	dest *pebbleengine.Engine,
	tmpDir string,
	destSyncID string,
	destSyncBytes []byte,
	input experimentalRunInput,
	bucket experimentalBucket[T],
	indexMode experimentalIndexMode,
) error {
	f, err := os.Open(input.path)
	if err != nil {
		return err
	}
	defer f.Close()
	section := input.sections[bucket.id]
	if _, err := f.Seek(section.offset, io.SeekStart); err != nil {
		return err
	}
	br := bufio.NewReaderSize(io.LimitReader(f, section.length), experimentalRunFileBufferSize)
	indexWriters, err := experimentalNewIndexWriterSet(tmpDir, bucket.name+"-index", indexMode)
	if err != nil {
		return err
	}
	defer indexWriters.cleanup()

	primaryPath := filepath.Join(tmpDir, fmt.Sprintf("kway-final-%s-primary.sst", bucket.name))
	primaryWriter, err := experimentalNewSSTWriter(primaryPath)
	if err != nil {
		return err
	}
	primarySuccess := false
	defer func() {
		if !primarySuccess {
			_ = primaryWriter.Close()
			_ = os.Remove(primaryPath)
		}
	}()
	var lastPrimaryKey []byte
	for {
		rec, ok, err := experimentalReadRunRecord(br)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bytes.Equal(rec.key, lastPrimaryKey) {
			if err := primaryWriter.Set(rec.key, rec.value); err != nil {
				return err
			}
			lastPrimaryKey = append(lastPrimaryKey[:0], rec.key...)
		}
		if indexMode == experimentalIndexModeAll {
			msg := bucket.mk()
			if err := proto.Unmarshal(rec.value, msg); err != nil {
				return err
			}
			if err := indexWriters.writeKeys(experimentalFilterIndexKeys(indexMode, experimentalBucketIndexKeys(bucket, destSyncBytes, msg))); err != nil {
				return err
			}
		}
	}
	if err := primaryWriter.Close(); err != nil {
		return err
	}
	primarySuccess = true
	defer func() { _ = os.Remove(primaryPath) }()
	if err := dest.DB().Ingest(ctx, []string{primaryPath}); err != nil {
		return fmt.Errorf("ingest %s primary: %w", bucket.name, err)
	}
	if indexMode == experimentalIndexModeAll {
		return indexWriters.closeSortAndIngest(ctx, dest)
	}
	return nil
}

type experimentalKWayInput struct {
	syncID   string
	filePath string
	engine   *pebbleengine.Engine
	close    func() error
}

func experimentalOpenKWayInputs(ctx context.Context, tmpDir string, inputs []*CompactableSync) ([]experimentalKWayInput, error) {
	out := make([]experimentalKWayInput, 0, len(inputs))
	for _, input := range inputs {
		w, err := dotc1z.NewStore(ctx, input.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
		if err != nil {
			experimentalCloseKWayInputs(out)
			return nil, err
		}
		eng, ok := pebbleengine.AsEngine(w)
		if !ok {
			_ = w.Close(ctx)
			experimentalCloseKWayInputs(out)
			return nil, fmt.Errorf("kway: input is not pebble: %s", input.FilePath)
		}
		store := w
		out = append(out, experimentalKWayInput{
			syncID: input.SyncID,
			engine: eng,
			close:  func() error { return store.Close(ctx) },
		})
	}
	return out, nil
}

func experimentalCloseKWayInputs(inputs []experimentalKWayInput) {
	for _, input := range inputs {
		if input.close != nil {
			_ = input.close()
		}
	}
}

func experimentalKWayMergePebbleGroup(
	ctx context.Context,
	outputDir string,
	tmpDir string,
	inputs []experimentalKWayInput,
	final bool,
	name string,
) (experimentalKWayInput, error) {
	var (
		destSyncID string
		destEng    *pebbleengine.Engine
		closeDest  func() error
		filePath   string
		err        error
	)
	if final {
		filePath = filepath.Join(outputDir, fmt.Sprintf("kway-final-%s.c1z", name))
		if removeErr := os.Remove(filePath); removeErr != nil && !os.IsNotExist(removeErr) {
			return experimentalKWayInput{}, removeErr
		}
		w, err := dotc1z.NewStore(ctx, filePath, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(tmpDir))
		if err != nil {
			return experimentalKWayInput{}, err
		}
		store, ok := w.(dotc1z.C1ZStore)
		if !ok {
			_ = w.Close(ctx)
			return experimentalKWayInput{}, fmt.Errorf("kway: destination does not implement C1ZStore: %T", w)
		}
		destEng, ok = pebbleengine.AsEngine(w)
		if !ok {
			_ = w.Close(ctx)
			return experimentalKWayInput{}, fmt.Errorf("kway: destination is not pebble")
		}
		destSyncID, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		if err != nil {
			_ = store.Close(ctx)
			return experimentalKWayInput{}, err
		}
		closeDest = func() error {
			if err := store.EndSync(ctx); err != nil {
				_ = store.Close(ctx)
				return err
			}
			return store.Close(ctx)
		}
	} else {
		dbDir := filepath.Join(tmpDir, "kway-intermediate-"+name)
		destEngRaw, err := pebbleengine.Open(ctx, dbDir)
		if err != nil {
			return experimentalKWayInput{}, err
		}
		destEng = destEngRaw
		adapter := pebbleengine.NewAdapter(destEng)
		destSyncID, err = adapter.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		if err != nil {
			_ = destEng.Close()
			return experimentalKWayInput{}, err
		}
		closeDest = func() error {
			if err := adapter.EndSync(ctx); err != nil {
				_ = destEng.Close()
				return err
			}
			return nil
		}
	}

	sources := make([]experimentalKWaySource, 0, len(inputs))
	for rank, input := range inputs {
		sources = append(sources, experimentalKWaySource{
			rank:   rank,
			syncID: input.syncID,
			engine: input.engine,
		})
	}

	destSyncBytes, err := codec.EncodeSyncID(destSyncID)
	if err != nil {
		_ = destEng.Close()
		return experimentalKWayInput{}, err
	}

	materializeIndexes := final
	if err := experimentalKWayMergeBucket(ctx, destEng, tmpDir, destSyncID, destSyncBytes, materializeIndexes, sources, experimentalResourceTypeBucket()); err != nil {
		_ = destEng.Close()
		return experimentalKWayInput{}, err
	}
	if err := experimentalKWayMergeBucket(ctx, destEng, tmpDir, destSyncID, destSyncBytes, materializeIndexes, sources, experimentalResourceBucket()); err != nil {
		_ = destEng.Close()
		return experimentalKWayInput{}, err
	}
	if err := experimentalKWayMergeBucket(ctx, destEng, tmpDir, destSyncID, destSyncBytes, materializeIndexes, sources, experimentalEntitlementBucket()); err != nil {
		_ = destEng.Close()
		return experimentalKWayInput{}, err
	}
	if err := experimentalKWayMergeBucket(ctx, destEng, tmpDir, destSyncID, destSyncBytes, materializeIndexes, sources, experimentalGrantBucket()); err != nil {
		_ = destEng.Close()
		return experimentalKWayInput{}, err
	}
	if err := closeDest(); err != nil {
		return experimentalKWayInput{}, err
	}
	if final {
		return experimentalKWayInput{syncID: destSyncID, filePath: filePath}, nil
	}
	return experimentalKWayInput{
		syncID: destSyncID,
		engine: destEng,
		close:  func() error { return destEng.Close() },
	}, nil
}

type experimentalKWaySource struct {
	rank   int
	syncID string
	engine *pebbleengine.Engine
}

type experimentalBucket[T proto.Message] struct {
	name       string
	id         int
	mk         func() T
	bounds     func([]byte) ([]byte, []byte)
	key        func(T) string
	ts         func(T) *timestamppb.Timestamp
	setSync    func(T, string)
	primaryKey func([]byte, T) []byte
	indexKeys  func([]byte, T) [][]byte
}

func experimentalResourceTypeBucket() experimentalBucket[*v3.ResourceTypeRecord] {
	return experimentalBucket[*v3.ResourceTypeRecord]{
		name: "resource_types",
		id:   experimentalRunBucketResourceTypes,
		mk:   func() *v3.ResourceTypeRecord { return &v3.ResourceTypeRecord{} },
		bounds: func(syncBytes []byte) ([]byte, []byte) {
			return pebbleengine.ResourceTypeSyncLowerBound(syncBytes), pebbleengine.ResourceTypeSyncUpperBound(syncBytes)
		},
		key:     func(r *v3.ResourceTypeRecord) string { return r.GetExternalId() },
		ts:      func(r *v3.ResourceTypeRecord) *timestamppb.Timestamp { return r.GetDiscoveredAt() },
		setSync: func(r *v3.ResourceTypeRecord, syncID string) { r.SetSyncId(syncID) },
		primaryKey: func(syncBytes []byte, r *v3.ResourceTypeRecord) []byte {
			return pebbleengine.ResourceTypeRecordKey(syncBytes, r.GetExternalId())
		},
	}
}

func experimentalResourceBucket() experimentalBucket[*v3.ResourceRecord] {
	return experimentalBucket[*v3.ResourceRecord]{
		name: "resources",
		id:   experimentalRunBucketResources,
		mk:   func() *v3.ResourceRecord { return &v3.ResourceRecord{} },
		bounds: func(syncBytes []byte) ([]byte, []byte) {
			return pebbleengine.ResourceSyncLowerBound(syncBytes), pebbleengine.ResourceSyncUpperBound(syncBytes)
		},
		key:     func(r *v3.ResourceRecord) string { return r.GetResourceTypeId() + "\x00" + r.GetResourceId() },
		ts:      func(r *v3.ResourceRecord) *timestamppb.Timestamp { return r.GetDiscoveredAt() },
		setSync: func(r *v3.ResourceRecord, syncID string) { r.SetSyncId(syncID) },
		primaryKey: func(syncBytes []byte, r *v3.ResourceRecord) []byte {
			return pebbleengine.ResourceRecordKey(syncBytes, r.GetResourceTypeId(), r.GetResourceId())
		},
		indexKeys: func(syncBytes []byte, r *v3.ResourceRecord) [][]byte {
			return pebbleengine.ResourceIndexKeys(syncBytes, r)
		},
	}
}

func experimentalEntitlementBucket() experimentalBucket[*v3.EntitlementRecord] {
	return experimentalBucket[*v3.EntitlementRecord]{
		name: "entitlements",
		id:   experimentalRunBucketEntitlements,
		mk:   func() *v3.EntitlementRecord { return &v3.EntitlementRecord{} },
		bounds: func(syncBytes []byte) ([]byte, []byte) {
			return pebbleengine.EntitlementSyncLowerBound(syncBytes), pebbleengine.EntitlementSyncUpperBound(syncBytes)
		},
		key:     func(r *v3.EntitlementRecord) string { return r.GetExternalId() },
		ts:      func(r *v3.EntitlementRecord) *timestamppb.Timestamp { return r.GetDiscoveredAt() },
		setSync: func(r *v3.EntitlementRecord, syncID string) { r.SetSyncId(syncID) },
		primaryKey: func(syncBytes []byte, r *v3.EntitlementRecord) []byte {
			return pebbleengine.EntitlementRecordKey(syncBytes, r.GetExternalId())
		},
		indexKeys: func(syncBytes []byte, r *v3.EntitlementRecord) [][]byte {
			return pebbleengine.EntitlementIndexKeys(syncBytes, r)
		},
	}
}

func experimentalGrantBucket() experimentalBucket[*v3.GrantRecord] {
	return experimentalBucket[*v3.GrantRecord]{
		name: "grants",
		id:   experimentalRunBucketGrants,
		mk:   func() *v3.GrantRecord { return &v3.GrantRecord{} },
		bounds: func(syncBytes []byte) ([]byte, []byte) {
			return pebbleengine.GrantSyncLowerBound(syncBytes), pebbleengine.GrantSyncUpperBound(syncBytes)
		},
		key:     func(r *v3.GrantRecord) string { return r.GetExternalId() },
		ts:      func(r *v3.GrantRecord) *timestamppb.Timestamp { return r.GetDiscoveredAt() },
		setSync: func(r *v3.GrantRecord, syncID string) { r.SetSyncId(syncID) },
		primaryKey: func(syncBytes []byte, r *v3.GrantRecord) []byte {
			return pebbleengine.GrantRecordKey(syncBytes, r.GetExternalId())
		},
		indexKeys: func(syncBytes []byte, r *v3.GrantRecord) [][]byte { return pebbleengine.GrantIndexKeys(syncBytes, r) },
	}
}

type experimentalHeapItem[T proto.Message] struct {
	sourceIdx int
	key       string
	record    T
}

type experimentalRecordHeap[T proto.Message] []experimentalHeapItem[T]

func (h experimentalRecordHeap[T]) Len() int { return len(h) }
func (h experimentalRecordHeap[T]) Less(i, j int) bool {
	if h[i].key == h[j].key {
		return h[i].sourceIdx < h[j].sourceIdx
	}
	return h[i].key < h[j].key
}
func (h experimentalRecordHeap[T]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *experimentalRecordHeap[T]) Push(x any) {
	*h = append(*h, x.(experimentalHeapItem[T]))
}
func (h *experimentalRecordHeap[T]) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type experimentalBucketStream[T proto.Message] struct {
	sourceRank int
	iter       *pebble.Iterator
	bucket     experimentalBucket[T]
}

func (s *experimentalBucketStream[T]) close() {
	if s.iter != nil {
		_ = s.iter.Close()
	}
}

func (s *experimentalBucketStream[T]) next() (experimentalHeapItem[T], bool, error) {
	if !s.iter.Valid() {
		if err := s.iter.Error(); err != nil {
			return experimentalHeapItem[T]{}, false, err
		}
		return experimentalHeapItem[T]{}, false, nil
	}
	rec := s.bucket.mk()
	if err := proto.Unmarshal(s.iter.Value(), rec); err != nil {
		return experimentalHeapItem[T]{}, false, err
	}
	item := experimentalHeapItem[T]{
		sourceIdx: s.sourceRank,
		key:       s.bucket.key(rec),
		record:    rec,
	}
	s.iter.Next()
	return item, true, nil
}

func experimentalKWayMergeBucket[T proto.Message](
	ctx context.Context,
	dest *pebbleengine.Engine,
	tmpDir string,
	destSyncID string,
	destSyncBytes []byte,
	materializeIndexes bool,
	sources []experimentalKWaySource,
	bucket experimentalBucket[T],
) error {
	streams := make([]*experimentalBucketStream[T], 0, len(sources))
	h := &experimentalRecordHeap[T]{}
	heap.Init(h)
	defer func() {
		for _, stream := range streams {
			stream.close()
		}
	}()

	for sourceIdx, source := range sources {
		syncBytes, err := codec.EncodeSyncID(source.syncID)
		if err != nil {
			return err
		}
		lower, upper := bucket.bounds(syncBytes)
		iter, err := source.engine.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		iter.First()
		stream := &experimentalBucketStream[T]{
			sourceRank: sourceIdx,
			iter:       iter,
			bucket:     bucket,
		}
		streams = append(streams, stream)
		item, ok, err := stream.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, item)
		}
	}

	primaryEntries := make([]experimentalSSTEntry, 0, experimentalKWayBatchSize)
	indexEntries := make([]experimentalSSTEntry, 0, experimentalKWayBatchSize)
	marshal := proto.MarshalOptions{Deterministic: true}

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		first := heap.Pop(h).(experimentalHeapItem[T])
		groupKey := first.key
		winner := first
		group := []experimentalHeapItem[T]{first}
		for h.Len() > 0 && (*h)[0].key == groupKey {
			group = append(group, heap.Pop(h).(experimentalHeapItem[T]))
		}
		for _, item := range group[1:] {
			if experimentalRecordIsNewer(bucket.ts(item.record), bucket.ts(winner.record)) {
				winner = item
			}
		}
		bucket.setSync(winner.record, destSyncID)
		value, err := marshal.Marshal(winner.record)
		if err != nil {
			return err
		}
		primaryEntries = append(primaryEntries, experimentalSSTEntry{
			key:   bucket.primaryKey(destSyncBytes, winner.record),
			value: value,
		})
		if materializeIndexes && bucket.indexKeys != nil {
			for _, key := range bucket.indexKeys(destSyncBytes, winner.record) {
				indexEntries = append(indexEntries, experimentalSSTEntry{key: key})
			}
		}

		for _, item := range group {
			next, ok, err := streams[item.sourceIdx].next()
			if err != nil {
				return err
			}
			if ok {
				heap.Push(h, next)
			}
		}
	}
	if err := experimentalWriteAndIngestSST(ctx, dest, tmpDir, bucket.name+"-primary", primaryEntries); err != nil {
		return err
	}
	if err := experimentalWriteAndIngestSST(ctx, dest, tmpDir, bucket.name+"-index", indexEntries); err != nil {
		return err
	}
	return nil
}

func experimentalRecordIsNewer(incoming, existing *timestamppb.Timestamp) bool {
	if incoming == nil {
		return false
	}
	if existing == nil {
		return true
	}
	return incoming.AsTime().After(existing.AsTime())
}

type experimentalSSTEntry struct {
	key   []byte
	value []byte
}

type experimentalSSTWriter struct {
	writer *sstable.Writer
	path   string
}

func experimentalNewSSTWriter(path string) (*experimentalSSTWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	writable := objstorageprovider.NewFileWritable(file)
	writer := sstable.NewWriter(writable, sstable.WriterOptions{
		TableFormat: pebbleengine.SDKPebbleFormat.MaxTableFormat(),
		BlockSize:   4 << 10,
	})
	return &experimentalSSTWriter{writer: writer, path: path}, nil
}

func (w *experimentalSSTWriter) Set(key, value []byte) error {
	return w.writer.Set(key, value)
}

func (w *experimentalSSTWriter) Close() error {
	if w == nil || w.writer == nil {
		return nil
	}
	err := w.writer.Close()
	w.writer = nil
	return err
}

func experimentalWriteAndIngestSST(
	ctx context.Context,
	dest *pebbleengine.Engine,
	tmpDir string,
	name string,
	entries []experimentalSSTEntry,
) error {
	if len(entries) == 0 {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	sstPath := filepath.Join(tmpDir, fmt.Sprintf("kway-sst-%s-%p.sst", name, &entries[0]))
	if err := experimentalWriteSSTFile(sstPath, entries); err != nil {
		return err
	}
	defer func() { _ = os.Remove(sstPath) }()
	if err := dest.DB().Ingest(ctx, []string{sstPath}); err != nil {
		return fmt.Errorf("ingest %s: %w", name, err)
	}
	return nil
}

func experimentalWriteSSTFile(path string, entries []experimentalSSTEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	writer, err := experimentalNewSSTWriter(path)
	if err != nil {
		return err
	}
	for i, entry := range entries {
		if i > 0 && bytes.Equal(entries[i-1].key, entry.key) {
			continue
		}
		if err := writer.Set(entry.key, entry.value); err != nil {
			_ = writer.Close()
			_ = os.Remove(path)
			return err
		}
	}
	if err := writer.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
