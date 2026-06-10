package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestProdScaleFoldOverlayCrossover sweeps the partial-volume ratio at
// a mid-scale base (~256MB, the auto-cutover floor) to locate where
// the fold and overlay curves cross. The fold gate's 25% ratio was
// derived from two points (1.1GB prod shape, MB-scale fixtures); this
// fills in the middle: a fixed 10 partials whose total grant volume is
// 5/10/25/50% of the base, each compacted with overlay and fold.
//
// Gated and cached like the other prod-scale experiments:
//
//	BATON_PROD_SCALE_CROSSOVER=1 go test ./pkg/synccompactor -run TestProdScaleFoldOverlayCrossover -v -timeout 60m
func TestProdScaleFoldOverlayCrossover(t *testing.T) {
	if os.Getenv("BATON_PROD_SCALE_CROSSOVER") == "" {
		t.Skip("set BATON_PROD_SCALE_CROSSOVER=1 to run the crossover experiment")
	}
	ctx := context.Background()
	require.NoError(t, ensurePebbleRegistered())

	fixDir := os.Getenv("BATON_PROD_SCALE_DIR")
	if fixDir == "" {
		fixDir = filepath.Join("..", "..", ".profiles", "prodscale-crossover")
	} else {
		fixDir = filepath.Join(fixDir, "crossover")
	}
	require.NoError(t, os.MkdirAll(fixDir, 0o755)) // #nosec G703 - developer-provided fixture dir env var.

	baseGrants := envInt("BATON_CROSSOVER_GRANTS", 1_200_000)
	baseUsers := envInt("BATON_CROSSOVER_USERS", 50_000)
	baseEnts := envInt("BATON_CROSSOVER_ENTS", 10_000)
	const partialCount = 10

	basePath := filepath.Join(fixDir, "base.c1z")
	// #nosec G703 - fixture paths derive from a developer-provided env var.
	if _, err := os.Stat(basePath); errors.Is(err, os.ErrNotExist) {
		t.Logf("building base c1z: grants=%d users=%d ents=%d", baseGrants, baseUsers, baseEnts)
		start := time.Now()
		buildProdScaleBase(t, ctx, basePath, baseGrants, baseUsers, baseEnts)
		t.Logf("base built in %s", time.Since(start).Round(time.Second))
	}
	logFileSize(t, "base", basePath)
	baseBytes := fileSizeOrZero(basePath)
	baseSyncID := manifestSyncID(t, basePath)

	for _, ratioPct := range []int{5, 10, 25, 50} {
		ratioPct := ratioPct
		t.Run(fmt.Sprintf("ratio=%d", ratioPct), func(t *testing.T) {
			perPartial := baseGrants * ratioPct / 100 / partialCount
			require.LessOrEqual(t, perPartial*partialCount, baseGrants,
				"override windows must fit in the base grant id space")

			entries := []*CompactableSync{{FilePath: basePath, SyncID: baseSyncID}}
			var partialBytes int64
			for p := 0; p < partialCount; p++ {
				path := filepath.Join(fixDir, fmt.Sprintf("partial-r%02d-%03d.c1z", ratioPct, p))
				// #nosec G703 - fixture paths derive from a developer-provided env var.
				if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
					buildCrossoverPartial(t, ctx, path, p, perPartial)
				}
				partialBytes += fileSizeOrZero(path)
				entries = append(entries, &CompactableSync{FilePath: path, SyncID: manifestSyncID(t, path)})
			}
			t.Logf("ratio=%d%%: grants/partial=%d partial_bytes=%.1fMB base_bytes=%.1fMB byte_ratio=%.1f%%",
				ratioPct, perPartial,
				float64(partialBytes)/(1<<20), float64(baseBytes)/(1<<20),
				100*float64(partialBytes)/float64(baseBytes))

			expectedGrants := int64(baseGrants + partialCount*200)
			for _, mode := range []string{"overlay", "fold"} {
				t.Run(mode, func(t *testing.T) {
					t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", mode)
					c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
						WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble), WithSkipGrantExpansion())
					require.NoError(t, err)
					defer func() { require.NoError(t, cleanup()) }()

					start := time.Now()
					out, err := c.Compact(ctx)
					elapsed := time.Since(start)
					require.NoError(t, err)
					require.NotNil(t, out)
					t.Logf("ratio=%d%% mode=%s elapsed=%s", ratioPct, mode, elapsed.Round(time.Millisecond))
					logFileSize(t, "output", out.FilePath)

					// Both modes must converge on the same union: base
					// grants plus the partial-only additions (overrides
					// replace, they don't add).
					w, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
					require.NoError(t, err)
					defer w.Close(ctx)
					eng, ok := enginepkg.AsEngine(w)
					require.True(t, ok)
					stats, err := enginepkg.ReadSyncStatsRecord(ctx, eng, out.SyncID)
					require.NoError(t, err)
					require.NotNil(t, stats)
					require.Equal(t, expectedGrants, stats.GetGrants())
				})
			}
		})
	}
}

// buildCrossoverPartial writes one partial sync sized for the
// crossover sweep: overrideGrants grant overrides into a disjoint
// window of the base's id space (window idx*overrideGrants) plus 200
// partial-only grants, with a small supporting catalog.
func buildCrossoverPartial(t *testing.T, ctx context.Context, path string, idx, overrideGrants int) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithPayloadEncoding(dotc1z.PayloadEncodingIndexedZstd),
		dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	_, err = w.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(t, w.PutResourceTypes(ctx, userRT, groupRT))

	resources := make([]*v2.Resource, 0, 1001)
	for i := 0; i < 1000; i++ {
		resources = append(resources, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: scaleID("u", i)}.Build(),
		}.Build())
	}
	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: scaleID("grp", 0)}.Build(),
	}.Build()
	resources = append(resources, group)
	require.NoError(t, w.PutResources(ctx, resources...))

	entObjs := make([]*v2.Entitlement, 0, 100)
	for i := 0; i < 100; i++ {
		entObjs = append(entObjs, v2.Entitlement_builder{
			Id:       scaleID("e", i),
			Resource: group,
			Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build())
	}
	require.NoError(t, w.PutEntitlements(ctx, entObjs...))

	mkGrant := func(id string, userIdx, entIdx int) *v2.Grant {
		user := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: scaleID("u", userIdx)}.Build(),
		}.Build()
		ent := v2.Entitlement_builder{
			Id:       scaleID("e", entIdx),
			Resource: group,
		}.Build()
		return v2.Grant_builder{Id: id, Principal: user, Entitlement: ent}.Build()
	}
	const batchSize = 10_000
	grantObjs := make([]*v2.Grant, 0, batchSize)
	flush := func() {
		if len(grantObjs) > 0 {
			require.NoError(t, w.PutGrants(ctx, grantObjs...))
			grantObjs = grantObjs[:0]
		}
	}
	// Overrides into a disjoint window of the base's grant id space.
	for i := 0; i < overrideGrants; i++ {
		grantObjs = append(grantObjs, mkGrant(scaleID("g", idx*overrideGrants+i), i%1000, i%100))
		if len(grantObjs) == batchSize {
			flush()
		}
	}
	// Partial-only additions.
	for i := 0; i < 200; i++ {
		grantObjs = append(grantObjs, mkGrant(scaleID(fmt.Sprintf("x%03d-g", idx), i), i%1000, i%100))
	}
	flush()

	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
}
