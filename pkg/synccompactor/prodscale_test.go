package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestProdScaleSkewedCompaction is the GB-scale experiment for the
// production skewed shape: one large base full sync plus many small
// partials, compacted with each strategy (kway, overlay, fold). It is
// gated behind BATON_PROD_SCALE_TEST=1 and caches its fixtures under
// BATON_PROD_SCALE_DIR (default .profiles/prodscale) because building
// the base takes minutes.
//
// Run with:
//
//	BATON_PROD_SCALE_TEST=1 go test ./pkg/synccompactor -run TestProdScaleSkewedCompaction -v -timeout 60m
func TestProdScaleSkewedCompaction(t *testing.T) {
	if os.Getenv("BATON_PROD_SCALE_TEST") == "" {
		t.Skip("set BATON_PROD_SCALE_TEST=1 to run the prod-scale experiment")
	}
	ctx := context.Background()
	require.NoError(t, ensurePebbleRegistered())

	fixDir := os.Getenv("BATON_PROD_SCALE_DIR")
	if fixDir == "" {
		fixDir = filepath.Join("..", "..", ".profiles", "prodscale")
	}
	require.NoError(t, os.MkdirAll(fixDir, 0o755)) // #nosec G703 - developer-provided fixture dir env var.

	grants := envInt("BATON_PROD_SCALE_GRANTS", 5_000_000)
	users := envInt("BATON_PROD_SCALE_USERS", 200_000)
	ents := envInt("BATON_PROD_SCALE_ENTS", 40_000)
	partialCount := envInt("BATON_PROD_SCALE_PARTIALS", 50)

	basePath := filepath.Join(fixDir, "base.c1z")
	// #nosec G703 - fixture paths derive from a developer-provided env var.
	if _, err := os.Stat(basePath); errors.Is(err, os.ErrNotExist) {
		t.Logf("building base c1z: grants=%d users=%d ents=%d", grants, users, ents)
		start := time.Now()
		buildProdScaleBase(t, ctx, basePath, grants, users, ents)
		t.Logf("base built in %s", time.Since(start).Round(time.Second))
	}
	logFileSize(t, "base", basePath)

	partialPaths := make([]string, 0, partialCount)
	for p := 0; p < partialCount; p++ {
		path := filepath.Join(fixDir, fmt.Sprintf("partial-%03d.c1z", p))
		// #nosec G703 - fixture paths derive from a developer-provided env var.
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			buildProdScalePartial(t, ctx, path, p)
		}
		partialPaths = append(partialPaths, path)
	}

	baseSyncID := manifestSyncID(t, basePath)
	entries := []*CompactableSync{{FilePath: basePath, SyncID: baseSyncID}}
	for _, p := range partialPaths {
		entries = append(entries, &CompactableSync{FilePath: p, SyncID: manifestSyncID(t, p)})
	}

	baseTs := grantDiscoveredAt(t, ctx, basePath, baseSyncID, scaleID("g", 0))
	expectedGrants := int64(grants + partialCount*200)

	for _, mode := range []string{"kway", "overlay", "fold"} {
		name := mode
		t.Run(name, func(t *testing.T) {
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
			t.Logf("mode=%s elapsed=%s", name, elapsed.Round(time.Millisecond))
			logFileSize(t, "output", out.FilePath)

			if mode == "fold" {
				require.NotEqual(t, baseSyncID, out.SyncID, "fold must mint a fresh sync id")
			}

			// Sentinel correctness: a partial-only grant exists, an
			// overridden grant carries the partial's (newer) stamp,
			// and the stats sidecar shows the expected union.
			w, err := dotc1z.NewStore(ctx, out.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			defer w.Close(ctx)
			eng, ok := enginepkg.AsEngine(w)
			require.True(t, ok)
			_, err = eng.GetGrantRecord(ctx, scaleID("p000-g", 0))
			require.NoError(t, err, "partial-only grant missing from output")
			overridden, err := eng.GetGrantRecord(ctx, scaleID("g", 0))
			require.NoError(t, err)
			require.True(t, overridden.GetDiscoveredAt().AsTime().After(baseTs),
				"overridden grant must carry the partial's newer discovered_at")
			stats, err := enginepkg.ReadSyncStatsRecord(ctx, eng, out.SyncID)
			require.NoError(t, err)
			require.NotNil(t, stats)
			require.Equal(t, expectedGrants, stats.GetGrants(),
				"output grants must be base + partial-only additions")
		})
	}
}

// scaleID builds a deterministic high-entropy id. Real-world ids
// (UUIDs, ARNs, KSUIDs) don't compress 30:1 the way sequential
// patterned test ids do; the fnv-derived hex suffix keeps the
// fixture's zstd ratio realistic while staying reproducible so cached
// fixtures and override windows line up across runs.
func scaleID(kind string, i int) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s|%d", kind, i)
	return fmt.Sprintf("%s-%08d-%016x", kind, i, h.Sum64())
}

func envInt(name string, def int) int {
	if raw := os.Getenv(name); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func logFileSize(t *testing.T, label, path string) {
	t.Helper()
	info, err := os.Stat(path) // #nosec G703 - test fixture path.
	require.NoError(t, err)
	t.Logf("%s: %s (%.1f MB)", label, path, float64(info.Size())/(1<<20))
}

// manifestSyncID reads the latest finished compactable sync id from a
// v3 envelope's manifest projection.
func manifestSyncID(t *testing.T, path string) string {
	t.Helper()
	sel, ok := selectSourceSyncFromManifest(path)
	require.True(t, ok, "no manifest sync projection in %s", path)
	return sel.syncID
}

// buildProdScaleBase writes one large full sync: a fixed user/group
// catalog, `ents` entitlements spread across groups, and `grants`
// grants spread across users × entitlements.
func buildProdScaleBase(t *testing.T, ctx context.Context, path string, grants, users, ents int) {
	t.Helper()
	buildProdScaleBaseStore(t, ctx, path, grants, users, ents,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithPayloadEncoding(dotc1z.PayloadEncodingIndexedZstd),
	)
}

// buildProdScaleBaseStore is buildProdScaleBase with caller-chosen store
// options, so the same generator can produce sqlite (v1) fixtures for the
// conversion experiments.
func buildProdScaleBaseStore(t *testing.T, ctx context.Context, path string, grants, users, ents int, storeOpts ...dotc1z.C1ZOption) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path,
		append(storeOpts, dotc1z.WithTmpDir(t.TempDir()))...)
	require.NoError(t, err)
	_, err = w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(t, w.PutResourceTypes(ctx, userRT, groupRT))

	groups := users / 10
	if groups < 1 {
		groups = 1
	}
	const batchSize = 10_000
	batch := make([]*v2.Resource, 0, batchSize)
	flushResources := func() {
		if len(batch) > 0 {
			require.NoError(t, w.PutResources(ctx, batch...))
			batch = batch[:0]
		}
	}
	for i := 0; i < users; i++ {
		batch = append(batch, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: scaleID("u", i)}.Build(),
		}.Build())
		if len(batch) == batchSize {
			flushResources()
		}
	}
	for i := 0; i < groups; i++ {
		batch = append(batch, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: scaleID("grp", i)}.Build(),
		}.Build())
		if len(batch) == batchSize {
			flushResources()
		}
	}
	flushResources()

	entBatch := make([]*v2.Entitlement, 0, batchSize)
	for i := 0; i < ents; i++ {
		group := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: scaleID("grp", i%groups)}.Build(),
		}.Build()
		entBatch = append(entBatch, v2.Entitlement_builder{
			Id:       scaleID("e", i),
			Resource: group,
			Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build())
		if len(entBatch) == batchSize {
			require.NoError(t, w.PutEntitlements(ctx, entBatch...))
			entBatch = entBatch[:0]
		}
	}
	if len(entBatch) > 0 {
		require.NoError(t, w.PutEntitlements(ctx, entBatch...))
	}

	grantBatch := make([]*v2.Grant, 0, batchSize)
	for i := 0; i < grants; i++ {
		user := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: scaleID("u", i%users)}.Build(),
		}.Build()
		ent := v2.Entitlement_builder{
			Id: scaleID("e", i%ents),
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: scaleID("grp", (i%ents)%groups)}.Build(),
			}.Build(),
		}.Build()
		grantBatch = append(grantBatch, v2.Grant_builder{
			Id:          scaleID("g", i),
			Principal:   user,
			Entitlement: ent,
		}.Build())
		if len(grantBatch) == batchSize {
			require.NoError(t, w.PutGrants(ctx, grantBatch...))
			grantBatch = grantBatch[:0]
		}
	}
	if len(grantBatch) > 0 {
		require.NoError(t, w.PutGrants(ctx, grantBatch...))
	}

	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
}

// buildProdScalePartial writes one small partial sync: 1000 grant
// overrides into the base's id space (g-<window>) plus 200 new
// partial-only grants, with the supporting principals/entitlements.
func buildProdScalePartial(t *testing.T, ctx context.Context, path string, idx int) {
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

	grantObjs := make([]*v2.Grant, 0, 1200)
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
	// Overrides into the base's grant id space.
	for i := 0; i < 1000; i++ {
		grantObjs = append(grantObjs, mkGrant(scaleID("g", idx*1000+i), i, i%100))
	}
	// Partial-only additions.
	for i := 0; i < 200; i++ {
		grantObjs = append(grantObjs, mkGrant(scaleID(fmt.Sprintf("p%03d-g", idx), i), i%1000, i%100))
	}
	require.NoError(t, w.PutGrants(ctx, grantObjs...))

	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))
}
