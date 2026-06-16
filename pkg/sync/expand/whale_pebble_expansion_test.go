package expand

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestRunWhalePebbleProjectionExpansion runs a full, persisted projection
// expansion against a real Pebble whale c1z. It is opt-in via env vars so it
// never runs in normal test sweeps:
//
//	BATON_WHALE_PEBBLE_PATH  seed c1z to copy and expand (required)
//	BATON_WHALE_OUT_PATH     destination c1z for the expanded copy (required)
//
// Unlike BenchmarkExpandWhalePebble, this does NOT discard its work: the
// expanded file at BATON_WHALE_OUT_PATH is left behind for inspection.
func TestRunWhalePebbleProjectionExpansion(t *testing.T) {
	seed := os.Getenv("BATON_WHALE_PEBBLE_PATH")
	out := os.Getenv("BATON_WHALE_OUT_PATH")
	if seed == "" || out == "" {
		t.Skip("set BATON_WHALE_PEBBLE_PATH and BATON_WHALE_OUT_PATH to run")
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	ctx := ctxzap.ToContext(context.Background(), logger)

	t.Logf("copying seed %s -> %s", seed, out)
	copyStart := time.Now()
	data, err := os.ReadFile(seed) // #nosec G304,G703 -- test-controlled env path.
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(out, data, 0600)) // #nosec G306,G703 -- test artifact.
	t.Logf("copied in %s", time.Since(copyStart))

	store, err := dotc1z.NewStore(ctx, out)
	require.NoError(t, err)

	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest, "seed has no full sync")
	_, started, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, latest.ID)
	require.NoError(t, err)
	require.False(t, started, "expected to resume the existing full sync, not start a new one")

	baseGrants := grantCount(ctx, t, store)
	t.Logf("base grants before expansion: %d", baseGrants)

	graph, err := loadEntitlementGraphFromStore(ctx, store)
	require.NoError(t, err)
	require.NotEmpty(t, graph.Edges, "graph has no expansion edges")
	require.NoError(t, graph.FixCycles(ctx))
	t.Logf("graph: %d nodes, %d edges (cycles fixed, has_no_cycles=%t)", len(graph.Nodes), len(graph.Edges), graph.HasNoCycles)

	expandStart := time.Now()
	expander := NewExpander(benchmarkExpanderStore{store: store}, graph)
	require.NoError(t, expander.RunTopologicalMergeProjection(ctx))
	t.Logf("projection expansion finished in %s", time.Since(expandStart))

	if m := graph.ExpansionMetrics; m != nil {
		t.Logf("metrics: algorithm=%s dirty_grants_written=%d projection_rows=%d nodes_reduced=%d dest_entitlements=%d",
			m.Algorithm, m.DirtyGrantsWritten, m.ProjectionRowsBuilt, m.NodesReduced, m.DestinationEntitlements)
	}

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	ro, err := dotc1z.NewStore(ctx, out, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, ro.Close(ctx)) }()
	require.NoError(t, ro.SetCurrentSync(ctx, latest.ID))

	finalGrants := grantCount(ctx, t, ro)
	if fi, err := os.Stat(out); err == nil { // #nosec G703 -- test artifact path.
		t.Logf("expanded c1z size: %d bytes", fi.Size())
	}
	t.Logf("grants: base=%d final=%d delta=%d", baseGrants, finalGrants, finalGrants-baseGrants)
}

// grantCount returns the current sync's grant count from the stats sidecar (a
// single LSM Get). EndSync recomputes that sidecar after expansion, so it
// already reflects the post-expansion total — no need for a second full
// ListGrants scan of tens of millions of rows just to log a count. On the
// pre-expansion (resumed) store it reads the seed sync's sidecar, i.e. the
// base count.
func grantCount(ctx context.Context, t *testing.T, store dotc1z.C1ZStore) int64 {
	t.Helper()
	stats, err := store.SyncMeta().Stats(ctx, connectorstore.SyncTypeAny, "")
	require.NoError(t, err)
	return stats["grants"]
}
