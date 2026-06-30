package expand

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
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

	// Engine-level write-path instrumentation (diagnostic): attributes the
	// expansion write cost to Get / marshal / index-delete / primary-commit /
	// index-commit and breaks index volume out per family. Reachable because
	// the Pebble store satisfies connectorstore.Writer.
	var eng *enginepkg.Engine
	var metricsBefore string
	if w, ok := store.(connectorstore.Writer); ok {
		if e, ok := enginepkg.AsEngine(w); ok {
			eng = e
			eng.EnableExpandWriteStats()
			metricsBefore = eng.DB().Metrics().String()
		}
	}

	expandStart := time.Now()
	expander := NewExpander(benchmarkExpanderStore{store: store}, graph)
	require.NoError(t, expander.RunTopologicalMergeProjection(ctx))
	elapsed := time.Since(expandStart)
	t.Logf("projection expansion finished in %s", elapsed)

	if eng != nil {
		if s, ok := eng.ExpandWriteStats(); ok {
			writeDur := s.GetDur + s.MarshalDur + s.DeleteIdxDur + s.PriCommitDur + s.IdxCommitDur
			t.Logf("write-path time split (sum across %d calls, %d records):", s.Calls, s.Records)
			t.Logf("  rbw-get      = %-12s (read-before-write; count=%d notfound=%d)", s.GetDur, s.GetCount, s.GetNotFound)
			t.Logf("  marshal      = %s", s.MarshalDur)
			t.Logf("  delete-index = %s", s.DeleteIdxDur)
			t.Logf("  pri-commit   = %-12s (main blob: %d keys, %d val bytes)", s.PriCommitDur, s.Records, s.PriValBytes)
			t.Logf("  idx-commit   = %s", s.IdxCommitDur)
			t.Logf("  write total  = %-12s (%.1f%% of %s expansion)",
				writeDur, 100*float64(writeDur)/float64(elapsed), elapsed)
			t.Logf("read-path materialization get (by_entitlement primary Get per index row):")
			t.Logf("  read-get     = %-12s (count=%d notfound=%d, %.1f%% of %s expansion)",
				s.ReadGetDur, s.ReadGetCount, s.ReadGetNotFound, 100*float64(s.ReadGetDur)/float64(elapsed), elapsed)
			t.Logf("  write+read get accounted = %.1f%% of expansion (remainder is merge compute, iteration, projection build/ingest)",
				100*float64(writeDur+s.ReadGetDur)/float64(elapsed))
			t.Logf("index family volume (keys / bytes):")
			t.Logf("  by_entitlement          = %d / %d", s.ByEntitlementKeys, s.ByEntitlementBytes)
			t.Logf("  by_entitlement_resource = %d / %d", s.ByEntitlementResourceKeys, s.ByEntitlementResourceBytes)
			t.Logf("  by_principal            = %d / %d", s.ByPrincipalKeys, s.ByPrincipalBytes)
			t.Logf("  by_principal_rtype      = %d / %d", s.ByPrincipalResourceTypeKeys, s.ByPrincipalResourceTypeBytes)
			t.Logf("  needs_expansion         = %d / %d", s.NeedsExpansionKeys, s.NeedsExpansionBytes)
			totalIdxKeys := s.ByEntitlementKeys + s.ByEntitlementResourceKeys + s.ByPrincipalKeys + s.ByPrincipalResourceTypeKeys + s.NeedsExpansionKeys
			totalIdxBytes := s.ByEntitlementBytes + s.ByEntitlementResourceBytes + s.ByPrincipalBytes + s.ByPrincipalResourceTypeBytes + s.NeedsExpansionBytes
			t.Logf("  TOTAL index             = %d keys / %d bytes (vs %d primary keys / %d primary bytes)",
				totalIdxKeys, totalIdxBytes, s.Records, s.PriValBytes)
		}
		t.Logf("pebble metrics BEFORE expansion:\n%s", metricsBefore)
		t.Logf("pebble metrics AFTER expansion (compaction/flush/WAL cumulative since open):\n%s", eng.DB().Metrics().String())
	}

	if m := graph.ExpansionMetrics; m != nil {
		t.Logf("metrics: algorithm=%s dirty_grants_written=%d synthesized=%d base_update=%d projection_rows=%d nodes_reduced=%d dest_entitlements=%d",
			m.Algorithm, m.DirtyGrantsWritten, m.SynthesizedGrants, m.BaseUpdateGrants, m.ProjectionRowsBuilt, m.NodesReduced, m.DestinationEntitlements)
		if total := m.SynthesizedGrants + m.BaseUpdateGrants; total > 0 {
			t.Logf("base-update fraction: %.4f (%d / %d) — sizes the proto-rewrite floor for a sorted index-build fast path",
				float64(m.BaseUpdateGrants)/float64(total), m.BaseUpdateGrants, total)
		}
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
