package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// testMetricReader backs the process-wide MeterProvider installed by TestMain.
// Its counters accumulate across tests, so assertions compare deltas.
var (
	testMetricReader  = sdkmetric.NewManualReader()
	testMeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader))
)

// c1zMetricTotals returns the cumulative c1z_grant_writes_total and the latest
// c1z_size_bytes (phase=finalize) from the shared reader.
func c1zMetricTotals(t *testing.T) (int64, int64) {
	t.Helper()
	var writes, size int64
	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "c1z_grant_writes_total":
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				for _, dp := range sum.DataPoints {
					if v, _ := dp.Attributes.Value(attribute.Key("slim")); !v.AsBool() {
						writes += dp.Value
					}
				}
			case "c1z_size_bytes":
				g, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				for _, dp := range g.DataPoints {
					if v, _ := dp.Attributes.Value(attribute.Key("phase")); v.AsString() == "finalize" {
						size = dp.Value
					}
				}
			}
		}
	}
	return writes, size
}

// TestPebbleStoreEmitsC1ZMetrics checks that the Pebble engine records the
// same c1z metrics as the SQLite writer: c1z_grant_writes_total (slim=false)
// for grants committed through a page or PutGrants, and c1z_size_bytes
// (phase=finalize) when Close saves the c1z.
//
// Other tests may write grants concurrently, so the write delta is a lower
// bound.
func TestPebbleStoreEmitsC1ZMetrics(t *testing.T) {
	writesBefore, _ := c1zMetricTotals(t)

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metrics.c1z")
	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	ledger, ok := store.(c1zstore.PageLedgerStore)
	require.True(t, ok)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, ledger.BeginCollecting(ctx, nil, nil))

	w := ledger.BeginPage()
	require.NoError(t, w.PutGrants(ctx,
		mkV2Grant("g1", "ent", "user", "alice"),
		mkV2Grant("g2", "ent", "user", "bob"),
	))
	id := c1zstore.LedgerActionIdentity{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github"}
	require.NoError(t, w.Commit(ctx, id, nil))
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g3", "ent", "user", "carol")))
	require.NoError(t, store.Close(ctx))

	writesAfter, size := c1zMetricTotals(t)
	require.GreaterOrEqual(t, writesAfter-writesBefore, int64(3))
	require.Positive(t, size)
}
