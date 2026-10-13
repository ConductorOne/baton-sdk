package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestPebbleStoreEmitsC1ZMetrics checks that the Pebble engine records the
// same c1z metrics as the SQLite writer: c1z_grant_writes_total (slim=false)
// for grants committed through a page or PutGrants, and c1z_size_bytes
// (phase=finalize) when Close saves the c1z.
//
// The package-level meter is bound to the first global MeterProvider set in
// this test binary, and other tests may write grants concurrently, so values
// are checked as lower bounds.
func TestPebbleStoreEmitsC1ZMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

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

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	var writes, size int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "c1z_grant_writes_total":
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				for _, dp := range sum.DataPoints {
					v, _ := dp.Attributes.Value(attribute.Key("slim"))
					require.False(t, v.AsBool())
					writes += dp.Value
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
	require.GreaterOrEqual(t, writes, int64(3))
	require.Positive(t, size)
}
