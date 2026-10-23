package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newManualHandler(t *testing.T) (Handler, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return NewOtelHandler(t.Context(), mp, "views-test"), reader
}

// points returns metric name -> "tenant" attribute -> value (sums and gauges).
func points(t *testing.T, reader *sdkmetric.ManualReader) map[string]map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	out := map[string]map[string]int64{}
	add := func(name string, attrs attribute.Set, v int64) {
		tenant, _ := attrs.Value("tenant")
		if out[name] == nil {
			out[name] = map[string]int64{}
		}
		_, dup := out[name][tenant.AsString()]
		require.False(t, dup, "%s reported twice for tenant %q", name, tenant.AsString())
		out[name][tenant.AsString()] = v
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					add(m.Name, dp.Attributes, dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					add(m.Name, dp.Attributes, dp.Value)
				}
			}
		}
	}
	return out
}

// TestWithTagsViewsRecordOwnTags: two views of one handler share instruments
// but each records with its own tags, and neither changes the base handler.
func TestWithTagsViewsRecordOwnTags(t *testing.T) {
	ctx := t.Context()
	h, reader := newManualHandler(t)
	a := h.WithTags(map[string]string{"tenant": "a"})
	b := h.WithTags(map[string]string{"tenant": "b"})

	// Interleave lookups so a lookup through one view cannot leak its tags
	// into the other's recordings.
	ca := a.Int64Counter("requests", "", Dimensionless)
	cb := b.Int64Counter("requests", "", Dimensionless)
	ca.Add(ctx, 1, nil)
	cb.Add(ctx, 2, nil)
	h.Int64Counter("requests", "", Dimensionless).Add(ctx, 4, nil)

	a.Int64Gauge("queue", "", Dimensionless).Observe(ctx, 10, nil)
	b.Int64Gauge("queue", "", Dimensionless).Observe(ctx, 20, nil)
	// Same attribute set: the last value wins.
	a.Int64Gauge("queue", "", Dimensionless).Observe(ctx, 11, nil)

	got := points(t, reader)
	require.Equal(t, map[string]int64{"a": 1, "b": 2, "": 4}, got["requests"])
	require.Equal(t, map[string]int64{"a": 11, "b": 20}, got["queue"])
}

// TestNewOtelHandlerWithTagsReturnValue covers the external usage
// NewOtelHandler(...).WithTags(map) with the returned handler.
func TestNewOtelHandlerWithTagsReturnValue(t *testing.T) {
	h, reader := newManualHandler(t)
	tagged := h.WithTags(map[string]string{"tenant": "t1"})
	tagged.Int64Counter("calls", "", Dimensionless).Add(t.Context(), 3, nil)
	require.Equal(t, map[string]int64{"t1": 3}, points(t, reader)["calls"])
}
