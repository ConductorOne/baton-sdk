package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestSharedOtelHandlerConcurrentUse runs instrument lookups and recordings
// from many goroutines on one shared handler, as uhttp clients, the
// connectorbuilder and progresslog do with NewDefaultHandler. Run with -race.
func TestSharedOtelHandlerConcurrentUse(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	h := &sharedHandler{Handler: NewOtelHandler(t.Context(), mp, DefaultHandlerName), provider: mp}
	ctx := t.Context()

	const goroutines, iterations = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				h.Int64Counter("race_counter", "", Dimensionless).Add(ctx, 1, map[string]string{"k": "v"})
				h.Int64Histogram("race_histo", "", Milliseconds).Record(ctx, int64(i), nil)
				h.Int64Gauge("race_gauge", "", Dimensionless).Observe(ctx, int64(i), nil)
				// Tagged handlers derived from the shared one must not
				// disturb it either.
				h.WithTags(map[string]string{"t": "x"}).Int64Counter("race_counter", "", Dimensionless).Add(ctx, 1, nil)
			}
		}()
	}
	// Collect concurrently so gauge callbacks race with Observe.
	for i := 0; i < 20; i++ {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(ctx, &rm))
	}
	wg.Wait()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "race_counter" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				total += dp.Value
			}
		}
	}
	require.Equal(t, int64(2*goroutines*iterations), total)
}
