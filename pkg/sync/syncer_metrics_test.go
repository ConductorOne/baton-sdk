package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	native_sync "sync"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/stretchr/testify/require"
)

// fakeMetricsHandler records gauge observations so the test can prove the
// caller's handler reaches the ProgressLog the syncer constructs.
type fakeMetricsHandler struct {
	mu     native_sync.Mutex
	gauges map[string][]int64
}

func newFakeMetricsHandler() *fakeMetricsHandler {
	return &fakeMetricsHandler{gauges: make(map[string][]int64)}
}

func (h *fakeMetricsHandler) Int64Counter(_, _ string, _ metrics.Unit) metrics.Int64Counter {
	return noopFakeCounter{}
}

func (h *fakeMetricsHandler) Int64Gauge(name, _ string, _ metrics.Unit) metrics.Int64Gauge {
	return &fakeMetricsGauge{handler: h, name: name}
}

func (h *fakeMetricsHandler) Int64Histogram(_, _ string, _ metrics.Unit) metrics.Int64Histogram {
	return noopFakeHistogram{}
}

func (h *fakeMetricsHandler) WithTags(_ map[string]string) metrics.Handler { return h }

func (h *fakeMetricsHandler) observations(name string) []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]int64, len(h.gauges[name]))
	copy(out, h.gauges[name])
	return out
}

type fakeMetricsGauge struct {
	handler *fakeMetricsHandler
	name    string
}

func (g *fakeMetricsGauge) Observe(_ context.Context, value int64, _ map[string]string) {
	g.handler.mu.Lock()
	defer g.handler.mu.Unlock()
	g.handler.gauges[g.name] = append(g.handler.gauges[g.name], value)
}

type noopFakeCounter struct{}

func (noopFakeCounter) Add(_ context.Context, _ int64, _ map[string]string) {}

type noopFakeHistogram struct{}

func (noopFakeHistogram) Record(_ context.Context, _ int64, _ map[string]string) {}

// TestNewSyncer_WithMetricsHandler_WiresThroughToProgressLog pins the wiring
// added to forward a caller's metrics.Handler into progresslog.NewProgressCounts.
// Without this, a refactor that drops the conditional append inside NewSyncer
// would silently disable every consumer's baton.sync.expand.* instruments —
// the structured logs would still fire, so existing tests would stay green.
func TestNewSyncer_WithMetricsHandler_WiresThroughToProgressLog(t *testing.T) {
	runWithSyncModes(t, func(t *testing.T, extraOpts []SyncOpt) {
		ctx := t.Context()

		fake := newFakeMetricsHandler()

		tempDir := t.TempDir()
		c1zpath := filepath.Join(tempDir, "wiring.c1z")
		opts := append([]SyncOpt{
			WithC1ZPath(c1zpath),
			WithTmpDir(tempDir),
			WithMetricsHandler(fake),
		}, extraOpts...)

		syncerIface, err := NewSyncer(ctx, nil, opts...)
		require.NoError(t, err)

		s, ok := syncerIface.(*syncer)
		require.True(t, ok, "NewSyncer returns *syncer")
		require.NotNil(t, s.counts, "ProgressLog should be constructed")

		actions := make([]*expand.EntitlementGraphAction, 7)
		s.counts.LogExpandProgress(ctx, actions)

		require.Equal(t, []int64{7}, fake.observations("baton.sync.expand.actions_remaining"),
			"caller's metrics.Handler did not receive the actions_remaining gauge observation — "+
				"WithMetricsHandler wiring in NewSyncer is broken")
	})
}

// TestNewSyncer_NoMetricsHandler_DoesNotPanic pins the default path: callers
// that omit WithMetricsHandler should get a working ProgressLog backed by the
// no-op handler. Mirrors progresslog's own NoMetricsHandlerIsSafe coverage
// at the syncer wiring layer.
func TestNewSyncer_NoMetricsHandler_DoesNotPanic(t *testing.T) {
	ctx := t.Context()

	tempDir := t.TempDir()
	c1zpath := filepath.Join(tempDir, "wiring.c1z")
	syncerIface, err := NewSyncer(ctx, nil, WithC1ZPath(c1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)

	s, ok := syncerIface.(*syncer)
	require.True(t, ok)
	require.NotNil(t, s.counts)

	require.NotPanics(t, func() {
		s.counts.LogExpandProgress(ctx, make([]*expand.EntitlementGraphAction, 3))
	})
}
