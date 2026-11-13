package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
)

type nonComparableProvider struct {
	noop.MeterProvider
	m map[string]int
}

func TestNewDefaultHandlerNonComparableProvider(t *testing.T) {
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	otel.SetMeterProvider(nonComparableProvider{m: map[string]int{}})
	require.NotPanics(t, func() {
		NewDefaultHandler(t.Context())
		NewDefaultHandler(t.Context())
	})
}
