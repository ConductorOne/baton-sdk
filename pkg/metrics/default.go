package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"reflect"
	"sync"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// DefaultHandlerName is the instrumentation scope name used by NewDefaultHandler.
const DefaultHandlerName = "baton-sdk"

// NewDefaultHandler returns the handler SDK components use when none is
// supplied: one handler per process, bound to the global OTel MeterProvider
// the way SDK spans use the global TracerProvider. It records nothing until a
// real MeterProvider is installed (uotel.InitOtel or the host), and handlers
// created before that are forwarded by the otel global delegate.
func NewDefaultHandler(ctx context.Context) Handler {
	mp := otel.GetMeterProvider()

	defaultHandlerMtx.Lock()
	defer defaultHandlerMtx.Unlock()
	if !sameProvider(defaultHandlerProvider, mp) || defaultHandlerInstance == nil {
		defaultHandlerProvider = mp
		defaultHandlerInstance = NewOtelHandler(ctx, mp, DefaultHandlerName)
	}
	return defaultHandlerInstance
}

var (
	defaultHandlerMtx      sync.Mutex
	defaultHandlerProvider otelmetric.MeterProvider
	defaultHandlerInstance Handler
)

func sameProvider(a, b otelmetric.MeterProvider) bool {
	if a == nil || b == nil {
		return false
	}
	if !reflect.TypeOf(a).Comparable() || !reflect.TypeOf(b).Comparable() {
		return false
	}
	return a == b
}
