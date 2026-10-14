package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"

	"go.opentelemetry.io/otel"
)

// DefaultHandlerName is the instrumentation scope name used by NewDefaultHandler.
const DefaultHandlerName = "baton-sdk"

// defaultHandler is bound at package init, before any caller can install a
// MeterProvider, so it holds the otel global delegate.
var defaultHandler = NewOtelHandler(context.Background(), otel.GetMeterProvider(), DefaultHandlerName)

// NewDefaultHandler returns the handler SDK components use when none is
// supplied. There is one per process, bound to the global OTel MeterProvider
// the way SDK spans use the global TracerProvider, so it records to the first
// MeterProvider installed with otel.SetMeterProvider (uotel.InitOtel or the
// host) and to no later one.
func NewDefaultHandler(_ context.Context) Handler {
	return defaultHandler
}
