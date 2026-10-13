package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// DefaultHandlerName is the instrumentation scope name used by NewDefaultHandler.
const DefaultHandlerName = "baton-sdk"

// NewDefaultHandler returns the Handler that SDK components use when the
// caller did not supply one. It follows the tracing model: SDK packages call
// otel.Tracer on the global TracerProvider, which is a no-op until
// uotel.InitOtel installs a real provider (CLI and Lambda alike, whenever an
// OTLP collector endpoint is configured). The returned handler is bound the
// same way to the global MeterProvider: it records nothing until a real
// MeterProvider is installed, and the global provider forwards instruments
// created before that, so components built before uotel.InitOtel export too.
//
// All callers share one underlying handler per MeterProvider, so observable
// gauges register a single callback per name no matter how many syncers or
// clients are created over the life of the process. WithTags on the returned
// handler yields a new, independent handler and never mutates the shared one.
func NewDefaultHandler(ctx context.Context) Handler {
	mp := otel.GetMeterProvider()

	defaultHandlerMtx.Lock()
	defer defaultHandlerMtx.Unlock()
	if defaultHandlerProvider != mp || defaultHandlerInstance == nil {
		defaultHandlerProvider = mp
		defaultHandlerInstance = &sharedHandler{
			Handler:  NewOtelHandler(ctx, mp, DefaultHandlerName),
			provider: mp,
		}
	}
	return defaultHandlerInstance
}

var (
	defaultHandlerMtx      sync.Mutex
	defaultHandlerProvider otelmetric.MeterProvider
	defaultHandlerInstance *sharedHandler
)

// sharedHandler is the process-wide default handler. Its WithTags returns a
// fresh handler because otelHandler.WithTags mutates the receiver.
type sharedHandler struct {
	Handler
	provider otelmetric.MeterProvider
}

func (s *sharedHandler) WithTags(tags map[string]string) Handler {
	return NewOtelHandler(context.Background(), s.provider, DefaultHandlerName).WithTags(tags)
}
