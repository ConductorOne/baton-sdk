package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// DefaultHandlerName is the instrumentation scope name used by NewDefaultHandler.
const DefaultHandlerName = "baton-sdk"

// exportingMeterProvider matches MeterProviders that actually export (the
// otel SDK MeterProvider and similar). The otel global no-op/delegating
// provider installed before otel.SetMeterProvider does not implement it.
type exportingMeterProvider interface {
	ForceFlush(ctx context.Context) error
}

// NewDefaultHandler returns the Handler that SDK components use when the
// caller did not supply one. When an exporting MeterProvider has been
// installed globally (uotel.InitOtel does this whenever an OTLP collector
// endpoint is configured), it returns an OpenTelemetry-backed Handler bound
// to that provider; otherwise it returns a no-op Handler, so nothing is
// allocated or exported when telemetry is not configured.
//
// The decision is made when the handler is created, so components must be
// constructed after uotel.InitOtel for their metrics to be exported.
//
// All callers share one underlying handler per MeterProvider, so observable
// gauges register a single callback per name no matter how many syncers or
// clients are created over the life of the process. WithTags on the returned
// handler yields a new, independent handler and never mutates the shared one.
func NewDefaultHandler(ctx context.Context) Handler {
	mp := otel.GetMeterProvider()
	if _, ok := mp.(exportingMeterProvider); !ok {
		return NewNoOpHandler(ctx)
	}

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
