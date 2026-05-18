package server

import (
	"errors"

	sdkmetrics "github.com/conductorone/baton-sdk/pkg/metrics"
)

var (
	// ErrNotImplemented is returned when a feature isn't implemented.
	ErrNotImplemented = errors.New("rtun/server: not implemented")
	// ErrProtocol indicates a protocol violation on the stream.
	ErrProtocol = errors.New("rtun/server: protocol error")
	// ErrHelloTimeout indicates the HELLO frame was not received in time.
	ErrHelloTimeout = errors.New("rtun/server: hello timeout")
)

// Option configures server components such as the handler and registry.
type Option func(*options)

type options struct {
	metrics sdkmetrics.Handler
}

// WithMetricsHandler injects a metrics handler for server components (handler/registry).
func WithMetricsHandler(h sdkmetrics.Handler) Option {
	return func(o *options) { o.metrics = h }
}
