package server

import (
	"errors"

	sdkmetrics "github.com/conductorone/baton-sdk/pkg/metrics"
)

var (
	ErrNotImplemented = errors.New("rtun/server: not implemented")
	ErrProtocol       = errors.New("rtun/server: protocol error")
	ErrHelloTimeout   = errors.New("rtun/server: hello timeout")
)

type Option func(*options)

type options struct {
	metrics sdkmetrics.Handler
}

// WithMetricsHandler injects a metrics handler for server components (handler/registry).
func WithMetricsHandler(h sdkmetrics.Handler) Option {
	return func(o *options) { o.metrics = h }
}
