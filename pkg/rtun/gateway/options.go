package gateway

import (
	sdkmetrics "github.com/conductorone/baton-sdk/pkg/metrics"
)

type Option func(*options)

type options struct {
	metrics sdkmetrics.Handler
}

// WithMetricsHandler injects a metrics handler for the gateway service.
func WithMetricsHandler(h sdkmetrics.Handler) Option {
	return func(o *options) { o.metrics = h }
}
