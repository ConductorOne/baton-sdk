package metrics

import (
	"context"
)

type Handler interface {
	Int64Counter(name string, description string, unit Unit) Int64Counter
	Int64Gauge(name string, description string, unit Unit) Int64Gauge
	Int64Histogram(name string, description string, unit Unit) Int64Histogram
	// RegisterInt64ObservableGauge registers an asynchronous gauge that will be observed
	// during collection by invoking the provided callback. The callback should return
	// the current value and optional tags at observation time.
	RegisterInt64ObservableGauge(name string, description string, unit Unit, callback func(ctx context.Context) (int64, map[string]string))
	WithTags(tags map[string]string) Handler
}

type Int64Counter interface {
	Add(ctx context.Context, value int64, tags map[string]string)
}

type Int64Histogram interface {
	Record(ctx context.Context, value int64, tags map[string]string)
}

type Int64Gauge interface {
	Observe(ctx context.Context, value int64, tags map[string]string)
}

type Unit string

const (
	Dimensionless Unit = "1"
	Bytes         Unit = "By"
	Milliseconds  Unit = "ms"
)
