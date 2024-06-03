package metrics

import (
	"context"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

type otelHandler struct {
	name     string
	meter    otelmetric.Meter
	provider otelmetric.MeterProvider

	int64CountersMtx sync.Mutex
	int64Counters    map[string]otelmetric.Int64Counter
	int64HistosMtx   sync.Mutex
	int64Histos      map[string]otelmetric.Int64Histogram
	int64GaugesMtx   sync.Mutex
	int64Gauges      map[string]Int64Gauge
}

type otelInt64Histogram func(ctx context.Context, incr int64, options ...otelmetric.RecordOption)

func (f otelInt64Histogram) Record(ctx context.Context, value int64, tags map[string]string) {
	attrs := makeAttrs(tags)

	f(ctx, value, otelmetric.WithAttributes(attrs...))
}

var _ Int64Histogram = (otelInt64Histogram)(nil)

type otelInt64Counter func(ctx context.Context, incr int64, options ...otelmetric.AddOption)

func (f otelInt64Counter) Add(ctx context.Context, value int64, tags map[string]string) {
	attrs := makeAttrs(tags)
	f(ctx, value, otelmetric.WithAttributes(attrs...))
}

var _ Int64Counter = (otelInt64Counter)(nil)

type syncInt64Gauge struct {
	value int64
	attrs []otelmetric.ObserveOption
	gauge otelmetric.Int64ObservableGauge
}

func (s *syncInt64Gauge) Observe(_ context.Context, value int64, tags map[string]string) {
	attrs := makeAttrs(tags)
	s.attrs = append(s.attrs, otelmetric.WithAttributes(attrs...))
	s.value = value
}

func newSyncInt64Gauge(meter otelmetric.Meter, name string, description string, unit Unit) *syncInt64Gauge {
	g, err := meter.Int64ObservableGauge(name, otelmetric.WithDescription(description), otelmetric.WithUnit(string(unit)))
	if err != nil {
		panic(err)
	}

	return &syncInt64Gauge{gauge: g}
}

var _ Int64Gauge = (*syncInt64Gauge)(nil)

func (h *otelHandler) Int64Histogram(name string, description string, unit Unit) Int64Histogram {
	h.int64HistosMtx.Lock()
	defer h.int64HistosMtx.Unlock()

	name = strings.ToLower(name)

	c, ok := h.int64Histos[name]
	var err error
	if !ok {
		c, err = h.meter.Int64Histogram(name, otelmetric.WithDescription(description), otelmetric.WithUnit(string(unit)))
		if err != nil {
			panic(err)
		}
		h.int64Histos[name] = c
	}

	return otelInt64Histogram(c.Record)
}

func (h *otelHandler) Int64Counter(name string, description string, unit Unit) Int64Counter {
	h.int64CountersMtx.Lock()
	defer h.int64CountersMtx.Unlock()

	name = strings.ToLower(name)

	c, ok := h.int64Counters[name]
	var err error
	if !ok {
		c, err = h.meter.Int64Counter(name, otelmetric.WithDescription(description), otelmetric.WithUnit(string(unit)))
		if err != nil {
			panic(err)
		}
		h.int64Counters[name] = c
	}

	return otelInt64Counter(c.Add)
}

func (h *otelHandler) Int64Gauge(name string, description string, unit Unit) Int64Gauge {
	h.int64GaugesMtx.Lock()
	defer h.int64GaugesMtx.Unlock()

	name = strings.ToLower(name)

	if c, ok := h.int64Gauges[name]; ok {
		return c
	}

	newGauge := newSyncInt64Gauge(h.meter, name, description, unit)

	_, err := h.meter.RegisterCallback(func(ctx context.Context, observer otelmetric.Observer) error {
		observer.ObserveInt64(newGauge.gauge, newGauge.value, newGauge.attrs...)
		return nil
	}, newGauge.gauge)

	if err != nil {
		panic(err)
	}

	h.int64Gauges[name] = newGauge

	return newGauge
}

func (h *otelHandler) WithTags(tags map[string]string) Handler {
	attrs := makeAttrs(tags)

	h.meter = h.provider.Meter(h.name, otelmetric.WithInstrumentationAttributes(attrs...))

	return h
}

func makeAttrs(tags map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, len(tags))
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	return attrs
}

func NewOtelHandler(_ context.Context, provider otelmetric.MeterProvider, name string) Handler {
	return &otelHandler{
		name:          name,
		meter:         provider.Meter(name),
		provider:      provider,
		int64Counters: make(map[string]otelmetric.Int64Counter),
		int64Histos:   make(map[string]otelmetric.Int64Histogram),
		int64Gauges:   make(map[string]Int64Gauge),
	}
}

var _ Handler = (*otelHandler)(nil)
