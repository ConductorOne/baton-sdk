package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

var (
	_ Handler        = (*otelHandler)(nil)
	_ Int64Counter   = (*otelInt64Counter)(nil)
	_ Int64Gauge     = (*otelInt64Gauge)(nil)
	_ Int64Histogram = (*otelInt64Histogram)(nil)
)

// otelHandler is a view over an otelCore with its own default attributes.
// WithTags returns a new view over the same core, so instruments are created
// (and gauge callbacks registered) once per name however many views exist,
// and each view records with its own tags.
type otelHandler struct {
	core *otelCore
	// defaultAttrs is never modified after the view is created.
	defaultAttrs []attribute.KeyValue
}

// otelCore holds the instruments shared by every view of a handler.
type otelCore struct {
	meter otelmetric.Meter

	countersMtx sync.Mutex
	counters    map[string]otelmetric.Int64Counter
	histosMtx   sync.Mutex
	histos      map[string]otelmetric.Int64Histogram
	gaugesMtx   sync.Mutex
	gauges      map[string]*gaugeState
}

// gaugeState keeps the last observed value per attribute set of one gauge;
// a single callback reports all of them at each collection.
type gaugeState struct {
	gauge otelmetric.Int64ObservableGauge

	mtx    sync.Mutex
	values map[attribute.Distinct]gaugePoint
}

type gaugePoint struct {
	set   attribute.Set
	value int64
}

// attributes returns the per-call tags followed by the view's defaults; on a
// duplicate key the default wins.
func (h *otelHandler) attributes(tags map[string]string) []attribute.KeyValue {
	attrs := makeAttrs(tags)
	return append(attrs, h.defaultAttrs...)
}

type otelInt64Counter struct {
	h       *otelHandler
	counter otelmetric.Int64Counter
}

func (c *otelInt64Counter) Add(ctx context.Context, value int64, tags map[string]string) {
	c.counter.Add(ctx, value, otelmetric.WithAttributes(c.h.attributes(tags)...))
}

type otelInt64Histogram struct {
	h     *otelHandler
	histo otelmetric.Int64Histogram
}

func (r *otelInt64Histogram) Record(ctx context.Context, value int64, tags map[string]string) {
	r.histo.Record(ctx, value, otelmetric.WithAttributes(r.h.attributes(tags)...))
}

type otelInt64Gauge struct {
	h     *otelHandler
	state *gaugeState
}

// Observe sets the gauge's value for this attribute set (tags plus the view's
// defaults). Different attribute sets are reported side by side; for the same
// set the last value observed wins, and it keeps being reported until it is
// observed again.
func (g *otelInt64Gauge) Observe(_ context.Context, value int64, tags map[string]string) {
	set := attribute.NewSet(g.h.attributes(tags)...)
	g.state.mtx.Lock()
	g.state.values[set.Equivalent()] = gaugePoint{set: set, value: value}
	g.state.mtx.Unlock()
}

func (s *gaugeState) observe(_ context.Context, observer otelmetric.Observer) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, p := range s.values {
		observer.ObserveInt64(s.gauge, p.value, otelmetric.WithAttributeSet(p.set))
	}
	return nil
}

func (h *otelHandler) Int64Histogram(name string, description string, unit Unit) Int64Histogram {
	c := h.core
	name = strings.ToLower(name)

	c.histosMtx.Lock()
	defer c.histosMtx.Unlock()
	histo, ok := c.histos[name]
	if !ok {
		var err error
		histo, err = c.meter.Int64Histogram(name, otelmetric.WithDescription(description), otelmetric.WithUnit(string(unit)))
		if err != nil {
			panic(err)
		}
		c.histos[name] = histo
	}
	return &otelInt64Histogram{h: h, histo: histo}
}

func (h *otelHandler) Int64Counter(name string, description string, unit Unit) Int64Counter {
	c := h.core
	name = strings.ToLower(name)

	c.countersMtx.Lock()
	defer c.countersMtx.Unlock()
	counter, ok := c.counters[name]
	if !ok {
		var err error
		counter, err = c.meter.Int64Counter(name, otelmetric.WithDescription(description), otelmetric.WithUnit(string(unit)))
		if err != nil {
			panic(err)
		}
		c.counters[name] = counter
	}
	return &otelInt64Counter{h: h, counter: counter}
}

func (h *otelHandler) Int64Gauge(name string, description string, unit Unit) Int64Gauge {
	c := h.core
	name = strings.ToLower(name)

	c.gaugesMtx.Lock()
	defer c.gaugesMtx.Unlock()
	state, ok := c.gauges[name]
	if !ok {
		gauge, err := c.meter.Int64ObservableGauge(name, otelmetric.WithDescription(description), otelmetric.WithUnit(string(unit)))
		if err != nil {
			panic(err)
		}
		state = &gaugeState{gauge: gauge, values: make(map[attribute.Distinct]gaugePoint)}
		if _, err := c.meter.RegisterCallback(state.observe, gauge); err != nil {
			panic(err)
		}
		c.gauges[name] = state
	}
	return &otelInt64Gauge{h: h, state: state}
}

// WithTags returns a view of this handler whose default attributes are tags
// (replacing, not merging with, the receiver's). The receiver is unchanged;
// both views share instruments.
func (h *otelHandler) WithTags(tags map[string]string) Handler {
	return &otelHandler{core: h.core, defaultAttrs: makeAttrs(tags)}
}

func makeAttrs(tags map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	return attrs
}

func NewOtelHandler(_ context.Context, provider otelmetric.MeterProvider, name string) Handler {
	return &otelHandler{core: &otelCore{
		meter:    provider.Meter(name),
		counters: make(map[string]otelmetric.Int64Counter),
		histos:   make(map[string]otelmetric.Int64Histogram),
		gauges:   make(map[string]*gaugeState),
	}}
}
