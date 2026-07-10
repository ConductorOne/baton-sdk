package session

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// Session op names are a fixed, bounded set shared by counters, annotations,
// and the syncer's stats token.
const (
	SessionOpGet     = "get"
	SessionOpGetMany = "get_many"
	SessionOpGetAll  = "get_all"
	SessionOpSet     = "set"
	SessionOpSetMany = "set_many"
	SessionOpDelete  = "delete"
	SessionOpClear   = "clear"
)

// SessionOpStat is one op's cumulative counters within a collection window.
// Timeouts are the deadline-exceeded subset of Errors — the signature of a
// backend whose every request times out before a fallback is MaxMs pinned at
// the deadline with Timeouts ≈ Count.
type SessionOpStat struct {
	Count    int64
	Errors   int64
	Timeouts int64
	TotalMs  int64
	MaxMs    int64
}

// SessionOpObserver receives one observation per session-store operation.
// Implementations must be safe for concurrent use.
type SessionOpObserver func(op string, elapsed time.Duration, err error)

// IsDeadlineExceeded reports whether err represents a timed-out operation,
// wrapped or carried as a gRPC status.
func IsDeadlineExceeded(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return status.Code(err) == codes.DeadlineExceeded
}

// UsageCollector accumulates session-store op stats for one request. It is
// carried in the request context so stats never outlive the request — no
// process-lifetime state, which keeps overlapping syncs sharing one warm
// container from cross-contaminating each other's numbers.
type UsageCollector struct {
	mu    sync.Mutex
	kind  string
	stats map[string]*SessionOpStat
}

type usageCollectorKey struct{}

// WithUsageCollector returns a child context carrying a fresh collector.
func WithUsageCollector(ctx context.Context) (context.Context, *UsageCollector) {
	c := &UsageCollector{stats: make(map[string]*SessionOpStat, 4)}
	return context.WithValue(ctx, usageCollectorKey{}, c), c
}

// UsageCollectorFromContext returns the collector installed by
// WithUsageCollector, or nil.
func UsageCollectorFromContext(ctx context.Context) *UsageCollector {
	c, _ := ctx.Value(usageCollectorKey{}).(*UsageCollector)
	return c
}

func (c *UsageCollector) record(kind, op string, elapsed time.Duration, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.kind == "" && kind != "" {
		c.kind = kind
	}
	stat := c.stats[op]
	if stat == nil {
		stat = &SessionOpStat{}
		c.stats[op] = stat
	}
	elapsedMs := elapsed.Milliseconds()
	stat.Count++
	stat.TotalMs += elapsedMs
	if elapsedMs > stat.MaxMs {
		stat.MaxMs = elapsedMs
	}
	if err != nil {
		stat.Errors++
		if IsDeadlineExceeded(err) {
			stat.Timeouts++
		}
	}
}

// Annotation renders the collected stats as a SessionStoreUsage annotation
// message, or nil when nothing was recorded. Ops are sorted for stable
// output.
func (c *UsageCollector) Annotation() *v2.SessionStoreUsage {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.stats) == 0 {
		return nil
	}
	ops := make([]string, 0, len(c.stats))
	for op := range c.stats {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	out := make([]*v2.SessionStoreUsage_OpStats, 0, len(ops))
	for _, op := range ops {
		stat := c.stats[op]
		out = append(out, v2.SessionStoreUsage_OpStats_builder{
			Op:       op,
			Count:    stat.Count,
			Errors:   stat.Errors,
			Timeouts: stat.Timeouts,
			TotalMs:  stat.TotalMs,
			MaxMs:    stat.MaxMs,
		}.Build())
	}
	return v2.SessionStoreUsage_builder{
		Kind: c.kind,
		Ops:  out,
	}.Build()
}

var _ sessions.SessionStore = (*InstrumentedSessionStore)(nil)

// InstrumentedSessionStore wraps a SessionStore with per-op latency and
// outcome observation, so session-store cost is attributable instead of
// vanishing into inflated connector-call latency. It holds no cumulative
// state: observations go to the request context's UsageCollector (connector
// side, reported back on response annotations) and/or the OnOp callback
// (syncer side, recorded into the sync stats token).
type InstrumentedSessionStore struct {
	backing sessions.SessionStore
	kind    string
	// opPrefix namespaces reported op names so multiple wrappers in one
	// stack (e.g. cache-level above backend-level) stay distinguishable
	// in a single collector.
	opPrefix string
	onOp     SessionOpObserver
}

// NewInstrumentedSessionStore wraps backing. kind is a bounded label naming
// the backend variant (e.g. c1z_pebble, c1z_sqlite, connector_backend);
// empty means this layer does not claim the collector's kind. opPrefix is
// prepended to reported op names ("" for the backend layer, "cache." for a
// cache-level view). onOp may be nil.
func NewInstrumentedSessionStore(backing sessions.SessionStore, kind string, opPrefix string, onOp SessionOpObserver) *InstrumentedSessionStore {
	return &InstrumentedSessionStore{
		backing:  backing,
		kind:     kind,
		opPrefix: opPrefix,
		onOp:     onOp,
	}
}

// Kind returns the backend label this wrapper was constructed with.
func (i *InstrumentedSessionStore) Kind() string {
	return i.kind
}

func (i *InstrumentedSessionStore) observe(ctx context.Context, op string, start time.Time, err error) {
	elapsed := time.Since(start)
	if i.opPrefix != "" {
		op = i.opPrefix + op
	}
	if c := UsageCollectorFromContext(ctx); c != nil {
		c.record(i.kind, op, elapsed, err)
	}
	if i.onOp != nil {
		i.onOp(op, elapsed, err)
	}
}

func (i *InstrumentedSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	start := time.Now()
	value, found, err := i.backing.Get(ctx, key, opt...)
	i.observe(ctx, SessionOpGet, start, err)
	return value, found, err
}

func (i *InstrumentedSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	start := time.Now()
	values, missing, err := i.backing.GetMany(ctx, keys, opt...)
	i.observe(ctx, SessionOpGetMany, start, err)
	return values, missing, err
}

func (i *InstrumentedSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	start := time.Now()
	err := i.backing.Set(ctx, key, value, opt...)
	i.observe(ctx, SessionOpSet, start, err)
	return err
}

func (i *InstrumentedSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	start := time.Now()
	err := i.backing.SetMany(ctx, values, opt...)
	i.observe(ctx, SessionOpSetMany, start, err)
	return err
}

func (i *InstrumentedSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	start := time.Now()
	err := i.backing.Delete(ctx, key, opt...)
	i.observe(ctx, SessionOpDelete, start, err)
	return err
}

func (i *InstrumentedSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	start := time.Now()
	err := i.backing.Clear(ctx, opt...)
	i.observe(ctx, SessionOpClear, start, err)
	return err
}

func (i *InstrumentedSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	start := time.Now()
	values, next, err := i.backing.GetAll(ctx, pageToken, opt...)
	i.observe(ctx, SessionOpGetAll, start, err)
	return values, next, err
}
