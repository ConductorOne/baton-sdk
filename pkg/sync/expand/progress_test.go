package expand

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// sizeAwareMockStore embeds MockExpanderStore and additionally implements
// the DBSizeProvider capability interface, so the Expander can include
// decompressed_bytes / delta_bytes in the progress log.
type sizeAwareMockStore struct {
	*MockExpanderStore
	size int64
	err  error
}

func (s *sizeAwareMockStore) CurrentDBSizeBytes() (int64, error) {
	return s.size, s.err
}

// capturedEntry is one log record captured by the capturingCore. Holds the
// Message and a fully-materialised field map so tests can assert on both.
type capturedEntry struct {
	Message string
	Fields  map[string]interface{}
}

// capturingCore is a minimal zapcore.Core that stores every entry in memory
// for test assertions. We don't depend on zaptest/observer because it isn't
// vendored.
type capturingCore struct {
	zapcore.LevelEnabler
	mu      sync.Mutex
	entries []capturedEntry
}

func newCapturingCore() *capturingCore {
	return &capturingCore{LevelEnabler: zapcore.InfoLevel}
}

func (c *capturingCore) With([]zapcore.Field) zapcore.Core { return c }

func (c *capturingCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *capturingCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	c.mu.Lock()
	c.entries = append(c.entries, capturedEntry{Message: ent.Message, Fields: enc.Fields})
	c.mu.Unlock()
	return nil
}

func (c *capturingCore) Sync() error { return nil }

func (c *capturingCore) filterMessage(msg string) []capturedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]capturedEntry, 0, len(c.entries))
	for _, e := range c.entries {
		if e.Message == msg {
			out = append(out, e)
		}
	}
	return out
}

// observedLogger returns a context carrying a zap.Logger whose entries are
// captured by the returned core. Lets us assert on the structured fields
// the Expander writes.
func observedLogger(t *testing.T) (context.Context, *capturingCore) {
	t.Helper()
	core := newCapturingCore()
	logger := zap.New(core)
	return ctxzap.ToContext(context.Background(), logger), core
}

func TestMaybeLogProgress_HonorsInterval(t *testing.T) {
	ctx, logs := observedLogger(t)

	e := NewExpander(NewMockExpanderStore(), NewEntitlementGraph(ctx))
	// 50ms gives us a window we can deterministically wait past without
	// making the test too slow.
	e.progressInterval = 50 * time.Millisecond

	// First call: no time has passed since construction, so no log.
	e.maybeLogProgress(ctx)
	require.Empty(t, logs.filterMessage("expander: progress"),
		"first call should not log when interval has not elapsed")

	// Wait past the interval; next call logs exactly one line.
	time.Sleep(75 * time.Millisecond)
	e.maybeLogProgress(ctx)
	require.Len(t, logs.filterMessage("expander: progress"), 1,
		"should emit one progress log after interval elapses")

	// Immediate subsequent call is still within the new window; no new log.
	e.maybeLogProgress(ctx)
	require.Len(t, logs.filterMessage("expander: progress"), 1,
		"no second log until the next interval boundary")
}

func TestMaybeLogProgress_IncludesDBSizeWhenProvided(t *testing.T) {
	ctx, logs := observedLogger(t)

	store := &sizeAwareMockStore{
		MockExpanderStore: NewMockExpanderStore(),
		size:              1_000_000,
	}
	e := NewExpander(store, NewEntitlementGraph(ctx))
	e.progressInterval = 1 * time.Nanosecond // always fires

	e.actionsProcessed = 7
	e.maybeLogProgress(ctx)

	entries := logs.filterMessage("expander: progress")
	require.Len(t, entries, 1)
	m := entries[0].Fields

	require.EqualValues(t, int64(1_000_000), m["decompressed_bytes"])
	require.EqualValues(t, int64(1_000_000), m["delta_bytes"],
		"first log's delta is size - 0")
	require.EqualValues(t, int64(7), m["actions_processed"])
	require.Contains(t, m, "actions_pending")
	require.Contains(t, m, "depth")
	require.Contains(t, m, "elapsed")

	// Second sample with larger size: delta reflects growth since last log.
	store.size = 1_250_000
	time.Sleep(1 * time.Millisecond) // just in case the Nanosecond shortcut is too tight
	e.maybeLogProgress(ctx)
	entries = logs.filterMessage("expander: progress")
	require.Len(t, entries, 2)
	m2 := entries[1].Fields
	require.EqualValues(t, int64(1_250_000), m2["decompressed_bytes"])
	require.EqualValues(t, int64(250_000), m2["delta_bytes"])
}

func TestMaybeLogProgress_OmitsDBSizeFieldsWhenUnsupported(t *testing.T) {
	ctx, logs := observedLogger(t)

	// Plain MockExpanderStore does NOT implement DBSizeProvider.
	e := NewExpander(NewMockExpanderStore(), NewEntitlementGraph(ctx))
	e.progressInterval = 1 * time.Nanosecond

	e.maybeLogProgress(ctx)

	entries := logs.filterMessage("expander: progress")
	require.Len(t, entries, 1)
	m := entries[0].Fields
	require.NotContains(t, m, "decompressed_bytes",
		"stores without DBSizeProvider must not get a size field")
	require.NotContains(t, m, "delta_bytes")
	require.Contains(t, m, "actions_processed")
	require.Contains(t, m, "depth")
}

func TestMaybeLogProgress_SkipsOnStatError(t *testing.T) {
	ctx, logs := observedLogger(t)

	store := &sizeAwareMockStore{
		MockExpanderStore: NewMockExpanderStore(),
		err:               errors.New("stat failed"),
	}
	e := NewExpander(store, NewEntitlementGraph(ctx))
	e.progressInterval = 1 * time.Nanosecond

	e.maybeLogProgress(ctx)

	// Log still fires, but without size fields — a stat error during
	// expansion shouldn't suppress the rest of the progress signal.
	entries := logs.filterMessage("expander: progress")
	require.Len(t, entries, 1)
	m := entries[0].Fields
	require.NotContains(t, m, "decompressed_bytes")
	require.NotContains(t, m, "delta_bytes")
	require.Contains(t, m, "actions_processed")
}

func TestMaybeLogProgress_DisabledWhenIntervalZero(t *testing.T) {
	ctx, logs := observedLogger(t)

	e := NewExpander(NewMockExpanderStore(), NewEntitlementGraph(ctx))
	e.progressInterval = 0 // disabled

	// Force the clock forward and confirm nothing is emitted.
	e.lastProgressLog = time.Now().Add(-1 * time.Hour)
	e.maybeLogProgress(ctx)

	require.Empty(t, logs.filterMessage("expander: progress"),
		"interval=0 must disable progress logging")
}

func TestResolveProgressInterval(t *testing.T) {
	t.Setenv("BATON_EXPANSION_PROGRESS_INTERVAL", "")
	require.Equal(t, defaultExpansionProgressInterval, resolveProgressInterval())

	t.Setenv("BATON_EXPANSION_PROGRESS_INTERVAL", "500ms")
	require.Equal(t, 500*time.Millisecond, resolveProgressInterval())

	t.Setenv("BATON_EXPANSION_PROGRESS_INTERVAL", "0")
	require.Equal(t, time.Duration(0), resolveProgressInterval())

	// Invalid values fall back to the default rather than panicking.
	t.Setenv("BATON_EXPANSION_PROGRESS_INTERVAL", "not-a-duration")
	require.Equal(t, defaultExpansionProgressInterval, resolveProgressInterval())
}
