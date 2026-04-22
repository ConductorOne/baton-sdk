package progresslog

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

	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// capturingCore is a minimal zapcore.Core that records entries in-memory so
// tests can assert on fields without depending on zaptest/observer (not vendored).
type capturingCore struct {
	zapcore.LevelEnabler
	mu      sync.Mutex
	entries []capturedEntry
}

type capturedEntry struct {
	Message string
	Fields  map[string]interface{}
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

func observedLogger(t *testing.T) (context.Context, *capturingCore) {
	t.Helper()
	core := newCapturingCore()
	logger := zap.New(core)
	return ctxzap.ToContext(context.Background(), logger), core
}

// sizeProbe implements connectorstore.DBSizeProvider with a controllable
// current size and optional error, for tests that exercise the expand-log
// size branch.
type sizeProbe struct {
	size int64
	err  error
}

func (s *sizeProbe) CurrentDBSizeBytes() (int64, error) { return s.size, s.err }

// TestLogExpandProgress_SizeFieldsAcrossCalls mirrors the production call
// shape for the Eli Lilly scenario: `LogExpandProgress` is invoked once per
// sync step (expandGrantsForEntitlements), each step freshly recreating the
// Expander. Only the ProgressLog persists across steps. This test pins the
// contract that:
//
//  1. The first eligible call emits a log with decompressed_bytes + delta_bytes.
//  2. Calls within the rate-limit window are suppressed.
//  3. After the window elapses, the next call reports delta relative to the
//     prior sample, not the full size (catches the regression where per-step
//     state reset would make delta == full size every time).
func TestLogExpandProgress_SizeFieldsAcrossCalls(t *testing.T) {
	ctx, logs := observedLogger(t)

	probe := &sizeProbe{size: 100_000_000}
	p := NewProgressCounts(ctx,
		WithLogFrequency(25*time.Millisecond),
		WithDBSizeProvider(probe),
	)

	// First call: must emit; delta == full size (baseline was 0).
	p.LogExpandProgress(ctx, nil)
	entries := logs.filterMessage("Expanding grants")
	require.Len(t, entries, 1, "first call should emit")
	require.EqualValues(t, int64(100_000_000), entries[0].Fields["decompressed_bytes"])
	require.EqualValues(t, int64(100_000_000), entries[0].Fields["delta_bytes"])

	// Immediate second call: suppressed by rate-limit.
	probe.size = 130_000_000
	p.LogExpandProgress(ctx, nil)
	require.Len(t, logs.filterMessage("Expanding grants"), 1,
		"second call within rate-limit window must be suppressed")

	// Wait past the window, call again: must emit; delta == growth since last log.
	time.Sleep(40 * time.Millisecond)
	p.LogExpandProgress(ctx, nil)
	entries = logs.filterMessage("Expanding grants")
	require.Len(t, entries, 2, "third call after rate-limit window must emit")
	require.EqualValues(t, int64(130_000_000), entries[1].Fields["decompressed_bytes"])
	require.EqualValues(t, int64(30_000_000), entries[1].Fields["delta_bytes"],
		"delta must be relative to previous log, not to zero — catches the per-step reset bug")
}

// TestLogExpandProgress_NoSizeProviderOmitsFields verifies the pre-existing
// behavior is preserved when no size provider is attached: the log still
// fires and includes actions_remaining, but no size fields appear.
func TestLogExpandProgress_NoSizeProviderOmitsFields(t *testing.T) {
	ctx, logs := observedLogger(t)
	p := NewProgressCounts(ctx, WithLogFrequency(1*time.Millisecond))

	p.LogExpandProgress(ctx, []*expand.EntitlementGraphAction{{}, {}, {}})
	entries := logs.filterMessage("Expanding grants")
	require.Len(t, entries, 1)
	require.EqualValues(t, int64(3), entries[0].Fields["actions_remaining"])
	require.NotContains(t, entries[0].Fields, "decompressed_bytes")
	require.NotContains(t, entries[0].Fields, "delta_bytes")
}

// TestLogExpandProgress_SizeErrorSkipsSizeFields verifies that a stat error
// on the size provider doesn't break the log line — we still record the
// actions_remaining portion, just without the size fields. A monitoring
// signal that disappears entirely on transient stat errors is worse than
// one that occasionally loses a minor field.
func TestLogExpandProgress_SizeErrorSkipsSizeFields(t *testing.T) {
	ctx, logs := observedLogger(t)

	probe := &sizeProbe{err: errors.New("stat failed")}
	p := NewProgressCounts(ctx,
		WithLogFrequency(1*time.Millisecond),
		WithDBSizeProvider(probe),
	)

	p.LogExpandProgress(ctx, nil)
	entries := logs.filterMessage("Expanding grants")
	require.Len(t, entries, 1)
	require.NotContains(t, entries[0].Fields, "decompressed_bytes")
	require.NotContains(t, entries[0].Fields, "delta_bytes")
	require.Contains(t, entries[0].Fields, "actions_remaining")
}

// TestLogExpandProgress_PerStepExpanderShape simulates the actual production
// call pattern from syncer.expandGrantsForEntitlements: each step
// constructs a new Expander and calls LogExpandProgress once. State lives
// on the shared ProgressLog, not the Expander. Without this structure
// (the original PR #779 held state on the Expander), no log would ever fire
// since the per-step elapsed is always ~nanoseconds.
func TestLogExpandProgress_PerStepExpanderShape(t *testing.T) {
	ctx, logs := observedLogger(t)

	probe := &sizeProbe{size: 1_000_000}
	p := NewProgressCounts(ctx,
		WithLogFrequency(15*time.Millisecond),
		WithDBSizeProvider(probe),
	)

	// Simulate ~50 steps in a ~60 ms window. Each step creates a new Expander
	// (we don't need to actually use it — the point is that the ProgressLog
	// survives all of them).
	totalSteps := 50
	for i := 0; i < totalSteps; i++ {
		_ = expand.NewExpander(nil, nil) // throwaway, matches per-step construction
		probe.size += 500_000            // simulate growth between steps
		p.LogExpandProgress(ctx, nil)
		time.Sleep(1500 * time.Microsecond)
	}

	entries := logs.filterMessage("Expanding grants")
	require.GreaterOrEqual(t, len(entries), 2,
		"at least two logs must emit across 50 steps in a ~75ms window with 15ms rate-limit")
	require.Less(t, len(entries), totalSteps,
		"logs must be rate-limited, not one-per-step")

	// The second log's delta must be growth since the first, not full size —
	// proves state persisted across the simulated per-step calls.
	require.Greater(t, entries[1].Fields["delta_bytes"], int64(0))
	require.Less(t, entries[1].Fields["delta_bytes"], entries[1].Fields["decompressed_bytes"],
		"delta must be < full size, proving the ProgressLog kept its prior sample")
}
