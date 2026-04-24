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
// shape: LogExpandProgress is invoked once per sync step, each step freshly
// recreating the Expander. Only the ProgressLog persists across steps. Pins
// three contracts:
//
//  1. First eligible emission includes decompressed_bytes but OMITS
//     decompressed_bytes_delta — a delta on sample 1 would equal full size
//     and spuriously spike any Datadog graph.
//  2. Calls within the rate-limit window are suppressed.
//  3. A call after the window reports decompressed_bytes_delta relative to
//     the prior sample (regression guard: per-step state reset would make
//     delta == full size every time).
func TestLogExpandProgress_SizeFieldsAcrossCalls(t *testing.T) {
	ctx, logs := observedLogger(t)

	probe := &sizeProbe{size: 100_000_000}
	p := NewProgressCounts(ctx,
		WithLogFrequency(10*time.Millisecond),
		WithDBSizeProvider(probe),
	)

	// First call: emits, size present, delta field omitted.
	p.LogExpandProgress(ctx, nil)
	entries := logs.filterMessage("Expanding grants")
	require.Len(t, entries, 1, "first call should emit")
	require.EqualValues(t, int64(100_000_000), entries[0].Fields["decompressed_bytes"])
	require.NotContains(t, entries[0].Fields, "decompressed_bytes_delta",
		"delta field must be omitted on first sample to avoid a spurious baseline-vs-zero spike")

	// Immediate second call: suppressed by rate-limit window.
	probe.size = 130_000_000
	p.LogExpandProgress(ctx, nil)
	require.Len(t, logs.filterMessage("Expanding grants"), 1,
		"second call within rate-limit window must be suppressed")

	// Wait well past the window (100ms vs 10ms rate — 10x margin keeps this
	// stable even on coarse-clock Windows runners). Next call emits with
	// delta relative to the prior sample, not full size.
	time.Sleep(100 * time.Millisecond)
	p.LogExpandProgress(ctx, nil)
	entries = logs.filterMessage("Expanding grants")
	require.Len(t, entries, 2, "third call after rate-limit window must emit")
	require.EqualValues(t, int64(130_000_000), entries[1].Fields["decompressed_bytes"])
	require.EqualValues(t, int64(30_000_000), entries[1].Fields["decompressed_bytes_delta"],
		"delta must be relative to previous log, not zero — catches the per-step reset bug")
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
	require.NotContains(t, entries[0].Fields, "decompressed_bytes_delta")
}

// TestLogExpandProgress_NilProviderDoesNotPanic verifies that explicitly
// passing nil to WithDBSizeProvider (equivalent to a failed typed-nil type
// assertion on the syncer side) is safe and produces a log without size
// fields. Guards the edge case called out on the PR review.
func TestLogExpandProgress_NilProviderDoesNotPanic(t *testing.T) {
	ctx, logs := observedLogger(t)
	p := NewProgressCounts(ctx,
		WithLogFrequency(1*time.Millisecond),
		WithDBSizeProvider(nil),
	)

	require.NotPanics(t, func() { p.LogExpandProgress(ctx, nil) })
	entries := logs.filterMessage("Expanding grants")
	require.Len(t, entries, 1)
	require.NotContains(t, entries[0].Fields, "decompressed_bytes")
	require.NotContains(t, entries[0].Fields, "decompressed_bytes_delta")
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
	require.NotContains(t, entries[0].Fields, "decompressed_bytes_delta")
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

	// Simulate ~50 steps across a ~100ms window. 2ms inter-step sleep keeps
	// the loop well outside sub-ms clock noise on Windows while still
	// producing enough samples to exercise rate-limiting.
	totalSteps := 50
	for i := 0; i < totalSteps; i++ {
		_ = expand.NewExpander(nil, nil) // throwaway, matches per-step construction
		probe.size += 500_000            // simulate growth between steps
		p.LogExpandProgress(ctx, nil)
		time.Sleep(2 * time.Millisecond)
	}

	entries := logs.filterMessage("Expanding grants")
	require.GreaterOrEqual(t, len(entries), 2,
		"at least two logs must emit across 50 steps in a ~100ms window with 15ms rate-limit")
	require.Less(t, len(entries), totalSteps,
		"logs must be rate-limited, not one-per-step")

	// Second log's delta must be growth since the first, not full size —
	// proves state persisted across the simulated per-step calls.
	delta, ok := entries[1].Fields["decompressed_bytes_delta"].(int64)
	require.True(t, ok, "delta field must be present on the second sample")
	size, ok := entries[1].Fields["decompressed_bytes"].(int64)
	require.True(t, ok)
	require.Greater(t, delta, int64(0))
	require.Less(t, delta, size,
		"delta must be < full size, proving the ProgressLog kept its prior sample")
}
