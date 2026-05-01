package connectorbuilder

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type closeHookCtxCloser struct {
	gotCtx context.Context
	err    error
}

func (c *closeHookCtxCloser) Close(ctx context.Context) error {
	c.gotCtx = ctx
	return c.err
}

type closeHookLegacyCloser struct {
	called bool
	err    error
}

func (c *closeHookLegacyCloser) Close() error {
	c.called = true
	return c.err
}

type closeHookLegacyCloserTwo struct {
	closeHookLegacyCloser
}

// capturingCore is a minimal zapcore.Core that records entries so tests can
// assert on warn output without depending on zaptest/observer (not vendored).
type capturingCore struct {
	mu      sync.Mutex
	entries []zapcore.Entry
}

func (c *capturingCore) Enabled(zapcore.Level) bool { return true }
func (c *capturingCore) With([]zapcore.Field) zapcore.Core {
	return c
}
func (c *capturingCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(ent, c)
}
func (c *capturingCore) Write(ent zapcore.Entry, _ []zapcore.Field) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, ent)
	return nil
}
func (c *capturingCore) Sync() error { return nil }

func (c *capturingCore) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.entries))
	for i, e := range c.entries {
		out[i] = e.Message
	}
	return out
}

func (c *capturingCore) countWithSubstring(substr string) int {
	count := 0
	for _, msg := range c.messages() {
		if strings.Contains(msg, substr) {
			count++
		}
	}
	return count
}

func withCapturedZap(t *testing.T) *capturingCore {
	t.Helper()

	core := &capturingCore{}
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)
	return core
}

func resetLegacyCloseWarned(t *testing.T) {
	t.Helper()

	legacyCloseWarned = sync.Map{}
	t.Cleanup(func() {
		legacyCloseWarned = sync.Map{}
	})
}

func TestCloseHookForReturnsNilForNil(t *testing.T) {
	if hook := closeHookFor(nil); hook != nil {
		t.Fatalf("closeHookFor(nil) = %v, want nil", hook)
	}
}

func TestCloseHookForReturnsNilWhenNeitherInterfaceImplemented(t *testing.T) {
	if hook := closeHookFor(struct{}{}); hook != nil {
		t.Fatalf("closeHookFor(struct{}{}) = %v, want nil", hook)
	}
}

func TestCloseHookForPrefersContextAwareClose(t *testing.T) {
	core := withCapturedZap(t)
	resetLegacyCloseWarned(t)

	closer := &closeHookCtxCloser{}
	hook := closeHookFor(closer)
	if hook == nil {
		t.Fatal("closeHookFor returned nil for ConnectorCloser implementation")
	}

	ctx := context.WithValue(context.Background(), struct{ name string }{"k"}, "v")
	if err := hook(ctx); err != nil {
		t.Fatalf("hook: %v", err)
	}
	if closer.gotCtx != ctx {
		t.Fatal("expected ctx to be forwarded to Close(ctx)")
	}
	if got := core.countWithSubstring("deprecated Close"); got != 0 {
		t.Fatalf("unexpected deprecation warning for ConnectorCloser: %+v", core.messages())
	}
}

func TestCloseHookForLegacyCloseWarnsOncePerType(t *testing.T) {
	core := withCapturedZap(t)
	resetLegacyCloseWarned(t)

	closerA1 := &closeHookLegacyCloser{}
	closerA2 := &closeHookLegacyCloser{}
	closerB := &closeHookLegacyCloserTwo{}

	for _, c := range []any{closerA1, closerA2, closerB} {
		hook := closeHookFor(c)
		if hook == nil {
			t.Fatalf("closeHookFor(%T) returned nil for legacy Close() implementation", c)
		}
		if err := hook(context.Background()); err != nil {
			t.Fatalf("hook: %v", err)
		}
	}

	if !closerA1.called || !closerA2.called || !closerB.called {
		t.Fatal("expected legacy Close() to be invoked on all three closers")
	}

	if got := core.countWithSubstring("deprecated Close"); got != 2 {
		t.Fatalf("got %d warnings, want 2 (one per concrete type): %+v", got, core.messages())
	}
}

func TestCloseHookForPropagatesLegacyError(t *testing.T) {
	withCapturedZap(t)
	resetLegacyCloseWarned(t)

	want := errors.New("boom")
	closer := &closeHookLegacyCloser{err: want}

	hook := closeHookFor(closer)
	if hook == nil {
		t.Fatal("closeHookFor returned nil")
	}
	if got := hook(context.Background()); !errors.Is(got, want) {
		t.Fatalf("hook error = %v, want %v", got, want)
	}
}
