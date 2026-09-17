package c1zstore

import "context"

// Under atomic pages every write a page makes goes through its PageWriter.
// A direct store write from inside a page is durable on its own, ahead of
// the page's ledger row; the hook observes those so a test can refuse the
// unregistered ones.

type openPageKey struct{}
type pageBypassKey struct{}

func WithOpenPage(ctx context.Context) context.Context {
	return context.WithValue(ctx, openPageKey{}, true)
}

func PageOpen(ctx context.Context) bool {
	v, _ := ctx.Value(openPageKey{}).(bool)
	return v
}

// reason says why the write is safe ahead of the page's ledger row.
func WithPageWriteBypass(ctx context.Context, reason string) context.Context {
	return context.WithValue(ctx, pageBypassKey{}, reason)
}

func PageWriteBypass(ctx context.Context) (string, bool) {
	r, ok := ctx.Value(pageBypassKey{}).(string)
	return r, ok && r != ""
}

// Bypass is "" for an unregistered write.
type WriteHookEvent struct {
	Method string
	Bypass string
}

// A non-nil error fails the write.
type WriteHook func(ctx context.Context, ev WriteHookEvent) error

type WriteHookStore interface {
	// nil removes it.
	SetWriteHook(hook WriteHook)
}
