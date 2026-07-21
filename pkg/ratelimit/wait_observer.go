package ratelimit

import (
	"context"
	"time"
)

// WaitEvent describes one completed rate-limit sleep. It is a struct so new
// fields can be added later without breaking WaitObserver implementations.
// Attribution (e.g. resource type) travels on the context via
// retry.WithWaitLabel rather than in the event.
type WaitEvent struct {
	// Duration is the time actually slept. When the sleep is cut short by
	// context cancellation this is the elapsed portion only, never the
	// planned wait, so cut-short sleeps don't inflate wait stats.
	Duration time.Duration
}

// WaitObserver is notified each time a rate-limit gate slept before (re)issuing
// a request. Callers that account for rate-limit wait (e.g. the syncer's sync
// stats) install one via WithWaitObserver; sleep sites report via ObserveWait.
type WaitObserver func(ctx context.Context, ev WaitEvent)

type waitObserverKey struct{}

// WithWaitObserver returns a context that carries fn. Any rate-limit gate that
// sleeps while handling a request made with this context (or one derived from
// it) reports the actual slept duration to fn after the sleep completes.
func WithWaitObserver(ctx context.Context, fn WaitObserver) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, waitObserverKey{}, fn)
}

// ObserveWait reports a rate-limit wait to the observer carried by ctx, if any.
// Sleep sites must call this after the sleep completes with the time actually
// slept — on context cancellation report only the elapsed portion, never the
// planned duration.
func ObserveWait(ctx context.Context, ev WaitEvent) {
	if fn, ok := ctx.Value(waitObserverKey{}).(WaitObserver); ok {
		fn(ctx, ev)
	}
}
