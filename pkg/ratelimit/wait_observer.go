package ratelimit

import (
	"context"
	"time"
)

// WaitEvent describes one completed wait: a rate-limit gate sleep or a retry
// backoff sleep. It is a struct so new fields can be added later without
// breaking WaitObserver implementations. Attribution (e.g. resource type)
// travels on the context via WithWaitLabel rather than in the event.
type WaitEvent struct {
	// Duration is the time actually slept. When the sleep is cut short by
	// context cancellation this is the elapsed portion only, never the
	// planned wait, so cut-short sleeps don't inflate wait stats.
	Duration time.Duration

	// Retry is true for plain retry backoff sleeps (transient errors with no
	// rate-limit evidence). The ZERO VALUE means rate-limited: emitters that
	// predate this field (rate-limit gates, the hosted connector manager)
	// only ever reported rate-limit sleeps, so their events keep landing in
	// the rate_limit_wait bucket without a code change.
	Retry bool
}

// WaitObserver is notified each time a rate-limit gate or retryer slept before
// (re)issuing a request. Callers that account for wait time (e.g. the syncer's
// sync stats) install one via WithWaitObserver; sleep sites report via
// ObserveWait.
type WaitObserver func(ctx context.Context, ev WaitEvent)

type waitObserverKey struct{}

// WithWaitObserver returns a context that carries fn. Any rate-limit gate or
// retryer that sleeps while handling a request made with this context (or one
// derived from it) reports the actual slept duration to fn after the sleep
// completes.
func WithWaitObserver(ctx context.Context, fn WaitObserver) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, waitObserverKey{}, fn)
}

// ObserveWait reports a completed wait to the observer carried by ctx, if any.
// Sleep sites must call this after the sleep completes with the time actually
// slept — on context cancellation report only the elapsed portion, never the
// planned duration.
func ObserveWait(ctx context.Context, ev WaitEvent) {
	if fn, ok := ctx.Value(waitObserverKey{}).(WaitObserver); ok {
		fn(ctx, ev)
	}
}

type waitLabelKey struct{}

// WithWaitLabel attaches a bounded attribution label (e.g. a resource type) to
// waits reported from this context. Lives here (not pkg/retry) because both
// the rate-limit gates and the retryer report through this package's observer;
// pkg/retry re-exports it for compatibility.
func WithWaitLabel(ctx context.Context, label string) context.Context {
	if label == "" {
		return ctx
	}
	return context.WithValue(ctx, waitLabelKey{}, label)
}

// WaitLabelFromContext returns the wait attribution label, if present.
func WaitLabelFromContext(ctx context.Context) (string, bool) {
	label, ok := ctx.Value(waitLabelKey{}).(string)
	return label, ok && label != ""
}
