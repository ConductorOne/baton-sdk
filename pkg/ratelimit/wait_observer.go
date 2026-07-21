package ratelimit

import (
	"context"
	"time"
)

// WaitObserver is notified each time a rate-limit gate sleeps before (re)issuing
// a request. Callers that account for rate-limit wait (e.g. the syncer's sync
// stats) install one via WithWaitObserver; sleep sites report via ObserveWait.
type WaitObserver func(ctx context.Context, wait time.Duration)

type waitObserverKey struct{}

// WithWaitObserver returns a context that carries fn. Any rate-limit gate that
// sleeps while handling a request made with this context (or one derived from
// it) reports the wait duration to fn before sleeping.
func WithWaitObserver(ctx context.Context, fn WaitObserver) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, waitObserverKey{}, fn)
}

// ObserveWait reports a rate-limit wait to the observer carried by ctx, if any.
// Sleep sites should call this immediately before sleeping.
func ObserveWait(ctx context.Context, wait time.Duration) {
	if fn, ok := ctx.Value(waitObserverKey{}).(WaitObserver); ok {
		fn(ctx, wait)
	}
}
