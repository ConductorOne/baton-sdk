package dpop_oauth2

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"time"
)

// Defaults chosen so a full retry cycle (attempts plus backoff) fits well
// within the 30 second budget Token() imposes on each call.
const (
	defaultRetryMaxAttempts  = 3
	defaultRetryInitialDelay = 500 * time.Millisecond
	defaultRetryMaxDelay     = 2 * time.Second
)

// RetryConfig controls how Token() retries transient token request failures:
// 5xx or 429 responses, transport-level errors, and timeouts. Every attempt
// re-runs the full token request with a freshly signed DPoP proof and client
// assertion; a proof's jti may be single-use, so an identical request is never
// replayed. Definitive OAuth protocol errors (e.g. invalid_client) are never
// retried.
type RetryConfig struct {
	// MaxAttempts is the total number of attempts, including the first.
	// Values below 1 are treated as 1 (retries disabled).
	MaxAttempts int
	// InitialDelay is the backoff before the first retry. It doubles on each
	// subsequent retry, capped at MaxDelay, with jitter applied.
	InitialDelay time.Duration
	// MaxDelay caps the backoff between attempts.
	MaxDelay time.Duration
}

// DefaultRetryConfig returns the retry behavior used when no WithRetryConfig
// option is supplied.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:  defaultRetryMaxAttempts,
		InitialDelay: defaultRetryInitialDelay,
		MaxDelay:     defaultRetryMaxDelay,
	}
}

func (c RetryConfig) normalized() RetryConfig {
	if c.MaxAttempts < 1 {
		c.MaxAttempts = 1
	}
	if c.InitialDelay <= 0 {
		c.InitialDelay = defaultRetryInitialDelay
	}
	if c.MaxDelay < c.InitialDelay {
		c.MaxDelay = c.InitialDelay
	}
	return c
}

// retryDelay computes the backoff preceding retry number `retry` (1-based):
// exponential doubling capped at MaxDelay, with equal jitter (half the delay
// is fixed, the other half randomized) so concurrent clients hitting the same
// outage don't retry in lockstep.
func (c RetryConfig) retryDelay(retry int) time.Duration {
	delay := c.InitialDelay
	for i := 1; i < retry; i++ {
		delay *= 2
		if delay >= c.MaxDelay {
			delay = c.MaxDelay
			break
		}
	}
	half := delay / 2
	return half + rand.N(half+1)
}

// sleepBeforeRetry blocks for the backoff delay preceding the given retry.
// It returns false if ctx expires first.
func sleepBeforeRetry(ctx context.Context, cfg RetryConfig, retry int) bool {
	timer := time.NewTimer(cfg.retryDelay(retry))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// IsTransient reports whether err is a token request failure that was
// classified as transient: the failure mode gives no indication the
// credential itself is bad, so retrying (with a fresh proof and assertion)
// may succeed.
func IsTransient(err error) bool {
	return errors.Is(err, ErrTokenRequestTransient)
}

// markTransient tags err as a transient token request failure. The result
// matches ErrTokenRequestTransient in addition to everything err already
// matched, and its message is unchanged.
func markTransient(err error) error {
	return &transientError{error: err}
}

type transientError struct{ error }

func (e *transientError) Unwrap() []error {
	return []error{e.error, ErrTokenRequestTransient}
}

// isRetryableStatus reports whether an HTTP response status is worth
// retrying: any 5xx (upstream failure) or 429 (throttling). 4xx OAuth
// protocol rejections are definitive and must not be retried.
func isRetryableStatus(code int) bool {
	return code >= 500 || code == http.StatusTooManyRequests
}
