package uhttp

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.uber.org/zap"
)

// maxTransientDrain is how much of a failed 5xx body we read before Close.
// Connection reuse only needs a small drain; an unbounded Copy can stall
// the retry until the client's Timeout (300s) fires.
const maxTransientDrain = 8 << 10

func (t *Transport) roundTripTransientRetries(
	ctx context.Context,
	rt http.RoundTripper,
	req *http.Request,
	resp *http.Response,
	err error,
) (*http.Response, error) {
	timeoutRetries := 0
	for attempt := 1; attempt < t.transientRetry.maxAttempts; attempt++ {
		retryReq, wait, isTimeout, ok := t.nextTransientRetry(req, resp, err, timeoutRetries, attempt)
		if !ok {
			return resp, err
		}
		if isTimeout {
			timeoutRetries++
		}
		if resp != nil {
			drainResponse(resp)
		}
		if err != nil && isStaleConnectionError(err) {
			t.closeIdleConnections()
			if freshRT, cycleErr := t.cycle(ctx); cycleErr == nil {
				rt = freshRT
			}
		}
		if waitErr := waitRetry(ctx, wait); waitErr != nil {
			return nil, waitErr
		}
		if t.log {
			t.l(ctx).Debug("uhttp: retrying request after transient failure",
				zap.String("http.method", req.Method),
				zap.String("http.url_details.host", req.URL.Host),
				zap.String("http.url_details.path", req.URL.Path),
				zap.Int("attempt", attempt+1),
				zap.Error(err),
			)
		}
		resp, err = rt.RoundTrip(retryReq)
		req = retryReq
	}
	return resp, err
}

func (t *Transport) nextTransientRetry(
	req *http.Request,
	resp *http.Response,
	err error,
	timeoutRetries int,
	retryIndex int,
) (*http.Request, time.Duration, bool, bool) {
	if req.Context().Err() != nil {
		return nil, 0, false, false
	}
	if err != nil {
		if !t.transientRetry.replaySafe {
			// Default: retry only requests that provably never reached
			// the origin (dial-phase). A timeout or stale-connection
			// reset means the origin may have processed the request.
			retryReq, ok := retryableRequest(req, err, false)
			return retryReq, 0, false, ok
		}
		if isRetryableTimeout(req, err) {
			if timeoutRetries >= maxTransientTimeoutRetries {
				return nil, 0, false, false
			}
			retryReq, ok := rewindRequest(req)
			return retryReq, 0, true, ok
		}
		retryReq, ok := retryableRequest(req, err, true)
		return retryReq, 0, false, ok
	}
	if resp != nil && resp.StatusCode >= 500 {
		if !t.transientRetry.replaySafe && isGatewayStatus(resp.StatusCode) {
			// 502/504 mean an intermediary lost the response; the origin
			// may have processed the request. Same hazard as a timeout,
			// observed one hop away.
			return nil, 0, false, false
		}
		retryReq, ok := rewindRequest(req)
		if !ok {
			return nil, 0, false, false
		}
		return retryReq, t.transientBackoffWait(retryIndex, resp), false, true
	}
	return nil, 0, false, false
}

func isGatewayStatus(code int) bool {
	return code == http.StatusBadGateway || code == http.StatusGatewayTimeout
}

func (t *Transport) transientBackoffWait(retryIndex int, resp *http.Response) time.Duration {
	wait := t.transientRetry.initialDelay
	for i := 1; i < retryIndex; i++ {
		if wait > t.transientRetry.maxDelay/2 {
			wait = t.transientRetry.maxDelay
			break
		}
		wait *= 2
	}
	if wait > t.transientRetry.maxDelay {
		wait = t.transientRetry.maxDelay
	}
	if resp != nil {
		if ra := parseRetryAfter(resp.Header.Get("Retry-After"), t.now()); ra > 0 {
			wait = ra
			if wait > t.transientRetry.maxDelay {
				wait = t.transientRetry.maxDelay
			}
		}
	}
	return jitterWait(wait, t.transientRetry.maxDelay)
}

// jitterWait spreads retries by ±25% of wait, then clamps to [0, maxDelay].
func jitterWait(wait, maxDelay time.Duration) time.Duration {
	if wait <= 0 {
		return wait
	}
	delta := wait / 4
	if delta > 0 {
		// #nosec G404 -- jitter for retry backoff, not a security value.
		wait += time.Duration(rand.Int64N(int64(2*delta)+1) - int64(delta))
	}
	if wait > maxDelay {
		wait = maxDelay
	}
	if wait < 0 {
		return 0
	}
	return wait
}

func isRetryableTimeout(req *http.Request, err error) bool {
	if err == nil || req.Context().Err() != nil {
		return false
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return isSocketTimeout(err)
}

func drainResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.CopyN(io.Discard, resp.Body, maxTransientDrain)
	_ = resp.Body.Close()
}

func waitRetry(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// parseRetryAfter accepts the HTTP Retry-After forms: an integer number of
// seconds, or an HTTP-date. Invalid values yield 0.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(v); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(v)
	if err != nil {
		return 0
	}
	d := when.Sub(now)
	if d <= 0 {
		return 0
	}
	return d
}
