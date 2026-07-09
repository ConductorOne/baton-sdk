package uhttp

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

const (
	defaultDialTimeout           = 30 * time.Second
	defaultDialKeepAlive         = 30 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultExpectContinueTimeout = 1 * time.Second
	defaultIdleConnTimeout       = 30 * time.Second
	defaultResponseHeaderTimeout = 60 * time.Second
	defaultHTTP2ReadIdleTimeout  = 15 * time.Second
	defaultHTTP2PingTimeout      = 15 * time.Second
	defaultMaxConnectionRetries  = 3
	defaultConnRetryBaseDelay    = 250 * time.Millisecond
	defaultConnRetryMaxDelay     = 2 * time.Second
)

var loggedResponseHeaders = []string{
	// Limit headers
	"X-Ratelimit-Limit",
	"Ratelimit-Limit",
	"X-RateLimit-Requests-Limit", // Linear uses a non-standard header
	"X-Rate-Limit-Limit",         // Okta uses a non-standard header

	// Remaining headers
	"X-Ratelimit-Remaining",
	"Ratelimit-Remaining",
	"X-RateLimit-Requests-Remaining", // Linear uses a non-standard header
	"X-Rate-Limit-Remaining",         // Okta uses a non-standard header

	// Reset headers
	"X-Ratelimit-Reset",
	"Ratelimit-Reset",
	"X-RateLimit-Requests-Reset", // Linear uses a non-standard header
	"X-Rate-Limit-Reset",         // Okta uses a non-standard header
	"Retry-After",                // Often returned with 429
}

// NewTransport creates a new Transport, applies the options, and then cycles the transport.
func NewTransport(ctx context.Context, options ...Option) (*Transport, error) {
	t := newTransport()
	for _, opt := range options {
		opt.Apply(t)
	}
	// CLI/config-provided defaults fill in any value not set via an option.
	// An explicit option always wins over the context value.
	if t.responseHeaderTimeout == nil {
		if d, ok := ctx.Value(ContextHTTPResponseHeaderTimeoutKey).(time.Duration); ok {
			t.responseHeaderTimeout = &d
		}
	}
	t.userAgent = t.userAgent + " baton-sdk/" + sdk.Version

	_, err := t.cycle(ctx)
	if err != nil {
		return nil, err
	}
	return t, nil
}

type Transport struct {
	userAgent       string
	tlsClientConfig *tls.Config
	roundTripper    http.RoundTripper
	logger          *zap.Logger
	log             bool
	timeout         time.Duration
	nextCycle       time.Time
	mtx             sync.RWMutex

	// Connection tuning. A nil pointer means "not set" and falls back to the
	// corresponding default* constant in make(); this lets an explicit 0 mean
	// "disabled/unlimited" (net/http semantics) rather than "use default".
	idleConnTimeout       *time.Duration
	responseHeaderTimeout *time.Duration
	readIdleTimeout       *time.Duration
	pingTimeout           *time.Duration
	maxConnRetries        *int
}

func newTransport() *Transport {
	return &Transport{
		tlsClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
}

func (t *Transport) cycle(ctx context.Context) (http.RoundTripper, error) {
	n := time.Now()
	t.mtx.RLock()
	if t.nextCycle.After(n) && t.roundTripper != nil {
		defer t.mtx.RUnlock()
		return t.roundTripper, nil
	}
	t.mtx.RUnlock()

	t.mtx.Lock()
	defer t.mtx.Unlock()
	n = time.Now()
	// other goroutine changed it under us
	if t.nextCycle.After(n) && t.roundTripper != nil {
		return t.roundTripper, nil
	}
	// Build into a local; only swap into t.roundTripper after make
	// succeeds. The previous direct assignment
	// (`t.roundTripper, err = t.make(ctx)`) clobbered the existing
	// working RoundTripper with nil on transient make() errors --
	// subsequent requests served by this Transport would then fail
	// until the next successful cycle.
	newRoundTripper, err := t.make(ctx)
	if err != nil {
		return nil, err
	}
	t.roundTripper = newRoundTripper
	t.nextCycle = n.Add(time.Minute * 5)
	return t.roundTripper, nil
}

type userAgentTripper struct {
	next      http.RoundTripper
	userAgent string
}

func (uat *userAgentTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", uat.userAgent)
	}
	return uat.next.RoundTrip(req)
}

func (t *Transport) make(_ context.Context) (http.RoundTripper, error) {
	// based on http.DefaultTransport
	//
	// Key tuning for Lambda-behind-proxy environments:
	// - IdleConnTimeout is set shorter than typical proxy idle timeouts (Squid
	//   defaults to ~60s) to avoid grabbing stale pooled connections on warm
	//   Lambda invocations.
	// - ResponseHeaderTimeout bounds how long we wait for the proxy/server to
	//   start responding, preventing zombie connections.
	//
	// These defaults are overridable per-connector via the With* options (and,
	// for the response-header timeout, via CLI/config); see newTransport and
	// the default* constants.
	baseTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   defaultDialTimeout,
			KeepAlive: defaultDialKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       durationOr(t.idleConnTimeout, defaultIdleConnTimeout),
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		ExpectContinueTimeout: defaultExpectContinueTimeout,
		ResponseHeaderTimeout: durationOr(t.responseHeaderTimeout, defaultResponseHeaderTimeout),
		TLSClientConfig:       t.tlsClientConfig,
	}
	// PING-frame keepalive: keeps streams alive across intermediaries (e.g.
	// NLB) that RST idle tunnels. TCP keepalive doesn't cover sockets with
	// an outstanding write — the state during a slow response.
	h2, err := http2.ConfigureTransports(baseTransport)
	if err != nil {
		return nil, err
	}
	h2.ReadIdleTimeout = durationOr(t.readIdleTimeout, defaultHTTP2ReadIdleTimeout)
	h2.PingTimeout = durationOr(t.pingTimeout, defaultHTTP2PingTimeout)

	var rv http.RoundTripper = baseTransport
	// Transparently reconnect-and-retry transient connection-level failures so a
	// proxy silently dropping a pooled/tunneled connection doesn't surface as a
	// fatal error to the caller (e.g. aborting a whole sync).
	if maxRetries := intOr(t.maxConnRetries, defaultMaxConnectionRetries); maxRetries > 0 {
		rv = &retryRoundTripper{
			next:      rv,
			max:       maxRetries,
			baseDelay: defaultConnRetryBaseDelay,
			maxDelay:  defaultConnRetryMaxDelay,
			log:       t.log,
		}
	}
	rv = &userAgentTripper{next: rv, userAgent: t.userAgent}
	return rv, nil
}

func durationOr(v *time.Duration, def time.Duration) time.Duration {
	if v != nil {
		return *v
	}
	return def
}

func intOr(v *int, def int) int {
	if v != nil {
		return *v
	}
	return def
}

// retryRoundTripper transparently re-issues a request on transient
// connection-level failures (e.g. a proxy silently dropping a pooled or
// tunneled connection). When the HTTP/2 health check tears down a dead
// ClientConn it is evicted from the pool, so the retried RoundTrip dials a
// fresh connection -- retry here also reconnects. Only reconnectable errors
// (see IsReconnectableError) on idempotent, rewindable requests are retried;
// slow-peer timeouts are not, since replaying would hit the same wall.
type retryRoundTripper struct {
	next      http.RoundTripper
	max       int
	baseDelay time.Duration
	maxDelay  time.Duration
	log       bool
}

func (rt *retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	canReplay := isIdempotentRequest(req) &&
		(req.Body == nil || req.Body == http.NoBody || req.GetBody != nil)

	var (
		resp *http.Response
		err  error
	)
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if rerr := rewindBody(req); rerr != nil {
				return nil, rerr
			}
		}
		resp, err = rt.next.RoundTrip(req)
		if err == nil {
			return resp, nil
		}
		if !canReplay || attempt >= rt.max || !IsReconnectableError(err) {
			return resp, err
		}
		if req.Context().Err() != nil {
			return resp, err
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		delay := retryBackoff(attempt+1, rt.baseDelay, rt.maxDelay)
		if rt.log {
			ctxzap.Extract(req.Context()).Debug("uhttp: retrying request after transient connection error",
				zap.String("http.method", req.Method),
				zap.String("http.url_details.host", req.URL.Host),
				zap.String("http.url_details.path", req.URL.Path),
				zap.Int("attempt", attempt+1),
				zap.Duration("delay", delay),
				zap.Error(err),
			)
		}
		timer := time.NewTimer(delay)
		select {
		case <-req.Context().Done():
			timer.Stop()
			return resp, err
		case <-timer.C:
		}
	}
}

// isIdempotentRequest reports whether re-issuing req is safe. It mirrors
// net/http's notion of idempotency: the safe methods, plus any request carrying
// an explicit idempotency key.
func isIdempotentRequest(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	return req.Header.Get("Idempotency-Key") != "" || req.Header.Get("X-Idempotency-Key") != ""
}

func rewindBody(req *http.Request) error {
	if req.Body == nil || req.Body == http.NoBody || req.GetBody == nil {
		return nil
	}
	body, err := req.GetBody()
	if err != nil {
		return fmt.Errorf("uhttp: cannot rewind request body for retry: %w", err)
	}
	req.Body = body
	return nil
}

// retryBackoff returns exponential backoff with full jitter, capped at limit.
func retryBackoff(attempt int, base, limit time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for i := 1; i < attempt && d < limit; i++ {
		d *= 2
	}
	if d > limit {
		d = limit
	}
	return rand.N(d + 1) //nolint:gosec // jitter only, not security-sensitive
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	rt, err := t.cycle(ctx)
	if err != nil {
		return nil, fmt.Errorf("uhttp: cycle failed: %w", err)
	}
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			if t.log {
				duration := time.Since(start)
				t.l(ctx).Error("HTTP request panic",
					zap.String("http.method", req.Method),
					zap.String("http.url_details.host", req.URL.Host),
					zap.String("http.url_details.path", req.URL.Path),
					zap.String("http.url_details.query", req.URL.RawQuery),
					zap.Duration("duration", duration),
					zap.Any("panic", r),
				)
			}
			panic(r)
		}
	}()
	resp, err := rt.RoundTrip(req)
	err = wrapTransientNetworkError(err)
	if t.log {
		duration := time.Since(start)
		fields := []zap.Field{
			zap.String("http.method", req.Method),
			zap.String("http.url_details.host", req.URL.Host),
			zap.String("http.url_details.path", req.URL.Path),
			zap.String("http.url_details.query", req.URL.RawQuery),
			zap.Duration("duration", duration),
		}

		if resp != nil {
			fields = append(fields, zap.Int("http.status_code", resp.StatusCode))

			headers := make(map[string][]string, len(resp.Header))
			for _, header := range loggedResponseHeaders {
				if v := resp.Header.Values(header); len(v) > 0 {
					headers[header] = v
				}
			}

			fields = append(fields, zap.Any("http.headers", headers))
		}

		l := t.l(ctx)
		switch {
		case err != nil:
			// Always log errors - request failed to complete
			fields = append(fields, zap.Error(err))
			l.Error("HTTP request failed", fields...)
		case resp != nil && resp.StatusCode >= 500:
			// Server errors are noteworthy
			l.Warn("HTTP request server error", fields...)
		case resp != nil && resp.StatusCode >= 400:
			// Client errors at debug - usually expected (404s, etc)
			l.Debug("HTTP request client error", fields...)
		default:
			// Success
			l.Debug("HTTP request complete", fields...)
		}
	}
	return resp, err
}

func (t *Transport) l(ctx context.Context) *zap.Logger {
	if t.logger != nil {
		return t.logger
	}
	return ctxzap.Extract(ctx)
}
