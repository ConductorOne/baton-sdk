package uhttp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
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

// wallClockGapThreshold is the wall-clock jump between requests treated as
// evidence the process was suspended (e.g. a Lambda freeze between
// invocations): the proxy/NLB has likely idle-closed our pooled connections
// while IdleConnTimeout's monotonic clock was stopped, so drop the pool
// rather than reuse a dead socket. A false positive only costs one
// CONNECT+TLS handshake.
const wallClockGapThreshold = 15 * time.Second

// NewTransport creates a new Transport, applies the options, and then cycles the transport.
func NewTransport(ctx context.Context, options ...Option) (*Transport, error) {
	t := newTransport()
	for _, opt := range options {
		opt.Apply(t)
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
	baseTransport   *http.Transport
	logger          *zap.Logger
	log             bool
	timeout         time.Duration
	nextCycle       time.Time
	mtx             sync.RWMutex

	wallMtx          sync.Mutex
	lastActivityWall time.Time
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
	newRoundTripper, newBase, err := t.make(ctx)
	if err != nil {
		return nil, err
	}
	t.roundTripper = newRoundTripper
	t.baseTransport = newBase
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

func (t *Transport) make(_ context.Context) (http.RoundTripper, *http.Transport, error) {
	// based on http.DefaultTransport
	//
	// Key tuning for Lambda-behind-proxy environments:
	// - IdleConnTimeout is set shorter than typical proxy idle timeouts (Squid
	//   defaults to ~60s) to avoid grabbing stale pooled connections on warm
	//   Lambda invocations.
	// - ResponseHeaderTimeout bounds how long we wait for the proxy/server to
	//   start responding, preventing zombie connections.
	baseTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		TLSClientConfig:       t.tlsClientConfig,
	}
	// PING-frame keepalive: keeps streams alive across intermediaries (e.g.
	// NLB) that RST idle tunnels. TCP keepalive doesn't cover sockets with
	// an outstanding write — the state during a slow response.
	h2, err := http2.ConfigureTransports(baseTransport)
	if err != nil {
		return nil, nil, err
	}
	h2.ReadIdleTimeout = 15 * time.Second
	h2.PingTimeout = 15 * time.Second
	var rv http.RoundTripper = baseTransport
	rv = &userAgentTripper{next: rv, userAgent: t.userAgent}
	return rv, baseTransport, nil
}

// touchWallClock records request activity on the wall clock. Round(0) strips
// the monotonic reading so gap comparisons use the wall clock, which (unlike
// the monotonic clock) is resynced across a Lambda freeze/thaw.
func (t *Transport) touchWallClock() {
	now := time.Now().Round(0)
	t.wallMtx.Lock()
	t.lastActivityWall = now
	t.wallMtx.Unlock()
}

// dropPooledConnsAfterClockJump closes idle pooled connections when the wall
// clock has jumped more than wallClockGapThreshold since the last request.
// IdleConnTimeout cannot do this: it ages connections on the monotonic clock,
// which stops while a Lambda execution environment is frozen.
func (t *Transport) dropPooledConnsAfterClockJump(ctx context.Context) {
	now := time.Now().Round(0)
	t.wallMtx.Lock()
	last := t.lastActivityWall
	t.lastActivityWall = now
	t.wallMtx.Unlock()
	if last.IsZero() {
		return
	}
	gap := now.Sub(last)
	if gap <= wallClockGapThreshold {
		return
	}

	t.mtx.RLock()
	base := t.baseTransport
	t.mtx.RUnlock()
	if base == nil {
		return
	}
	base.CloseIdleConnections()
	t.l(ctx).Debug("uhttp: wall clock jumped since last request; dropped pooled connections",
		zap.Duration("gap", gap))
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	t.dropPooledConnsAfterClockJump(ctx)
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
	// A slow response is activity, not suspension — refresh the wall-clock
	// marker so long round trips don't read as freeze gaps.
	t.touchWallClock()
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
