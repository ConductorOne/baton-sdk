package uhttp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

// staleConnectionGap is how long the transport can go unused before its
// pooled connections are presumed dead and dropped on the next request.
// The gap is measured on the wall clock, not the monotonic clock: in AWS
// Lambda the monotonic clock does not advance while the execution
// environment is frozen, so IdleConnTimeout never observes the gap, while
// proxies and origins time the same connections out in real time.
// 60s matches typical proxy idle timeouts (Squid defaults to ~60s).
const staleConnectionGap = 60 * time.Second

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
	// baseTransport is the concrete transport inside roundTripper, kept so
	// idle connections can be closed. Guarded by mtx.
	baseTransport *http.Transport
	logger        *zap.Logger
	log           bool
	timeout       time.Duration
	nextCycle     time.Time
	// lastActivityNs is the wall-clock unix-nano time of the last request.
	lastActivityNs atomic.Int64
	// gapMu serializes gap-triggered pool drops. The drop must complete
	// before concurrent callers may treat the pool as live, so the fresh
	// timestamp is published only after CloseIdleConnections returns and
	// peers that observed the same gap block here until then.
	gapMu sync.Mutex
	// nowFn overrides time.Now in tests.
	nowFn func() time.Time
	mtx   sync.RWMutex
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
	oldBase := t.baseTransport
	newRoundTripper, err := t.make(ctx)
	if err != nil {
		return nil, err
	}
	t.roundTripper = newRoundTripper
	t.nextCycle = n.Add(time.Minute * 5)
	// The replaced pool would otherwise keep its connections open until
	// their idle timers fire; close them now so they end with a clean FIN.
	if oldBase != nil {
		oldBase.CloseIdleConnections()
	}
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
		return nil, err
	}
	h2.ReadIdleTimeout = 15 * time.Second
	h2.PingTimeout = 15 * time.Second
	var rv http.RoundTripper = baseTransport
	rv = &userAgentTripper{next: rv, userAgent: t.userAgent}
	t.baseTransport = baseTransport
	return rv, nil
}

func (t *Transport) now() time.Time {
	if t.nowFn != nil {
		return t.nowFn()
	}
	return time.Now()
}

// dropIdleConnectionsAfterGap discards pooled connections when this
// transport has been unused for longer than staleConnectionGap. After a
// Lambda thaw the pool can hold connections that a proxy or origin closed
// during the freeze, and reusing one fails with a connection reset; the
// UnixNano comparison reads the wall clock, which is resynced on thaw,
// where a monotonic time.Since would report no gap at all.
func (t *Transport) dropIdleConnectionsAfterGap(ctx context.Context) {
	nowNs := t.now().UnixNano()
	lastNs := t.lastActivityNs.Load()
	if lastNs == 0 || time.Duration(nowNs-lastNs) < staleConnectionGap {
		return
	}

	// Serialize the drop and publish the fresh timestamp only after the
	// pool is clean. Publishing first would let concurrent callers see a
	// small gap and draw a presumed-dead connection while the drop is
	// still running; instead they block here until it has completed.
	t.gapMu.Lock()
	defer t.gapMu.Unlock()
	lastNs = t.lastActivityNs.Load()
	if lastNs == 0 || time.Duration(nowNs-lastNs) < staleConnectionGap {
		return
	}

	if t.log {
		t.l(ctx).Debug("uhttp: dropping idle connections after activity gap",
			zap.Duration("gap", time.Duration(nowNs-lastNs)),
		)
	}
	t.closeIdleConnections()
	t.lastActivityNs.Store(nowNs)
}

// closeIdleConnections drops the pooled connections on the current base
// transport, if one has been built.
func (t *Transport) closeIdleConnections() {
	t.mtx.RLock()
	bt := t.baseTransport
	t.mtx.RUnlock()
	if bt != nil {
		bt.CloseIdleConnections()
	}
}

// retryableRequest reports whether a failed request may be retried safely,
// and returns the request to retry with. Two cases qualify: errors raised
// before any request bytes were written (safe for every method), and
// stale-pooled-connection errors on requests that declare idempotence via
// a safe method or an Idempotency-Key header. net/http covers the second
// case itself for HTTP/1.1 but never for HTTP/2, where its retry gate
// requires a reused persistConn and h2 connections are always wrapped in
// fresh ones.
func retryableRequest(req *http.Request, err error) (*http.Request, bool) {
	retrySafe := requestNeverSent(err) || (declaredIdempotent(req) && isStaleConnectionError(err))
	if !retrySafe {
		return nil, false
	}
	// The transport closes the original body on failure, so any request
	// that carries one needs a fresh copy to be retried.
	if req.Body == nil || req.Body == http.NoBody {
		return req, true
	}
	if req.GetBody == nil {
		return nil, false
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, false
	}
	retryReq := req.Clone(req.Context())
	retryReq.Body = body
	return retryReq, true
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	t.dropIdleConnectionsAfterGap(ctx)
	// Record activity on completion too, so a single long request is not
	// mistaken for an idle gap by the request that follows it.
	defer func() { t.lastActivityNs.Store(t.now().UnixNano()) }()
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
	if err != nil && req.Context().Err() == nil {
		if retryReq, ok := retryableRequest(req, err); ok {
			// A mass connection death (e.g. a proxy instance replaced) can
			// leave further corpses in the pool; drop them so the retry
			// dials fresh instead of drawing the next one. Re-resolve the
			// round tripper afterwards: cycle() may have swapped transports
			// since rt was acquired, and the retry must run on the pool
			// that was just dropped, not an older one still holding corpses.
			if isStaleConnectionError(err) {
				t.closeIdleConnections()
				if freshRT, cycleErr := t.cycle(ctx); cycleErr == nil {
					rt = freshRT
				}
			}
			if t.log {
				t.l(ctx).Debug("uhttp: retrying request after transport error",
					zap.String("http.method", req.Method),
					zap.String("http.url_details.host", req.URL.Host),
					zap.String("http.url_details.path", req.URL.Path),
					zap.Error(err),
				)
			}
			resp, err = rt.RoundTrip(retryReq)
		}
	}
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
