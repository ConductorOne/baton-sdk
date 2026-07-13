package uhttp

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

type contextKeyType struct{}

// ContextHTTPTimeoutKey is the context key used to pass the HTTP timeout duration
// from the CLI configuration to uhttp.NewClient.
var ContextHTTPTimeoutKey = contextKeyType{}

type responseHeaderTimeoutContextKeyType struct{}

// ContextHTTPResponseHeaderTimeoutKey is the context key used to pass the HTTP
// response-header timeout duration from the CLI configuration to the transport.
// An explicit WithResponseHeaderTimeout option takes precedence over this value.
var ContextHTTPResponseHeaderTimeoutKey = responseHeaderTimeoutContextKeyType{}

type tlsClientConfigOption struct {
	config *tls.Config
}

func (o tlsClientConfigOption) Apply(c *Transport) {
	c.tlsClientConfig = o.config
}

// WithTLSClientConfig returns an Option that sets the TLS client configuration.
// `tlsConfig` is a structure that is used to configure a TLS client or server.
func WithTLSClientConfig(tlsConfig *tls.Config) Option {
	return tlsClientConfigOption{config: tlsConfig}
}

type loggerOption struct {
	log    bool
	logger *zap.Logger
}

func (o loggerOption) Apply(c *Transport) {
	c.log = o.log
	c.logger = o.logger
}

// WithLogger sets a logger options to the transport layer.
func WithLogger(log bool, logger *zap.Logger) Option {
	return loggerOption{
		log:    log,
		logger: logger,
	}
}

type userAgentOption struct {
	userAgent string
}

func (o userAgentOption) Apply(c *Transport) {
	c.userAgent = o.userAgent
}

// WithUserAgent sets a user agent option to the transport layer.
func WithUserAgent(userAgent string) Option {
	return userAgentOption{
		userAgent: userAgent,
	}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) Apply(c *Transport) {
	c.timeout = o.timeout
}

// WithTimeout sets the HTTP client timeout. Defaults to 300s (5 minutes) if not specified.
func WithTimeout(timeout time.Duration) Option {
	return timeoutOption{timeout: timeout}
}

type idleConnTimeoutOption struct{ d time.Duration }

func (o idleConnTimeoutOption) Apply(c *Transport) { d := o.d; c.idleConnTimeout = &d }

// WithIdleConnTimeout sets how long an idle keep-alive connection stays in the
// pool before being closed. Defaults to 30s -- intentionally shorter than common
// proxy idle timeouts (Squid ~60s) so the client discards pooled connections
// before the proxy silently drops them. Raise this ONLY if the upstream proxy's
// idle timeout is also higher; a value at/above the proxy's timeout reintroduces
// "http2: client connection lost" from stale-connection reuse. 0 = no limit.
func WithIdleConnTimeout(d time.Duration) Option { return idleConnTimeoutOption{d} }

type responseHeaderTimeoutOption struct{ d time.Duration }

func (o responseHeaderTimeoutOption) Apply(c *Transport) { d := o.d; c.responseHeaderTimeout = &d }

// WithResponseHeaderTimeout bounds the wait from finishing the request write to
// the first byte of the response headers. Applies to HTTP/1.1 and -- because the
// transport uses http2.ConfigureTransports -- to HTTP/2 as well. Defaults to 60s.
// Raise this for endpoints that block before responding (e.g. synchronous report
// generation), which would otherwise fail with "timeout awaiting response
// headers". 0 = no timeout.
func WithResponseHeaderTimeout(d time.Duration) Option { return responseHeaderTimeoutOption{d} }

type http2KeepAliveOption struct{ readIdle, pingTimeout time.Duration }

func (o http2KeepAliveOption) Apply(c *Transport) {
	readIdle, ping := o.readIdle, o.pingTimeout
	c.readIdleTimeout = &readIdle
	c.pingTimeout = &ping
}

// WithHTTP2KeepAlive tunes the HTTP/2 PING health check. readIdle is how long a
// connection may be silent before a PING is sent; pingTimeout is how long to wait
// for the PING ack before declaring the connection dead ("http2: client
// connection lost"). Total detection latency is readIdle+pingTimeout. Defaults
// are 15s + 15s (= 30s). Increase to tolerate slow-but-alive servers longer at
// the cost of slower dead-tunnel detection; readIdle of 0 disables the check.
func WithHTTP2KeepAlive(readIdle, pingTimeout time.Duration) Option {
	return http2KeepAliveOption{readIdle: readIdle, pingTimeout: pingTimeout}
}

type maxConnectionRetriesOption struct{ n int }

func (o maxConnectionRetriesOption) Apply(c *Transport) { n := o.n; c.maxConnRetries = &n }

// WithMaxConnectionRetries sets how many times a request is transparently
// re-issued on a transient, reconnectable transport error (a dropped pooled or
// tunneled connection). Only idempotent requests with a rewindable body are
// retried. Defaults to 3; 0 disables retrying.
func WithMaxConnectionRetries(n int) Option { return maxConnectionRetriesOption{n} }

type Option interface {
	Apply(*Transport)
}

// NewClient creates a new HTTP client that uses the given context and options to create a new transport layer.
func NewClient(ctx context.Context, options ...Option) (*http.Client, error) {
	t, err := NewTransport(ctx, options...)
	if err != nil {
		return nil, err
	}

	timeout := 300 * time.Second // 5 minutes default
	if t.timeout > 0 {
		timeout = t.timeout
	} else if ctxTimeout, ok := ctx.Value(ContextHTTPTimeoutKey).(time.Duration); ok && ctxTimeout > 0 {
		timeout = ctxTimeout
	}

	return &http.Client{
		Timeout:   timeout,
		Transport: t,
	}, nil
}

type icache interface {
	Get(req *http.Request) (*http.Response, error)
	Set(req *http.Request, value *http.Response) error
	Clear(ctx context.Context) error
	Stats(ctx context.Context) CacheStats
}

// CreateCacheKey generates a cache key based on the request URL, query parameters, and headers.
func CreateCacheKey(req *http.Request) (string, error) {
	if req == nil {
		return "", fmt.Errorf("request is nil")
	}
	var sortedParams []string
	// Normalize the URL path
	path := strings.ToLower(req.URL.Path)
	// Combine the path with sorted query parameters
	queryParams := req.URL.Query()
	for k, v := range queryParams {
		for _, value := range v {
			sortedParams = append(sortedParams, fmt.Sprintf("%s=%s", k, value))
		}
	}

	sort.Strings(sortedParams)
	queryString := strings.Join(sortedParams, "&")
	// Include relevant headers in the cache key
	var headerParts []string
	for key, values := range req.Header {
		for _, value := range values {
			if key == "Accept" || key == "Content-Type" || key == "Cookie" || key == "Range" {
				headerParts = append(headerParts, fmt.Sprintf("%s=%s", key, value))
			}
		}
	}

	sort.Strings(headerParts)
	headersString := strings.Join(headerParts, "&")
	// Create a unique string for the cache key
	cacheString := fmt.Sprintf("%s?%s&headers=%s", path, queryString, headersString)

	// Hash the cache string to create a key
	hash := sha256.New()
	_, err := hash.Write([]byte(cacheString))
	if err != nil {
		return "", err
	}

	cacheKey := fmt.Sprintf("%x", hash.Sum(nil))
	return cacheKey, nil
}
