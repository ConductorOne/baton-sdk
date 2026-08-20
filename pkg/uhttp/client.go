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
	Get(req *http.Request, opts ...CacheOption) (*http.Response, error)
	Set(req *http.Request, value *http.Response, opts ...CacheOption) error
	Clear(ctx context.Context) error
	Stats(ctx context.Context) CacheStats
}

type cacheKeyConfig struct {
	headers []string
}

// CacheOption configures how CreateCacheKey computes its key, beyond the
// default set of headers (Accept, Content-Type, Cookie, Range). Kept as an
// interface so future dimensions (TTL, query-param keying, etc.) can be
// added without changing CreateCacheKey's or icache's signatures again.
type CacheOption interface {
	applyCache(*cacheKeyConfig)
}

type cacheKeyHeadersOption []string

func (o cacheKeyHeadersOption) applyCache(c *cacheKeyConfig) {
	c.headers = append(c.headers, o...)
}

// CacheKeyHeaders returns a CacheOption that folds the named headers into
// the cache key computed by CreateCacheKey (and by GoCache/DBCache's
// Get/Set), beyond the default set (Accept, Content-Type, Cookie, Range).
// The value folded in is always read from req.Header at key-computation
// time, so the key can never describe a value other than the one actually
// present on the request. Named headers must therefore be set on the
// request before it reaches the cache lookup; a header only added by a
// transport-level RoundTripper or a cookie jar after that point is not
// seen.
func CacheKeyHeaders(headers ...string) CacheOption {
	return cacheKeyHeadersOption(headers)
}

// CreateCacheKey generates a cache key based on the request URL, query parameters, and headers.
func CreateCacheKey(req *http.Request, opts ...CacheOption) (string, error) {
	if req == nil {
		return "", fmt.Errorf("request is nil")
	}
	var cfg cacheKeyConfig
	for _, o := range opts {
		o.applyCache(&cfg)
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
	seenHeaders := map[string]bool{
		"Accept":       true,
		"Content-Type": true,
		"Cookie":       true,
		"Range":        true,
	}
	for key, values := range req.Header {
		for _, value := range values {
			if seenHeaders[key] {
				headerParts = append(headerParts, fmt.Sprintf("%s=%s", key, value))
			}
		}
	}
	// Opted-in headers are folded in on top of the default set above.
	// seenHeaders already marks the default set, and gets marked as each
	// opted-in header is processed, so a header named in cfg.headers -- by
	// one CacheOption or by several -- is never folded in more than once.
	for _, h := range cfg.headers {
		key := http.CanonicalHeaderKey(h)
		if seenHeaders[key] {
			continue
		}
		seenHeaders[key] = true
		for _, value := range req.Header[key] {
			headerParts = append(headerParts, fmt.Sprintf("%s=%s", key, value))
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
