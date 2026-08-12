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
	Get(req *http.Request, extraCacheKeyHeaders ...string) (*http.Response, error)
	Set(req *http.Request, value *http.Response, extraCacheKeyHeaders ...string) error
	Clear(ctx context.Context) error
	Stats(ctx context.Context) CacheStats
}

// defaultCacheKeyHeaders are always folded into the cache key returned by
// CreateCacheKey. Headers outside this set are ignored unless a caller
// names them via extraCacheKeyHeaders: folding in every header by default
// would key the cache on values that have nothing to do with the response
// -- transport-injected headers like User-Agent, tracing/correlation IDs,
// etc. -- and would defeat caching for callers who never asked for that.
var defaultCacheKeyHeaders = map[string]struct{}{
	"Accept":       {},
	"Content-Type": {},
	"Cookie":       {},
	"Range":        {},
}

// CreateCacheKey generates a cache key based on the request URL, query
// parameters, and headers. Only defaultCacheKeyHeaders -- plus any headers
// named in extraCacheKeyHeaders -- are folded in; see defaultCacheKeyHeaders
// for why the set isn't just "every header." Pass extraCacheKeyHeaders when
// a request varies by a header outside the default set -- e.g. a per-call
// Authorization token or a tenant/version header -- so requests that only
// differ in that header don't collide in the cache.
func CreateCacheKey(req *http.Request, extraCacheKeyHeaders ...string) (string, error) {
	if req == nil {
		return "", fmt.Errorf("request is nil")
	}
	allowedHeaders := make(map[string]struct{}, len(defaultCacheKeyHeaders)+len(extraCacheKeyHeaders))
	for h := range defaultCacheKeyHeaders {
		allowedHeaders[h] = struct{}{}
	}
	for _, h := range extraCacheKeyHeaders {
		allowedHeaders[http.CanonicalHeaderKey(h)] = struct{}{}
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
	// Include only the allowed headers in the cache key.
	var headerParts []string
	for key, values := range req.Header {
		if _, ok := allowedHeaders[key]; !ok {
			continue
		}
		for _, value := range values {
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
