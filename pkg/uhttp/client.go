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

const (
	defaultTransientRetryAttempts = 3
	defaultTransientRetryInitial  = 500 * time.Millisecond
	defaultTransientRetryMaxDelay = 2 * time.Second
	// A mid-flight timeout has already blocked for up to ResponseHeaderTimeout
	// (60s). Retrying that N times would stall Token() for minutes; one extra
	// attempt is enough to cover a hung origin that then recovers.
	maxTransientTimeoutRetries = 1
)

// TransientRetryConfig configures WithTransientRetries. A zero value selects
// the defaults: 3 attempts (1 try + 2 retries), 500ms initial delay, 2s max
// delay. MaxAttempts <= 0, InitialDelay <= 0, and MaxDelay <= 0 each mean
// "use the default"; 0 does not mean unlimited.
//
// MaxAttempts is the total number of tries including the first. A value of 1
// disables 5xx and timeout retries; the pre-existing one-shot never-sent /
// stale-connection retry still runs.
type TransientRetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	// ReplaySafe declares that this client's requests are side-effect-free
	// and may be replayed even when the origin might already have processed
	// them. It adds retries for mid-flight timeouts, stale-connection
	// resets, and gateway 502/504 responses — all "response lost, request
	// possibly processed" signals. Leave it false (the default) when a
	// replay could double-apply state. Rotating refresh-token grants are
	// the canonical example: replaying a grant the origin already processed
	// sends the superseded token, which can trip reuse detection and revoke
	// the token family. The zero value retries only failures the origin
	// itself answered (5xx other than 502/504) or that provably never
	// reached it (dial-phase errors).
	ReplaySafe bool
}

type transientRetrySettings struct {
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
	replaySafe   bool
}

func resolveTransientRetry(cfg TransientRetryConfig) transientRetrySettings {
	s := transientRetrySettings{
		maxAttempts:  cfg.MaxAttempts,
		initialDelay: cfg.InitialDelay,
		maxDelay:     cfg.MaxDelay,
		replaySafe:   cfg.ReplaySafe,
	}
	if s.maxAttempts <= 0 {
		s.maxAttempts = defaultTransientRetryAttempts
	}
	if s.initialDelay <= 0 {
		s.initialDelay = defaultTransientRetryInitial
	}
	if s.maxDelay <= 0 {
		s.maxDelay = defaultTransientRetryMaxDelay
	}
	return s
}

type transientRetriesOption struct {
	cfg TransientRetryConfig
}

func (o transientRetriesOption) Apply(c *Transport) {
	s := resolveTransientRetry(o.cfg)
	c.transientRetry = &s
}

// WithTransientRetries retries transient failures on this client with
// jittered exponential backoff. Use it only for OAuth token endpoints (and
// similarly side-effect-free token fetches).
//
// By default only failures that cannot mean "the origin processed the
// request" are retried: 5xx responses other than 502/504, and dial-phase
// errors where the request was never sent. Set ReplaySafe to also retry
// mid-flight timeouts, stale-connection resets, and 502/504 — the response
// was lost, but the request may have been processed — which is only safe
// for stateless requests such as client-credentials and JWT grants.
//
// Do not enable this on a client used for general API reads or for
// provisioning (grants, revokes, tickets, SetIamPolicy). Those requests are
// not safe to replay after they have been sent; a retried POST can double-apply.
// AuthCredentials.GetClient always strips this option from the returned
// client; OAuth helpers apply it to the token-endpoint client only.
//
// 429 / rate-limit responses are not retried here: they belong to pkg/retry
// via RateLimitDescription.
func WithTransientRetries(cfg TransientRetryConfig) Option {
	return transientRetriesOption{cfg: cfg}
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
