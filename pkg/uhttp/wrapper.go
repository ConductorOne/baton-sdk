package uhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	uRateLimit "go.uber.org/ratelimit"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
)

const (
	ContentType               = "Content-Type"
	applicationJSON           = "application/json"
	applicationXML            = "application/xml"
	applicationFormUrlencoded = "application/x-www-form-urlencoded"
	applicationVndApiJSON     = "application/vnd.api+json"
	acceptHeader              = "Accept"
	authorizationHeader       = "Authorization"

	httpCacheHitCounterName  = "baton_sdk.http_cache_hit"
	httpCacheMissCounterName = "baton_sdk.http_cache_miss"
	httpCacheHitCounterDesc  = "number of HTTP cache hits"
	httpCacheMissCounterDesc = "number of HTTP cache misses"
)

type WrapperResponse struct {
	Header     http.Header
	Body       []byte
	Status     string
	StatusCode int
}

type rateLimiterOption struct {
	rate int
	per  time.Duration
}

func (o rateLimiterOption) Apply(c *BaseHttpClient) {
	opts := []uRateLimit.Option{}
	if o.per > 0 {
		opts = append(opts, uRateLimit.Per(o.per))
	}
	c.rateLimiter = uRateLimit.New(o.rate, opts...)
}

// WithRateLimiter returns a WrapperOption that sets the rate limiter for the http client.
// `rate` is the number of requests allowed per `per` duration.
// `per` is the duration in which the rate limit is enforced.
// Example: WithRateLimiter(10, time.Second) will allow 10 requests per second.
func WithRateLimiter(rate int, per time.Duration) WrapperOption {
	return rateLimiterOption{rate: rate, per: per}
}

type metricsHandlerOption struct {
	handler metrics.Handler
}

func (o metricsHandlerOption) Apply(c *BaseHttpClient) {
	c.metricsHandler = o.handler
}

// WithMetricsHandler returns a WrapperOption that sets the metrics handler for the http client.
// When set, cache hits and misses will be recorded as metrics.
func WithMetricsHandler(handler metrics.Handler) WrapperOption {
	return metricsHandlerOption{handler: handler}
}

type cacheKeyHeadersWrapperOption struct {
	opt CacheOption
}

func (o cacheKeyHeadersWrapperOption) Apply(c *BaseHttpClient) {
	c.cacheOptions = append(c.cacheOptions, o.opt)
}

// WithCacheKeyHeaders returns a WrapperOption that additionally folds the
// named headers into the HTTP response cache key for every request this
// client makes, on top of the default set (Accept, Content-Type, Cookie,
// Range). Use this when requests through this client vary by a header the
// cache wouldn't otherwise key on -- e.g. a per-call Authorization token or
// a tenant/version header -- so requests that only differ in that header
// don't collide in the cache. The value folded in is always read from
// req.Header at request time, so the key can never describe a value other
// than the one actually sent.
//
// Named headers must be set on the request before it reaches Do; a header
// only added later by a transport-level RoundTripper or a cookie jar is not
// seen by the cache lookup and will not be reflected in the key.
func WithCacheKeyHeaders(headers ...string) WrapperOption {
	return cacheKeyHeadersWrapperOption{opt: CacheKeyHeaders(headers...)}
}

type WrapperOption interface {
	Apply(*BaseHttpClient)
}

// Keep a handle on all caches so we can clear them later.
var (
	caches    []icache
	cachesMtx sync.RWMutex
)

func ClearCaches(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	l.Debug("clearing caches")
	var errs []error
	cachesMtx.RLock()
	defer cachesMtx.RUnlock()
	for _, cache := range caches {
		l.Debug("clearing cache", zap.String("cache", fmt.Sprintf("%T", cache)), zap.Any("stats", cache.Stats(ctx)))
		err := cache.Clear(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type (
	HttpClient interface {
		HttpClient() *http.Client
		Do(req *http.Request, options ...DoOption) (*http.Response, error)
		NewRequest(ctx context.Context, method string, url *url.URL, options ...RequestOption) (*http.Request, error)
	}
	BaseHttpClient struct {
		HttpClient     *http.Client
		rateLimiter    uRateLimit.Limiter
		baseHttpCache  icache
		metricsHandler metrics.Handler
		cacheOptions   []CacheOption
	}

	DoOption      func(resp *WrapperResponse) error
	RequestOption func() (io.ReadWriter, map[string]string, error)
)

func NewBaseHttpClient(httpClient *http.Client, opts ...WrapperOption) *BaseHttpClient {
	ctx := context.TODO()
	client, err := NewBaseHttpClientWithContext(ctx, httpClient, opts...)
	if err != nil {
		return nil
	}
	return client
}

func NewBaseHttpClientWithContext(ctx context.Context, httpClient *http.Client, opts ...WrapperOption) (*BaseHttpClient, error) {
	l := ctxzap.Extract(ctx)

	cache, err := NewHttpCache(ctx, nil)
	if err != nil {
		l.Error("error creating http cache", zap.Error(err))
		return nil, err
	}
	cli := &BaseHttpClient{
		HttpClient:    httpClient,
		baseHttpCache: cache,
	}

	cachesMtx.Lock()
	caches = append(caches, cache)
	cachesMtx.Unlock()

	for _, opt := range opts {
		opt.Apply(cli)
	}

	return cli, nil
}

// decodeError is a success decoder failing to read a body into its target.
// Do leaves it out of the error for a non-2xx response: that body was never
// the success shape, and the decoder's complaint would read as the failure.
type decodeError struct {
	err error
}

func (e *decodeError) Error() string {
	return e.err.Error()
}

func (e *decodeError) Unwrap() error {
	return e.err
}

func decodeFailure(err error) error {
	if err == nil {
		return nil
	}
	return &decodeError{err: err}
}

func withoutDecodeErrors(errs []error) []error {
	kept := make([]error, 0, len(errs))
	for _, err := range errs {
		var decodeErr *decodeError
		if !errors.As(err, &decodeErr) {
			kept = append(kept, err)
		}
	}
	return kept
}

// WithJSONResponse is a wrapper that marshals the returned response body into
// the provided shape. If the API should return an empty JSON body (i.e. HTTP
// status code 204 No Content), then pass a `nil` to `response`.
func WithJSONResponse(response any) DoOption {
	return func(resp *WrapperResponse) error {
		contentHeader := resp.Header.Get(ContentType)

		if !IsJSONContentType(contentHeader) {
			// to print the response, set the envvar BATON_DEBUG_PRINT_RESPONSE_BODY as non-empty, instead
			return decodeFailure(fmt.Errorf("unexpected content type for JSON response: %s. status code: %d", contentHeader, resp.StatusCode))
		}
		if response == nil && len(resp.Body) == 0 {
			return nil
		}
		err := json.Unmarshal(resp.Body, response)
		if err != nil {
			// to print the response, set the envvar BATON_DEBUG_PRINT_RESPONSE_BODY as non-empty, instead
			return decodeFailure(fmt.Errorf("failed to unmarshal json response: %w. status code: %d", err, resp.StatusCode))
		}
		return nil
	}
}

// Ignore content type header and always try to parse the response as JSON.
func WithAlwaysJSONResponse(response any) DoOption {
	return func(resp *WrapperResponse) error {
		if response == nil && len(resp.Body) == 0 {
			return nil
		}
		err := json.Unmarshal(resp.Body, response)
		if err != nil {
			// to print the response, set the envvar BATON_DEBUG_PRINT_RESPONSE_BODY as non-empty, instead
			return decodeFailure(fmt.Errorf("failed to unmarshal json response: %w. status code: %d", err, resp.StatusCode))
		}
		return nil
	}
}

func WithXMLResponse(response any) DoOption {
	return func(resp *WrapperResponse) error {
		if !IsXMLContentType(resp.Header.Get(ContentType)) {
			return decodeFailure(fmt.Errorf("unexpected content type for xml response: %s", resp.Header.Get(ContentType)))
		}
		if response == nil && len(resp.Body) == 0 {
			return nil
		}
		err := xml.Unmarshal(resp.Body, response)
		if err != nil {
			return decodeFailure(fmt.Errorf("failed to unmarshal xml response: %w. status code: %d", err, resp.StatusCode))
		}
		return nil
	}
}

// Ignore content type header and always try to parse the response as XML.
//
// A *map[string]any target is decoded through the generic xmlMap decoder, since
// encoding/xml cannot unmarshal into a map and would fail for every input. Any
// other target keeps going straight to xml.Unmarshal, so callers passing a typed
// struct are unaffected.
func WithAlwaysXMLResponse(response any) DoOption {
	return func(resp *WrapperResponse) error {
		if response == nil && len(resp.Body) == 0 {
			return nil
		}
		if genericResponse, ok := response.(*map[string]any); ok {
			// Scoped to the map target so the struct path below keeps its exact
			// existing behavior, including for 204 and empty bodies.
			if resp.StatusCode == http.StatusNoContent {
				return nil
			}
			if isSuccessStatusCode(resp.StatusCode) && len(resp.Body) == 0 {
				return nil
			}
			return decodeFailure(unmarshalXMLToMap(genericResponse, resp))
		}
		err := xml.Unmarshal(resp.Body, response)
		if err != nil {
			return decodeFailure(fmt.Errorf("failed to unmarshal xml response: %w. status code: %d", err, resp.StatusCode))
		}
		return nil
	}
}

// unmarshalXMLToMap decodes an XML body into a generic map via the xmlMap decoder.
// encoding/xml cannot unmarshal into a map directly, so any caller wanting an
// arbitrary XML document as a map has to route through xmlMap.
func unmarshalXMLToMap(response *map[string]any, resp *WrapperResponse) error {
	// A typed-nil target, e.g. WithAlwaysXMLResponse((*map[string]any)(nil)), gets
	// past an `any == nil` check because the interface still carries a type. Guard
	// here rather than at each call site so assigning through it cannot panic.
	// encoding/xml rejected this with "nil pointer passed to Unmarshal".
	if response == nil {
		return status.Error(codes.InvalidArgument, "response is nil")
	}

	var xm xmlMap
	if err := xml.Unmarshal(resp.Body, &xm); err != nil {
		return fmt.Errorf("failed to unmarshal xml response: %w. status code: %d", err, resp.StatusCode)
	}
	vMap, ok := xm.data.(map[string]any)
	if !ok {
		// The root's content has no map representation in two cases, and both used
		// to fail outright with "unsupported XML structure":
		//
		//   - Its direct children repeat: <Users><User/><User/></Users> decodes to
		//     a []map[string]any. A root-level list is a common API shape, so this
		//     was a real gap rather than an edge case.
		//   - It holds only text: <Code>OK</Code> decodes to a string.
		//
		// Key it by the root element name, which is otherwise discarded, so the
		// document is reachable by a path instead of being an error.
		//
		// Note the arity seam this leaves: a root holding a *single* <User> decodes
		// to a map and keeps the root stripped, so its path is "User" while the
		// repeated case is "Users". One config cannot serve both. Closing that
		// needs the decoder to group repeated children under their shared name,
		// which would also make the slice case here unreachable.
		*response = map[string]any{xm.root: xm.data}
		return nil
	}
	*response = vMap
	return nil
}

type ErrorResponse interface {
	Message() string
}

// maxQuotedBodyLength bounds the text quotedBody puts into an error message.
const maxQuotedBodyLength = 256

// quotedBody renders an error body WithErrorResponse could not decode, as a
// suffix for its message. Only plain text is quoted, up to the first markup
// and maxQuotedBodyLength runes: a structured or markup body becomes part of
// every log line that carries the error, and can hold a user's login or email.
func quotedBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return ""
	}
	if markup := strings.IndexByte(text, '<'); markup >= 0 {
		text = strings.TrimSpace(text[:markup])
	}
	if text == "" || strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") {
		return fmt.Sprintf(". body omitted (%d bytes)", len(body))
	}
	if runes := []rune(text); len(runes) > maxQuotedBodyLength {
		text = string(runes[:maxQuotedBodyLength]) + "..."
	}
	return ". body: " + text
}

// requestLogFields identify the request a log line is about. These fields omit
// the query string. A zap.Error on the same line still prints a *url.Error URL.
func requestLogFields(req *http.Request) []zap.Field {
	if req == nil {
		return nil
	}
	if req.URL == nil {
		return []zap.Field{zap.String("http.method", req.Method)}
	}
	return []zap.Field{
		zap.String("http.method", req.Method),
		zap.String("http.url_details.host", req.URL.Host),
		zap.String("http.url_details.path", req.URL.Path),
	}
}

// isSuccessStatusCode reports whether code is in the 2xx success class.
// http.StatusOK (200) is the inclusive lower bound and
// http.StatusMultipleChoices (300) the exclusive upper bound.
func isSuccessStatusCode(code int) bool {
	return code >= http.StatusOK && code < http.StatusMultipleChoices
}

// GrpcCodeFromHTTPStatus maps an HTTP status code to the appropriate gRPC status code.
func GrpcCodeFromHTTPStatus(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusRequestTimeout:
		return codes.DeadlineExceeded
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return codes.Unavailable
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusNotImplemented:
		return codes.Unimplemented
	}
	switch {
	case httpStatus >= 500 && httpStatus <= 599:
		return codes.Unavailable
	case httpStatus >= 400 && httpStatus <= 499:
		return codes.InvalidArgument
	default:
		return codes.Unknown
	}
}

func WithErrorResponse(resource ErrorResponse) DoOption {
	return func(resp *WrapperResponse) error {
		if resp.StatusCode < http.StatusMultipleChoices {
			return nil
		}

		contentHeader := resp.Header.Get(ContentType)

		grpcCode := GrpcCodeFromHTTPStatus(resp.StatusCode)

		if !IsJSONContentType(contentHeader) {
			return status.Errorf(grpcCode,
				"unexpected content type for JSON error response: %s. status code: %d%s",
				contentHeader, resp.StatusCode, quotedBody(resp.Body))
		}

		// Decode the JSON response body into the ErrorResponse
		if err := json.Unmarshal(resp.Body, &resource); err != nil {
			return status.Errorf(grpcCode,
				"failed to unmarshal JSON error response: %v. status code: %d%s",
				err, resp.StatusCode, quotedBody(resp.Body))
		}

		// Construct a more detailed error message
		errMsg := fmt.Sprintf("Request failed with status %d: %s", resp.StatusCode, resource.Message())

		return status.Error(grpcCode, errMsg)
	}
}

func WithRatelimitData(resource *v2.RateLimitDescription) DoOption {
	return func(resp *WrapperResponse) error {
		if resource == nil {
			return fmt.Errorf("WithRatelimitData: rate limit description is nil")
		}
		rl, err := ratelimit.ExtractRateLimitData(resp.StatusCode, &resp.Header)
		if err != nil {
			return err
		}

		resource.SetLimit(rl.GetLimit())
		resource.SetRemaining(rl.GetRemaining())
		resource.SetResetAt(rl.GetResetAt())
		resource.SetStatus(rl.GetStatus())

		return nil
	}
}

func WithResponse(response any) DoOption {
	return func(resp *WrapperResponse) error {
		if IsJSONContentType(resp.Header.Get(ContentType)) {
			return WithJSONResponse(response)(resp)
		}
		if IsXMLContentType(resp.Header.Get(ContentType)) {
			return WithXMLResponse(response)(resp)
		}

		return decodeFailure(status.Error(codes.Unknown, "unsupported content type"))
	}
}

// Handle anything that can be marshaled into JSON or XML.
// If the response is a list, its values will be put into the "items" field.
// If the response is a single value (int, string, bool, etc), it will be put into the "value" field.
// A response of `null` results in the "value" field being set to `nil`.
func WithGenericResponse(response *map[string]any) DoOption {
	return func(resp *WrapperResponse) error {
		if response == nil {
			return status.Error(codes.InvalidArgument, "response is nil")
		}

		if resp.StatusCode == http.StatusNoContent {
			return nil
		}

		if isSuccessStatusCode(resp.StatusCode) && len(resp.Body) == 0 {
			return nil
		}

		var v any
		var err error

		if IsJSONContentType(resp.Header.Get(ContentType)) {
			err = WithJSONResponse(&v)(resp)
			if err != nil {
				return err
			}
			if list, ok := v.([]any); ok {
				(*response)["items"] = list
			} else if vMap, ok := v.(map[string]any); ok {
				*response = vMap
			} else if boolValue, ok := v.(bool); ok {
				(*response) = map[string]any{"value": boolValue}
			} else if floatValue, ok := v.(float64); ok {
				(*response) = map[string]any{"value": floatValue}
			} else if stringValue, ok := v.(string); ok {
				(*response) = map[string]any{"value": stringValue}
			} else if v == nil {
				// JSON response is literally `null`.
				(*response) = map[string]any{"value": nil}
			} else {
				return decodeFailure(status.Errorf(codes.Internal, "unsupported value type: %T", v))
			}
			return nil
		}

		if IsXMLContentType(resp.Header.Get(ContentType)) {
			return decodeFailure(unmarshalXMLToMap(response, resp))
		}

		return decodeFailure(status.Error(codes.Unknown, fmt.Sprintf("unsupported content type: %s", resp.Header.Get(ContentType))))
	}
}

func WrapErrors(preferredCode codes.Code, statusMsg string, errs ...error) error {
	st := status.New(preferredCode, statusMsg)

	if len(errs) == 0 {
		return st.Err()
	}
	return joinWithStatus(st, errs)
}

func WrapErrorsWithRateLimitInfo(preferredCode codes.Code, resp *http.Response, errs ...error) error {
	st := status.New(preferredCode, resp.Status)

	description, err := ratelimit.ExtractRateLimitData(resp.StatusCode, &resp.Header)
	// Ignore any error extracting rate limit data
	if err == nil {
		st, _ = st.WithDetails(description)
	}

	if len(errs) == 0 {
		return st.Err()
	}
	return joinWithStatus(st, errs)
}

// joinWithStatus joins errs under st. st is left off only when the first
// status in the chain already states it: status.Code reads that one. A later
// match does not count, or an earlier status of another code would win. A
// non-2xx Do error passed back through WrapErrors would otherwise print its
// status line twice.
func joinWithStatus(st *status.Status, errs []error) error {
	joined := errors.Join(errs...)
	if statusStates(st, joined) {
		return joined
	}
	return errors.Join(append([]error{st.Err()}, errs...)...)
}

// statusStates reports whether the status status.Code would read from err
// already has st's code, message, and details.
func statusStates(st *status.Status, err error) bool {
	var carrier interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &carrier) {
		return false
	}
	carried := carrier.GRPCStatus()
	return carried.Code() == st.Code() && carried.Message() == st.Message() &&
		detailsCover(carried.Details(), st.Details())
}

// detailsCover reports whether every detail in want is already in have.
func detailsCover(have, want []any) bool {
	for _, detail := range want {
		wantMsg, ok := detail.(proto.Message)
		if !ok || !hasDetail(have, wantMsg) {
			return false
		}
	}
	return true
}

func hasDetail(have []any, want proto.Message) bool {
	for _, detail := range have {
		got, ok := detail.(proto.Message)
		if ok && detailMatches(got, want) {
			return true
		}
	}
	return false
}

// detailMatches compares details. A rate-limit detail matches on status,
// limit, and remaining. ResetAt is time.Now plus a relative header, so two
// reads of the same header are not proto.Equal.
func detailMatches(got, want proto.Message) bool {
	gotRL, gotOK := got.(*v2.RateLimitDescription)
	wantRL, wantOK := want.(*v2.RateLimitDescription)
	if gotOK || wantOK {
		return gotOK && wantOK &&
			gotRL.GetStatus() == wantRL.GetStatus() &&
			gotRL.GetLimit() == wantRL.GetLimit() &&
			gotRL.GetRemaining() == wantRL.GetRemaining()
	}
	return proto.Equal(got, want)
}

func (c *BaseHttpClient) recordCacheHit(ctx context.Context) {
	if c.metricsHandler == nil {
		return
	}
	counter := c.metricsHandler.Int64Counter(httpCacheHitCounterName, httpCacheHitCounterDesc, metrics.Dimensionless)
	counter.Add(ctx, 1, nil)
}

func (c *BaseHttpClient) recordCacheMiss(ctx context.Context) {
	if c.metricsHandler == nil {
		return
	}
	counter := c.metricsHandler.Int64Counter(httpCacheMissCounterName, httpCacheMissCounterDesc, metrics.Dimensionless)
	counter.Add(ctx, 1, nil)
}

func (c *BaseHttpClient) Do(req *http.Request, options ...DoOption) (*http.Response, error) {
	var (
		err  error
		resp *http.Response
	)
	l := ctxzap.Extract(req.Context())

	// If a rate limiter is defined, take a token before making the request.
	if c.rateLimiter != nil {
		c.rateLimiter.Take()
	}

	if req.Method == http.MethodGet && req.Header.Get("Cache-Control") != "no-cache" {
		resp, err = c.baseHttpCache.Get(req, c.cacheOptions...)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			c.recordCacheMiss(req.Context())
		} else {
			c.recordCacheHit(req.Context())
		}
	}

	if resp == nil {
		resp, err = c.HttpClient.Do(req) // #nosec G704 -- this HTTP wrapper intentionally supports arbitrary connector-defined endpoints.
		if err != nil {
			err = wrapRequestError(err)
			l.Error("base-http-client: HTTP error response", append(requestLogFields(req), zap.Error(err))...)
			return resp, err
		}
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if len(body) > 0 {
			resp.Body = io.NopCloser(bytes.NewBuffer(body))
		}
		return resp, wrapRequestError(err)
	}

	// Replace resp.Body with a no-op closer so nobody has to worry about closing the reader.
	shouldPrint := os.Getenv("BATON_DEBUG_PRINT_RESPONSE_BODY")
	if shouldPrint != "" {
		resp.Body = io.NopCloser(wrapPrintBody(bytes.NewBuffer(body)))
	} else {
		resp.Body = io.NopCloser(bytes.NewBuffer(body))
	}

	wresp := WrapperResponse{
		Header:     resp.Header,
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Body:       body,
	}

	var optErrs []error
	for _, option := range options {
		optErr := option(&wresp)
		if optErr != nil {
			optErrs = append(optErrs, optErr)
		}
	}

	// Log response headers directly for certain errors
	if resp.StatusCode >= 400 {
		redactedHeaders := RedactSensitiveHeaders(resp.Header)
		logFields := append(requestLogFields(req),
			zap.Int("status_code", resp.StatusCode),
			zap.String("status", resp.Status),
			zap.Any("headers", redactedHeaders),
		)
		if resp.StatusCode >= 500 {
			l.Error("base-http-client: HTTP error status", logFields...)
		} else {
			l.Warn("base-http-client: HTTP error status", logFields...)
		}
	}

	if !isSuccessStatusCode(resp.StatusCode) {
		grpcCode := GrpcCodeFromHTTPStatus(resp.StatusCode)
		return resp, WrapErrorsWithRateLimitInfo(grpcCode, resp, withoutDecodeErrors(optErrs)...)
	}

	if req.Method == http.MethodGet && resp.StatusCode == http.StatusOK {
		cacheErr := c.baseHttpCache.Set(req, resp, c.cacheOptions...)
		if cacheErr != nil {
			l.Warn("error setting cache", zap.String("url", req.URL.String()), zap.Error(cacheErr))
		}
	}

	return resp, errors.Join(optErrs...)
}

var sensitiveStrings = []string{
	"api-key",
	"auth",
	"cookie",
	"proxy-authorization",
	"set-cookie",
	"x-forwarded-for",
	"x-forwarded-proto",
}

func RedactSensitiveHeaders(h http.Header) http.Header {
	if h == nil {
		return nil
	}
	safe := make(http.Header, len(h))
	for k, v := range h {
		sensitive := false
		headerKey := strings.ToLower(k)
		for _, sensitiveString := range sensitiveStrings {
			if strings.Contains(headerKey, sensitiveString) {
				sensitive = true
				break
			}
		}

		if sensitive {
			safe[k] = []string{"REDACTED"}
		} else {
			safe[k] = v
		}
	}
	return safe
}

func WithHeader(key, value string) RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		return nil, map[string]string{
			key: value,
		}, nil
	}
}

func WithNoCache() RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		return nil, map[string]string{
			"Cache-Control": "no-cache",
		}, nil
	}
}

func WithBody(body []byte) RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		return bytes.NewBuffer(body), nil, nil
	}
}

func WithJSONBody(body any) RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		buffer := new(bytes.Buffer)
		err := json.NewEncoder(buffer).Encode(body)
		if err != nil {
			return nil, nil, err
		}

		_, headers, err := WithContentTypeJSONHeader()()
		if err != nil {
			return nil, nil, err
		}

		return buffer, headers, nil
	}
}

func WithFormBody(body string) RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		var buffer bytes.Buffer
		_, err := buffer.WriteString(body)
		if err != nil {
			return nil, nil, err
		}

		_, headers, err := WithContentTypeFormHeader()()
		if err != nil {
			return nil, nil, err
		}

		return &buffer, headers, nil
	}
}

func WithXMLBody(body any) RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		var buffer bytes.Buffer

		err := xml.NewEncoder(&buffer).Encode(body)
		if err != nil {
			return nil, nil, err
		}

		_, headers, err := WithContentTypeXMLHeader()()
		if err != nil {
			return nil, nil, err
		}

		return &buffer, headers, nil
	}
}

func WithAcceptJSONHeader() RequestOption {
	return WithAccept(applicationJSON)
}

func WithContentTypeJSONHeader() RequestOption {
	return WithContentType(applicationJSON)
}

func WithAcceptXMLHeader() RequestOption {
	return WithAccept(applicationXML)
}

func WithContentTypeXMLHeader() RequestOption {
	return WithContentType(applicationXML)
}

func WithContentTypeFormHeader() RequestOption {
	return WithContentType(applicationFormUrlencoded)
}

func WithContentTypeVndHeader() RequestOption {
	return WithContentType(applicationVndApiJSON)
}

func WithAcceptVndJSONHeader() RequestOption {
	return WithAccept(applicationVndApiJSON)
}

func WithContentType(ctype string) RequestOption {
	return WithHeader(ContentType, ctype)
}

func WithAccept(value string) RequestOption {
	return WithHeader(acceptHeader, value)
}

func WithBearerToken(token string) RequestOption {
	return WithHeader(authorizationHeader, fmt.Sprintf("Bearer %s", token))
}

func (c *BaseHttpClient) NewRequest(ctx context.Context, method string, url *url.URL, options ...RequestOption) (*http.Request, error) {
	var buffer io.ReadWriter
	var headers = make(map[string]string)
	for _, option := range options {
		buf, h, err := option()
		if err != nil {
			return nil, err
		}

		if buf != nil {
			buffer = buf
		}

		for k, v := range h {
			headers[k] = v
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), buffer)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return req, nil
}
