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
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/helpers"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ContentType               = "Content-Type"
	applicationJSON           = "application/json"
	applicationXML            = "application/xml"
	applicationFormUrlencoded = "application/x-www-form-urlencoded"
	applicationVndApiJSON     = "application/vnd.api+json"
	acceptHeader              = "Accept"
)

type WrapperResponse struct {
	Header     http.Header
	Body       []byte
	Status     string
	StatusCode int
}

type (
	HttpClient interface {
		HttpClient() *http.Client
		Do(req *http.Request, options ...DoOption) (*http.Response, error)
		NewRequest(ctx context.Context, method string, url *url.URL, options ...RequestOption) (*http.Request, error)
	}
	BaseHttpClient struct {
		HttpClient    *http.Client
		baseHttpCache GoCache
	}

	DoOption      func(resp *WrapperResponse) error
	RequestOption func() (io.ReadWriter, map[string]string, error)
	ContextKey    struct{}
	CacheConfig   struct {
		LogDebug     bool
		CacheTTL     int32
		CacheMaxSize int
		DisableCache bool
	}
)

func NewBaseHttpClient(httpClient *http.Client) *BaseHttpClient {
	ctx := context.TODO()
	client, err := NewBaseHttpClientWithContext(ctx, httpClient)
	if err != nil {
		return nil
	}
	return client
}

func NewBaseHttpClientWithContext(ctx context.Context, httpClient *http.Client) (*BaseHttpClient, error) {
	l := ctxzap.Extract(ctx)
	disableCache, err := strconv.ParseBool(os.Getenv("BATON_DISABLE_HTTP_CACHE"))
	if err != nil {
		disableCache = false
	}

	var (
		config = CacheConfig{
			LogDebug:     l.Level().Enabled(zap.DebugLevel),
			CacheTTL:     int32(3600), // seconds
			CacheMaxSize: int(2048),   // MB
			DisableCache: disableCache,
		}
		ok bool
	)
	if v := ctx.Value(ContextKey{}); v != nil {
		if config, ok = v.(CacheConfig); !ok {
			return nil, fmt.Errorf("error casting config values from context")
		}
	}

	cache, err := NewGoCache(ctx, config)
	if err != nil {
		l.Error("error creating http cache", zap.Error(err))
		return nil, err
	}

	return &BaseHttpClient{
		HttpClient:    httpClient,
		baseHttpCache: cache,
	}, nil
}

// WithJSONResponse is a wrapper that marshals the returned response body into
// the provided shape. If the API should return an empty JSON body (i.e. HTTP
// status code 204 No Content), then pass a `nil` to `response`.
func WithJSONResponse(response interface{}) DoOption {
	return func(resp *WrapperResponse) error {
		if !helpers.IsJSONContentType(resp.Header.Get(ContentType)) {
			return fmt.Errorf("unexpected content type for json response: %s", resp.Header.Get(ContentType))
		}
		if response == nil && len(resp.Body) == 0 {
			return nil
		}
		err := json.Unmarshal(resp.Body, response)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json response: %w. body %v", err, resp.Body)
		}
		return nil
	}
}

type ErrorResponse interface {
	Message() string
}

func WithErrorResponse(resource ErrorResponse) DoOption {
	return func(resp *WrapperResponse) error {
		if resp.StatusCode < 300 {
			return nil
		}

		if !helpers.IsJSONContentType(resp.Header.Get(ContentType)) {
			return fmt.Errorf("%v", string(resp.Body))
		}

		// Decode the JSON response body into the ErrorResponse
		if err := json.Unmarshal(resp.Body, &resource); err != nil {
			return status.Error(codes.Unknown, "Request failed with unknown error")
		}

		// Construct a more detailed error message
		errMsg := fmt.Sprintf("Request failed with status %d: %s", resp.StatusCode, resource.Message())

		return status.Error(codes.Unknown, errMsg)
	}
}

func WithRatelimitData(resource *v2.RateLimitDescription) DoOption {
	return func(resp *WrapperResponse) error {
		rl, err := helpers.ExtractRateLimitData(resp.StatusCode, &resp.Header)
		if err != nil {
			return err
		}

		resource.Limit = rl.Limit
		resource.Remaining = rl.Remaining
		resource.ResetAt = rl.ResetAt
		resource.Status = rl.Status

		return nil
	}
}

func WithXMLResponse(response interface{}) DoOption {
	return func(resp *WrapperResponse) error {
		if !helpers.IsXMLContentType(resp.Header.Get(ContentType)) {
			return fmt.Errorf("unexpected content type for xml response: %s", resp.Header.Get(ContentType))
		}
		if response == nil && len(resp.Body) == 0 {
			return nil
		}
		err := xml.Unmarshal(resp.Body, response)
		if err != nil {
			return fmt.Errorf("failed to unmarshal xml response: %w. body %v", err, resp.Body)
		}
		return nil
	}
}

func WithResponse(response interface{}) DoOption {
	return func(resp *WrapperResponse) error {
		if helpers.IsJSONContentType(resp.Header.Get(ContentType)) {
			return WithJSONResponse(response)(resp)
		}
		if helpers.IsXMLContentType(resp.Header.Get(ContentType)) {
			return WithXMLResponse(response)(resp)
		}

		return status.Error(codes.Unknown, "unsupported content type")
	}
}

func (c *BaseHttpClient) Do(req *http.Request, options ...DoOption) (*http.Response, error) {
	var (
		cacheKey string
		err      error
		resp     *http.Response
	)
	l := ctxzap.Extract(req.Context())
	if req.Method == http.MethodGet {
		cacheKey, err = CreateCacheKey(req)
		if err != nil {
			return nil, err
		}

		resp, err = c.baseHttpCache.Get(cacheKey)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			l.Debug("http cache miss", zap.String("cacheKey", cacheKey), zap.String("url", req.URL.String()))
		} else {
			l.Debug("http cache hit", zap.String("cacheKey", cacheKey), zap.String("url", req.URL.String()))
		}
	}

	if resp == nil {
		resp, err = c.HttpClient.Do(req)
		if err != nil {
			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				if urlErr.Timeout() {
					return nil, status.Error(codes.DeadlineExceeded, fmt.Sprintf("request timeout: %v", urlErr.URL))
				}
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, status.Error(codes.DeadlineExceeded, "request timeout")
			}
			return nil, err
		}
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Replace resp.Body with a no-op closer so nobody has to worry about closing the reader.
	resp.Body = io.NopCloser(bytes.NewBuffer(body))

	wresp := WrapperResponse{
		Header:     resp.Header,
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Body:       body,
	}
	for _, option := range options {
		err = option(&wresp)
		if err != nil {
			return resp, err
		}
	}

	switch resp.StatusCode {
	case http.StatusRequestTimeout:
		return resp, status.Error(codes.DeadlineExceeded, resp.Status)
	case http.StatusTooManyRequests:
		return resp, status.Error(codes.Unavailable, resp.Status)
	case http.StatusNotFound:
		return resp, status.Error(codes.NotFound, resp.Status)
	case http.StatusUnauthorized:
		return resp, status.Error(codes.Unauthenticated, resp.Status)
	case http.StatusForbidden:
		return resp, status.Error(codes.PermissionDenied, resp.Status)
	case http.StatusNotImplemented:
		return resp, status.Error(codes.Unimplemented, resp.Status)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, status.Error(codes.Unknown, fmt.Sprintf("unexpected status code: %d", resp.StatusCode))
	}

	if req.Method == http.MethodGet && resp.StatusCode == http.StatusOK {
		err := c.baseHttpCache.Set(cacheKey, resp)
		if err != nil {
			l.Debug("error setting cache", zap.String("cacheKey", cacheKey), zap.String("url", req.URL.String()), zap.Error(err))
			return resp, err
		}
	}

	return resp, err
}

func WithHeader(key, value string) RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		return nil, map[string]string{
			key: value,
		}, nil
	}
}

func WithJSONBody(body interface{}) RequestOption {
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

func WithAcceptJSONHeader() RequestOption {
	return WithAccept(applicationJSON)
}

func WithContentTypeJSONHeader() RequestOption {
	return WithContentType(applicationJSON)
}

func WithAcceptXMLHeader() RequestOption {
	return WithAccept(applicationXML)
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

func (c *BaseHttpClient) NewRequest(ctx context.Context, method string, url *url.URL, options ...RequestOption) (*http.Request, error) {
	var buffer io.ReadWriter
	var headers map[string]string = make(map[string]string)
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
