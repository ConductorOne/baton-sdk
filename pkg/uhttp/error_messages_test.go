package uhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/retry"
)

// statusLines counts the gRPC status lines an error prints. Every error a
// failed request returns states its status exactly once.
func statusLines(err error) int {
	return strings.Count(err.Error(), "rpc error:")
}

func respond(statusCode int, contentType, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if contentType != "" {
			w.Header().Set(ContentType, contentType)
		}
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}
}

func newGet(ctx context.Context, t *testing.T, c *BaseHttpClient, rawURL string) *http.Request {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	req, err := c.NewRequest(ctx, http.MethodGet, u, WithNoCache())
	require.NoError(t, err)
	return req
}

func doGet(ctx context.Context, t *testing.T, c *BaseHttpClient, rawURL string, opts ...DoOption) error {
	t.Helper()
	resp, err := c.Do(newGet(ctx, t, c, rawURL), opts...)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return ln
}

// closedAddr is an address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln := listen(t)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func newPlainClient(t *testing.T) *BaseHttpClient {
	t.Helper()
	c, err := NewBaseHttpClientWithContext(t.Context(), &http.Client{})
	require.NoError(t, err)
	return c
}

func newTransportClient(t *testing.T) *BaseHttpClient {
	t.Helper()
	httpClient, err := NewClient(t.Context(), WithLogger(true, zap.NewNop()))
	require.NoError(t, err)
	c, err := NewBaseHttpClientWithContext(t.Context(), httpClient)
	require.NoError(t, err)
	return c
}

type decodeTarget struct {
	Name string `json:"name"`
}

func TestDo_NonSuccessLeavesOutSuccessDecoderErrors(t *testing.T) {
	var errResp ErrResponse
	tests := []struct {
		name     string
		handler  http.HandlerFunc
		opts     func() []DoOption
		wantCode codes.Code
		want     string
		notWant  []string
	}{
		{
			name:     "empty 401 with WithJSONResponse",
			handler:  respond(http.StatusUnauthorized, "", ""),
			opts:     func() []DoOption { return []DoOption{WithJSONResponse(&decodeTarget{})} },
			wantCode: codes.Unauthenticated,
			want:     "rpc error: code = Unauthenticated desc = 401 Unauthorized",
			notWant:  []string{"unexpected content type"},
		},
		{
			name:     "JSON 403 with WithErrorResponse and a list decoder",
			handler:  respond(http.StatusForbidden, "application/json", `{"title":"forbidden","detail":"insufficient privileges"}`),
			opts:     func() []DoOption { return []DoOption{WithErrorResponse(&errResp), WithJSONResponse(&[]decodeTarget{})} },
			wantCode: codes.PermissionDenied,
			want:     "Request failed with status 403: forbidden: insufficient privileges",
			notWant:  []string{"unmarshal", "decodeTarget"},
		},
		{
			name:     "HTML 404 with WithResponse",
			handler:  respond(http.StatusNotFound, "text/html", "<html></html>"),
			opts:     func() []DoOption { return []DoOption{WithResponse(&decodeTarget{})} },
			wantCode: codes.NotFound,
			notWant:  []string{"unsupported content type"},
		},
		{
			name:    "HTML 500 with WithGenericResponse",
			handler: respond(http.StatusInternalServerError, "text/html", "<html></html>"),
			opts: func() []DoOption {
				generic := map[string]any{}
				return []DoOption{WithGenericResponse(&generic)}
			},
			wantCode: codes.Unavailable,
			notWant:  []string{"unsupported content type"},
		},
		{
			name:     "JSON 400 with WithXMLResponse",
			handler:  respond(http.StatusBadRequest, "application/json", `{"error":"bad"}`),
			opts:     func() []DoOption { return []DoOption{WithXMLResponse(&decodeTarget{})} },
			wantCode: codes.InvalidArgument,
			notWant:  []string{"unexpected content type for xml"},
		},
		{
			name:     "text 502 with WithAlwaysJSONResponse",
			handler:  respond(http.StatusBadGateway, "text/plain", "bad gateway"),
			opts:     func() []DoOption { return []DoOption{WithAlwaysJSONResponse(&decodeTarget{})} },
			wantCode: codes.Unavailable,
			notWant:  []string{"failed to unmarshal json response"},
		},
		{
			name:     "text 502 with WithAlwaysXMLResponse",
			handler:  respond(http.StatusBadGateway, "text/plain", "bad gateway"),
			opts:     func() []DoOption { return []DoOption{WithAlwaysXMLResponse(&decodeTarget{})} },
			wantCode: codes.Unavailable,
			notWant:  []string{"failed to unmarshal xml response"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			err := doGet(t.Context(), t, newPlainClient(t), srv.URL, tt.opts()...)
			require.Error(t, err)
			require.Equal(t, tt.wantCode, status.Code(err))
			require.Contains(t, err.Error(), tt.want)
			for _, notWant := range tt.notWant {
				require.NotContains(t, err.Error(), notWant)
			}
		})
	}
}

// The success decoder keeps running on a non-2xx, so a connector that reads
// the error body through the success shape still finds it decoded.
func TestDo_NonSuccessStillDecodesIntoTheTarget(t *testing.T) {
	srv := httptest.NewServer(respond(http.StatusForbidden, "application/json", `{"name":"denied"}`))
	defer srv.Close()

	var target decodeTarget
	err := doGet(t.Context(), t, newPlainClient(t), srv.URL, WithJSONResponse(&target))
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, "denied", target.Name)
}

func TestDo_SuccessDecodeFailureIsStillReported(t *testing.T) {
	srv := httptest.NewServer(respond(http.StatusOK, "text/plain", "not json"))
	defer srv.Close()

	err := doGet(t.Context(), t, newPlainClient(t), srv.URL, WithJSONResponse(&decodeTarget{}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected content type for JSON response: text/plain")
}

func TestWrapErrors_DoNotRestateACarriedStatus(t *testing.T) {
	header := http.Header{}
	header.Set("X-Ratelimit-Limit", "100")
	header.Set("X-Ratelimit-Remaining", "0")
	header.Set("X-Ratelimit-Reset", "60")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range header {
			w.Header()[k] = v
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := newPlainClient(t)
	resp, doErr := c.Do(newGet(t.Context(), t, c, srv.URL), WithJSONResponse(&decodeTarget{}))
	require.Error(t, doErr)
	defer resp.Body.Close()

	t.Run("same status from WrapErrorsWithRateLimitInfo", func(t *testing.T) {
		got := WrapErrorsWithRateLimitInfo(codes.Unauthenticated, resp, doErr)
		require.Equal(t, 1, statusLines(got), got.Error())
		require.Equal(t, codes.Unauthenticated, status.Code(got))
		require.ErrorIs(t, got, doErr)

		var found *v2.RateLimitDescription
		for _, detail := range status.Convert(got).Details() {
			if rl, ok := detail.(*v2.RateLimitDescription); ok {
				found = rl
			}
		}
		require.NotNil(t, found, "the rate limit must survive")
	})

	t.Run("a stale rate limit does not hide the new one", func(t *testing.T) {
		stale, err := status.New(codes.Unauthenticated, resp.Status).WithDetails(&v2.RateLimitDescription{
			Limit:     100,
			Remaining: 50,
		})
		require.NoError(t, err)

		got := WrapErrorsWithRateLimitInfo(codes.Unauthenticated, resp, stale.Err())
		var found *v2.RateLimitDescription
		for _, detail := range status.Convert(got).Details() {
			if rl, ok := detail.(*v2.RateLimitDescription); ok {
				found = rl
			}
		}
		require.NotNil(t, found)
		require.Equal(t, int64(0), found.GetRemaining())
	})

	t.Run("a carried status without the rate limit does not drop it", func(t *testing.T) {
		got := WrapErrorsWithRateLimitInfo(codes.Unauthenticated, resp, WrapErrors(codes.Unauthenticated, resp.Status))
		var found *v2.RateLimitDescription
		for _, detail := range status.Convert(got).Details() {
			if rl, ok := detail.(*v2.RateLimitDescription); ok {
				found = rl
			}
		}
		require.NotNil(t, found)
	})

	t.Run("a different code is a reclassification and adds its status", func(t *testing.T) {
		got := WrapErrorsWithRateLimitInfo(codes.Unavailable, resp, doErr)
		require.Equal(t, codes.Unavailable, status.Code(got))
		require.ErrorIs(t, got, doErr)
	})

	t.Run("a different message still adds its context", func(t *testing.T) {
		got := WrapErrors(codes.Unauthenticated, "list users", doErr)
		require.Contains(t, got.Error(), "list users")
		require.Equal(t, codes.Unauthenticated, status.Code(got))
	})

	t.Run("same status from WrapErrors", func(t *testing.T) {
		inner := status.Error(codes.NotFound, "user not found")
		got := WrapErrors(codes.NotFound, "user not found", inner)
		require.Equal(t, 1, statusLines(got), got.Error())
		require.ErrorIs(t, got, inner)
	})

	t.Run("a carried status keeps priority over other errors", func(t *testing.T) {
		other := errors.New("decode failed")
		inner := status.Error(codes.NotFound, "user not found")
		got := WrapErrors(codes.NotFound, "user not found", other, inner)
		require.Equal(t, codes.NotFound, status.Code(got))
		require.Equal(t, 1, statusLines(got), got.Error())
		require.ErrorIs(t, got, other)
	})

	t.Run("an earlier status of another code does not hide the one this call asked for", func(t *testing.T) {
		earlier := status.Error(codes.Internal, "decode failed")
		inner := status.Error(codes.NotFound, "user not found")
		got := WrapErrors(codes.NotFound, "user not found", earlier, inner)
		require.Equal(t, codes.NotFound, status.Code(got))
		require.ErrorIs(t, got, earlier)
		require.ErrorIs(t, got, inner)
	})
}

// Reclassifying a non-2xx reads the new code and keeps what the vendor said
// through WithErrorResponse, which is a status of its own.
func TestWrapErrors_ReclassifyingKeepsTheVendorMessage(t *testing.T) {
	srv := httptest.NewServer(respond(http.StatusForbidden, "application/json", `{"title":"not found","detail":"no such group"}`))
	defer srv.Close()

	c := newPlainClient(t)
	var errResp ErrResponse
	resp, doErr := c.Do(newGet(t.Context(), t, c, srv.URL), WithErrorResponse(&errResp))
	require.Error(t, doErr)
	defer resp.Body.Close()

	got := WrapErrorsWithRateLimitInfo(codes.NotFound, resp, doErr)
	require.Equal(t, codes.NotFound, status.Code(got))
	require.Contains(t, got.Error(), "Request failed with status 403: not found: no such group")
}

// transportFailure starts a server that fails the way name says and returns
// the URL to request and the context to request it with.
type transportFailure struct {
	name              string
	setup             func(t *testing.T) (string, context.Context)
	wantCode          codes.Code
	descOmitsEndpoint bool
	check             func(t *testing.T, err error)
}

func transportFailures() []transportFailure {
	return []transportFailure{
		{
			name: "connection refused",
			setup: func(t *testing.T) (string, context.Context) {
				return "http://" + closedAddr(t) + "/users?token=secret", t.Context()
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "connection closed before response",
			setup: func(t *testing.T) (string, context.Context) {
				ln := listen(t)
				t.Cleanup(func() { _ = ln.Close() })
				go func() {
					for {
						conn, err := ln.Accept()
						if err != nil {
							return
						}
						buf := make([]byte, 1024)
						_, _ = conn.Read(buf)
						_ = conn.Close()
					}
				}()
				return "http://" + ln.Addr().String() + "/users?token=secret", t.Context()
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "untrusted certificate",
			setup: func(t *testing.T) (string, context.Context) {
				srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
				srv.Config.ErrorLog = nil
				srv.StartTLS()
				t.Cleanup(srv.Close)
				return srv.URL + "/users?token=secret", t.Context()
			},
			wantCode: codes.FailedPrecondition,
			check: func(t *testing.T, err error) {
				var certErr *tls.CertificateVerificationError
				require.ErrorAs(t, err, &certErr)
				require.Contains(t, status.Convert(err).Message(), "tls certificate verification failed")
				require.Contains(t, status.Convert(err).Message(), "x509:", "the reason the certificate failed must reach the status")
			},
		},
		{
			name: "context deadline",
			setup: func(t *testing.T) (string, context.Context) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
					}
				}))
				t.Cleanup(srv.Close)
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				t.Cleanup(cancel)
				return srv.URL + "/users?token=secret", ctx
			},
			wantCode: codes.DeadlineExceeded,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			},
		},
		{
			name: "context canceled",
			setup: func(t *testing.T) (string, context.Context) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					<-r.Context().Done()
				}))
				t.Cleanup(srv.Close)
				ctx, cancel := context.WithCancel(t.Context())
				time.AfterFunc(100*time.Millisecond, cancel)
				return srv.URL + "/users?token=secret", ctx
			},
			wantCode: codes.Canceled,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, context.Canceled)
			},
		},
	}
}

func TestWrapTransientNetworkError_DescriptionNamesHostAndPath(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "dns",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.invalid/users?token=secret",
				Err: &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: &net.DNSError{Err: "no such host", Name: "example.invalid", IsNotFound: true},
				},
			},
			want: "dns lookup failed: NXDOMAIN: example.invalid/users",
		},
		{
			name: "http2",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com/v1?token=secret",
				Err: errors.New("http2: client connection lost"),
			},
			want: "http2 client connection lost: example.com/v1",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapTransientNetworkError(tt.err)
			desc, _, _ := strings.Cut(status.Convert(got).Message(), "\n")
			require.Contains(t, desc, tt.want)
			require.NotContains(t, desc, "token=secret")
		})
	}
}

func TestDo_TransportErrorsStateTheStatusOnceAndNameTheHost(t *testing.T) {
	clients := map[string]func(t *testing.T) *BaseHttpClient{
		"uhttp transport": newTransportClient,
		"plain client":    newPlainClient,
	}
	for clientName, newClient := range clients {
		for _, tt := range transportFailures() {
			t.Run(clientName+"/"+tt.name, func(t *testing.T) {
				rawURL, ctx := tt.setup(t)
				u, err := url.Parse(rawURL)
				require.NoError(t, err)

				err = doGet(ctx, t, newClient(t), rawURL)
				require.Error(t, err)
				require.Equal(t, tt.wantCode, status.Code(err), err.Error())
				require.Equal(t, 1, statusLines(err), err.Error())
				require.Contains(t, status.Convert(err).Message(), u.Host)
				if !tt.descOmitsEndpoint {
					require.Contains(t, status.Convert(err).Message(), ": "+u.Host+u.Path)
				}

				var urlErr *url.Error
				require.ErrorAs(t, err, &urlErr, "the original chain must stay reachable")
				if tt.check != nil {
					tt.check(t, err)
				}
			})
		}
	}
}

func TestWrapTransientNetworkError_UntrustedCertificateIsTerminal(t *testing.T) {
	err := wrapTransientNetworkError(&url.Error{
		Op:  "Get",
		URL: "https://example.com",
		Err: &tls.CertificateVerificationError{Err: errors.New("x509: certificate signed by unknown authority")},
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	retryer := retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond})
	require.False(t, retryer.ShouldWaitAndRetry(t.Context(), err), "a certificate that is not trusted will not become trusted on retry")
}

func TestWithErrorResponse_BoundsTheBodyItQuotes(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		want        string
		notWant     []string
	}{
		{
			name:        "markup is omitted",
			contentType: "text/html",
			body:        "<html><body>internal page</body></html>",
			want:        "body omitted",
			notWant:     []string{"internal page"},
		},
		{
			name:        "the text ahead of markup is kept",
			contentType: "text/html",
			body:        "CSRF nonce validation failed<!DOCTYPE html><html></html>",
			want:        "body: CSRF nonce validation failed",
			notWant:     []string{"DOCTYPE"},
		},
		{
			name:        "long text is capped",
			contentType: "text/plain",
			body:        strings.Repeat("a", 1000),
			want:        strings.Repeat("a", maxQuotedBodyLength) + "...",
			notWant:     []string{strings.Repeat("a", maxQuotedBodyLength+1)},
		},
		{
			name:        "a JSON body that does not decode is omitted",
			contentType: "application/json",
			body:        `["alice@example.com"]`,
			want:        "body omitted",
			notWant:     []string{"alice@example.com"},
		},
		{
			name:        "an empty body says nothing about the body",
			contentType: "text/plain",
			body:        "",
			notWant:     []string{"body"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			header.Set(ContentType, tt.contentType)
			var errResp ErrResponse
			err := WithErrorResponse(&errResp)(&WrapperResponse{
				Header:     header,
				StatusCode: http.StatusForbidden,
				Body:       []byte(tt.body),
			})
			require.Error(t, err)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Contains(t, err.Error(), tt.want)
			for _, notWant := range tt.notWant {
				require.NotContains(t, err.Error(), notWant)
			}
		})
	}
}

func TestDo_ErrorLogsIdentifyTheRequest(t *testing.T) {
	// The request fields leave the query string out. The error field is the
	// error as it is, so it still prints what the standard library put in it.
	requestFields := func(t *testing.T, entry observer.LoggedEntry, u *url.URL) {
		t.Helper()
		fields := entry.ContextMap()
		require.Equal(t, http.MethodGet, fields["http.method"])
		require.Equal(t, u.Host, fields["http.url_details.host"])
		require.Equal(t, u.Path, fields["http.url_details.path"])
		for key, value := range fields {
			if s, ok := value.(string); ok && key != "error" {
				require.NotContains(t, s, "token=secret")
			}
		}
	}

	t.Run("error status", func(t *testing.T) {
		srv := httptest.NewServer(respond(http.StatusNotFound, "", ""))
		defer srv.Close()
		u, err := url.Parse(srv.URL + "/users/42?token=secret")
		require.NoError(t, err)

		core, logs := observer.New(zapcore.DebugLevel)
		ctx := ctxzap.ToContext(t.Context(), zap.New(core))
		err = doGet(ctx, t, newPlainClient(t), u.String())
		require.Error(t, err)

		entries := logs.FilterMessage("base-http-client: HTTP error status").All()
		require.Len(t, entries, 1)
		requestFields(t, entries[0], u)
	})

	t.Run("no response", func(t *testing.T) {
		u, err := url.Parse("http://" + closedAddr(t) + "/users/42?token=secret")
		require.NoError(t, err)

		core, logs := observer.New(zapcore.DebugLevel)
		ctx := ctxzap.ToContext(t.Context(), zap.New(core))
		err = doGet(ctx, t, newPlainClient(t), u.String())
		require.Error(t, err)

		entries := logs.FilterMessage("base-http-client: HTTP error response").All()
		require.Len(t, entries, 1)
		requestFields(t, entries[0], u)
	})
}

func TestDo_NilURLReturnsTheClientError(t *testing.T) {
	c := newPlainClient(t)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.com/users", nil)
	require.NoError(t, err)
	req.URL = nil

	resp, err := c.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	require.ErrorContains(t, err, "http: nil Request.URL")
}
