package uhttp

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/pkg/retry"
)

// wrapAsTokenRequestError mirrors the shape wrapTransientNetworkError
// actually receives in production: oauth2.Transport's RoundTrip returns the
// token-source error unwrapped, and http.Client.Do wraps it in *url.Error
// before BaseHttpClient.Do ever sees it.
func wrapAsTokenRequestError(retrieveErr *oauth2.RetrieveError) error {
	return &url.Error{Op: "Post", URL: "https://example.com/oauth/token", Err: retrieveErr}
}

func TestWrapTransientNetworkError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "unexpected EOF",
			err:      io.ErrUnexpectedEOF,
			wantCode: codes.Unavailable,
			wantMsg:  "unexpected EOF",
		},
		{
			name: "connection reset",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: &net.OpError{
					Op:  "read",
					Net: "tcp",
					Err: os.NewSyscallError("read", syscall.ECONNRESET),
				},
			},
			wantCode: codes.Unavailable,
			wantMsg:  "connection reset",
		},
		{
			name: "connection refused",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: os.NewSyscallError("connect", syscall.ECONNREFUSED),
				},
			},
			wantCode: codes.Unavailable,
			wantMsg:  "connection refused",
		},
		{
			name: "broken pipe",
			err: &net.OpError{
				Op:  "write",
				Net: "tcp",
				Err: os.NewSyscallError("write", syscall.EPIPE),
			},
			wantCode: codes.Unavailable,
			wantMsg:  "broken pipe",
		},
		{
			name: "bare EOF",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: io.EOF,
			},
			wantCode: codes.Unavailable,
			wantMsg:  "connection closed before response",
		},
		{
			name: "host unreachable",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: os.NewSyscallError("connect", syscall.EHOSTUNREACH),
			},
			wantCode: codes.Unavailable,
			wantMsg:  "network unreachable",
		},
		{
			name: "network unreachable",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: os.NewSyscallError("connect", syscall.ENETUNREACH),
			},
			wantCode: codes.Unavailable,
			wantMsg:  "network unreachable",
		},
		{
			name: "network down",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: os.NewSyscallError("connect", syscall.ENETDOWN),
			},
			wantCode: codes.Unavailable,
			wantMsg:  "network unreachable",
		},
		{
			// NXDOMAIN is a configuration error, not a blip: terminal, so the
			// sync fails instead of retrying a hostname that will never
			// resolve. Pinned in detail by TestWrapTransientNetworkError_NXDOMAINIsTerminal.
			name: "dns not found",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.invalid",
				Err: &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: &net.DNSError{
						Err:        "no such host",
						Name:       "example.invalid",
						IsNotFound: true,
					},
				},
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "dns lookup failed",
		},
		{
			name: "dns timeout",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: &net.DNSError{
						Err:       "i/o timeout",
						Name:      "example.com",
						IsTimeout: true,
					},
				},
			},
			wantCode: codes.DeadlineExceeded,
			wantMsg:  "dns lookup timeout",
		},
		{
			name: "dns temporary",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: &net.DNSError{
						Err:         "server misbehaving",
						Name:        "example.com",
						IsTemporary: true,
					},
				},
			},
			wantCode: codes.Unavailable,
			wantMsg:  "temporary dns lookup failure",
		},
		{
			// http2 surfaces this as a plain error with no typed sentinel.
			name:     "http2 client connection lost",
			err:      fmt.Errorf(`Get "https://example.com": http2: client connection lost`),
			wantCode: codes.Unavailable,
			wantMsg:  "http2 client connection lost",
		},
		{
			// Production always delivers *oauth2.RetrieveError wrapped in
			// *url.Error (http.Client.Do's own wrapping), so this — not a
			// bare RetrieveError — is the shape the errors.As unwrap in
			// wrapTransientNetworkError actually has to see through.
			name: "oauth2 invalid_client (RFC 6749 error param, 401)",
			err: wrapAsTokenRequestError(&oauth2.RetrieveError{
				ErrorCode:        "invalid_client",
				ErrorDescription: "client authentication failed",
				Response:         &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized"},
			}),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_client: client authentication failed",
		},
		{
			// RFC 6749 §5.2 makes 400 the default status for invalid_client/
			// invalid_grant (401 is only a MAY for invalid_client). Relying on
			// GrpcCodeFromHTTPStatus(400) alone would produce InvalidArgument
			// for exactly the credentials-rejected case this exists to catch;
			// the "error" param must take priority over the HTTP status.
			name: "oauth2 invalid_client on the RFC default status (400)",
			err: wrapAsTokenRequestError(&oauth2.RetrieveError{
				ErrorCode: "invalid_client",
				Response:  &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
			}),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_client",
		},
		{
			// Some token endpoints report the RFC 6749 error param on a 2xx
			// response. GrpcCodeFromHTTPStatus(200) would silently map this to
			// Unknown with a misleading "200 OK" message if the error param
			// weren't consulted first.
			name: "oauth2 error param on a 200 response",
			err: wrapAsTokenRequestError(&oauth2.RetrieveError{
				ErrorCode: "invalid_client",
				Response:  &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			}),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_client",
		},
		{
			name: "oauth2 access_denied",
			err: wrapAsTokenRequestError(&oauth2.RetrieveError{
				ErrorCode: "access_denied",
				Response:  &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden"},
			}),
			wantCode: codes.PermissionDenied,
			wantMsg:  "access_denied",
		},
		{
			name: "oauth2 invalid_grant maps to InvalidArgument, not an auth failure",
			err: wrapAsTokenRequestError(&oauth2.RetrieveError{
				ErrorCode: "invalid_grant",
				Response:  &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
			}),
			wantCode: codes.InvalidArgument,
			wantMsg:  "invalid_grant",
		},
		{
			// No RFC 6749 error param at all (a non-compliant or proxy-mangled
			// response) falls back to the plain HTTP status.
			name: "oauth2 token request rejected with no error param (403)",
			err: wrapAsTokenRequestError(&oauth2.RetrieveError{
				Response: &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden"},
			}),
			wantCode: codes.PermissionDenied,
			wantMsg:  "403 Forbidden",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapTransientNetworkError(tt.err)
			st, ok := status.FromError(got)
			require.True(t, ok, "got %v", got)
			require.Equal(t, tt.wantCode, st.Code())
			require.Contains(t, st.Message(), tt.wantMsg)
			require.ErrorIs(t, got, tt.err)
		})
	}
}

// NXDOMAIN is the one classification here that is deliberately terminal: a
// hostname that does not resolve is a misconfiguration, so retrying it burns
// the retry budget on every action and preserving the sync hides the cause.
// The transient DNS case is the control, proving the assertion is about
// NXDOMAIN specifically rather than about DNS failures in general.
func TestWrapTransientNetworkError_NXDOMAINIsTerminal(t *testing.T) {
	dnsFailure := func(dnsErr *net.DNSError) error {
		return &url.Error{
			Op:  "Get",
			URL: "https://example.invalid",
			Err: &net.OpError{Op: "dial", Net: "tcp", Err: dnsErr},
		}
	}
	newRetryer := func() *retry.Retryer {
		return retry.NewRetryer(t.Context(), retry.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Millisecond,
		})
	}

	nxdomain := wrapTransientNetworkError(dnsFailure(&net.DNSError{
		Err:        "no such host",
		Name:       "example.invalid",
		IsNotFound: true,
	}))
	require.Equal(t, codes.InvalidArgument, status.Code(nxdomain))
	require.False(t, newRetryer().ShouldWaitAndRetry(t.Context(), nxdomain),
		"a hostname that does not resolve must not be retried")

	temporary := wrapTransientNetworkError(dnsFailure(&net.DNSError{
		Err:         "server misbehaving",
		Name:        "example.invalid",
		IsTemporary: true,
	}))
	require.Equal(t, codes.Unavailable, status.Code(temporary))
	require.True(t, newRetryer().ShouldWaitAndRetry(t.Context(), temporary),
		"a temporary resolver failure must still be retried")
}

func TestWrapTransientNetworkError_LeavesNonTransientAlone(t *testing.T) {
	err := fmt.Errorf("something went wrong")
	got := wrapTransientNetworkError(err)
	require.Equal(t, err, got)

	st, ok := status.FromError(got)
	require.False(t, ok)
	require.Equal(t, codes.Unknown, st.Code())
}

// isStaleConnectionError gates the transport's own retry of idempotent
// requests, so the set it recognizes is a behavioral contract.
func TestIsStaleConnectionError(t *testing.T) {
	stale := []struct {
		name string
		err  error
	}{
		{
			name: "connection reset",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: os.NewSyscallError("read", syscall.ECONNRESET),
			},
		},
		{
			name: "broken pipe",
			err: &net.OpError{
				Op:  "write",
				Net: "tcp",
				Err: os.NewSyscallError("write", syscall.EPIPE),
			},
		},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF},
		{name: "http2 client connection lost", err: fmt.Errorf("http2: client connection lost")},
	}
	for _, tt := range stale {
		t.Run(tt.name, func(t *testing.T) {
			require.True(t, isStaleConnectionError(tt.err))
		})
	}

	notStale := []struct {
		name string
		err  error
	}{
		{
			name: "connection refused is a dial failure, not a stale pooled conn",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: os.NewSyscallError("connect", syscall.ECONNREFUSED),
			},
		},
		{name: "unrelated error", err: fmt.Errorf("something went wrong")},
	}
	for _, tt := range notStale {
		t.Run(tt.name, func(t *testing.T) {
			require.False(t, isStaleConnectionError(tt.err))
		})
	}
}
