package uhttp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsReconnectableError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"http2 connection lost", errors.New("http2: client connection lost"), true},
		{"econnreset", &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, true},
		{"epipe", os.NewSyscallError("write", syscall.EPIPE), true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"eof", io.EOF, true},
		{"net closed", net.ErrClosed, true},
		{"deadline exceeded is not reconnectable", context.DeadlineExceeded, false},
		{"unrelated error", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsReconnectableError(tc.err))
		})
	}
}

// fakeRoundTripper invokes fn on each RoundTrip, passing the zero-based attempt
// number, and records how many times it was called.
type fakeRoundTripper struct {
	calls int
	fn    func(attempt int, req *http.Request) (*http.Response, error)
}

func (f *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	n := f.calls
	f.calls++
	return f.fn(n, req)
}

func okResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("ok")),
		Header:     make(http.Header),
	}
}

func testRetryTripper(next http.RoundTripper, maxAttempts int) *retryRoundTripper {
	return &retryRoundTripper{next: next, max: maxAttempts, baseDelay: time.Millisecond, maxDelay: 2 * time.Millisecond}
}

func connLost() error { return errors.New("http2: client connection lost") }

func getReq(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	return req
}

func TestRetryRoundTripper_RetriesIdempotentUntilSuccess(t *testing.T) {
	f := &fakeRoundTripper{fn: func(attempt int, _ *http.Request) (*http.Response, error) {
		if attempt < 2 {
			return nil, connLost()
		}
		return okResponse(), nil
	}}

	resp, err := testRetryTripper(f, 3).RoundTrip(getReq(t))
	if resp != nil {
		defer resp.Body.Close()
	}
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 3, f.calls) // 2 failures + 1 success
}

func TestRetryRoundTripper_SuccessOnFirstTry(t *testing.T) {
	f := &fakeRoundTripper{fn: func(_ int, _ *http.Request) (*http.Response, error) {
		return okResponse(), nil
	}}

	resp, err := testRetryTripper(f, 3).RoundTrip(getReq(t))
	if resp != nil {
		defer resp.Body.Close()
	}
	require.NoError(t, err)
	require.Equal(t, 1, f.calls)
}

func TestRetryRoundTripper_StopsAtMax(t *testing.T) {
	f := &fakeRoundTripper{fn: func(_ int, _ *http.Request) (*http.Response, error) {
		return nil, connLost()
	}}

	resp, err := testRetryTripper(f, 2).RoundTrip(getReq(t))
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 3, f.calls) // initial attempt + 2 retries
}

func TestRetryRoundTripper_DoesNotRetryNonReconnectable(t *testing.T) {
	f := &fakeRoundTripper{fn: func(_ int, _ *http.Request) (*http.Response, error) {
		return nil, errors.New("some non-connection error")
	}}

	resp, err := testRetryTripper(f, 3).RoundTrip(getReq(t))
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 1, f.calls)
}

func TestRetryRoundTripper_DoesNotRetryPOST(t *testing.T) {
	f := &fakeRoundTripper{fn: func(_ int, _ *http.Request) (*http.Response, error) {
		return nil, connLost()
	}}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example.com", strings.NewReader("payload"))
	require.NoError(t, err)

	resp, err := testRetryTripper(f, 3).RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 1, f.calls) // non-idempotent, not retried
}

func TestRetryRoundTripper_RetriesPOSTWithIdempotencyKeyAndRewindsBody(t *testing.T) {
	var bodies []string
	f := &fakeRoundTripper{fn: func(attempt int, r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		if attempt == 0 {
			return nil, connLost()
		}
		return okResponse(), nil
	}}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example.com", strings.NewReader("payload"))
	require.NoError(t, err)
	req.Header.Set("Idempotency-Key", "abc")

	resp, err := testRetryTripper(f, 3).RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// Body was rewound and replayed intact on the retry.
	require.Equal(t, []string{"payload", "payload"}, bodies)
}

func TestRetryRoundTripper_DoesNotRetryNonRewindableBody(t *testing.T) {
	f := &fakeRoundTripper{fn: func(_ int, _ *http.Request) (*http.Response, error) {
		return nil, connLost()
	}}
	req := getReq(t)
	// A GET is idempotent, but a body without GetBody cannot be safely replayed.
	req.Body = io.NopCloser(strings.NewReader("x"))
	req.GetBody = nil

	resp, err := testRetryTripper(f, 3).RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 1, f.calls)
}

func TestRetryRoundTripper_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeRoundTripper{fn: func(attempt int, _ *http.Request) (*http.Response, error) {
		if attempt == 0 {
			cancel()
		}
		return nil, connLost()
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := testRetryTripper(f, 5).RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 1, f.calls) // context cancelled after first failure; no retry
}

func TestResponseHeaderTimeout_OptionAndContextResolution(t *testing.T) {
	t.Run("unset leaves nil (make applies default)", func(t *testing.T) {
		tr, err := NewTransport(context.Background())
		require.NoError(t, err)
		require.Nil(t, tr.responseHeaderTimeout)
	})
	t.Run("resolved from context (CLI/config)", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ContextHTTPResponseHeaderTimeoutKey, 90*time.Second)
		tr, err := NewTransport(ctx)
		require.NoError(t, err)
		require.NotNil(t, tr.responseHeaderTimeout)
		require.Equal(t, 90*time.Second, *tr.responseHeaderTimeout)
	})
	t.Run("explicit option beats context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ContextHTTPResponseHeaderTimeoutKey, 90*time.Second)
		tr, err := NewTransport(ctx, WithResponseHeaderTimeout(5*time.Minute))
		require.NoError(t, err)
		require.NotNil(t, tr.responseHeaderTimeout)
		require.Equal(t, 5*time.Minute, *tr.responseHeaderTimeout)
	})
}

func TestRetryRoundTripper_DisabledWhenMaxZero(t *testing.T) {
	// max <= 0 is guarded at construction in make(); assert the tripper itself
	// makes no retries when max is 0.
	f := &fakeRoundTripper{fn: func(_ int, _ *http.Request) (*http.Response, error) {
		return nil, connLost()
	}}

	resp, err := testRetryTripper(f, 0).RoundTrip(getReq(t))
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 1, f.calls)
}
