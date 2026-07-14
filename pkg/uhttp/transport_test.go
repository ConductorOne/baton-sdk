package uhttp

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type errorRoundTripper struct {
	err error
}

func (rt errorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, rt.err
}

func TestTransportRoundTrip_ClassifiesConnectionReset(t *testing.T) {
	tp := &Transport{
		roundTripper: errorRoundTripper{
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: &net.OpError{
					Op:  "read",
					Net: "tcp",
					Err: os.NewSyscallError("read", syscall.ECONNRESET),
				},
			},
		},
		nextCycle: time.Now().Add(time.Minute),
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := tp.RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Nil(t, resp)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, st.Message(), "connection reset")
}

func TestTransportRoundTrip_ClassifiesTimeout(t *testing.T) {
	tp := &Transport{
		roundTripper: errorRoundTripper{
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: timeoutError{err: fmt.Errorf("i/o timeout")},
			},
		},
		nextCycle: time.Now().Add(time.Minute),
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := tp.RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Nil(t, resp)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
	require.Contains(t, st.Message(), "request timeout")
}

func TestTransportRoundTrip_ClassifiesHTTP2ClientConnectionLost(t *testing.T) {
	tp := &Transport{
		roundTripper: errorRoundTripper{
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: fmt.Errorf("http2: client connection lost"),
			},
		},
		nextCycle: time.Now().Add(time.Minute),
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := tp.RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Nil(t, resp)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, st.Message(), "http2 client connection lost")
}

func TestTransportRoundTrip_ClassifiesNetErrorTimeout(t *testing.T) {
	tp := &Transport{
		roundTripper: errorRoundTripper{
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: timeoutError{err: fmt.Errorf("net/http: TLS handshake timeout")},
			},
		},
		nextCycle: time.Now().Add(time.Minute),
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)

	resp, err := tp.RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Nil(t, resp)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
	require.Contains(t, st.Message(), "network timeout")
}

type timeoutError struct {
	err error
}

func (e timeoutError) Error() string {
	return e.err.Error()
}

func (timeoutError) Timeout() bool {
	return true
}

func (timeoutError) Temporary() bool {
	return false
}

func TestTransportDropsPooledConnsAfterWallClockJump(t *testing.T) {
	var newConns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	ctx := context.Background()
	tp, err := NewTransport(ctx)
	require.NoError(t, err)
	client := &http.Client{Transport: tp}

	doGet := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}

	doGet()
	require.Equal(t, int32(1), newConns.Load())

	// no wall-clock gap: the pooled connection is reused
	doGet()
	require.Equal(t, int32(1), newConns.Load())

	// simulate a Lambda freeze/thaw: the wall clock jumped while the
	// monotonic clock (and IdleConnTimeout) stood still
	tp.wallMtx.Lock()
	tp.lastActivityWall = time.Now().Round(0).Add(-time.Minute)
	tp.wallMtx.Unlock()

	doGet()
	require.Equal(t, int32(2), newConns.Load())
}
