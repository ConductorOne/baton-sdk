package uhttp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
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
	require.Nil(t, resp)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
	require.Contains(t, st.Message(), "request timeout")
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
