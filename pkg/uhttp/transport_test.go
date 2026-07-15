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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// connCountingServer is an httptest server that counts connections the
// server has accepted and seen closed.
type connCountingServer struct {
	*httptest.Server
	mu     sync.Mutex
	opened int
	closed int
}

func newConnCountingServer(t *testing.T) *connCountingServer {
	t.Helper()
	s := &connCountingServer{}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch state {
		case http.StateNew:
			s.opened++
		case http.StateClosed:
			s.closed++
		default:
		}
	}
	s.Start()
	t.Cleanup(s.Close)
	return s
}

func (s *connCountingServer) openedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opened
}

func (s *connCountingServer) closedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func mustGet(t *testing.T, client *http.Client, url string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

// sequencedRoundTripper fails with errs in order (one per call), then
// answers 200 OK, recording call count and each request's body.
type sequencedRoundTripper struct {
	mu     sync.Mutex
	errs   []error
	calls  int
	bodies []string
}

func (s *sequencedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body := ""
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
		body = string(b)
	}
	s.bodies = append(s.bodies, body)
	call := s.calls
	s.calls++
	if call < len(s.errs) {
		return nil, s.errs[call]
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("ok")),
		Header:     http.Header{},
	}, nil
}

func (s *sequencedRoundTripper) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func connResetError() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://example.com",
		Err: &net.OpError{
			Op:  "read",
			Net: "tcp",
			Err: os.NewSyscallError("read", syscall.ECONNRESET),
		},
	}
}

func proxyConnectError() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://example.com",
		Err: &net.OpError{
			Op:  "proxyconnect",
			Net: "tcp",
			Err: fmt.Errorf("connect: connection timed out"),
		},
	}
}

func newRetryTestTransport(rt http.RoundTripper) *Transport {
	return &Transport{
		roundTripper: rt,
		nextCycle:    time.Now().Add(time.Minute),
	}
}

func postWithBody(t *testing.T, payload string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://example.com", strings.NewReader(payload))
	require.NoError(t, err)
	return req
}

func TestTransportRetriesDialPhaseErrorsForAnyMethod(t *testing.T) {
	seq := &sequencedRoundTripper{errs: []error{proxyConnectError()}}
	tp := newRetryTestTransport(seq)

	resp, err := tp.RoundTrip(postWithBody(t, "payload"))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 2, seq.callCount())
	// The retried request carries a rewound copy of the body.
	require.Equal(t, []string{"payload", "payload"}, seq.bodies)
}

func TestTransportRetriesStaleConnForDeclaredIdempotentPOST(t *testing.T) {
	seq := &sequencedRoundTripper{errs: []error{connResetError()}}
	tp := newRetryTestTransport(seq)

	req := postWithBody(t, "payload")
	req.Header.Set("Idempotency-Key", "test-key")
	resp, err := tp.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 2, seq.callCount())
}

func TestTransportRetriesStaleConnForGET(t *testing.T) {
	seq := &sequencedRoundTripper{errs: []error{connResetError()}}
	tp := newRetryTestTransport(seq)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	resp, err := tp.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 2, seq.callCount())
}

func TestTransportDropsIdlePoolBeforeStaleConnRetry(t *testing.T) {
	srv := newConnCountingServer(t)

	// Park one live idle connection in a real base transport's pool.
	base := &http.Transport{}
	t.Cleanup(base.CloseIdleConnections)
	mustGet(t, &http.Client{Transport: base}, srv.URL)
	require.Equal(t, 1, srv.openedCount())

	seq := &sequencedRoundTripper{errs: []error{connResetError()}}
	tp := newRetryTestTransport(seq)
	tp.baseTransport = base

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	resp, err := tp.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 2, seq.callCount())

	// The stale-connection retry must drop the pooled connections before
	// its second attempt, so it cannot draw another dead connection after
	// a mass death event.
	require.Eventually(t, func() bool { return srv.closedCount() >= 1 }, 2*time.Second, 10*time.Millisecond)
}

func TestTransportDoesNotRetryStaleConnForUndeclaredPOST(t *testing.T) {
	seq := &sequencedRoundTripper{errs: []error{connResetError()}}
	tp := newRetryTestTransport(seq)

	resp, err := tp.RoundTrip(postWithBody(t, "payload"))
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 1, seq.callCount())

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
}

func TestTransportDoesNotRetryWithoutRewindableBody(t *testing.T) {
	seq := &sequencedRoundTripper{errs: []error{proxyConnectError()}}
	tp := newRetryTestTransport(seq)

	req := postWithBody(t, "payload")
	req.GetBody = nil
	resp, err := tp.RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, 1, seq.callCount())
}

func TestTransportDropsIdleConnectionsAfterWallClockGap(t *testing.T) {
	srv := newConnCountingServer(t)

	tp, err := NewTransport(t.Context())
	require.NoError(t, err)

	now := time.Now()
	tp.nowFn = func() time.Time { return now }

	client := &http.Client{Transport: tp}

	mustGet(t, client, srv.URL)
	require.Equal(t, 1, srv.openedCount())

	// A short pause keeps the pooled connection.
	now = now.Add(time.Second)
	mustGet(t, client, srv.URL)
	require.Equal(t, 1, srv.openedCount())

	// A freeze-sized wall-clock gap drops the pool; the next request
	// dials a fresh connection instead of reusing a presumed-dead one.
	now = now.Add(staleConnectionGap + time.Second)
	mustGet(t, client, srv.URL)
	require.Equal(t, 2, srv.openedCount())
	require.Eventually(t, func() bool { return srv.closedCount() >= 1 }, 2*time.Second, 10*time.Millisecond)
}

func TestTransportCycleClosesAbandonedConnections(t *testing.T) {
	srv := newConnCountingServer(t)

	tp, err := NewTransport(t.Context())
	require.NoError(t, err)

	client := &http.Client{Transport: tp}

	mustGet(t, client, srv.URL)
	require.Equal(t, 1, srv.openedCount())

	// Force the next request to rebuild the round tripper.
	tp.mtx.Lock()
	tp.nextCycle = time.Time{}
	tp.mtx.Unlock()

	// The rebuild swaps in a fresh pool and closes the abandoned one.
	mustGet(t, client, srv.URL)
	require.Equal(t, 2, srv.openedCount())
	require.Eventually(t, func() bool { return srv.closedCount() >= 1 }, 2*time.Second, 10*time.Millisecond)
}

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
