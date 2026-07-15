package sourcecache

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

type fakeLookupClient struct {
	resp *v1.LookupResponse
	err  error
}

func (f *fakeLookupClient) Lookup(context.Context, *v1.LookupRequest, ...grpc.CallOption) (*v1.LookupResponse, error) {
	return f.resp, f.err
}

// TestGRPCLookupDegradesRPCErrorToMiss pins the Lookup contract for the
// subprocess topology: a transport failure to the parent degrades to a
// miss (the connector fetches fresh — always safe) instead of failing the
// connector's list call, whose error would rarely keep a retryable status
// code by the time it reaches the syncer's retryer.
func TestGRPCLookupDegradesRPCErrorToMiss(t *testing.T) {
	ctx := context.Background()
	lookup := NewGRPCLookup(&fakeLookupClient{err: errors.New("connection refused")})

	scope := HashScope("https://example.test/teams/1/members?page=1")
	_, found, err := lookup.Lookup(ctx, RowKindGrants, scope)
	require.NoError(t, err, "an RPC failure must degrade to a miss, not fail the connector call")
	require.False(t, found)
}

// capturedLog is one recorded zap entry (message + flattened fields).
// zaptest/observer is not vendored, so recordingCore stands in for it
// (same pattern as pkg/c1zsanitize).
type capturedLog struct {
	msg    string
	fields map[string]any
}

type recordingCore struct {
	mu   *sync.Mutex
	logs *[]capturedLog
}

func (c recordingCore) Enabled(zapcore.Level) bool        { return true }
func (c recordingCore) With([]zapcore.Field) zapcore.Core { return c }
func (c recordingCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(e, c)
}
func (c recordingCore) Sync() error { return nil }
func (c recordingCore) Write(e zapcore.Entry, fs []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fs {
		f.AddTo(enc)
	}
	c.mu.Lock()
	*c.logs = append(*c.logs, capturedLog{msg: e.Message, fields: enc.Fields})
	c.mu.Unlock()
	return nil
}

func newRecordingContext() (context.Context, *[]capturedLog) {
	logs := &[]capturedLog{}
	logger := zap.New(recordingCore{mu: &sync.Mutex{}, logs: logs})
	return ctxzap.ToContext(context.Background(), logger), logs
}

// TestGRPCLookupFailureLogRateLimited pins the failure-log policy: the
// degraded-to-miss warning is rate-limited (not once-ever — this object
// lives for the whole subprocess run), carries a running failure count,
// and is skipped entirely when the caller's context is already done so
// teardown-time cancellation can't consume the rate window.
func TestGRPCLookupFailureLogRateLimited(t *testing.T) {
	ctx, logs := newRecordingContext()
	lookup := NewGRPCLookup(&fakeLookupClient{err: errors.New("connection refused")})
	scope := HashScope("https://example.test/teams/1/members?page=1")

	// A failure on an already-canceled context degrades silently: the
	// connector call is dying anyway, and logging would burn the rate
	// window a real failure needs.
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, found, err := lookup.Lookup(canceledCtx, RowKindGrants, scope)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, *logs, "a failure on a done context must not log")

	// The first live-context failure logs immediately, with the running
	// count including the silent canceled-context failure above.
	_, found, err = lookup.Lookup(ctx, RowKindGrants, scope)
	require.NoError(t, err)
	require.False(t, found)
	require.Len(t, *logs, 1)
	require.Equal(t, "source cache rpc lookup failed; treating as miss", (*logs)[0].msg)
	require.EqualValues(t, 2, (*logs)[0].fields["failures_since_start"])

	// Immediate subsequent failures still degrade to misses but stay
	// within the rate window and do not log again.
	for range 10 {
		_, found, err = lookup.Lookup(ctx, RowKindGrants, scope)
		require.NoError(t, err)
		require.False(t, found)
	}
	require.Len(t, *logs, 1, "failures inside the rate window must not log")
}

// TestGRPCLookupValidationStaysLoud pins the other half: malformed
// arguments are connector bugs and must fail, never degrade — a miss here
// would let a broken connector silently cold-fetch forever.
func TestGRPCLookupValidationStaysLoud(t *testing.T) {
	ctx := context.Background()
	lookup := NewGRPCLookup(&fakeLookupClient{resp: v1.LookupResponse_builder{Found: true, CacheValidator: "e"}.Build()})

	_, _, err := lookup.Lookup(ctx, RowKind("bogus"), HashScope("s"))
	require.Error(t, err, "invalid row kind must fail loudly")

	_, _, err = lookup.Lookup(ctx, RowKindGrants, "")
	require.Error(t, err, "empty scope key must fail loudly")
}

func TestGRPCLookupHitAndMiss(t *testing.T) {
	ctx := context.Background()
	scope := HashScope("https://example.test/teams/1/members?page=1")

	hit := NewGRPCLookup(&fakeLookupClient{resp: v1.LookupResponse_builder{Found: true, CacheValidator: `W/"etag-1"`}.Build()})
	entry, found, err := hit.Lookup(ctx, RowKindGrants, scope)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, `W/"etag-1"`, entry.CacheValidator)

	miss := NewGRPCLookup(&fakeLookupClient{resp: v1.LookupResponse_builder{Found: false}.Build()})
	_, found, err = miss.Lookup(ctx, RowKindGrants, scope)
	require.NoError(t, err)
	require.False(t, found)
}
