package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// capturingCore is a minimal zapcore.Core that records entries in-memory so
// tests can assert on fields without depending on zaptest/observer (not vendored).
type capturingCore struct {
	zapcore.LevelEnabler
	mu      sync.Mutex
	entries []capturedEntry
}

type capturedEntry struct {
	Message string
	Fields  map[string]interface{}
}

func newCapturingCore() *capturingCore {
	return &capturingCore{LevelEnabler: zapcore.InfoLevel}
}

func (c *capturingCore) With([]zapcore.Field) zapcore.Core { return c }
func (c *capturingCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *capturingCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	c.mu.Lock()
	c.entries = append(c.entries, capturedEntry{Message: ent.Message, Fields: enc.Fields})
	c.mu.Unlock()
	return nil
}

func (c *capturingCore) Sync() error { return nil }

func (c *capturingCore) filterMessage(msg string) []capturedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]capturedEntry, 0, len(c.entries))
	for _, e := range c.entries {
		if e.Message == msg {
			out = append(out, e)
		}
	}
	return out
}

func observedLogger(t *testing.T) (context.Context, *capturingCore) {
	t.Helper()
	core := newCapturingCore()
	logger := zap.New(core)
	return ctxzap.ToContext(context.Background(), logger), core
}

type pagedListEventsClient struct {
	types.ConnectorClient
	mu    sync.Mutex
	reqs  []*v2.ListEventsRequest
	resps []*v2.ListEventsResponse
}

func (f *pagedListEventsClient) ListEvents(
	_ context.Context,
	req *v2.ListEventsRequest,
	_ ...grpc.CallOption,
) (*v2.ListEventsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := len(f.reqs)
	f.reqs = append(f.reqs, req)
	return f.resps[idx], nil
}

func TestEventFeed_Process_HonorsConfiguredPageSize(t *testing.T) {
	ctx, _ := observedLogger(t)
	cc := &pagedListEventsClient{
		resps: []*v2.ListEventsResponse{
			v2.ListEventsResponse_builder{
				Events:  []*v2.Event{v2.Event_builder{Id: "e1"}.Build()},
				Cursor:  "cursor-1",
				HasMore: true,
			}.Build(),
			v2.ListEventsResponse_builder{
				Events:  []*v2.Event{v2.Event_builder{Id: "e2"}.Build(), v2.Event_builder{Id: "e3"}.Build()},
				Cursor:  "cursor-2",
				HasMore: true,
			}.Build(),
			v2.ListEventsResponse_builder{
				Events:  nil,
				Cursor:  "cursor-3",
				HasMore: false,
			}.Build(),
		},
	}

	const wantPageSize = 250
	mgr := NewEventFeed(ctx, "usage_event_feed", time.Now(), "", WithEventFeedPageSize(wantPageSize))
	task, _, err := mgr.Next(ctx)
	require.NoError(t, err)

	require.NoError(t, mgr.Process(ctx, task, cc))

	require.Len(t, cc.reqs, 3)
	for i, req := range cc.reqs {
		require.Equalf(t, uint32(wantPageSize), req.GetPageSize(), "request %d page size", i)
		require.Equal(t, "usage_event_feed", req.GetEventFeedId())
	}
	// Each page's cursor is the previous page's returned cursor.
	require.Equal(t, "", cc.reqs[0].GetCursor())
	require.Equal(t, "cursor-1", cc.reqs[1].GetCursor())
	require.Equal(t, "cursor-2", cc.reqs[2].GetCursor())
}

func TestEventFeed_Process_LogsPerPageCursorCountAndDuration(t *testing.T) {
	ctx, logs := observedLogger(t)
	cc := &pagedListEventsClient{
		resps: []*v2.ListEventsResponse{
			v2.ListEventsResponse_builder{
				Events:  []*v2.Event{v2.Event_builder{Id: "e1"}.Build()},
				Cursor:  "cursor-1",
				HasMore: true,
			}.Build(),
			v2.ListEventsResponse_builder{
				Events:  []*v2.Event{v2.Event_builder{Id: "e2"}.Build(), v2.Event_builder{Id: "e3"}.Build()},
				Cursor:  "cursor-2",
				HasMore: true,
			}.Build(),
			v2.ListEventsResponse_builder{
				Events:  nil,
				Cursor:  "cursor-3",
				HasMore: false,
			}.Build(),
		},
	}

	mgr := NewEventFeed(ctx, "usage_event_feed", time.Now(), "")
	task, _, err := mgr.Next(ctx)
	require.NoError(t, err)
	require.NoError(t, mgr.Process(ctx, task, cc))

	entries := logs.filterMessage("event feed page")
	require.Len(t, entries, 3)

	wantEvents := []interface{}{int64(1), int64(2), int64(0)}
	wantCursors := []string{"cursor-1", "cursor-2", "cursor-3"}
	for i, e := range entries {
		require.Contains(t, e.Fields, "page")
		require.Contains(t, e.Fields, "events")
		require.Contains(t, e.Fields, "duration_ms")
		require.Contains(t, e.Fields, "cursor")

		require.EqualValues(t, i+1, e.Fields["page"])
		require.EqualValues(t, wantEvents[i], e.Fields["events"])
		require.Equal(t, wantCursors[i], e.Fields["cursor"])

		durMs, ok := e.Fields["duration_ms"].(int64)
		require.True(t, ok, "duration_ms should be an int64")
		require.GreaterOrEqual(t, durMs, int64(0))
	}
}

// singlePageClient answers every ListEvents with one empty, final page.
type singlePageClient struct {
	types.ConnectorClient
	reqs []*v2.ListEventsRequest
}

func (f *singlePageClient) ListEvents(
	_ context.Context,
	req *v2.ListEventsRequest,
	_ ...grpc.CallOption,
) (*v2.ListEventsResponse, error) {
	f.reqs = append(f.reqs, req)
	return v2.ListEventsResponse_builder{HasMore: false}.Build(), nil
}

// TestEventFeed_PageSizeOptionDefaulting pins the behavior that keeps
// NewEventFeed's old call shape working: with no EventFeedOption the run
// requests EventsPerPageLocally, exactly as it did when the page size was a
// hardcoded constant.
func TestEventFeed_PageSizeOptionDefaulting(t *testing.T) {
	ctx, _ := observedLogger(t)

	t.Run("no option requests EventsPerPageLocally", func(t *testing.T) {
		cc := &singlePageClient{}
		mgr := NewEventFeed(ctx, "feed", time.Now(), "")
		task, _, err := mgr.Next(ctx)
		require.NoError(t, err)
		require.NoError(t, mgr.Process(ctx, task, cc))

		require.Len(t, cc.reqs, 1)
		require.Equal(t, uint32(EventsPerPageLocally), cc.reqs[0].GetPageSize())
	})

	t.Run("explicit 0 is passed through so the connector can default", func(t *testing.T) {
		cc := &singlePageClient{}
		mgr := NewEventFeed(ctx, "feed", time.Now(), "", WithEventFeedPageSize(0))
		task, _, err := mgr.Next(ctx)
		require.NoError(t, err)
		require.NoError(t, mgr.Process(ctx, task, cc))

		require.Len(t, cc.reqs, 1)
		require.Zero(t, cc.reqs[0].GetPageSize())
	})

	t.Run("last option wins", func(t *testing.T) {
		cc := &singlePageClient{}
		mgr := NewEventFeed(ctx, "feed", time.Now(), "", WithEventFeedPageSize(7), WithEventFeedPageSize(9))
		task, _, err := mgr.Next(ctx)
		require.NoError(t, err)
		require.NoError(t, mgr.Process(ctx, task, cc))

		require.Len(t, cc.reqs, 1)
		require.Equal(t, uint32(9), cc.reqs[0].GetPageSize())
	})
}
