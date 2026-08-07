package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/conductorone/baton-sdk/pkg/uotel/uotelzap"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventsPerPageLocally is the page size a local event feed run requests when
// no explicit size is configured. It matches the default on the
// event-feed-page-size CLI flag (field.EventFeedPageSizeField), so a run
// driven from the CLI and one driven programmatically behave the same when
// neither sets a size.
const EventsPerPageLocally = 100

type localEventFeed struct {
	o        sync.Once
	feedId   string
	startAt  time.Time
	cursor   string
	pageSize uint32
}

// EventFeedOption configures a local event feed task manager.
type EventFeedOption func(*localEventFeed)

// WithEventFeedPageSize sets the page size a local event feed run requests.
// A page size of 0 is passed through to the connector as-is, which lets the
// connector fall back to its own default.
func WithEventFeedPageSize(pageSize uint32) EventFeedOption {
	return func(m *localEventFeed) {
		m.pageSize = pageSize
	}
}

func (m *localEventFeed) GetTempDir() string {
	return ""
}

func (m *localEventFeed) ShouldDebug() bool {
	return false
}

func (m *localEventFeed) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			EventFeed: v1.Task_EventFeedTask_builder{
				StartAt: timestamppb.New(m.startAt),
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localEventFeed) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localEventFeed.Process", trace.WithNewRoot())
	ctx = uotelzap.WithSpanLogFields(ctx)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	page := 0
	pageToken := m.cursor
	for {
		page++
		start := time.Now()
		var resp *v2.ListEventsResponse
		resp, err = cc.ListEvents(ctx, v2.ListEventsRequest_builder{
			PageSize:    m.pageSize,
			Cursor:      pageToken,
			StartAt:     task.GetEventFeed().GetStartAt(),
			EventFeedId: m.feedId,
		}.Build())
		elapsed := time.Since(start)
		if err != nil {
			ctxzap.Extract(ctx).Error("event feed page failed",
				zap.Int("page", page),
				zap.String("cursor", pageToken),
				zap.Int64("duration_ms", elapsed.Milliseconds()),
				zap.Error(err),
			)
			return err
		}
		ctxzap.Extract(ctx).Info("event feed page",
			zap.Int("page", page),
			zap.Int("events", len(resp.GetEvents())),
			zap.Int64("duration_ms", elapsed.Milliseconds()),
			zap.String("cursor", resp.GetCursor()),
		)
		for _, event := range resp.GetEvents() {
			var bytes []byte
			bytes, err = protojson.Marshal(event)
			if err != nil {
				return err
			}
			//nolint:forbidigo
			fmt.Println(string(bytes))
		}
		pageToken = resp.GetCursor()
		if !resp.GetHasMore() {
			break
		}
	}

	return nil
}

// NewEventFeed returns a task manager that queues an event feed task.
// Page size defaults to EventsPerPageLocally; override it with
// WithEventFeedPageSize.
func NewEventFeed(ctx context.Context, feedId string, startAt time.Time, cursor string, opts ...EventFeedOption) tasks.Manager {
	m := &localEventFeed{
		feedId:   feedId,
		startAt:  startAt,
		cursor:   cursor,
		pageSize: EventsPerPageLocally,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}
