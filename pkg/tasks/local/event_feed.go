package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type localEventFeed struct {
	o       sync.Once
	feedId  string
	startAt time.Time
	cursor  string
}

const EventsPerPageLocally = 100

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
	defer span.End()

	pageToken := m.cursor
	for {
		resp, err := cc.ListEvents(ctx, v2.ListEventsRequest_builder{
			PageSize:    EventsPerPageLocally,
			Cursor:      pageToken,
			StartAt:     task.GetEventFeed().GetStartAt(),
			EventFeedId: m.feedId,
		}.Build())
		if err != nil {
			return err
		}
		for _, event := range resp.GetEvents() {
			bytes, err := protojson.Marshal(event)
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
func NewEventFeed(ctx context.Context, feedId string, startAt time.Time, cursor string) tasks.Manager {
	return &localEventFeed{
		feedId:  feedId,
		startAt: startAt,
		cursor:  cursor,
	}
}
