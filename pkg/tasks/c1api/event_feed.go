package c1api

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type eventFeedHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type eventFeedHandler struct {
	task    *v1.Task
	helpers eventFeedHelpers
}

func (c *eventFeedHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "eventFeedHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)
	cc := c.helpers.ConnectorClient()

	t := c.task.GetEventFeed()
	if t == nil {
		l.Error("get event feed task was nil", zap.Any("get_event_feed_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed get event feed task"), ErrTaskNonRetryable))
	}

	var (
		pageToken = ""
		events    []*v2.Event
	)

	for {
		feeds, err := cc.ListEvents(ctx, v2.ListEventsRequest_builder{
			EventFeedId: t.GetEventFeedId(),
			StartAt:     t.GetStartAt(),
			Cursor:      pageToken,
		}.Build())
		if err != nil {
			return err
		}

		events = append(events, feeds.GetEvents()...)
		pageToken = feeds.GetCursor()
		if !feeds.GetHasMore() {
			break
		}
	}

	resp := v2.ListEventsResponse_builder{
		Events: events,
	}.Build()
	return c.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func NewEventFeedHandler(task *v1.Task, helpers eventFeedHelpers) *eventFeedHandler {
	return &eventFeedHandler{
		task:    task,
		helpers: helpers,
	}
}
