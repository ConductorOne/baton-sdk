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

type listEventsHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type listEventsHandler struct {
	task    *v1.Task
	helpers listEventsHelpers
}

func (c *listEventsHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "listEventHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)
	cc := c.helpers.ConnectorClient()

	t := c.task.GetListEvents()
	if t == nil {
		l.Error("get list event task was nil", zap.Any("get_list_event_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed get list event task"), ErrTaskNonRetryable))
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
			PageSize:    t.GetPageSize(),
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

func NewListEventsHandler(task *v1.Task, helpers listEventsHelpers) *listEventsHandler {
	return &listEventsHandler{
		task:    task,
		helpers: helpers,
	}
}
