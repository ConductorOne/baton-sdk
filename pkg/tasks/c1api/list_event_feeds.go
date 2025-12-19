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

type listEventFeedsHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type listEventFeedsHandler struct {
	task    *v1.Task
	helpers listEventFeedsHelpers
}

func (c *listEventFeedsHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "listEventFeedsHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)
	cc := c.helpers.ConnectorClient()

	t := c.task.GetListEventFeeds()
	if t == nil {
		l.Error("get list event feeds task was nil", zap.Any("get_list_event_feeds_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed get list event feeds task"), ErrTaskNonRetryable))
	}

	feeds, err := cc.ListEventFeeds(ctx, &v2.ListEventFeedsRequest{})
	if err != nil {
		return err
	}

	resp := v2.ListEventFeedsResponse_builder{
		List: feeds.GetList(),
	}.Build()
	return c.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func NewListEventFeedsHandler(task *v1.Task, helpers listEventFeedsHelpers) *listEventFeedsHandler {
	return &listEventFeedsHandler{
		task:    task,
		helpers: helpers,
	}
}
