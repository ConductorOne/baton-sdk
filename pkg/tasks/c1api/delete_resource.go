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
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type deleteResourceHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, response proto.Message, annos annotations.Annotations, err error) error
}

type deleteResourceTaskHandler struct {
	task    *v1.Task
	helpers deleteResourceHelpers
}

func (g *deleteResourceTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", g.task.Id), zap.Stringer("task_type", tasks.GetType(g.task)))

	t := g.task.GetDeleteResource()
	if t == nil || t.GetResourceId() == nil || t.GetResourceId().GetResource() == "" || t.GetResourceId().GetResourceType() == "" {
		l.Error(
			"delete resource task was nil or missing resource id",
			zap.Any("delete_resource_task", t),
		)
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed delete resource task"), ErrTaskNonRetryable))
	}

	cc := g.helpers.ConnectorClient()
	resp, err := cc.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: t.GetResourceId(),
	})
	if err != nil {
		l.Error("failed delete resource task", zap.Error(err))
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return g.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newDeleteResourceTaskHandler(task *v1.Task, helpers deleteResourceHelpers) tasks.TaskHandler {
	return &deleteResourceTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
