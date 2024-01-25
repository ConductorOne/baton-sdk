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

type createResourceHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, response proto.Message, annos annotations.Annotations, err error) error
}

type createResourceTaskHandler struct {
	task    *v1.Task
	helpers createResourceHelpers
}

func (g *createResourceTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", g.task.Id), zap.Stringer("task_type", tasks.GetType(g.task)))

	t := g.task.GetCreateResource()
	if t == nil || t.GetResource() == nil {
		l.Error(
			"create resource task was nil or missing resource",
			zap.Any("create_resource_task", t),
		)
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed create resource task"), ErrTaskNonRetryable))
	}

	cc := g.helpers.ConnectorClient()
	resp, err := cc.CreateResource(ctx, &v2.CreateResourceRequest{
		Resource: t.GetResource(),
	})
	if err != nil {
		l.Error("failed create resource task", zap.Error(err))
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return g.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newCreateResourceTaskHandler(task *v1.Task, helpers createResourceHelpers) tasks.TaskHandler {
	return &createResourceTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
