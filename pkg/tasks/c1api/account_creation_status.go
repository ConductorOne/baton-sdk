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

type accountCreationStatusHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type AccountCreationStatusTaskHandler struct {
	task    *v1.Task
	helpers accountCreationStatusHelpers
}

func (g *AccountCreationStatusTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", g.task.Id), zap.Stringer("task_type", tasks.GetType(g.task)))

	t := g.task.GetGetAccountCreationStatus()
	if t == nil || t.GetTaskId() == "" {
		l.Error(
			"task ID was nil or empty",
		)
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed get account creation status task"), ErrTaskNonRetryable))
	}

	cc := g.helpers.ConnectorClient()
	resp, err := cc.GetAccountCreationStatus(ctx, &v2.GetAccountCreationStatusRequest{
		TaskId: t.GetTaskId(),
	})
	if err != nil {
		l.Error("failed looking up resource", zap.Error(err))
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return g.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newAccountCreationStatusTaskHandler(task *v1.Task, helpers accountCreationStatusHelpers) tasks.TaskHandler {
	return &AccountCreationStatusTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
