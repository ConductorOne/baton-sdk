package c1api

import (
	"context"
	"errors"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type grantHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, err error) error
}

type grantTaskHandler struct {
	task    *v1.Task
	helpers grantHelpers
}

func (g *grantTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", g.task.Id), zap.Stringer("task_type", tasks.GetType(g.task)))

	if g.task.GetGrant() == nil {
		l.Error("grant task was nil")
		return g.helpers.FinishTask(ctx, errors.Join(errors.New("invalid task type"), ErrTaskFatality))
	}

	grant := g.task.GetGrant()

	cc := g.helpers.ConnectorClient()
	_, err := cc.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Entitlement: grant.Entitlement,
		Principal:   grant.Principal,
	})
	if err != nil {
		l.Error("failed while granting entitlement", zap.Error(err))
		return g.helpers.FinishTask(ctx, errors.Join(err, ErrTaskFatality))
	}

	return g.helpers.FinishTask(ctx, nil)
}

func newGrantTaskHandler(task *v1.Task, helpers grantHelpers) tasks.TaskHandler {
	return &grantTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
