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

type grantHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type grantTaskHandler struct {
	task    *v1.Task
	helpers grantHelpers
}

func (g *grantTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", g.task.Id), zap.Stringer("task_type", tasks.GetType(g.task)))

	if g.task.GetGrant() == nil || g.task.GetGrant().GetEntitlement() == nil || g.task.GetGrant().GetPrincipal() == nil {
		l.Error(
			"grant task was nil or missing entitlement or principal",
			zap.Any("grant", g.task.GetGrant()),
			zap.Any("entitlement", g.task.GetGrant().GetEntitlement()),
			zap.Any("principal", g.task.GetGrant().GetPrincipal()),
		)
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed grant task"), ErrTaskNonRetryable))
	}

	grant := g.task.GetGrant()

	cc := g.helpers.ConnectorClient()
	resp, err := cc.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Entitlement: grant.Entitlement,
		Principal:   grant.Principal,
	})
	if err != nil {
		l.Error("failed while granting entitlement", zap.Error(err))
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return g.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newGrantTaskHandler(task *v1.Task, helpers grantHelpers) tasks.TaskHandler {
	return &grantTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
