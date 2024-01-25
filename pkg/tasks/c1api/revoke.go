package c1api

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/conductorone/baton-sdk/pkg/annotations"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type revokeHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type revokeTaskHandler struct {
	task    *v1.Task
	helpers revokeHelpers
}

func (r *revokeTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", r.task.Id), zap.Stringer("task_type", tasks.GetType(r.task)))

	if r.task.GetRevoke() == nil || r.task.GetRevoke().GetGrant() == nil {
		l.Error("revoke task was nil or missing grant", zap.Any("revoke", r.task.GetRevoke()), zap.Any("grant", r.task.GetRevoke().GetGrant()))
		return r.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("invalid task type"), ErrTaskNonRetryable))
	}

	cc := r.helpers.ConnectorClient()
	resp, err := cc.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: r.task.GetRevoke().GetGrant(),
	})
	if err != nil {
		l.Error("failed while granting entitlement", zap.Error(err))
		return r.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return r.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newRevokeTaskHandler(task *v1.Task, helpers revokeHelpers) tasks.TaskHandler {
	return &revokeTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
