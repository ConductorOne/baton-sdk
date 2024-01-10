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

type rotateCredentialsHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, response proto.Message, annos annotations.Annotations, err error) error
}

type rotateCredentialsTaskHandler struct {
	task    *v1.Task
	helpers rotateCredentialsHelpers
}

func (g *rotateCredentialsTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", g.task.Id), zap.Stringer("task_type", tasks.GetType(g.task)))

	t := g.task.GetRotateCredentials()
	if t == nil || t.GetResourceId() == nil {
		l.Error(
			"rotate credentials task was nil or missing resource info",
			zap.Any("rotate_credentials_task", t),
		)
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed rotate credentials task"), ErrTaskNonRetryable))
	}

	cc := g.helpers.ConnectorClient()
	resp, err := cc.RotateCredential(ctx, &v2.RotateCredentialRequest{
		ResourceId:        t.GetResourceId(),
		CredentialOptions: t.GetCredentialOptions(),
		EncryptionConfigs: t.GetEncryptionConfigs(),
	})
	if err != nil {
		l.Error("failed rotating credentials", zap.Error(err))
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return g.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newRotateCredentialsTaskHandler(task *v1.Task, helpers rotateCredentialsHelpers) tasks.TaskHandler {
	return &rotateCredentialsTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
