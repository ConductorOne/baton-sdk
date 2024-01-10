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

type createAccountHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type createAccountTaskHandler struct {
	task    *v1.Task
	helpers createAccountHelpers
}

func (g *createAccountTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", g.task.Id), zap.Stringer("task_type", tasks.GetType(g.task)))

	t := g.task.GetCreateAccount()
	if t == nil || t.GetAccountInfo() == nil {
		l.Error(
			"account task was nil or missing account info",
			zap.Any("create_account_task", t),
		)
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed create account task"), ErrTaskNonRetryable))
	}

	cc := g.helpers.ConnectorClient()
	resp, err := cc.CreateAccount(ctx, &v2.CreateAccountRequest{
		AccountInfo:       t.GetAccountInfo(),
		CredentialOptions: t.GetCredentialOptions(),
		EncryptionConfigs: t.GetEncryptionConfigs(),
	})
	if err != nil {
		l.Error("failed creating account", zap.Error(err))
		return g.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return g.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newCreateAccountTaskHandler(task *v1.Task, helpers createAccountHelpers) tasks.TaskHandler {
	return &createAccountTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
