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
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

type issueCredentialHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, response proto.Message, annos annotations.Annotations, err error) error
}

type issueCredentialTaskHandler struct {
	task    *v1.Task
	helpers issueCredentialHelpers
}

func (h *issueCredentialTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "issueCredentialTaskHandler.HandleTask")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx).With(zap.String("task_id", h.task.GetId()), zap.Stringer("task_type", tasks.GetType(h.task)))

	t := h.task.GetIssueCredential()
	if t == nil || t.GetIdentityId() == nil || t.GetCredentialOptions() == nil || len(t.GetEncryptionConfigs()) == 0 {
		l.Error("issue credential task is malformed")
		return h.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed issue credential task"), ErrTaskNonRetryable))
	}

	resp, err := h.helpers.ConnectorClient().IssueCredential(ctx, v2.IssueCredentialRequest_builder{
		IdentityId:        t.GetIdentityId(),
		CredentialOptions: t.GetCredentialOptions(),
		EncryptionConfigs: t.GetEncryptionConfigs(),
		RequestId:         h.task.GetId(),
		ExpiresAt:         t.GetExpiresAt(),
	}.Build())
	if err != nil {
		// Issuance may have succeeded before transport failure. Until a connector
		// advertises and implements durable idempotency, never replay ambiguity.
		l.Error("failed issuing credential", zap.Error(err))
		return h.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}
	return h.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newIssueCredentialTaskHandler(task *v1.Task, helpers issueCredentialHelpers) tasks.TaskHandler {
	return &issueCredentialTaskHandler{task: task, helpers: helpers}
}
