package c1api

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// issueTypedCredentialTaskHandler is the task-backed transport for the typed
// issuance contract. It is a distinct handler, and the task is a distinct arm,
// so a runtime that predates the contract does not recognise the task at all
// and refuses it before any Issue implementation runs. There is deliberately no
// fall back to the legacy handler: a typed issuance that cannot be served is an
// error, not a request to mint legacy bytes.
type issueTypedCredentialTaskHandler struct {
	task    *v1.Task
	helpers issueCredentialHelpers
}

func (h *issueTypedCredentialTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "issueTypedCredentialTaskHandler.HandleTask")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx).With(zap.String("task_id", h.task.GetId()))

	t := h.task.GetIssueTypedCredential()
	if t == nil || t.GetIdentityId() == nil || t.GetCredentialOptions() == nil ||
		len(t.GetEncryptionConfigs()) == 0 || t.GetOutputContentType() == "" {
		l.Error("issue typed credential task is malformed")
		return h.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed issue typed credential task"), ErrTaskNonRetryable))
	}

	resp, err := h.helpers.ConnectorClient().IssueCredentialV2(ctx, v2.IssueCredentialRequest_builder{
		IdentityId:        t.GetIdentityId(),
		CredentialOptions: t.GetCredentialOptions(),
		EncryptionConfigs: t.GetEncryptionConfigs(),
		RequestId:         h.task.GetId(),
		ExpiresAt:         t.GetExpiresAt(),
		OutputContentType: t.GetOutputContentType(),
	}.Build())
	if err != nil {
		// Issuance may have succeeded before transport failure. Until a connector
		// advertises and implements durable idempotency, never replay ambiguity.
		l.Error("failed issuing typed credential", zap.Error(err))
		return h.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}
	return h.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newIssueTypedCredentialTaskHandler(task *v1.Task, helpers issueCredentialHelpers) tasks.TaskHandler {
	return &issueTypedCredentialTaskHandler{task: task, helpers: helpers}
}
