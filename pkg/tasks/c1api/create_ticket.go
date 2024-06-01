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
	"github.com/conductorone/baton-sdk/pkg/types"
)

type createTicketTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type createTicketTaskHandler struct {
	task    *v1.Task
	helpers createTicketTaskHelpers
}

func (c *createTicketTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	t := c.task.GetCreateTicketTask()
	if t == nil || t.GetTicketRequest() == nil {
		l.Error("create ticket task was nil or missing ticket request", zap.Any("create_ticket_task", t))
		return c.helpers.FinishTask(ctx, nil, t.GetAnnotations(), errors.Join(errors.New("malformed create ticket task"), ErrTaskNonRetryable))
	}

	cc := c.helpers.ConnectorClient()
	resp, err := cc.CreateTicket(ctx, &v2.TicketsServiceCreateTicketRequest{
		Request:     t.GetTicketRequest(),
		Schema:      t.GetTicketSchema(),
		Annotations: t.GetAnnotations(),
	})
	if err != nil {
		l.Error("failed creating ticket", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, t.GetAnnotations(), err)
	}

	respAnnos := annotations.Annotations(resp.GetAnnotations())
	respAnnos.Merge(t.GetAnnotations()...)

	resp.Annotations = respAnnos

	return c.helpers.FinishTask(ctx, resp, respAnnos, nil)
}

func newCreateTicketTaskHandler(task *v1.Task, helpers createTicketTaskHelpers) *createTicketTaskHandler {
	return &createTicketTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
