package c1api

import (
	"context"
	"errors"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type getTicketTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type getTicketTaskHandler struct {
	task    *v1.Task
	helpers getTicketTaskHelpers
}

func (c *getTicketTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	cc := c.helpers.ConnectorClient()

	t := c.task.GetGetTicket()
	if t == nil || t.GetTicketId() == "" {
		l.Error("get ticket task was nil or missing ticket id", zap.Any("get_ticket_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed get ticket task"), ErrTaskNonRetryable))
	}

	ticket, err := cc.GetTicket(ctx, &v2.TicketsServiceGetTicketRequest{
		Id: t.GetTicketId(),
	})
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, t.GetAnnotations(), err)
	}

	if ticket.GetTicket() == nil {
		return c.helpers.FinishTask(ctx, nil, t.GetAnnotations(), errors.Join(errors.New("connector returned empty ticket"), ErrTaskNonRetryable))
	}

	resp := &v2.TicketsServiceGetTicketResponse{
		Ticket: ticket.GetTicket(),
	}

	respAnnos := annotations.Annotations(resp.GetAnnotations())
	respAnnos.Merge(t.GetAnnotations()...)

	resp.Annotations = respAnnos

	l.Debug("GetTicket response", zap.Any("resp", resp))

	return c.helpers.FinishTask(ctx, resp, respAnnos, nil)
}

func newGetTicketTaskHandler(task *v1.Task, helpers getTicketTaskHelpers) *getTicketTaskHandler {
	return &getTicketTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
