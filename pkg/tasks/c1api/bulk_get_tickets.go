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

type bulkGetTicketsTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type bulkGetTicketTaskHandler struct {
	task    *v1.Task
	helpers bulkGetTicketsTaskHelpers
}

func (c *bulkGetTicketTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	cc := c.helpers.ConnectorClient()

	t := c.task.GetBulkGetTickets()
	if t == nil || t.GetTicketRequests() == nil {
		l.Error("get ticket task was nil or missing ticket id", zap.Any("get_ticket_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed get ticket task"), ErrTaskNonRetryable))
	}

	ticketRequests := make([]*v2.TicketsServiceGetTicketRequest, 0)
	for _, getTicketTask := range t.GetTicketRequests() {
		ticketRequests = append(ticketRequests, &v2.TicketsServiceGetTicketRequest{
			Id:          getTicketTask.GetTicketId(),
			Annotations: getTicketTask.GetAnnotations(),
		})
	}

	tickets, err := cc.BulkGetTickets(ctx, &v2.TicketsServiceBulkGetTicketRequest{
		TicketRequests: ticketRequests,
	})
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	if tickets.GetTickets() == nil {
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("connector returned empty ticket"), ErrTaskNonRetryable))
	}

	resp := &v2.TicketsServiceBulkGetTicketResponse{
		Tickets: tickets.GetTickets(),
	}

	l.Debug("BulkGetTickets response", zap.Any("resp", resp))

	return c.helpers.FinishTask(ctx, resp, nil, nil)
}

func newBulkGetTicketTaskHandler(task *v1.Task, helpers bulkGetTicketsTaskHelpers) *bulkGetTicketTaskHandler {
	return &bulkGetTicketTaskHandler{
		task:    task,
		helpers: helpers,
	}
}