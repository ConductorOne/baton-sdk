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

type bulkCreateTicketTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type bulkCreateTicketTaskHandler struct {
	task    *v1.Task
	helpers bulkCreateTicketTaskHelpers
}

func (c *bulkCreateTicketTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "bulkCreateTicketTaskHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)

	t := c.task.GetBulkCreateTickets()
	if t == nil || len(t.GetTicketRequests()) == 0 {
		l.Error("bulk create ticket task was nil or missing ticket requests", zap.Any("bulk_create_ticket_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed bulk create ticket task"), ErrTaskNonRetryable))
	}

	ticketRequests := make([]*v2.TicketsServiceCreateTicketRequest, 0)
	for _, createTicketTask := range t.GetTicketRequests() {
		ticketRequests = append(ticketRequests, &v2.TicketsServiceCreateTicketRequest{
			Request:     createTicketTask.GetTicketRequest(),
			Schema:      createTicketTask.GetTicketSchema(),
			Annotations: createTicketTask.GetAnnotations(),
		})
	}

	cc := c.helpers.ConnectorClient()
	resp, err := cc.BulkCreateTickets(ctx, &v2.TicketsServiceBulkCreateTicketsRequest{
		TicketRequests: ticketRequests,
	})
	if err != nil {
		l.Error("failed bulk creating tickets", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	if len(resp.GetTickets()) != len(t.GetTicketRequests()) {
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("connector returned incorrect number of tickets"), ErrTaskNonRetryable))
	}

	return c.helpers.FinishTask(ctx, resp, nil, nil)
}

func newBulkCreateTicketTaskHandler(task *v1.Task, helpers bulkCreateTicketTaskHelpers) *bulkCreateTicketTaskHandler {
	return &bulkCreateTicketTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
