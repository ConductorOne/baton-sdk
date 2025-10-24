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

type bulkGetTicketsTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type bulkGetTicketTaskHandler struct {
	task    *v1.Task
	helpers bulkGetTicketsTaskHelpers
}

func (c *bulkGetTicketTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "bulkGetTicketTaskHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)

	cc := c.helpers.ConnectorClient()

	t := c.task.GetBulkGetTickets()
	if t == nil || len(t.GetTicketRequests()) == 0 {
		l.Error("bulk get ticket task was nil or missing tickets", zap.Any("bulk_get_tickets_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed bulk get tickets task"), ErrTaskNonRetryable))
	}

	ticketRequests := make([]*v2.TicketsServiceGetTicketRequest, 0)
	for _, getTicketTask := range t.GetTicketRequests() {
		ticketRequests = append(ticketRequests, v2.TicketsServiceGetTicketRequest_builder{
			Id:          getTicketTask.GetTicketId(),
			Annotations: getTicketTask.GetAnnotations(),
		}.Build())
	}

	resp, err := cc.BulkGetTickets(ctx, v2.TicketsServiceBulkGetTicketsRequest_builder{
		TicketRequests: ticketRequests,
	}.Build())
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	if len(resp.GetTickets()) != len(t.GetTicketRequests()) {
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("connector returned incorrect number of tickets"), ErrTaskNonRetryable))
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
