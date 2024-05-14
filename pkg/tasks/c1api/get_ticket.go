package c1api

import (
	"context"
	"fmt"

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

	ticket, err := cc.GetTicket(ctx, &v2.TicketsServiceGetTicketRequest{
		Id: c.task.GetId(),
	})
	if err != nil {
		return err
	}

	if ticket.GetTicket() == nil {
		return fmt.Errorf("connector returned empt ticket schema")
	}

	resp := &v2.TicketsServiceGetTicketResponse{
		Ticket: ticket.GetTicket(),
	}

	l.Debug("GetTicket response", zap.Any("resp", resp))

	return c.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newGetTicketTaskHandler(task *v1.Task, helpers getTicketTaskHelpers) *getTicketTaskHandler {
	return &getTicketTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
