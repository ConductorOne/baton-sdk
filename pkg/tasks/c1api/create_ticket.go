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
	l.Info("******* HandleTask create ticket here")

	cc := c.helpers.ConnectorClient()
	resp, err := cc.CreateTicket(ctx, &v2.TicketsServiceCreateTicketRequest{})
	if err != nil {
		l.Error("failed creating ticket", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	return c.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newCreateTicketTaskHandler(task *v1.Task, helpers createTicketTaskHelpers) *createTicketTaskHandler {
	return &createTicketTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
