package c1api

import (
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type listTicketSchemasTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type listTicketSchemasTaskHandler struct {
	task    *v1.Task
	helpers listTicketSchemasTaskHelpers
}

func (c *listTicketSchemasTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	t := c.task.GetListTicketSchemas()
	if t == nil {
		l.Error("list ticket schemas was nil", zap.Any("list_ticket_schemas_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed list ticket schemas task"), ErrTaskNonRetryable))
	}

	cc := c.helpers.ConnectorClient()
	var ticketSchemas []*v2.TicketSchema
	var err error
	pageToken := ""
	for {
		schemas, err := cc.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{
			PageToken: pageToken,
		})
		if err != nil {
			return err
		}

		ticketSchemas = append(ticketSchemas, schemas.GetList()...)

		if schemas.GetNextPageToken() == "" {
			break
		}
		pageToken = schemas.GetNextPageToken()
	}

	if len(ticketSchemas) == 0 {
		err = fmt.Errorf("connector returned no ticket schemas")
	}

	if err != nil {
		l.Error("failed listing ticket schemas", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	resp := &v2.TicketsServiceListTicketSchemasResponse{
		List:          ticketSchemas,
		NextPageToken: "",
	}

	return c.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newListSchemasTaskHandler(task *v1.Task, helpers listTicketSchemasTaskHelpers) *listTicketSchemasTaskHandler {
	return &listTicketSchemasTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
