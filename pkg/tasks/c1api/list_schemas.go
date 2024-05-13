package c1api

import (
	"context"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type listSchemasTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type listSchemasTaskHandler struct {
	task    *v1.Task
	helpers listSchemasTaskHelpers
}

func (c *listSchemasTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	t := c.task.GetListSchemas()
	if t == nil {
		l.Error("list schemas was nil", zap.Any("list_schemas_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed list schemas task"), ErrTaskNonRetryable))
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
		l.Error("failed listing schemas", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}

	resp := &v2.TicketsServiceListTicketSchemasResponse{
		List:          ticketSchemas,
		NextPageToken: "",
	}

	return c.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

func newListSchemasTaskHandler(task *v1.Task, helpers listSchemasTaskHelpers) *listSchemasTaskHandler {
	return &listSchemasTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
