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

type actionListSchemasTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type actionListSchemasTaskHandler struct {
	task    *v1.Task
	helpers actionListSchemasTaskHelpers
}

func (c *actionListSchemasTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "actionListSchemasTaskHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)

	cc := c.helpers.ConnectorClient()

	t := c.task.GetActionListSchemas()
	if t == nil {
		return c.helpers.FinishTask(ctx, nil, nil, errors.New("action list schemas task is nil"))
	}
	reqBuilder := v2.ListActionSchemasRequest_builder{
		Annotations: t.GetAnnotations(),
	}
	if resourceTypeID := t.GetResourceTypeId(); resourceTypeID != "" {
		reqBuilder.ResourceTypeId = resourceTypeID
	}
	resp, err := cc.ListActionSchemas(ctx, reqBuilder.Build())
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	l.Debug("ActionListSchemas response", zap.Any("resp", resp))

	return c.helpers.FinishTask(ctx, resp, nil, nil)
}

func newActionListSchemasTaskHandler(task *v1.Task, helpers actionListSchemasTaskHelpers) *actionListSchemasTaskHandler {
	return &actionListSchemasTaskHandler{
		task:    task,
		helpers: helpers,
	}
}

type actionGetSchemaTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type actionGetSchemaTaskHandler struct {
	task    *v1.Task
	helpers actionGetSchemaTaskHelpers
}

func (c *actionGetSchemaTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "actionGetSchemaTaskHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)

	cc := c.helpers.ConnectorClient()

	t := c.task.GetActionGetSchema()
	if t == nil || t.GetName() == "" {
		return c.helpers.FinishTask(ctx, nil, nil, errors.New("action name required"))
	}

	resp, err := cc.GetActionSchema(ctx, v2.GetActionSchemaRequest_builder{
		Name:        t.GetName(),
		Annotations: t.GetAnnotations(),
	}.Build())
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	l.Debug("ActionGetSchema response", zap.Any("resp", resp))

	return c.helpers.FinishTask(ctx, resp, nil, nil)
}

func newActionGetSchemaTaskHandler(task *v1.Task, helpers actionGetSchemaTaskHelpers) *actionGetSchemaTaskHandler {
	return &actionGetSchemaTaskHandler{
		task:    task,
		helpers: helpers,
	}
}

type actionInvokeTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type actionInvokeTaskHandler struct {
	task    *v1.Task
	helpers actionInvokeTaskHelpers
}

func (c *actionInvokeTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "actionInvokeTaskHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)

	cc := c.helpers.ConnectorClient()

	t := c.task.GetActionInvoke()
	if t == nil || t.GetName() == "" {
		return c.helpers.FinishTask(ctx, nil, nil, errors.New("action name required"))
	}

	reqBuilder := v2.InvokeActionRequest_builder{
		Name:        t.GetName(),
		Args:        t.GetArgs(),
		Annotations: t.GetAnnotations(),
	}
	if resourceTypeID := t.GetResourceTypeId(); resourceTypeID != "" {
		reqBuilder.ResourceTypeId = resourceTypeID
	}
	resp, err := cc.InvokeAction(ctx, reqBuilder.Build())
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	l.Debug("ActionInvoke response", zap.Any("resp", resp))

	return c.helpers.FinishTask(ctx, resp, nil, nil)
}

func newActionInvokeTaskHandler(task *v1.Task, helpers actionInvokeTaskHelpers) *actionInvokeTaskHandler {
	return &actionInvokeTaskHandler{
		task:    task,
		helpers: helpers,
	}
}

type actionStatusTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type actionStatusTaskHandler struct {
	task    *v1.Task
	helpers actionStatusTaskHelpers
}

func (c *actionStatusTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "actionStatusTaskHandler.HandleTask")
	defer span.End()

	l := ctxzap.Extract(ctx)

	cc := c.helpers.ConnectorClient()

	t := c.task.GetActionStatus()
	if t == nil || t.GetId() == "" {
		return c.helpers.FinishTask(ctx, nil, nil, errors.New("action id required"))
	}

	resp, err := cc.GetActionStatus(ctx, v2.GetActionStatusRequest_builder{
		Name:        t.GetName(),
		Id:          t.GetId(),
		Annotations: t.GetAnnotations(),
	}.Build())
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	l.Debug("ActionStatus response", zap.Any("resp", resp))

	return c.helpers.FinishTask(ctx, resp, nil, nil)
}

func newActionStatusTaskHandler(task *v1.Task, helpers actionStatusTaskHelpers) *actionStatusTaskHandler {
	return &actionStatusTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
