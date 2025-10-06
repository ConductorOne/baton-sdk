package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
)

func (b *builderImpl) ListActionSchemas(ctx context.Context, request *v2.ListActionSchemasRequest) (*v2.ListActionSchemasResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListActionSchemas")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionListSchemasType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	actionSchemas, annos, err := b.actionManager.ListActionSchemas(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing action schemas failed: %w", err)
	}

	rv := &v2.ListActionSchemasResponse{
		Schemas:     actionSchemas,
		Annotations: annos,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) GetActionSchema(ctx context.Context, request *v2.GetActionSchemaRequest) (*v2.GetActionSchemaResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetActionSchema")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionGetSchemaType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	actionSchema, annos, err := b.actionManager.GetActionSchema(ctx, request.GetName())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: getting action schema failed: %w", err)
	}

	rv := &v2.GetActionSchemaResponse{
		Schema:      actionSchema,
		Annotations: annos,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) InvokeAction(ctx context.Context, request *v2.InvokeActionRequest) (*v2.InvokeActionResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.InvokeAction")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionInvokeType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	id, status, resp, annos, err := b.actionManager.InvokeAction(ctx, request.GetName(), request.GetArgs())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: invoking action failed: %w", err)
	}

	rv := &v2.InvokeActionResponse{
		Id:          id,
		Name:        request.GetName(),
		Status:      status,
		Annotations: annos,
		Response:    resp,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) GetActionStatus(ctx context.Context, request *v2.GetActionStatusRequest) (*v2.GetActionStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetActionStatus")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionStatusType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	status, name, rv, annos, err := b.actionManager.GetActionStatus(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: getting action status failed: %w", err)
	}

	resp := &v2.GetActionStatusResponse{
		Id:          request.GetId(),
		Name:        name,
		Status:      status,
		Annotations: annos,
		Response:    rv,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}
