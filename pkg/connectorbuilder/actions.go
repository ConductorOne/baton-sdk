package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"google.golang.org/protobuf/types/known/structpb"
)

// CustomActionManager defines capabilities for handling custom actions.
//
// Note: RegisterActionManager is preferred for new connectors.
//
// This interface allows connectors to define and execute custom actions
// that can be triggered from Baton.
type CustomActionManager interface {
	ListActionSchemas(ctx context.Context) ([]*v2.BatonActionSchema, annotations.Annotations, error)
	GetActionSchema(ctx context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error)
	InvokeAction(ctx context.Context, name string, args *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error)
	GetActionStatus(ctx context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error)
}

// RegisterActionManager extends ConnectorBuilder to add capabilities for registering custom actions.
//
// This is the recommended interface for implementing custom action support in new connectors.
// It provides a mechanism to register a CustomActionManager with the connector.
type RegisterActionManager interface {
	ConnectorBuilder
	RegisterActionManagerLimited
}

type RegisterActionManagerLimited interface {
	RegisterActionManager(ctx context.Context) (CustomActionManager, error)
}

func (b *builder) ListActionSchemas(ctx context.Context, request *v2.ListActionSchemasRequest) (*v2.ListActionSchemasResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListActionSchemas")
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

	rv := v2.ListActionSchemasResponse_builder{
		Schemas:     actionSchemas,
		Annotations: annos,
	}.Build()

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builder) GetActionSchema(ctx context.Context, request *v2.GetActionSchemaRequest) (*v2.GetActionSchemaResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetActionSchema")
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

	rv := v2.GetActionSchemaResponse_builder{
		Schema:      actionSchema,
		Annotations: annos,
	}.Build()

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builder) InvokeAction(ctx context.Context, request *v2.InvokeActionRequest) (*v2.InvokeActionResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.InvokeAction")
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

	rv := v2.InvokeActionResponse_builder{
		Id:          id,
		Name:        request.GetName(),
		Status:      status,
		Annotations: annos,
		Response:    resp,
	}.Build()

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builder) GetActionStatus(ctx context.Context, request *v2.GetActionStatusRequest) (*v2.GetActionStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetActionStatus")
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

	resp := v2.GetActionStatusResponse_builder{
		Id:          request.GetId(),
		Name:        name,
		Status:      status,
		Annotations: annos,
		Response:    rv,
	}.Build()

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

func (b *builder) addActionManager(ctx context.Context, in interface{}) error {
	if actionManager, ok := in.(CustomActionManager); ok {
		if b.actionManager != nil {
			return fmt.Errorf("error: cannot set multiple action managers")
		}
		b.actionManager = actionManager
	}

	if registerActionManager, ok := in.(RegisterActionManagerLimited); ok {
		if b.actionManager != nil {
			return fmt.Errorf("error: cannot register multiple action managers")
		}
		actionManager, err := registerActionManager.RegisterActionManager(ctx)
		if err != nil {
			return fmt.Errorf("error: registering action manager failed: %w", err)
		}
		if actionManager == nil {
			return fmt.Errorf("error: action manager is nil")
		}
		b.actionManager = actionManager
	}
	return nil
}
