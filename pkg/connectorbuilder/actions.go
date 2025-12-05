package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"google.golang.org/protobuf/types/known/structpb"
)

// CustomActionManager defines capabilities for handling custom actions.
//
// Note: RegisterActionManager is preferred for new connectors.
//
// This interface allows connectors to define and execute custom actions
// that can be triggered from Baton. It supports both global actions and
// resource-scoped actions through the resourceTypeID parameter.
type CustomActionManager interface {
	// ListActionSchemas returns all action schemas, optionally filtered by resource type.
	// If resourceTypeID is empty, returns all actions (both global and resource-scoped).
	// If resourceTypeID is set, returns only actions for that resource type.
	ListActionSchemas(ctx context.Context, resourceTypeID string) ([]*v2.BatonActionSchema, annotations.Annotations, error)

	// GetActionSchema returns the schema for a specific action by name.
	GetActionSchema(ctx context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error)

	// InvokeAction invokes an action. If resourceTypeID is set, invokes a resource-scoped action.
	InvokeAction(ctx context.Context, name string, resourceTypeID string, args *structpb.Struct, encryptionConfigs []*v2.EncryptionConfig) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error)

	// GetActionStatus returns the status of an outstanding action.
	GetActionStatus(ctx context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error)

	// GetTypeRegistry returns a registry for registering resource-scoped actions.
	GetTypeRegistry(ctx context.Context, resourceTypeID string) (actions.ResourceTypeActionRegistry, error)
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

	resourceTypeID := request.GetResourceTypeId()

	var allSchemas []*v2.BatonActionSchema

	// Get schemas from the connector-provided action manager (if any)
	if b.actionManager != nil {
		actionSchemas, _, err := b.actionManager.ListActionSchemas(ctx, resourceTypeID)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: listing action schemas failed: %w", err)
		}
		allSchemas = append(allSchemas, actionSchemas...)
	}

	// Get schemas from the internal action manager (for resource-scoped actions)
	if b.internalActionManager != nil {
		internalSchemas, _, err := b.internalActionManager.ListActionSchemas(ctx, resourceTypeID)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: listing internal action schemas failed: %w", err)
		}
		allSchemas = append(allSchemas, internalSchemas...)
	}

	rv := v2.ListActionSchemasResponse_builder{
		Schemas: allSchemas,
	}.Build()

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builder) GetActionSchema(ctx context.Context, request *v2.GetActionSchemaRequest) (*v2.GetActionSchemaResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetActionSchema")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionGetSchemaType

	// Try connector-provided action manager first
	if b.actionManager != nil {
		actionSchema, annos, err := b.actionManager.GetActionSchema(ctx, request.GetName())
		if err == nil {
			rv := v2.GetActionSchemaResponse_builder{
				Schema:      actionSchema,
				Annotations: annos,
			}.Build()
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return rv, nil
		}
	}

	// Try internal action manager
	if b.internalActionManager != nil {
		actionSchema, annos, err := b.internalActionManager.GetActionSchema(ctx, request.GetName())
		if err == nil {
			rv := v2.GetActionSchemaResponse_builder{
				Schema:      actionSchema,
				Annotations: annos,
			}.Build()
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return rv, nil
		}
	}

	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, fmt.Errorf("error: action schema %s not found", request.GetName())
}

func (b *builder) InvokeAction(ctx context.Context, request *v2.InvokeActionRequest) (*v2.InvokeActionResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.InvokeAction")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionInvokeType

	resourceTypeID := request.GetResourceTypeId()
	encryptionConfigs := request.GetEncryptionConfigs()

	// If resource type is specified, use the internal action manager for resource-scoped actions
	if resourceTypeID != "" {
		if b.internalActionManager == nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: internal action manager not configured")
		}

		id, actionStatus, resp, annos, err := b.internalActionManager.InvokeAction(ctx, request.GetName(), resourceTypeID, request.GetArgs(), encryptionConfigs)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: invoking resource action failed: %w", err)
		}

		rv := v2.InvokeActionResponse_builder{
			Id:          id,
			Name:        request.GetName(),
			Status:      actionStatus,
			Annotations: annos,
			Response:    resp,
		}.Build()

		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return rv, nil
	}

	// For global actions, try connector-provided action manager first
	if b.actionManager != nil {
		id, actionStatus, resp, annos, err := b.actionManager.InvokeAction(ctx, request.GetName(), "", request.GetArgs(), encryptionConfigs)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: invoking action failed: %w", err)
		}

		rv := v2.InvokeActionResponse_builder{
			Id:          id,
			Name:        request.GetName(),
			Status:      actionStatus,
			Annotations: annos,
			Response:    resp,
		}.Build()

		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return rv, nil
	}

	// Fall back to internal action manager for global actions
	if b.internalActionManager != nil {
		id, actionStatus, resp, annos, err := b.internalActionManager.InvokeAction(ctx, request.GetName(), "", request.GetArgs(), encryptionConfigs)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: invoking action failed: %w", err)
		}

		rv := v2.InvokeActionResponse_builder{
			Id:          id,
			Name:        request.GetName(),
			Status:      actionStatus,
			Annotations: annos,
			Response:    resp,
		}.Build()

		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return rv, nil
	}

	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, fmt.Errorf("error: no action manager configured")
}

func (b *builder) GetActionStatus(ctx context.Context, request *v2.GetActionStatusRequest) (*v2.GetActionStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetActionStatus")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionStatusType

	// Try connector-provided action manager first
	if b.actionManager != nil {
		actionStatus, name, rv, annos, err := b.actionManager.GetActionStatus(ctx, request.GetId())
		if err == nil {
			resp := v2.GetActionStatusResponse_builder{
				Id:          request.GetId(),
				Name:        name,
				Status:      actionStatus,
				Annotations: annos,
				Response:    rv,
			}.Build()
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return resp, nil
		}
	}

	// Try internal action manager
	if b.internalActionManager != nil {
		actionStatus, name, rv, annos, err := b.internalActionManager.GetActionStatus(ctx, request.GetId())
		if err == nil {
			resp := v2.GetActionStatusResponse_builder{
				Id:          request.GetId(),
				Name:        name,
				Status:      actionStatus,
				Annotations: annos,
				Response:    rv,
			}.Build()
			b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
			return resp, nil
		}
	}

	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, fmt.Errorf("error: action status for id %s not found", request.GetId())
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
