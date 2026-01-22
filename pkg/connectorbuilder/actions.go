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

// ActionManager defines the interface for managing actions in the connector builder.
// This is the internal interface used by the builder for dispatch.
// The *actions.ActionManager type implements this interface.
type ActionManager interface {
	// ListActionSchemas returns all action schemas, optionally filtered by resource type.
	// If resourceTypeID is empty, returns all actions (both global and resource-scoped).
	// If resourceTypeID is set, returns only actions for that resource type.
	ListActionSchemas(ctx context.Context, resourceTypeID string) ([]*v2.BatonActionSchema, annotations.Annotations, error)

	// GetActionSchema returns the schema for a specific action by name.
	GetActionSchema(ctx context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error)

	// InvokeAction invokes an action. If resourceTypeID is set, invokes a resource-scoped action.
	InvokeAction(
		ctx context.Context,
		name string,
		resourceTypeID string,
		args *structpb.Struct,
	) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error)

	// GetActionStatus returns the status of an outstanding action.
	GetActionStatus(ctx context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error)

	// GetTypeRegistry returns a registry for registering resource-scoped actions.
	GetTypeRegistry(ctx context.Context, resourceTypeID string) (actions.ActionRegistry, error)

	// HasActions returns true if there are any registered actions.
	HasActions() bool
}

// GlobalActionProvider allows connectors to register global (non-resource-scoped) actions.
// This is the preferred method for registering global actions in new connectors.
// Implement this interface instead of the deprecated CustomActionManager or RegisterActionManagerLimited.
type GlobalActionProvider interface {
	GlobalActions(ctx context.Context, registry actions.ActionRegistry) error
}

// ResourceActionProvider is an interface that resource builders can implement
// to provide resource-scoped actions for their resource type.
type ResourceActionProvider interface {
	// ResourceActions returns the schemas and handlers for all resource actions
	// supported by this resource type.
	ResourceActions(ctx context.Context, registry actions.ActionRegistry) error
}

// Deprecated: CustomActionManager is deprecated. Implement GlobalActionProvider instead,
// which registers actions directly into the SDK's ActionManager.
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
	InvokeAction(
		ctx context.Context,
		name string,
		resourceTypeID string,
		args *structpb.Struct,
	) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error)

	// GetActionStatus returns the status of an outstanding action.
	GetActionStatus(ctx context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error)
}

// Deprecated: RegisterActionManager is deprecated. Implement GlobalActionProvider instead.
//
// RegisterActionManager extends ConnectorBuilder to add capabilities for registering custom actions.
// It provides a mechanism to register a CustomActionManager with the connector.
type RegisterActionManager interface {
	ConnectorBuilder
	RegisterActionManagerLimited
}

// Deprecated: RegisterActionManagerLimited is deprecated. Implement GlobalActionProvider instead.
type RegisterActionManagerLimited interface {
	RegisterActionManager(ctx context.Context) (CustomActionManager, error)
}

func (b *builder) ListActionSchemas(ctx context.Context, request *v2.ListActionSchemasRequest) (*v2.ListActionSchemasResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListActionSchemas")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionListSchemasType

	resourceTypeID := request.GetResourceTypeId()

	actionSchemas, _, err := b.actionManager.ListActionSchemas(ctx, resourceTypeID)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: listing action schemas failed: %w", err)
	}

	rv := v2.ListActionSchemasResponse_builder{
		Schemas: actionSchemas,
	}.Build()

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builder) GetActionSchema(ctx context.Context, request *v2.GetActionSchemaRequest) (*v2.GetActionSchemaResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetActionSchema")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionGetSchemaType

	actionSchema, annos, err := b.actionManager.GetActionSchema(ctx, request.GetName())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: action schema %s not found: %w", request.GetName(), err)
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

	resourceTypeID := request.GetResourceTypeId()

	id, actionStatus, resp, annos, err := b.actionManager.InvokeAction(ctx, request.GetName(), resourceTypeID, request.GetArgs())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
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

func (b *builder) GetActionStatus(ctx context.Context, request *v2.GetActionStatusRequest) (*v2.GetActionStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetActionStatus")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionStatusType

	actionStatus, name, rv, annos, err := b.actionManager.GetActionStatus(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: action status for id %s not found: %w", request.GetId(), err)
	}

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

// registerLegacyAction wraps a legacy CustomActionManager action as an ActionHandler and registers it.
func registerLegacyAction(ctx context.Context, registry actions.ActionRegistry, schema *v2.BatonActionSchema, legacyManager CustomActionManager) error {
	handler := func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		_, _, resp, annos, err := legacyManager.InvokeAction(ctx, schema.GetName(), "", args)
		return resp, annos, err
	}
	return registry.Register(ctx, schema, handler)
}

// addActionManager handles deprecated CustomActionManager and RegisterActionManagerLimited interfaces
// by extracting their actions and registering them into the unified ActionManager.
func (b *builder) addActionManager(ctx context.Context, in interface{}, registry actions.ActionRegistry) error {
	// Handle deprecated CustomActionManager - extract and re-register actions
	if customManager, ok := in.(CustomActionManager); ok {
		schemas, _, err := customManager.ListActionSchemas(ctx, "")
		if err != nil {
			return fmt.Errorf("error listing schemas from custom action manager: %w", err)
		}
		for _, schema := range schemas {
			if err := registerLegacyAction(ctx, registry, schema, customManager); err != nil {
				return fmt.Errorf("error registering legacy action %s: %w", schema.GetName(), err)
			}
		}
		return nil
	}

	// Handle deprecated RegisterActionManagerLimited
	if registerManager, ok := in.(RegisterActionManagerLimited); ok {
		customManager, err := registerManager.RegisterActionManager(ctx)
		if err != nil {
			return fmt.Errorf("error registering action manager: %w", err)
		}
		if customManager == nil {
			return nil // No action manager provided
		}
		schemas, _, err := customManager.ListActionSchemas(ctx, "")
		if err != nil {
			return fmt.Errorf("error listing schemas from custom action manager: %w", err)
		}
		for _, schema := range schemas {
			if err := registerLegacyAction(ctx, registry, schema, customManager); err != nil {
				return fmt.Errorf("error registering legacy action %s: %w", schema.GetName(), err)
			}
		}
	}
	return nil
}
