package connectorbuilder

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

const (
	// maxConsecutiveStatusErrors bounds how many status-check failures or
	// indeterminate statuses in a row the legacy action poll loop tolerates
	// before failing the action.
	maxConsecutiveStatusErrors = 3
)

// The legacy status poll starts fast and backs off to a cap so a slow
// action doesn't drain a remote manager's rate-limit budget. Variables so
// tests can drive the loop without real-time waits.
var (
	initialStatusPollInterval = time.Second
	maxStatusPollInterval     = 30 * time.Second
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

	// InvokeActionWithWait is InvokeAction with an explicit bound on how long
	// the call may block waiting for the handler; zero or negative keeps the
	// manager's default.
	InvokeActionWithWait(
		ctx context.Context,
		name string,
		resourceTypeID string,
		args *structpb.Struct,
		inlineWait time.Duration,
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
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

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
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

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
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.ActionInvokeType

	resourceTypeID := request.GetResourceTypeId()

	id, actionStatus, resp, annos, err := b.actionManager.InvokeActionWithWait(ctx, request.GetName(), resourceTypeID, request.GetArgs(), request.GetInlineWait().AsDuration())
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
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

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
		// The inner call keeps the detached handler context; its one-hour
		// deadline is the execution backstop for however long the legacy
		// manager runs.
		id, actionStatus, resp, annos, err := legacyManager.InvokeAction(ctx, schema.GetName(), "", args)
		if err != nil {
			return resp, annos, err
		}

		// Legacy managers were never required to populate id or status — the
		// wrapper used to discard both — so the never-populated shape
		// resolves the outer action with the response, as it always did.
		if actionStatus == v2.BatonActionStatus_BATON_ACTION_STATUS_UNSPECIFIED {
			return resp, annos, nil
		}

		// A terminal status at the invoke seam resolves like a terminal
		// poll. The SDK's own manager reports handler failures in-band as
		// FAILED with a nil error, so this must not resolve as success.
		if !isInFlightActionStatus(actionStatus) {
			return resp, annos, legacyStatusErr(schema.GetName(), actionStatus)
		}

		// An in-flight claim without an id cannot be polled; resolve with
		// the response, matching the old fire-and-forget behavior.
		if id == "" {
			return resp, annos, nil
		}

		// Poll to a terminal status so the outer result carries the action's
		// real outcome. A few consecutive status-check failures are tolerated:
		// one flaky remote lookup must not convert a succeeding action into a
		// failure. The interval backs off to a cap so a slow action doesn't
		// drain a remote manager's rate-limit budget.
		l := ctxzap.Extract(ctx)
		statusErrs := 0
		interval := initialStatusPollInterval
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return resp, annos, fmt.Errorf("legacy action %q did not reach a terminal status: %w", schema.GetName(), context.Cause(ctx))
			case <-timer.C:
			}
			interval = min(interval*2, maxStatusPollInterval)
			timer.Reset(interval)

			st, _, pollResp, pollAnnos, err := legacyManager.GetActionStatus(ctx, id)
			if err != nil {
				statusErrs++
				if statusErrs >= maxConsecutiveStatusErrors {
					return resp, annos, err
				}
				l.Warn("legacy action status check failed, retrying",
					zap.String("action", schema.GetName()),
					zap.Int("consecutive_anomalies", statusErrs),
					zap.Error(err))
				continue
			}
			// Keep the last meaningful response for the error exits above;
			// an indeterminate poll's payload must not replace it.
			if pollResp != nil && (isInFlightActionStatus(st) || st == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE || st == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED) {
				resp, annos = pollResp, pollAnnos
			}

			switch {
			case isInFlightActionStatus(st):
				statusErrs = 0
			case st == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE || st == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED:
				return resp, annos, legacyStatusErr(schema.GetName(), st)
			default:
				// An indeterminate status gets the same tolerance as a
				// lookup error: transient anomalies recover, persistent
				// ones fail closed.
				statusErrs++
				if statusErrs >= maxConsecutiveStatusErrors {
					return resp, annos, fmt.Errorf("legacy action %q returned unexpected status %s", schema.GetName(), st.String())
				}
				l.Warn("legacy action returned indeterminate status, retrying",
					zap.String("action", schema.GetName()),
					zap.String("status", st.String()),
					zap.Int("consecutive_anomalies", statusErrs))
			}
		}
	}
	return registry.Register(ctx, schema, handler)
}

// legacyStatusErr maps a settled legacy status to the outer handler error:
// COMPLETE resolves clean, anything else fails the action.
func legacyStatusErr(name string, st v2.BatonActionStatus) error {
	switch st {
	case v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE:
		return nil
	case v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED:
		return fmt.Errorf("legacy action %q failed", name)
	default:
		return fmt.Errorf("legacy action %q returned unexpected status %s", name, st.String())
	}
}

func isInFlightActionStatus(s v2.BatonActionStatus) bool {
	return s == v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING || s == v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING
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
