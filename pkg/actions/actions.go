package actions

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type ActionHandler func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error)

type OutstandingAction struct {
	Id        string
	Name      string
	Status    v2.BatonActionStatus
	Rv        *structpb.Struct
	Annos     annotations.Annotations
	Err       error
	StartedAt time.Time
	sync.Mutex
}

func NewOutstandingAction(id, name string) *OutstandingAction {
	return &OutstandingAction{
		Id:        id,
		Name:      name,
		Status:    v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING,
		StartedAt: time.Now(),
	}
}

func (oa *OutstandingAction) SetStatus(ctx context.Context, status v2.BatonActionStatus) {
	oa.Lock()
	defer oa.Unlock()
	l := ctxzap.Extract(ctx).With(
		zap.String("action_id", oa.Id),
		zap.String("action_name", oa.Name),
		zap.String("status", status.String()),
	)
	if oa.Status == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE || oa.Status == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		l.Error("cannot set status on completed action")
	}
	if status == v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING && oa.Status != v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING {
		l.Error("cannot set status to running unless action is pending")
	}

	oa.Status = status
}

func (oa *OutstandingAction) setError(_ context.Context, err error) {
	oa.Lock()
	defer oa.Unlock()
	if oa.Rv == nil {
		oa.Rv = &structpb.Struct{}
	}
	if oa.Rv.Fields == nil {
		oa.Rv.Fields = make(map[string]*structpb.Value)
	}
	oa.Rv.Fields["error"] = &structpb.Value{
		Kind: &structpb.Value_StringValue{
			StringValue: err.Error(),
		},
	}
	oa.Err = err
}

func (oa *OutstandingAction) SetError(ctx context.Context, err error) {
	oa.setError(ctx, err)
	oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED)
}

const maxOldActions = 1000

// ActionRegistry provides methods for registering actions.
// Used by both GlobalActionProvider (global actions) and ResourceActionProvider (resource-scoped actions).
type ActionRegistry interface {
	// Register registers an action using the name from the schema.
	Register(ctx context.Context, schema *v2.BatonActionSchema, handler ActionHandler) error

	// Deprecated: Use Register instead.
	// RegisterAction registers an action.
	RegisterAction(ctx context.Context, name string, schema *v2.BatonActionSchema, handler ActionHandler) error
}

// Deprecated: Use ActionRegistry instead.
// ResourceTypeActionRegistry is an alias for ActionRegistry for backwards compatibility.
type ResourceTypeActionRegistry = ActionRegistry

// ActionManager manages both global actions and resource-scoped actions.
type ActionManager struct {
	// Global actions (no resource type)
	schemas  map[string]*v2.BatonActionSchema // actionName -> schema
	handlers map[string]ActionHandler         // actionName -> handler

	// Resource-scoped actions (keyed by resource type)
	resourceSchemas  map[string]map[string]*v2.BatonActionSchema // resourceTypeID -> actionName -> schema
	resourceHandlers map[string]map[string]ActionHandler         // resourceTypeID -> actionName -> handler

	// Outstanding actions (shared across global and resource-scoped)
	actions map[string]*OutstandingAction // actionID -> outstanding action

	mu sync.RWMutex
}

func NewActionManager(_ context.Context) *ActionManager {
	return &ActionManager{
		schemas:          make(map[string]*v2.BatonActionSchema),
		handlers:         make(map[string]ActionHandler),
		resourceSchemas:  make(map[string]map[string]*v2.BatonActionSchema),
		resourceHandlers: make(map[string]map[string]ActionHandler),
		actions:          make(map[string]*OutstandingAction),
	}
}

func (a *ActionManager) GetNewActionId() string {
	uid := ksuid.New()
	return uid.String()
}

func (a *ActionManager) GetNewAction(name string) *OutstandingAction {
	a.mu.Lock()
	defer a.mu.Unlock()
	actionId := a.GetNewActionId()
	oa := NewOutstandingAction(actionId, name)
	a.actions[actionId] = oa
	return oa
}

func (a *ActionManager) CleanupOldActions(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.actions) < maxOldActions {
		return
	}

	l := ctxzap.Extract(ctx)
	l.Debug("cleaning up old actions")
	// Create a slice to hold the actions
	actionList := make([]*OutstandingAction, 0, len(a.actions))
	for _, action := range a.actions {
		actionList = append(actionList, action)
	}

	// Sort the actions by StartedAt time
	sort.Slice(actionList, func(i, j int) bool {
		return actionList[i].StartedAt.Before(actionList[j].StartedAt)
	})

	count := 0
	// Delete the oldest actions
	for i := 0; i < len(actionList)-maxOldActions; i++ {
		action := actionList[i]
		if action.Status == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE || action.Status == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
			count++
			delete(a.actions, actionList[i].Id)
		}
	}
	l.Debug("cleaned up old actions", zap.Int("count", count))
}

func (a *ActionManager) registerActionSchema(_ context.Context, name string, schema *v2.BatonActionSchema) error {
	if name == "" {
		return errors.New("action name cannot be empty")
	}
	if schema == nil {
		return errors.New("action schema cannot be nil")
	}
	if _, ok := a.schemas[name]; ok {
		return fmt.Errorf("action schema %s already registered", name)
	}
	a.schemas[name] = schema
	return nil
}

// Register registers a global action using the name from the schema.
func (a *ActionManager) Register(ctx context.Context, schema *v2.BatonActionSchema, handler ActionHandler) error {
	if schema == nil {
		return errors.New("action schema cannot be nil")
	}
	return a.RegisterAction(ctx, schema.GetName(), schema, handler)
}

// Deprecated: Use Register instead.
// RegisterAction registers a global action (not scoped to a resource type).
func (a *ActionManager) RegisterAction(ctx context.Context, name string, schema *v2.BatonActionSchema, handler ActionHandler) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if handler == nil {
		return errors.New("action handler cannot be nil")
	}
	err := a.registerActionSchema(ctx, name, schema)
	if err != nil {
		return err
	}

	if _, ok := a.handlers[name]; ok {
		return fmt.Errorf("action handler %s already registered", name)
	}
	a.handlers[name] = handler

	l := ctxzap.Extract(ctx)
	l.Debug("registered action", zap.String("name", name))

	return nil
}

func (a *ActionManager) HasActions() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.schemas) > 0 || len(a.resourceSchemas) > 0
}

// RegisterResourceAction registers a resource-scoped action.
func (a *ActionManager) RegisterResourceAction(
	ctx context.Context,
	resourceTypeID string,
	schema *v2.BatonActionSchema,
	handler ActionHandler,
) error {
	if resourceTypeID == "" {
		return errors.New("resource type ID cannot be empty")
	}
	if schema == nil {
		return errors.New("action schema cannot be nil")
	}
	if schema.GetName() == "" {
		return errors.New("action schema name cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil for action %s", schema.GetName())
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Set the resource type ID on the schema
	schema.SetResourceTypeId(resourceTypeID)

	if a.resourceSchemas[resourceTypeID] == nil {
		a.resourceSchemas[resourceTypeID] = make(map[string]*v2.BatonActionSchema)
	}
	if a.resourceHandlers[resourceTypeID] == nil {
		a.resourceHandlers[resourceTypeID] = make(map[string]ActionHandler)
	}

	actionName := schema.GetName()

	// Check for duplicate action names
	if _, ok := a.resourceSchemas[resourceTypeID][actionName]; ok {
		return fmt.Errorf("action schema %s already registered for resource type %s", actionName, resourceTypeID)
	}

	// Check for duplicate action types
	if len(schema.GetActionType()) > 0 {
		for existingName, existingSchema := range a.resourceSchemas[resourceTypeID] {
			if existingSchema == nil || len(existingSchema.GetActionType()) == 0 {
				continue
			}
			// Check if any ActionType in the new schema matches any in existing schemas
			for _, newActionType := range schema.GetActionType() {
				if newActionType == v2.ActionType_ACTION_TYPE_UNSPECIFIED || newActionType == v2.ActionType_ACTION_TYPE_DYNAMIC {
					continue // Skip unspecified and dynamic types as they can overlap
				}
				for _, existingActionType := range existingSchema.GetActionType() {
					if newActionType == existingActionType {
						return fmt.Errorf("action type %s already registered for resource type %s (existing action: %s)", newActionType.String(), resourceTypeID, existingName)
					}
				}
			}
		}
	}

	a.resourceSchemas[resourceTypeID][actionName] = schema
	a.resourceHandlers[resourceTypeID][actionName] = handler

	ctxzap.Extract(ctx).Debug("registered resource action", zap.String("resource_type", resourceTypeID), zap.String("action_name", actionName))

	return nil
}

// resourceTypeActionRegistry implements ResourceTypeActionRegistry for a specific resource type.
type resourceTypeActionRegistry struct {
	resourceTypeID string
	actionManager  *ActionManager
}

func (r *resourceTypeActionRegistry) Register(ctx context.Context, schema *v2.BatonActionSchema, handler ActionHandler) error {
	return r.actionManager.RegisterResourceAction(ctx, r.resourceTypeID, schema, handler)
}

// Deprecated: Use Register instead.
// RegisterAction registers a resource-scoped action. The name parameter is ignored; the name from schema is used.
func (r *resourceTypeActionRegistry) RegisterAction(ctx context.Context, name string, schema *v2.BatonActionSchema, handler ActionHandler) error {
	return r.Register(ctx, schema, handler)
}

// GetTypeRegistry returns an ActionRegistry for registering actions scoped to a specific resource type.
func (a *ActionManager) GetTypeRegistry(_ context.Context, resourceTypeID string) (ActionRegistry, error) {
	if resourceTypeID == "" {
		return nil, errors.New("resource type ID cannot be empty")
	}
	return &resourceTypeActionRegistry{resourceTypeID: resourceTypeID, actionManager: a}, nil
}

func (a *ActionManager) UnregisterAction(ctx context.Context, name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.schemas[name]; !ok {
		return fmt.Errorf("action %s not registered", name)
	}
	delete(a.schemas, name)
	if _, ok := a.handlers[name]; !ok {
		return fmt.Errorf("action handler %s not registered", name)
	}
	delete(a.handlers, name)

	l := ctxzap.Extract(ctx)
	l.Debug("unregistered action", zap.String("name", name))

	// TODO: cancel & clean up outstanding actions?

	return nil
}

// ListActionSchemas returns all action schemas, optionally filtered by resource type.
// If resourceTypeID is empty, returns all global actions plus all resource-scoped actions.
// If resourceTypeID is set, returns only actions for that resource type.
func (a *ActionManager) ListActionSchemas(_ context.Context, resourceTypeID string) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var rv []*v2.BatonActionSchema

	if resourceTypeID == "" {
		// Return all global actions
		rv = make([]*v2.BatonActionSchema, 0, len(a.schemas))
		for _, schema := range a.schemas {
			rv = append(rv, schema)
		}

		// Also return all resource-scoped actions
		for _, schemas := range a.resourceSchemas {
			for _, schema := range schemas {
				rv = append(rv, schema)
			}
		}
	} else {
		// Return only actions for the specified resource type
		schemas, ok := a.resourceSchemas[resourceTypeID]
		if !ok {
			return []*v2.BatonActionSchema{}, nil, nil
		}

		rv = make([]*v2.BatonActionSchema, 0, len(schemas))
		for _, schema := range schemas {
			rv = append(rv, schema)
		}
	}

	return rv, nil, nil
}

func (a *ActionManager) GetActionSchema(_ context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	schema, ok := a.schemas[name]
	if !ok {
		return nil, nil, status.Error(codes.NotFound, fmt.Sprintf("action %s not found", name))
	}
	return schema, nil, nil
}

func (a *ActionManager) GetActionStatus(_ context.Context, actionId string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	oa := a.actions[actionId]
	if oa == nil {
		return v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, "", nil, nil, status.Error(codes.NotFound, fmt.Sprintf("action id %s not found", actionId))
	}

	// Don't return oa.Err here because error is for GetActionStatus, not the action itself.
	// oa.Rv contains any error.
	return oa.Status, oa.Name, oa.Rv, oa.Annos, nil
}

// InvokeAction invokes an action. If resourceTypeID is set, it invokes a resource-scoped action.
// Otherwise, it invokes a global action.
func (a *ActionManager) InvokeAction(
	ctx context.Context,
	name string,
	resourceTypeID string,
	args *structpb.Struct,
) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	if resourceTypeID != "" {
		return a.invokeResourceAction(ctx, resourceTypeID, name, args)
	}

	return a.invokeGlobalAction(ctx, name, args)
}

// invokeGlobalAction invokes a global (non-resource-scoped) action.
func (a *ActionManager) invokeGlobalAction(ctx context.Context, name string, args *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	a.mu.RLock()
	handler, ok := a.handlers[name]
	schema, schemaOk := a.schemas[name]
	a.mu.RUnlock()

	if !ok {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.NotFound, fmt.Sprintf("handler for action %s not found", name))
	}
	if !schemaOk || schema == nil {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.Internal, fmt.Sprintf("schema for action %s not found", name))
	}

	// Validate constraints
	if err := validateActionConstraints(schema.GetConstraints(), args); err != nil {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}

	oa := a.GetNewAction(name)

	done := make(chan struct{})

	// If handler exits within a second, return result.
	// If handler takes longer than 1 second, return status pending.
	// If handler takes longer than an hour, return status failed.
	go func() {
		defer close(done)
		oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
		handlerCtx, cancel := context.WithTimeoutCause(context.Background(), 1*time.Hour, errors.New("action handler timed out"))
		defer cancel()
		var oaErr error
		oa.Rv, oa.Annos, oaErr = handler(handlerCtx, args)
		if oaErr == nil {
			oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE)
		} else {
			oa.SetError(ctx, oaErr)
		}
	}()

	select {
	case <-done:
		return oa.Id, oa.Status, oa.Rv, oa.Annos, nil
	case <-time.After(1 * time.Second):
		return oa.Id, oa.Status, oa.Rv, oa.Annos, nil
	case <-ctx.Done():
		oa.SetError(ctx, ctx.Err())
		return oa.Id, oa.Status, oa.Rv, oa.Annos, ctx.Err()
	}
}

// invokeResourceAction invokes a resource-scoped action.
func (a *ActionManager) invokeResourceAction(
	ctx context.Context,
	resourceTypeID string,
	actionName string,
	args *structpb.Struct,
) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	if resourceTypeID == "" {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.InvalidArgument, "resource type ID is required")
	}
	if actionName == "" {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.InvalidArgument, "action name is required")
	}

	a.mu.RLock()
	handlers, ok := a.resourceHandlers[resourceTypeID]
	if !ok {
		a.mu.RUnlock()
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.NotFound, fmt.Sprintf("no actions found for resource type %s", resourceTypeID))
	}

	handler, ok := handlers[actionName]
	if !ok {
		a.mu.RUnlock()
		return "",
			v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED,
			nil,
			nil,
			status.Error(codes.NotFound, fmt.Sprintf("handler for action %s not found for resource type %s", actionName, resourceTypeID))
	}

	schemas, ok := a.resourceSchemas[resourceTypeID]
	if !ok {
		a.mu.RUnlock()
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.Internal, fmt.Sprintf("schemas not found for resource type %s", resourceTypeID))
	}

	schema, ok := schemas[actionName]
	if !ok {
		a.mu.RUnlock()
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.Internal, fmt.Sprintf("schema not found for action %s", actionName))
	}
	a.mu.RUnlock()

	// Validate constraints
	if err := validateActionConstraints(schema.GetConstraints(), args); err != nil {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}

	oa := a.GetNewAction(actionName)
	done := make(chan struct{})

	// Invoke handler in goroutine
	go func() {
		defer close(done)
		oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
		handlerCtx, cancel := context.WithTimeoutCause(context.Background(), 1*time.Hour, errors.New("action handler timed out"))
		defer cancel()
		var oaErr error
		oa.Rv, oa.Annos, oaErr = handler(handlerCtx, args)
		if oaErr == nil {
			oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE)
		} else {
			oa.SetError(ctx, oaErr)
		}
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		return oa.Id, oa.Status, oa.Rv, oa.Annos, nil
	case <-time.After(1 * time.Second):
		return oa.Id, oa.Status, oa.Rv, oa.Annos, nil
	case <-ctx.Done():
		oa.SetError(ctx, ctx.Err())
		return oa.Id, oa.Status, oa.Rv, oa.Annos, ctx.Err()
	}
}

// validateActionConstraints validates that the provided args satisfy the schema constraints.
func validateActionConstraints(constraints []*config.Constraint, args *structpb.Struct) error {
	if len(constraints) == 0 {
		return nil
	}

	// Build map of present fields (non-null values in struct)
	present := make(map[string]bool)
	if args != nil {
		for fieldName, value := range args.GetFields() {
			if !isNullValue(value) {
				present[fieldName] = true
			}
		}
	}

	// Validate each constraint
	for _, constraint := range constraints {
		if err := validateConstraint(constraint, present); err != nil {
			return err
		}
	}
	return nil
}

func validateConstraint(c *config.Constraint, present map[string]bool) error {
	// Deduplicate field names to handle cases where the same field is listed multiple times
	uniqueFieldNames := deduplicateStrings(c.GetFieldNames())

	// Count how many unique primary fields are present
	var primaryPresent int
	for _, name := range uniqueFieldNames {
		if present[name] {
			primaryPresent++
		}
	}

	switch c.GetKind() {
	case config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER:
		if primaryPresent > 0 && primaryPresent < len(uniqueFieldNames) {
			return fmt.Errorf("fields required together: %v", uniqueFieldNames)
		}
	case config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE:
		if primaryPresent > 1 {
			return fmt.Errorf("fields are mutually exclusive: %v", uniqueFieldNames)
		}
	case config.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE:
		if primaryPresent == 0 {
			return fmt.Errorf("at least one required: %v", uniqueFieldNames)
		}
	case config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON:
		if primaryPresent > 0 {
			// Deduplicate secondary field names and check they are all present
			uniqueSecondaryFieldNames := deduplicateStrings(c.GetSecondaryFieldNames())
			for _, name := range uniqueSecondaryFieldNames {
				if !present[name] {
					return fmt.Errorf("fields %v depend on %v which must also be present", uniqueFieldNames, uniqueSecondaryFieldNames)
				}
			}
		}
	case config.ConstraintKind_CONSTRAINT_KIND_UNSPECIFIED:
		return nil
	default:
		return fmt.Errorf("unknown constraint kind: %v", c.GetKind())
	}
	return nil
}

// deduplicateStrings returns a new slice with duplicate strings removed, preserving order.
func deduplicateStrings(input []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(input))
	for _, s := range input {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}

func isNullValue(v *structpb.Value) bool {
	if v == nil {
		return true
	}
	_, isNull := v.GetKind().(*structpb.Value_NullValue)
	return isNull
}
