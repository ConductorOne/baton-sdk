package actions

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type ActionHandler func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error)

type OutstandingAction struct {
	Name   string
	Status v2.BatonActionStatus
	Rv     *structpb.Struct
	Annos  annotations.Annotations
	Err    error
}

// TODO: use syncmaps here
type ActionManager struct {
	actionId uint64
	schemas  map[string]*v2.BatonActionSchema // map of action name to schema
	handlers map[string]ActionHandler
	actions  map[string]OutstandingAction // map of actions IDs
}

func NewActionManager(_ context.Context) *ActionManager {
	return &ActionManager{
		schemas:  make(map[string]*v2.BatonActionSchema),
		handlers: make(map[string]ActionHandler),
		actions:  make(map[string]OutstandingAction),
	}
}

func (a *ActionManager) GetNewActionId() string {
	a.actionId++
	return strconv.FormatUint(a.actionId, 10)
}

func (a *ActionManager) registerActionSchema(ctx context.Context, name string, schema *v2.BatonActionSchema) error {
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

func (a *ActionManager) RegisterAction(ctx context.Context, name string, schema *v2.BatonActionSchema, handler ActionHandler) error {
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

func (a *ActionManager) UnregisterAction(ctx context.Context, name string) error {
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

func (a *ActionManager) ListActionSchemas(ctx context.Context) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	rv := make([]*v2.BatonActionSchema, 0, len(a.schemas))
	for _, schema := range a.schemas {
		rv = append(rv, schema)
	}

	return rv, nil, nil
}

func (a *ActionManager) GetActionSchema(ctx context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	schema, ok := a.schemas[name]
	if !ok {
		return nil, nil, status.Error(codes.NotFound, fmt.Sprintf("action %s not found", name))
	}
	return schema, nil, nil
}

func (a *ActionManager) GetActionStatus(ctx context.Context, actionId string) (v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	oa, ok := a.actions[actionId]
	if !ok {
		return v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, nil, nil, status.Error(codes.NotFound, fmt.Sprintf("action id %s not found", actionId))
	}

	return oa.Status, oa.Rv, nil, nil
}

func (a *ActionManager) InvokeAction(ctx context.Context, name string, args *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	handler, ok := a.handlers[name]
	if !ok {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.NotFound, fmt.Sprintf("handler for action %s not found", name))
	}

	actionId := a.GetNewActionId()
	oa := OutstandingAction{
		Name:   name,
		Status: v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING,
	}
	a.actions[actionId] = oa

	oa.Rv, oa.Annos, oa.Err = handler(ctx, args)
	if oa.Err == nil {
		oa.Status = v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE
	} else {
		oa.Status = v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED
	}

	return actionId, oa.Status, oa.Rv, oa.Annos, oa.Err
}
