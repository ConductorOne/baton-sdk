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
	"google.golang.org/protobuf/types/known/structpb"
)

type SyncActionHandler func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error)

// 	Status(ctx context.Context) (v2.BatonActionStatus, annotations.Annotations, error)
// 	// Finish(ctx context.Context) (v2.BatonActionStatus, annotations.Annotations, error)
// }

// TODO: Add support for async handlers
type AsyncActionHandler interface {
	Start(ctx context.Context, args *structpb.Struct) (*structpb.Struct, error)
	Status(ctx context.Context) (v2.BatonActionStatus, annotations.Annotations, error)
	Finish(ctx context.Context) (v2.BatonActionStatus, annotations.Annotations, error)
}

type ActionHandler SyncActionHandler

type OutstandingAction struct {
	handler ActionHandler
	status  v2.BatonActionStatus
}

type ActionManager struct {
	actionId           uint64
	schemas            map[string]*v2.BatonActionSchema // map of action name to schema
	handlers           map[string]ActionHandler         // map of action names to action handlers
	outstandingActions map[string]OutstandingAction     // map of action ids to action handlers
}

func NewActionManager(_ context.Context) *ActionManager {
	return &ActionManager{
		schemas:            make(map[string]*v2.BatonActionSchema),
		handlers:           make(map[string]ActionHandler),
		outstandingActions: make(map[string]OutstandingAction),
	}
}

func (a *ActionManager) GetNewActionId() string {
	a.actionId++
	return strconv.FormatUint(a.actionId, 10)
}

func (a *ActionManager) RegisterAction(ctx context.Context, name string, schema *v2.BatonActionSchema, handler ActionHandler) error {
	if name == "" {
		return errors.New("action name cannot be empty")
	}
	if schema == nil {
		return errors.New("action schema cannot be nil")
	}
	if handler == nil {
		return errors.New("action handler cannot be nil")
	}
	if _, ok := a.schemas[name]; ok {
		return fmt.Errorf("action schema %s already registered", name)
	}
	a.schemas[name] = schema
	if _, ok := a.handlers[name]; ok {
		return fmt.Errorf("action handler %s already registered", name)
	}
	a.handlers[name] = handler

	l := ctxzap.Extract(ctx)
	l.Debug("registered action", zap.String("name", name))

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
		return nil, nil, fmt.Errorf("action %s not found", name)
	}
	return schema, nil, nil
}

func (a *ActionManager) GetActionStatus(ctx context.Context, actionId string) (v2.BatonActionStatus, annotations.Annotations, error) {
	_, ok := a.outstandingActions[actionId]
	if !ok {
		return v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, nil, fmt.Errorf("action %s not found", actionId)
	}

	// TODO: for async, check handler and run Status() on it
	return v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, nil, nil

	// switch handler.(type) {
	// case AsyncActionHandler:
	// 	return handler.asyncHandler.Status(ctx)
	// case SyncActionHandler:
	// 	return v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, nil, nil
	// default:
	// 	return v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, nil, fmt.Errorf("unknown action handler type")
	// }
	// 	return status, annos, nil
}

func (a *ActionManager) InvokeAction(ctx context.Context, name string, args *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	handler, ok := a.handlers[name]
	if !ok {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, fmt.Errorf("action %s not found", name)
	}

	// TODO: check interface type of handler and behave different for sync vs async

	// TODO: maybe put this in its own function?
	actionId := a.GetNewActionId()
	a.outstandingActions[actionId] = OutstandingAction{
		handler: handler,
		status:  v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING,
	}
	defer func() {
		delete(a.outstandingActions, actionId)
	}()

	rv, annos, err := handler(ctx, args)
	if err != nil {
		return actionId, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, rv, annos, err
	}

	return actionId, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, rv, annos, nil
}
