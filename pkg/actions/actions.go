package actions

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

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

func (oa *OutstandingAction) SetStatus(status v2.BatonActionStatus) error {
	oa.Mutex.Lock()
	defer oa.Mutex.Unlock()
	if oa.Status == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE || oa.Status == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		return errors.New("cannot set status on completed action")
	}
	if status == v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING && oa.Status != v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING {
		return errors.New("cannot set status to running unless action is pending")
	}

	oa.Status = status
	return nil
}

const maxOldActions = 1000

// TODO: use syncmaps or some other sort of locking mechanism
type ActionManager struct {
	schemas  map[string]*v2.BatonActionSchema // map of action name to schema
	handlers map[string]ActionHandler
	actions  map[string]*OutstandingAction // map of actions IDs
}

func NewActionManager(_ context.Context) *ActionManager {
	return &ActionManager{
		schemas:  make(map[string]*v2.BatonActionSchema),
		handlers: make(map[string]ActionHandler),
		actions:  make(map[string]*OutstandingAction),
	}
}

func (a *ActionManager) GetNewActionId() string {
	uid := ksuid.New()
	return uid.String()
}

func (a *ActionManager) GetNewAction(name string) *OutstandingAction {
	actionId := a.GetNewActionId()
	oa := NewOutstandingAction(actionId, name)
	a.actions[actionId] = oa
	return oa
}

func (a *ActionManager) CleanupOldActions(ctx context.Context) {
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
	oa := a.actions[actionId]
	if oa == nil {
		return v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, nil, nil, status.Error(codes.NotFound, fmt.Sprintf("action id %s not found", actionId))
	}

	return oa.Status, oa.Rv, nil, nil
}

func (a *ActionManager) InvokeAction(ctx context.Context, name string, args *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	handler, ok := a.handlers[name]
	if !ok {
		return "", v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, nil, nil, status.Error(codes.NotFound, fmt.Sprintf("handler for action %s not found", name))
	}

	oa := a.GetNewAction(name)

	done := make(chan struct{})

	// If handler exits within a second, return result.
	// If handler takes longer than 10 seconds, return status pending.
	// If handler takes longer than an hour, return status failed.
	handlerCtx, _ := context.WithTimeoutCause(ctx, 1*time.Hour, errors.New("action handler timed out"))
	go func() {
		oa.SetStatus(v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
		oa.Rv, oa.Annos, oa.Err = handler(handlerCtx, args)
		if oa.Err == nil {
			oa.SetStatus(v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE)
		} else {
			oa.SetStatus(v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED)
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
		return oa.Id, oa.Status, oa.Rv, oa.Annos, oa.Err
	case <-time.After(10 * time.Second):
		return oa.Id, oa.Status, oa.Rv, oa.Annos, oa.Err
	case <-ctx.Done():
		oa.SetStatus(v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED)
		return oa.Id, oa.Status, nil, nil, ctx.Err()
	}
}
