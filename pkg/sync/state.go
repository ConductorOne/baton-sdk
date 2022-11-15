package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type State interface {
	PushAction(ctx context.Context, action Action)
	FinishAction(ctx context.Context)
	NextPage(ctx context.Context, pageToken string) error
	ResourceTypeID(ctx context.Context) string
	ResourceID(ctx context.Context) string
	PageToken(ctx context.Context) string
	Current() *Action
	Marshal() (string, error)
	Unmarshal(input string) error
}

// ActionOp represents a sync operation.
type ActionOp uint8

// String() returns the string representation for an ActionOp. This is used for marshalling the op.
func (s ActionOp) String() string {
	switch s {
	case InitOp:
		return "init"
	case SyncResourceTypesOp:
		return "list-resource-types"
	case SyncResourcesOp:
		return "list-resources"
	case SyncEntitlementsOp:
		return "list-entitlements"
	case SyncGrantsOp:
		return "list-grants"
	case SyncAssetsOp:
		return "fetch-assets"
	default:
		return "unknown"
	}
}

// MarshalJSON marshals the ActionOp insto a json string.
func (s *ActionOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON unmarshal's the input byte slice and updates this action op.
func (s *ActionOp) UnmarshalJSON(data []byte) error {
	var v string
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}

	*s = newActionOp(v)
	return nil
}

// newActionOp returns a new ActionOp given a string name. This is useful for unmarshalling.
func newActionOp(str string) ActionOp {
	switch str {
	case InitOp.String():
		return InitOp
	case SyncResourceTypesOp.String():
		return SyncResourceTypesOp
	case SyncResourcesOp.String():
		return SyncResourcesOp
	case SyncEntitlementsOp.String():
		return SyncEntitlementsOp
	case SyncGrantsOp.String():
		return SyncGrantsOp
	case SyncAssetsOp.String():
		return SyncAssetsOp
	default:
		return UnknownOp
	}
}

const (
	UnknownOp ActionOp = iota
	InitOp
	SyncResourceTypesOp
	SyncResourcesOp
	SyncEntitlementsOp
	ListResourcesForEntitlementsOp
	SyncGrantsOp
	SyncAssetsOp
)

// Action stores the current operation, page token, and optional fields for which resource is being worked with.
type Action struct {
	Op             ActionOp `json:"operation"`
	PageToken      string   `json:"page_token"`
	ResourceTypeID string   `json:"resource_type_id"`
	ResourceID     string   `json:"resource_id"`
}

// state is an object used for tracking the current status of a connector sync. It operates like a stack.
type state struct {
	mtx           sync.RWMutex
	actions       []Action
	currentAction *Action
}

// serializedToken is used to serialize the token to JSON. This separate object is used to avoid having exported fields
// on the object used externally. We should interface this, probably.
type serializedToken struct {
	Actions       []Action `json:"actions"`
	CurrentAction *Action  `json:"current_action"`
}

// push adds a new action to the stack. If there is no current state, the action is directly set to current, else
// the current state is appened to the slice of actions, and the new action is set to current.
func (st *state) push(action Action) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if st.currentAction == nil {
		st.currentAction = &action
		return
	}

	st.actions = append(st.actions, *st.currentAction)
	st.currentAction = &action
}

// pop returns nil if there is no current action. Otherwise it returns the current action, and replace it with the last
// item in the actions slice.
func (st *state) pop() *Action {
	st.mtx.Lock()
	defer st.mtx.Unlock()
	if st.currentAction == nil {
		return nil
	}

	ret := *st.currentAction

	if len(st.actions) > 0 {
		st.currentAction = &st.actions[len(st.actions)-1]
		st.actions = st.actions[:len(st.actions)-1]
	} else {
		st.currentAction = nil
	}

	return &ret
}

// Current returns nil if there is no current action. Otherwise it returns a pointer to a copy of the current state.
func (st *state) Current() *Action {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	if st.currentAction == nil {
		return nil
	}

	current := *st.currentAction
	return &current
}

// Unmarshal takes an input string and unmarshals it onto the state object. If the input is empty, we set the state to
// have an init action.
func (st *state) Unmarshal(input string) error {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	token := serializedToken{}

	if input != "" {
		err := json.Unmarshal([]byte(input), &token)
		if err != nil {
			return fmt.Errorf("syncer token corrust: %w", err)
		}

		st.actions = token.Actions
		st.currentAction = token.CurrentAction
	} else {
		st.actions = nil
		st.currentAction = &Action{Op: InitOp}
	}

	return nil
}

// Marshal returns a string encoding of the state object. This is useful for datastores to checkpoint the current state.
func (st *state) Marshal() (string, error) {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	data, err := json.Marshal(serializedToken{
		Actions:       st.actions,
		CurrentAction: st.currentAction,
	})
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// PushAction adds a new action to the stack.
func (st *state) PushAction(ctx context.Context, action Action) {
	st.push(action)
	ctxzap.Extract(ctx).Debug("pushing action", zap.Any("action", action))
}

// FinishAction pops the current action from the state.
func (st *state) FinishAction(ctx context.Context) {
	action := st.pop()
	ctxzap.Extract(ctx).Debug("finishing action", zap.Any("action", action))
}

// NextPage pops the current action, and pushes a copy of it with the provided page token. This is useful for paginating
// requests.
func (st *state) NextPage(ctx context.Context, pageToken string) error {
	action := st.pop()
	if action == nil {
		return fmt.Errorf("no active syncer action")
	}

	action.PageToken = pageToken

	st.push(*action)
	ctxzap.Extract(ctx).Debug("pushing next page action", zap.Any("action", action))

	return nil
}

// PageToken returns the page token for the current action.
func (st *state) PageToken(ctx context.Context) string {
	c := st.Current()
	if c == nil {
		panic("no current state")
	}

	return c.PageToken
}

// ResourceTypeID returns the resource type id for the current action.
func (st *state) ResourceTypeID(ctx context.Context) string {
	c := st.Current()
	if c == nil {
		panic("no current state")
	}

	return c.ResourceTypeID
}

// ResourceID returns the resource ID for the current action.
func (st *state) ResourceID(ctx context.Context) string {
	c := st.Current()
	if c == nil {
		panic("no current state")
	}

	return c.ResourceID
}
