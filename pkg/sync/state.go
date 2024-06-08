package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/conductorone/baton-sdk/pkg/sync/action"
	"github.com/conductorone/baton-sdk/pkg/sync/entitlements_graph"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type State interface {
	PushAction(ctx context.Context, action action.Action)
	FinishAction(ctx context.Context)
	NextPage(ctx context.Context, pageToken string) error
	ResourceTypeID(ctx context.Context) string
	ResourceID(ctx context.Context) string
	EntitlementGraph(ctx context.Context) *entitlements_graph.EntitlementGraph
	ParentResourceID(ctx context.Context) string
	ParentResourceTypeID(ctx context.Context) string
	PageToken(ctx context.Context) string
	Current() *action.Action
	Marshal() (string, error)
	Unmarshal(input string) error
	NeedsExpansion() bool
	SetNeedsExpansion()
}

// state is an object used for tracking the current status of a connector sync. It operates like a stack.
type state struct {
	mtx              sync.RWMutex
	actions          []action.Action
	currentAction    *action.Action
	entitlementGraph *entitlements_graph.EntitlementGraph
	needsExpansion   bool
}

// serializedToken is used to serialize the token to JSON. This separate object is used to avoid having exported fields
// on the object used externally. We should interface this, probably.
type serializedToken struct {
	Actions          []action.Action                      `json:"actions"`
	CurrentAction    *action.Action                       `json:"current_action"`
	NeedsExpansion   bool                                 `json:"needs_expansion"`
	EntitlementGraph *entitlements_graph.EntitlementGraph `json:"entitlement_graph"`
}

// push adds a new action to the stack. If there is no current state, the action is directly set to current, else
// the current state is appended to the slice of actions, and the new action is set to current.
func (st *state) push(action action.Action) {
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
func (st *state) pop() *action.Action {
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
func (st *state) Current() *action.Action {
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
			return fmt.Errorf("syncer token corrupt: %w", err)
		}

		st.actions = token.Actions
		st.currentAction = token.CurrentAction
		st.needsExpansion = token.NeedsExpansion
	} else {
		st.actions = nil
		st.entitlementGraph = nil
		st.currentAction = &action.Action{Op: action.InitOp}
	}

	return nil
}

// Marshal returns a string encoding of the state object. This is useful for datastores to checkpoint the current state.
func (st *state) Marshal() (string, error) {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	data, err := json.Marshal(serializedToken{
		Actions:          st.actions,
		CurrentAction:    st.currentAction,
		NeedsExpansion:   st.needsExpansion,
		EntitlementGraph: st.entitlementGraph,
	})
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// PushAction adds a new action to the stack.
func (st *state) PushAction(ctx context.Context, action action.Action) {
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

func (st *state) NeedsExpansion() bool {
	return st.needsExpansion
}

func (st *state) SetNeedsExpansion() {
	st.needsExpansion = true
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

// EntitlementGraph returns the entitlement graph for the current action.
func (st *state) EntitlementGraph(ctx context.Context) *entitlements_graph.EntitlementGraph {
	c := st.Current()
	if c == nil {
		panic("no current state")
	}
	if st.entitlementGraph == nil {
		st.entitlementGraph = entitlements_graph.New(ctx)
	}
	return st.entitlementGraph
}

func (st *state) ParentResourceID(ctx context.Context) string {
	c := st.Current()
	if c == nil {
		panic("no current state")
	}

	return c.ParentResourceID
}

func (st *state) ParentResourceTypeID(ctx context.Context) string {
	c := st.Current()
	if c == nil {
		panic("no current state")
	}

	return c.ParentResourceTypeID
}
