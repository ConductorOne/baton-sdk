package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type State interface {
	PushAction(ctx context.Context, action Action)
	FinishAction(ctx context.Context)
	NextPage(ctx context.Context, pageToken string) error
	ResourceTypeID(ctx context.Context) string
	ResourceID(ctx context.Context) string
	EntitlementGraph(ctx context.Context) *expand.EntitlementGraph
	ClearEntitlementGraph(ctx context.Context)
	ParentResourceID(ctx context.Context) string
	ParentResourceTypeID(ctx context.Context) string
	PageToken(ctx context.Context) string
	Current() *Action
	Marshal() (string, error)
	Unmarshal(input string) error
	NeedsExpansion() bool
	SetNeedsExpansion()
	HasExternalResourcesGrants() bool
	SetHasExternalResourcesGrants()
	ShouldFetchRelatedResources() bool
	SetShouldFetchRelatedResources()
	ShouldSkipEntitlementsAndGrants() bool
	SetShouldSkipEntitlementsAndGrants()
	ShouldSkipGrants() bool
	SetShouldSkipGrants()
	GetCompletedActionsCount() uint64
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
	case ListResourcesForEntitlementsOp:
		return "list-resources-for-entitlements"
	case SyncGrantsOp:
		return "list-grants"
	case SyncExternalResourcesOp:
		return "list-external-resources"
	case SyncAssetsOp:
		return "fetch-assets"
	case SyncGrantExpansionOp:
		return "grant-expansion"
	case SyncTargetedResourceOp:
		return "targeted-resource-sync"
	case SyncStaticEntitlementsOp:
		return "list-static-entitlements"
	default:
		return "unknown"
	}
}

// MarshalJSON marshals the ActionOp into a json string.
func (s *ActionOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON unmarshals the input byte slice and updates this action op.
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
	case SyncGrantExpansionOp.String():
		return SyncGrantExpansionOp
	case SyncExternalResourcesOp.String():
		return SyncExternalResourcesOp
	case SyncTargetedResourceOp.String():
		return SyncTargetedResourceOp
	case SyncStaticEntitlementsOp.String():
		return SyncStaticEntitlementsOp
	case ListResourcesForEntitlementsOp.String():
		return ListResourcesForEntitlementsOp
	default:
		return UnknownOp
	}
}

// Do not change the order of these constants, and only append new ones at the end.
// Otherwise resuming a sync started by an older version of baton-sdk will cause very strange behavior.
const (
	UnknownOp ActionOp = iota
	InitOp
	SyncResourceTypesOp
	SyncResourcesOp
	SyncEntitlementsOp
	ListResourcesForEntitlementsOp
	SyncGrantsOp
	SyncExternalResourcesOp
	SyncAssetsOp
	SyncGrantExpansionOp
	SyncTargetedResourceOp
	SyncStaticEntitlementsOp
)

// Action stores the current operation, page token, and optional fields for which resource is being worked with.
type Action struct {
	Op                   ActionOp `json:"operation,omitempty"`
	PageToken            string   `json:"page_token,omitempty"`
	ResourceTypeID       string   `json:"resource_type_id,omitempty"`
	ResourceID           string   `json:"resource_id,omitempty"`
	ParentResourceTypeID string   `json:"parent_resource_type_id,omitempty"`
	ParentResourceID     string   `json:"parent_resource_id,omitempty"`
}

// state is an object used for tracking the current status of a connector sync. It operates like a stack.
type state struct {
	mtx                             sync.RWMutex
	actions                         []Action
	currentAction                   *Action
	entitlementGraph                *expand.EntitlementGraph
	needsExpansion                  bool
	hasExternalResourceGrants       bool
	shouldFetchRelatedResources     bool
	shouldSkipEntitlementsAndGrants bool
	shouldSkipGrants                bool
	completedActionsCount           uint64
}

// serializedToken is used to serialize the token to JSON. This separate object is used to avoid having exported fields
// on the object used externally. We should interface this, probably.
type serializedToken struct {
	Actions                         []Action                 `json:"actions,omitempty"`
	CurrentAction                   *Action                  `json:"current_action,omitempty"`
	NeedsExpansion                  bool                     `json:"needs_expansion,omitempty"`
	EntitlementGraph                *expand.EntitlementGraph `json:"entitlement_graph,omitempty"`
	HasExternalResourceGrants       bool                     `json:"has_external_resource_grants,omitempty"`
	ShouldFetchRelatedResources     bool                     `json:"should_fetch_related_resources,omitempty"`
	ShouldSkipEntitlementsAndGrants bool                     `json:"should_skip_entitlements_and_grants,omitempty"`
	ShouldSkipGrants                bool                     `json:"should_skip_grants,omitempty"`
	CompletedActionsCount           uint64                   `json:"completed_actions_count,omitempty"`
}

// push adds a new action to the stack. If there is no current state, the action is directly set to current, else
// the current state is appended to the slice of actions, and the new action is set to current.
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
	st.completedActionsCount++

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
	st.mtx.Lock()
	defer st.mtx.Unlock()

	token := serializedToken{}

	if input != "" {
		err := json.Unmarshal([]byte(input), &token)
		if err != nil {
			return fmt.Errorf("syncer token corrupt: %w", err)
		}

		st.actions = token.Actions
		st.currentAction = token.CurrentAction
		st.needsExpansion = token.NeedsExpansion
		st.entitlementGraph = token.EntitlementGraph
		st.hasExternalResourceGrants = token.HasExternalResourceGrants
		st.shouldSkipEntitlementsAndGrants = token.ShouldSkipEntitlementsAndGrants
		st.shouldSkipGrants = token.ShouldSkipGrants
		st.shouldFetchRelatedResources = token.ShouldFetchRelatedResources
		st.completedActionsCount = token.CompletedActionsCount
	} else {
		st.actions = nil
		st.entitlementGraph = nil
		st.currentAction = &Action{Op: InitOp}
		st.completedActionsCount = 0
	}

	return nil
}

// Marshal returns a string encoding of the state object. This is useful for datastores to checkpoint the current state.
func (st *state) Marshal() (string, error) {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	data, err := json.Marshal(serializedToken{
		Actions:                         st.actions,
		CurrentAction:                   st.currentAction,
		NeedsExpansion:                  st.needsExpansion,
		EntitlementGraph:                st.entitlementGraph,
		HasExternalResourceGrants:       st.hasExternalResourceGrants,
		ShouldFetchRelatedResources:     st.shouldFetchRelatedResources,
		ShouldSkipEntitlementsAndGrants: st.shouldSkipEntitlementsAndGrants,
		ShouldSkipGrants:                st.shouldSkipGrants,
		CompletedActionsCount:           st.completedActionsCount,
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

func (st *state) NeedsExpansion() bool {
	return st.needsExpansion
}

func (st *state) SetNeedsExpansion() {
	st.needsExpansion = true
}

func (st *state) HasExternalResourcesGrants() bool {
	return st.hasExternalResourceGrants
}

func (st *state) SetHasExternalResourcesGrants() {
	st.hasExternalResourceGrants = true
}

func (st *state) ShouldFetchRelatedResources() bool {
	return st.shouldFetchRelatedResources
}

func (st *state) SetShouldFetchRelatedResources() {
	st.shouldFetchRelatedResources = true
}

func (st *state) ShouldSkipEntitlementsAndGrants() bool {
	return st.shouldSkipEntitlementsAndGrants
}

func (st *state) SetShouldSkipEntitlementsAndGrants() {
	st.shouldSkipEntitlementsAndGrants = true
}

func (st *state) ShouldSkipGrants() bool {
	return st.shouldSkipGrants
}

func (st *state) SetShouldSkipGrants() {
	st.shouldSkipGrants = true
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
func (st *state) EntitlementGraph(ctx context.Context) *expand.EntitlementGraph {
	c := st.Current()
	if c == nil {
		panic("no current state")
	}
	if st.entitlementGraph == nil {
		st.entitlementGraph = expand.NewEntitlementGraph(ctx)
	}
	return st.entitlementGraph
}

// ClearEntitlementGraph clears the entitlement graph. This is meant to make the final sync token less confusing.
func (st *state) ClearEntitlementGraph(ctx context.Context) {
	st.entitlementGraph = nil
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

func (st *state) GetCompletedActionsCount() uint64 {
	st.mtx.RLock()
	defer st.mtx.RUnlock()
	return st.completedActionsCount
}
