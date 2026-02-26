package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// If you make a breaking change to the state token, you must increment this version.
const StateTokenVersion = 1

var ErrUnsupportedStateTokenVersion error = fmt.Errorf("unsupported syncer token version: %d", StateTokenVersion)

type State interface {
	PushAction(ctx context.Context, action Action)
	FinishAction(ctx context.Context, action *Action)
	NextPage(ctx context.Context, actionID string, pageToken string) error
	EntitlementGraph(ctx context.Context) *expand.EntitlementGraph
	ClearEntitlementGraph(ctx context.Context)
	Current() *Action
	GetAction(id string) *Action
	PeekMatchingActions(ctx context.Context, op ActionOp) []*Action
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
func (s ActionOp) MarshalJSON() ([]byte, error) {
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
	ID                   string   `json:"id,omitempty"`
	Op                   ActionOp `json:"operation,omitempty"`
	PageToken            string   `json:"page_token,omitempty"`
	ResourceTypeID       string   `json:"resource_type_id,omitempty"`
	ResourceID           string   `json:"resource_id,omitempty"`
	ParentResourceTypeID string   `json:"parent_resource_type_id,omitempty"`
	ParentResourceID     string   `json:"parent_resource_id,omitempty"`
}

var _ State = &state{}

// state is an object used for tracking the current status of a connector sync. It operates like a stack.
type state struct {
	mtx                             sync.RWMutex
	actions                         map[string]Action
	actionOrder                     []string
	currentActionID                 uint64 // Counter for generating new action IDs.
	entitlementGraph                *expand.EntitlementGraph
	needsExpansion                  bool
	hasExternalResourceGrants       bool
	shouldFetchRelatedResources     bool
	shouldSkipEntitlementsAndGrants bool
	shouldSkipGrants                bool
	completedActionsCount           uint64
}

// Original serialized token format. Needed to resume syncs started by older versions of baton-sdk.
type serializedTokenV0 struct {
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

// serializedToken is used to serialize the token to JSON. This separate object is used to avoid having exported fields
// on the object used externally. We should interface this, probably.
type serializedToken struct {
	ActionsMap                      map[string]Action        `json:"actions_map,omitempty"`
	ActionOrder                     []string                 `json:"action_order,omitempty"`
	CurrentActionID                 uint64                   `json:"current_action_id,omitempty"`
	NeedsExpansion                  bool                     `json:"needs_expansion,omitempty"`
	EntitlementGraph                *expand.EntitlementGraph `json:"entitlement_graph,omitempty"`
	HasExternalResourceGrants       bool                     `json:"has_external_resource_grants,omitempty"`
	ShouldFetchRelatedResources     bool                     `json:"should_fetch_related_resources,omitempty"`
	ShouldSkipEntitlementsAndGrants bool                     `json:"should_skip_entitlements_and_grants,omitempty"`
	ShouldSkipGrants                bool                     `json:"should_skip_grants,omitempty"`
	CompletedActionsCount           uint64                   `json:"completed_actions_count,omitempty"`
	Version                         uint64                   `json:"version,omitempty"`
}

func newState() *state {
	return &state{
		actions:          make(map[string]Action),
		actionOrder:      []string{},
		currentActionID:  0,
		entitlementGraph: nil,
		needsExpansion:   false,
	}
}

// Current returns nil if there is no current action. Otherwise it returns a pointer to a copy of the current state.
func (st *state) Current() *Action {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	if len(st.actionOrder) == 0 {
		return nil
	}

	currentID := st.actionOrder[len(st.actionOrder)-1]
	currentAction := st.actions[currentID]
	return &currentAction
}

// GetAction returns a copy of the action with the given ID, or nil if it doesn't exist.
func (st *state) GetAction(id string) *Action {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	a, ok := st.actions[id]
	if !ok {
		return nil
	}
	return &a
}

const maxPeekActionsCount = 100

// PeekMatchingActions returns copies of all consecutive actions from the top of
// the stack that match the given op. Actions are returned in stack order (top first).
func (st *state) PeekMatchingActions(ctx context.Context, op ActionOp) []*Action {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	var actions []*Action
	for i := len(st.actionOrder) - 1; i >= 0; i-- {
		id := st.actionOrder[i]
		action := st.actions[id]
		if action.Op != op || len(actions) >= maxPeekActionsCount {
			break
		}
		a := action
		actions = append(actions, &a)
	}
	return actions
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

		if token.Version != StateTokenVersion {
			return ErrUnsupportedStateTokenVersion
		}
		st.actions = token.ActionsMap
		st.actionOrder = token.ActionOrder
		st.currentActionID = token.CurrentActionID
		st.needsExpansion = token.NeedsExpansion
		st.entitlementGraph = token.EntitlementGraph
		st.hasExternalResourceGrants = token.HasExternalResourceGrants
		st.shouldSkipEntitlementsAndGrants = token.ShouldSkipEntitlementsAndGrants
		st.shouldSkipGrants = token.ShouldSkipGrants
		st.shouldFetchRelatedResources = token.ShouldFetchRelatedResources
		st.completedActionsCount = token.CompletedActionsCount
	} else {
		st.actions = make(map[string]Action)
		st.actionOrder = []string{}
		st.entitlementGraph = nil
		actionID := fmt.Sprintf("%010d", st.currentActionID)
		st.currentActionID++
		if _, ok := st.actions[actionID]; ok {
			// This should never happen.
			panic(fmt.Sprintf("action ID for new action %s already exists", actionID))
		}
		st.actions[actionID] = Action{Op: InitOp, ID: actionID}
		st.actionOrder = append(st.actionOrder, actionID)
		st.completedActionsCount = 0
	}

	return nil
}

// Marshal returns a string encoding of the state object. This is useful for datastores to checkpoint the current state.
func (st *state) Marshal() (string, error) {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	data, err := json.Marshal(serializedToken{
		ActionsMap:                      st.actions,
		ActionOrder:                     st.actionOrder,
		CurrentActionID:                 st.currentActionID,
		NeedsExpansion:                  st.needsExpansion,
		EntitlementGraph:                st.entitlementGraph,
		HasExternalResourceGrants:       st.hasExternalResourceGrants,
		ShouldFetchRelatedResources:     st.shouldFetchRelatedResources,
		ShouldSkipEntitlementsAndGrants: st.shouldSkipEntitlementsAndGrants,
		ShouldSkipGrants:                st.shouldSkipGrants,
		CompletedActionsCount:           st.completedActionsCount,
		Version:                         1,
	})
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// PushAction adds a new action to the stack.
func (st *state) PushAction(ctx context.Context, action Action) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if action.ID != "" {
		panic("action ID must be empty for new actions")
	}

	action.ID = fmt.Sprintf("%010d", st.currentActionID)
	st.currentActionID++
	if _, ok := st.actions[action.ID]; ok {
		// This should never happen.
		panic(fmt.Sprintf("action ID for new action %s already exists", action.ID))
	}
	st.actions[action.ID] = action
	st.actionOrder = append(st.actionOrder, action.ID)
	ctxzap.Extract(ctx).Debug("pushed action", zap.Any("action", action))
}

// FinishAction pops the current action from the state.
func (st *state) FinishAction(ctx context.Context, action *Action) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if action == nil {
		panic("action cannot be nil")
	}
	if _, ok := st.actions[action.ID]; !ok {
		panic(fmt.Sprintf("action ID %s does not exist", action.ID))
	}

	// Find the action in the action order and remove it.
	index, ok := slices.BinarySearch(st.actionOrder, action.ID)
	if !ok {
		panic(fmt.Sprintf("action ID %s does not exist in action order", action.ID))
	}
	st.actionOrder = slices.Delete(st.actionOrder, index, index+1)
	delete(st.actions, action.ID)
	st.completedActionsCount++
	ctxzap.Extract(ctx).Debug("finishing action", zap.Any("action", action))
}

// NextPage updates the current action with the provided page token. This is useful for paginating
// requests.
func (st *state) NextPage(ctx context.Context, actionID string, pageToken string) error {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	_, ok := st.actions[actionID]
	if !ok {
		return fmt.Errorf("action ID %s does not exist", actionID)
	}

	action := st.actions[actionID]
	action.PageToken = pageToken
	st.actions[actionID] = action
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

// EntitlementGraph returns the entitlement graph for the current action.
func (st *state) EntitlementGraph(ctx context.Context) *expand.EntitlementGraph {
	if st.entitlementGraph == nil {
		st.entitlementGraph = expand.NewEntitlementGraph(ctx)
	}
	return st.entitlementGraph
}

// ClearEntitlementGraph clears the entitlement graph. This is meant to make the final sync token less confusing.
func (st *state) ClearEntitlementGraph(ctx context.Context) {
	st.entitlementGraph = nil
}

func (st *state) GetCompletedActionsCount() uint64 {
	st.mtx.RLock()
	defer st.mtx.RUnlock()
	return st.completedActionsCount
}
