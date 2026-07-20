package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// If you make a breaking change to the state token, you must increment this version.
const StateTokenVersion = 1

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
	AddStepDuration(bucket string, duration time.Duration)
	StepDurations() map[string]int64
	RecordConnectorCall(method string, duration time.Duration)
	MergeConnectorCallStat(method string, add ConnectorCallStat)
	ConnectorCallStats() map[string]ConnectorCallStat
	RecordSessionOp(op string, duration time.Duration, opErr error, timedOut bool)
	MergeSessionStat(op string, add SessionStoreStat)
	SessionStoreStats() map[string]SessionStoreStat
}

func NeedsExpansion(stateStr string) (bool, error) {
	state := newState()
	err := state.Unmarshal(stateStr)
	if err != nil {
		return false, err
	}
	return state.NeedsExpansion(), nil
}

// PrepareExpansionReplayToken rewrites a finished sync's state token so the
// sync can be re-run through grant expansion, preserving the token's other
// recorded state rather than discarding it. It marks the sync as needing
// expansion and, when the action stack is empty, pushes an InitOp so the
// resumed syncer drives its work from the top. A finished sync's token has an
// empty action stack, so without the InitOp a resume would find nothing to do
// and exit before expanding; clearing the whole token would also drop the
// skip flags and exclusion-group bookkeeping the token carries.
func PrepareExpansionReplayToken(stateStr string) (string, error) {
	st := newState()
	if err := st.Unmarshal(stateStr); err != nil {
		return "", err
	}
	st.SetNeedsExpansion()
	if st.Current() == nil {
		// A finished sync deserializes with no action map, so seed one before
		// queuing the InitOp that drives the resumed run.
		if st.actions == nil {
			st.actions = make(map[string]Action)
		}
		st.PushAction(context.Background(), Action{Op: InitOp})
	}
	return st.Marshal()
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
	stepDurationsMs                 map[string]int64
	connectorCallStats              map[string]*ConnectorCallStat
	sessionStoreStats               map[string]*SessionStoreStat
	// compaction is provenance written by the sync compactor via
	// BuildCompactedToken; the syncer itself never sets it. Kept on the
	// state so Unmarshal→Marshal round trips (e.g. expansion replay
	// tokens) preserve it.
	compaction *CompactionTokenStats
}

// ConnectorCallStat contains cumulative latency statistics for one connector method.
type ConnectorCallStat struct {
	Count   int64 `json:"count"`
	TotalMs int64 `json:"total_ms"`
	MaxMs   int64 `json:"max_ms"`
}

// SessionStoreStat contains cumulative latency and outcome counters for one
// session-store operation. Timeouts is the deadline-exceeded subset of
// Errors; MaxMs pinned at a fixed value with Timeouts ≈ Count is the
// signature of a backend whose every request times out.
type SessionStoreStat struct {
	Count    int64 `json:"count"`
	Errors   int64 `json:"errors,omitempty"`
	Timeouts int64 `json:"timeouts,omitempty"`
	TotalMs  int64 `json:"total_ms"`
	MaxMs    int64 `json:"max_ms"`
}

// Original serialized token format. Needed to parse/resume syncs started by older versions of baton-sdk.
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

// serializedTokenV1 is used to serialize the token to JSON. This separate object is used to avoid having exported fields
// on the object used externally. We should interface this, probably.
type serializedTokenV1 struct {
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
	// Exclusion-group tracking maps (exclusion_group_resource_types,
	// exclusion_group_defaults, exclusion_group_counts) were removed
	// when the streaming exclusion-group validation was replaced by
	// ingestion invariant I5 over the stored keyspace: old tokens
	// carrying them still parse (unknown JSON fields are ignored).
	StepDurationsMs    map[string]int64              `json:"step_durations_ms,omitempty"`
	ConnectorCallStats map[string]*ConnectorCallStat `json:"connector_call_stats,omitempty"`
	SessionStoreStats  map[string]*SessionStoreStat  `json:"session_store_stats,omitempty"`
	Compaction         *CompactionTokenStats         `json:"compaction,omitempty"`
	Version            uint64                        `json:"version"`
}

func newState() *state {
	return &state{
		actions:            make(map[string]Action),
		actionOrder:        []string{},
		currentActionID:    0,
		entitlementGraph:   nil,
		needsExpansion:     false,
		stepDurationsMs:    make(map[string]int64),
		connectorCallStats: make(map[string]*ConnectorCallStat),
		sessionStoreStats:  make(map[string]*SessionStoreStat),
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

// unmarshalTokenV0 unmarshals the original serialized token format into a serialized token of the new format.
func unmarshalTokenV0(input string) (serializedTokenV1, error) {
	tokenV0 := serializedTokenV0{}
	err := json.Unmarshal([]byte(input), &tokenV0)
	if err != nil {
		return serializedTokenV1{}, fmt.Errorf("syncer token corrupt: %w", err)
	}
	actionsMap := make(map[string]Action)
	actions := tokenV0.Actions
	actionOrder := []string{}
	var currentActionID uint64

	for _, action := range actions {
		action.ID = makeActionID(currentActionID)
		currentActionID++
		actionsMap[action.ID] = action
		actionOrder = append(actionOrder, action.ID)
	}
	if tokenV0.CurrentAction != nil {
		tokenV0.CurrentAction.ID = makeActionID(currentActionID)
		currentActionID++
		actionsMap[tokenV0.CurrentAction.ID] = *tokenV0.CurrentAction
		actionOrder = append(actionOrder, tokenV0.CurrentAction.ID)
	}

	return serializedTokenV1{
		ActionsMap:                      actionsMap,
		ActionOrder:                     actionOrder,
		CurrentActionID:                 currentActionID,
		NeedsExpansion:                  tokenV0.NeedsExpansion,
		EntitlementGraph:                tokenV0.EntitlementGraph,
		HasExternalResourceGrants:       tokenV0.HasExternalResourceGrants,
		ShouldFetchRelatedResources:     tokenV0.ShouldFetchRelatedResources,
		ShouldSkipEntitlementsAndGrants: tokenV0.ShouldSkipEntitlementsAndGrants,
		ShouldSkipGrants:                tokenV0.ShouldSkipGrants,
		CompletedActionsCount:           tokenV0.CompletedActionsCount,
		Version:                         1,
	}, nil
}

// Unmarshal takes an input string and unmarshals it onto the state object. If the input is empty, we set the state to
// have an init action.
func (st *state) Unmarshal(input string) error {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	token := serializedTokenV1{}

	if input != "" {
		err := json.Unmarshal([]byte(input), &token)
		if err != nil || token.Version != StateTokenVersion {
			// Fall back to old serialized token format.
			token, err = unmarshalTokenV0(input)
			if err != nil {
				return err
			}
		}

		st.actions = token.ActionsMap
		if st.actions == nil {
			st.actions = make(map[string]Action)
		}
		st.actionOrder = token.ActionOrder
		if st.actionOrder == nil {
			st.actionOrder = []string{}
		}
		st.currentActionID = token.CurrentActionID
		st.needsExpansion = token.NeedsExpansion
		st.entitlementGraph = token.EntitlementGraph
		st.hasExternalResourceGrants = token.HasExternalResourceGrants
		st.shouldSkipEntitlementsAndGrants = token.ShouldSkipEntitlementsAndGrants
		st.shouldSkipGrants = token.ShouldSkipGrants
		st.shouldFetchRelatedResources = token.ShouldFetchRelatedResources
		st.completedActionsCount = token.CompletedActionsCount
		st.stepDurationsMs = token.StepDurationsMs
		if st.stepDurationsMs == nil {
			st.stepDurationsMs = make(map[string]int64)
		}
		st.connectorCallStats = token.ConnectorCallStats
		if st.connectorCallStats == nil {
			st.connectorCallStats = make(map[string]*ConnectorCallStat)
		}
		st.sessionStoreStats = token.SessionStoreStats
		if st.sessionStoreStats == nil {
			st.sessionStoreStats = make(map[string]*SessionStoreStat)
		}
		st.compaction = token.Compaction
	} else {
		st.actions = make(map[string]Action)
		st.actionOrder = []string{}
		st.entitlementGraph = nil
		actionID := makeActionID(st.currentActionID)
		st.currentActionID++
		if _, ok := st.actions[actionID]; ok {
			// This should never happen.
			panic(fmt.Sprintf("action ID for new action %s already exists", actionID))
		}
		st.actions[actionID] = Action{Op: InitOp, ID: actionID}
		st.actionOrder = append(st.actionOrder, actionID)
		st.completedActionsCount = 0
		st.stepDurationsMs = make(map[string]int64)
		st.connectorCallStats = make(map[string]*ConnectorCallStat)
		st.sessionStoreStats = make(map[string]*SessionStoreStat)
		st.compaction = nil
	}

	return nil
}

// Marshal returns a string encoding of the state object. This is useful for datastores to checkpoint the current state.
func (st *state) Marshal() (string, error) {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	data, err := json.Marshal(serializedTokenV1{
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
		StepDurationsMs:                 st.stepDurationsMs,
		ConnectorCallStats:              st.connectorCallStats,
		SessionStoreStats:               st.sessionStoreStats,
		Compaction:                      st.compaction,
		Version:                         1,
	})
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (st *state) AddStepDuration(bucket string, duration time.Duration) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if st.stepDurationsMs == nil {
		st.stepDurationsMs = make(map[string]int64)
	}
	st.stepDurationsMs[bucket] += duration.Milliseconds()
}

func (st *state) StepDurations() map[string]int64 {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	out := make(map[string]int64, len(st.stepDurationsMs))
	for bucket, duration := range st.stepDurationsMs {
		out[bucket] = duration
	}
	return out
}

func (st *state) RecordConnectorCall(method string, duration time.Duration) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if st.connectorCallStats == nil {
		st.connectorCallStats = make(map[string]*ConnectorCallStat)
	}
	stat := st.connectorCallStats[method]
	if stat == nil {
		stat = &ConnectorCallStat{}
		st.connectorCallStats[method] = stat
	}
	durationMs := duration.Milliseconds()
	stat.Count++
	stat.TotalMs += durationMs
	if durationMs > stat.MaxMs {
		stat.MaxMs = durationMs
	}
}

func (st *state) ConnectorCallStats() map[string]ConnectorCallStat {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	out := make(map[string]ConnectorCallStat, len(st.connectorCallStats))
	for method, stat := range st.connectorCallStats {
		if stat != nil {
			out[method] = *stat
		}
	}
	return out
}

func (st *state) RecordSessionOp(op string, duration time.Duration, opErr error, timedOut bool) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if st.sessionStoreStats == nil {
		st.sessionStoreStats = make(map[string]*SessionStoreStat)
	}
	stat := st.sessionStoreStats[op]
	if stat == nil {
		stat = &SessionStoreStat{}
		st.sessionStoreStats[op] = stat
	}
	durationMs := duration.Milliseconds()
	stat.Count++
	stat.TotalMs += durationMs
	if durationMs > stat.MaxMs {
		stat.MaxMs = durationMs
	}
	if opErr != nil {
		stat.Errors++
		if timedOut {
			stat.Timeouts++
		}
	}
}

// MergeConnectorCallStat folds pre-aggregated connector-call stats (e.g. a
// compacted partial's totals) into method's cumulative counters.
func (st *state) MergeConnectorCallStat(method string, add ConnectorCallStat) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if st.connectorCallStats == nil {
		st.connectorCallStats = make(map[string]*ConnectorCallStat)
	}
	stat := st.connectorCallStats[method]
	if stat == nil {
		stat = &ConnectorCallStat{}
		st.connectorCallStats[method] = stat
	}
	stat.Count += add.Count
	stat.TotalMs += add.TotalMs
	if add.MaxMs > stat.MaxMs {
		stat.MaxMs = add.MaxMs
	}
}

// MergeSessionStat folds pre-aggregated session stats (e.g. a connector's
// per-request usage report) into op's cumulative counters.
func (st *state) MergeSessionStat(op string, add SessionStoreStat) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if st.sessionStoreStats == nil {
		st.sessionStoreStats = make(map[string]*SessionStoreStat)
	}
	stat := st.sessionStoreStats[op]
	if stat == nil {
		stat = &SessionStoreStat{}
		st.sessionStoreStats[op] = stat
	}
	stat.Count += add.Count
	stat.Errors += add.Errors
	stat.Timeouts += add.Timeouts
	stat.TotalMs += add.TotalMs
	if add.MaxMs > stat.MaxMs {
		stat.MaxMs = add.MaxMs
	}
}

func (st *state) SessionStoreStats() map[string]SessionStoreStat {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	out := make(map[string]SessionStoreStat, len(st.sessionStoreStats))
	for op, stat := range st.sessionStoreStats {
		if stat != nil {
			out[op] = *stat
		}
	}
	return out
}

func makeActionID(id uint64) string {
	return fmt.Sprintf("%010d", id)
}

// PushAction adds a new action to the stack.
func (st *state) PushAction(ctx context.Context, action Action) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if action.ID != "" {
		panic("action ID must be empty for new actions")
	}

	action.ID = makeActionID(st.currentActionID)
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
