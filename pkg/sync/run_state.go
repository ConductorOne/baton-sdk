package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

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
	// Spawned marks a sibling cursor enqueued by EnqueuePageTokens.
	// Progress accounting counts only the origin action for per-resource
	// phases. The marker is checkpointed so resume preserves that rule.
	Spawned bool `json:"spawned,omitempty"`
	// TypeScoped distinguishes whole-type grant/entitlement cursors from
	// per-resource actions. Do not infer this from an empty ResourceID:
	// malformed connector resources with empty ids can exist in old stores
	// and must retain the pre-type-scoped per-resource behavior.
	TypeScoped bool `json:"type_scoped,omitempty"`
	// TypeScopedPlanned records that a root entitlement/grant action has
	// already scheduled whole-type collection. Legacy checkpoints omit it,
	// causing an upgraded syncer to plan type-scoped work once on resume.
	TypeScopedPlanned bool `json:"type_scoped_planned,omitempty"`
}

// ActionCount is the per-op tally runState keeps and the token carries: how
// many actions of one op finished, and how many of those ended in a warning.
type ActionCount struct {
	CompletedCount uint64 `json:"completed_count,omitempty"`
	WarningCount   uint64 `json:"warning_count,omitempty"`
}

func makeActionID(id uint64) string {
	return fmt.Sprintf("%010d", id)
}

// runState is what the syncer reads to decide what work happens next: the
// action stack, which operates like a stack, plus the facts the run has
// established. Actions say what is left to do; facts gate which work happens
// at all — needs_expansion decides whether the expansion phase runs,
// should_skip_grants whether grant actions get pushed.
//
// That, not durability, is what separates it from runStats and expansionGraph,
// which used to live here: runStats rides the same token, but nothing in it
// steers the action stack.
//
// actions, actionOrder, currentActionID, completedActions, actionCounts and
// facts are the serialized fields. spawnedInFlight and spawnedAdmitted are
// indexes over actions, which unmarshalToken rebuilds by scanning the decoded
// map.
type runState struct {
	mu               sync.RWMutex
	actions          map[string]Action
	actionOrder      []string
	currentActionID  uint64 // Counter for generating new action IDs.
	completedActions uint64
	// actionCounts is the per-op completion and warning tally
	// tooManyListResourceWarnings judges. It rides the token, so the ratio
	// it feeds spans resumes rather than restarting each process.
	actionCounts map[string]ActionCount
	facts        syncFacts
	// spawnedInFlight is the evidence set behind ingest invariant I10:
	// every spawned sibling cursor (EnqueuePageTokens) admitted to the
	// stack, keyed by action ID, removed only by the two legitimate
	// completion paths (finishAction and transitionAction's finish
	// branch). A silent drop — any code path that loses an admitted
	// action without finishing it — leaves its entry behind, and the
	// invariant pass names it at sync quiesce. Decoding a token rebuilds
	// the set from the checkpointed actions, so the evidence survives
	// resume: a restored spawned cursor must still drain in the process
	// that completes the sync. Guarded by mu; bounded by the number
	// of in-flight spawned actions (entries are deleted on finish).
	spawnedInFlight map[string]Action
	// spawnedAdmitted maps the identity digest (op, resource type,
	// resource, page token, type-scope) of every spawned cursor admitted
	// in THIS PROCESS to its action ID. It is the termination and
	// idempotency guard for re-mentioned spawns: connectors legitimately
	// re-mention a cursor another response already spawned (DAG-shaped
	// shard discovery, or post-crash answers that shifted under a
	// resumed checkpoint). This is the pipeline's ONLY spawn dedup: the
	// parallel queue keeps no identity history (RFC 0007 phase 1), so
	// without this process-lifetime set, two cursors mentioning each
	// other would re-admit each other forever.
	// transitionAction skips a spawned child whose
	// identity is already here. Entries are deliberately NEVER pruned on
	// finish — a completed spawn must stay skippable or cycles resume.
	// Not serialized: after a crash the set rebuilds from the surviving
	// stack, so a re-mention of work completed before the crash is redone
	// once, idempotently, and the set re-accumulates — cycles still
	// terminate. Guarded by mu; bounded by total spawned admissions in
	// the process (32-byte keys).
	spawnedAdmitted map[parallelActionKey]string
}

func newRunState() *runState {
	return &runState{
		actions:         make(map[string]Action),
		actionOrder:     []string{},
		currentActionID: 0,
		actionCounts:    make(map[string]ActionCount),
		spawnedInFlight: make(map[string]Action),
		spawnedAdmitted: make(map[parallelActionKey]string),
	}
}

// seedInitAction resets the stack to a single InitOp action, which is how a
// sync with no checkpoint starts. Unlike pushAction it does not log: this is
// decoding an absent token, not scheduling work.
func (r *runState) seedInitAction() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.actions = make(map[string]Action)
	r.actionOrder = []string{}
	actionID := makeActionID(r.currentActionID)
	r.currentActionID++
	r.actions[actionID] = Action{Op: InitOp, ID: actionID}
	r.actionOrder = append(r.actionOrder, actionID)
	r.completedActions = 0
	r.actionCounts = make(map[string]ActionCount)
	r.spawnedInFlight = make(map[string]Action)
	r.spawnedAdmitted = make(map[parallelActionKey]string)
}

// setFact records a sync-level fact. See syncFacts for the naming contract.
func (r *runState) setFact(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.facts.set(name)
}

// hasFact reports whether the run has established the named fact.
func (r *runState) hasFact(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.facts.has(name)
}

// current returns nil if there is no current action. Otherwise it returns a pointer to a copy of the current state.
func (r *runState) current() *Action {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.actionOrder) == 0 {
		return nil
	}

	currentID := r.actionOrder[len(r.actionOrder)-1]
	currentAction := r.actions[currentID]
	return &currentAction
}

// getAction returns a copy of the action with the given ID, or nil if it doesn't exist.
func (r *runState) getAction(id string) *Action {
	r.mu.RLock()
	defer r.mu.RUnlock()

	a, ok := r.actions[id]
	if !ok {
		return nil
	}
	return &a
}

const maxPeekActionsCount = 100

// peekMatchingActions returns copies of all consecutive actions from the top of
// the stack that match the given op. Actions are returned in stack order (top first).
func (r *runState) peekMatchingActions(ctx context.Context, op ActionOp) []*Action {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var actions []*Action
	for i := len(r.actionOrder) - 1; i >= 0; i-- {
		id := r.actionOrder[i]
		action := r.actions[id]
		if action.Op != op || len(actions) >= maxPeekActionsCount {
			break
		}
		a := action
		actions = append(actions, &a)
	}
	return actions
}

// maxUndrainedTokenChars caps the page-token excerpt carried on I10
// verdict lines (spawned tokens can be up to 1 MiB).
const maxUndrainedTokenChars = 64

// undrainedSpawnedCursors describes every spawned cursor that was
// admitted to the action stack but never completed through a legitimate
// finish path — the I10 evidence read. Empty on a healthy run. Sorted
// by action ID so verdicts are byte-stable.
func (r *runState) undrainedSpawnedCursors() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.spawnedInFlight) == 0 {
		return nil
	}
	ids := make([]string, 0, len(r.spawnedInFlight))
	for id := range r.spawnedInFlight {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		action := r.spawnedInFlight[id]
		token := action.PageToken
		if len(token) > maxUndrainedTokenChars {
			token = fmt.Sprintf("%s… (%d bytes)", token[:maxUndrainedTokenChars], len(action.PageToken))
		}
		out = append(out, fmt.Sprintf("action %s %s %s/%s token=%q",
			id, action.Op.String(), action.ResourceTypeID, action.ResourceID, token))
	}
	return out
}

// pushAction adds an action and returns the checkpointed copy, including its
// assigned ID. The scheduler uses that copy to admit spawned work.
func (r *runState) pushAction(ctx context.Context, action Action) *Action {
	r.mu.Lock()
	defer r.mu.Unlock()

	if action.ID != "" {
		panic("action ID must be empty for new actions")
	}

	action.ID = makeActionID(r.currentActionID)
	r.currentActionID++
	if _, ok := r.actions[action.ID]; ok {
		// This should never happen.
		panic(fmt.Sprintf("action ID for new action %s already exists", action.ID))
	}
	r.actions[action.ID] = action
	r.actionOrder = append(r.actionOrder, action.ID)
	r.recordSpawnedAdmissionLocked(action)
	ctxzap.Extract(ctx).Debug("pushed action", zap.Any("action", action))
	return &action
}

// recordSpawnedAdmissionLocked enrolls a spawned cursor in the I10
// drain-evidence set and the re-mention guard index. Caller holds r.mu.
func (r *runState) recordSpawnedAdmissionLocked(action Action) {
	if !action.Spawned {
		return
	}
	if r.spawnedInFlight == nil {
		r.spawnedInFlight = make(map[string]Action)
	}
	r.spawnedInFlight[action.ID] = action
	if r.spawnedAdmitted == nil {
		r.spawnedAdmitted = make(map[parallelActionKey]string)
	}
	r.spawnedAdmitted[makeParallelActionKey(&action)] = action.ID
}

func (r *runState) markTypeScopedPlanned(actionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	action, ok := r.actions[actionID]
	if !ok {
		return
	}
	action.TypeScopedPlanned = true
	r.actions[actionID] = action
}

func (r *runState) transitionAction(
	ctx context.Context,
	parent *Action,
	nextPageToken string,
	childActions []Action,
) ([]*Action, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if parent == nil {
		return nil, errors.New("parent action cannot be nil")
	}
	if _, ok := r.actions[parent.ID]; !ok {
		return nil, fmt.Errorf("action ID %s does not exist", parent.ID)
	}
	for _, child := range childActions {
		if child.ID != "" {
			return nil, errors.New("action ID must be empty for new actions")
		}
	}

	pushed := make([]*Action, 0, len(childActions))
	for _, child := range childActions {
		// Re-mention guard: a spawned child whose identity was already
		// admitted in this process is the same work, already scheduled
		// or done. Re-admitting it duplicates work at best; at worst it
		// never terminates (mutual mentions re-admitting each other
		// forever — this is the pipeline's only spawn dedup; the queue
		// keeps no identity history, RFC 0007 phase 1). Skip it, loudly.
		if child.Spawned {
			if priorID, dup := r.spawnedAdmitted[makeParallelActionKey(&child)]; dup {
				ctxzap.Extract(ctx).Warn(
					"skipping re-mentioned spawned cursor: identical work was already admitted this sync",
					zap.String("existing_action_id", priorID),
					zap.String("op", child.Op.String()),
					zap.String("resource_type_id", child.ResourceTypeID),
					zap.String("resource_id", child.ResourceID),
				)
				continue
			}
		}
		child.ID = makeActionID(r.currentActionID)
		r.currentActionID++
		if _, ok := r.actions[child.ID]; ok {
			panic(fmt.Sprintf("action ID for new action %s already exists", child.ID))
		}
		r.actions[child.ID] = child
		r.actionOrder = append(r.actionOrder, child.ID)
		r.recordSpawnedAdmissionLocked(child)
		childCopy := child
		pushed = append(pushed, &childCopy)
		ctxzap.Extract(ctx).Debug("pushed action", zap.Any("action", child))
	}

	if nextPageToken != "" {
		updated := r.actions[parent.ID]
		updated.PageToken = nextPageToken
		r.actions[parent.ID] = updated
		return pushed, nil
	}

	r.finishActionLocked(ctx, parent, false)
	return pushed, nil
}

// finishAction pops the given action from the stack.
func (r *runState) finishAction(ctx context.Context, action *Action) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishActionLocked(ctx, action, false)
}

// finishActionWithWarning finishes an action that ended in a warning, which
// counts toward the per-op warning ratio tooManyListResourceWarnings judges.
func (r *runState) finishActionWithWarning(ctx context.Context, action *Action) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishActionLocked(ctx, action, true)
}

// finishActionLocked requires mu to be held.
func (r *runState) finishActionLocked(ctx context.Context, action *Action, isWarning bool) {
	if action == nil {
		panic("action cannot be nil")
	}
	if _, ok := r.actions[action.ID]; !ok {
		panic(fmt.Sprintf("action ID %s does not exist", action.ID))
	}

	// Find the action in the action order and remove it.
	index, ok := slices.BinarySearch(r.actionOrder, action.ID)
	if !ok {
		panic(fmt.Sprintf("action ID %s does not exist in action order", action.ID))
	}
	r.actionOrder = slices.Delete(r.actionOrder, index, index+1)
	delete(r.actions, action.ID)
	delete(r.spawnedInFlight, action.ID)
	r.completedActions++
	actionCount := r.actionCounts[action.Op.String()]
	actionCount.CompletedCount++
	if isWarning {
		actionCount.WarningCount++
	}
	r.actionCounts[action.Op.String()] = actionCount
	ctxzap.Extract(ctx).Debug("finishing action", zap.Any("action", action))
}

// nextPage updates the given action with the provided page token. This is useful for paginating
// requests.
func (r *runState) nextPage(ctx context.Context, actionID string, pageToken string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.actions[actionID]
	if !ok {
		return fmt.Errorf("action ID %s does not exist", actionID)
	}

	action := r.actions[actionID]
	action.PageToken = pageToken
	r.actions[actionID] = action
	return nil
}

func (r *runState) completedActionsCount() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.completedActions
}

// getActionCount returns the completion and warning tally for one op. A zero
// ActionCount means the op has finished nothing yet, which is what
// tooManyListResourceWarnings reads before any list-resource work completes.
func (r *runState) getActionCount(op ActionOp) ActionCount {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.actionCounts[op.String()]
}
