package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// The sync token is the checkpoint format: a sync started by one SDK version
// is resumed by another against the same artifact, so the bytes below are a
// compatibility surface. pkg/sync/testdata/tokens holds captured tokens and
// token_golden_test.go asserts that decoding and re-encoding each one
// reproduces its bytes.
//
// Encoding and decoding reach into runState's and runStats' fields directly,
// under their mutexes, so a token is a consistent view of both. marshalToken
// takes runState's read lock and then runStats'; if some future caller needs
// both locks, take them in that order.

// If you make a breaking change to the state token, you must increment this version.
const StateTokenVersion = 1

// StateTokenVersionTypeScoped marks checkpoints whose action state carries
// type-scoped or spawned-cursor markers. Older SDKs cannot interpret those
// actions: their JSON parser silently drops the marker fields, and the
// resulting actions dead-end against store pagination, sealing the sync as
// complete while missing every pending cursor's data. Version 2 defeats
// that: an older SDK fails the version check, falls back to the V0 parser,
// gets an empty action state, and restarts collection from Init inside the
// same sync run — redone work instead of silent data loss.
const StateTokenVersionTypeScoped = 2

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
// on the object used externally.
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
	ActionCountsMap                 map[string]ActionCount   `json:"action_counts,omitempty"`
	// Exclusion-group tracking maps (exclusion_group_resource_types,
	// exclusion_group_defaults, exclusion_group_counts) were removed
	// when the streaming exclusion-group validation was replaced by
	// ingestion invariant I5 over the stored keyspace: old tokens
	// carrying them still parse (unknown JSON fields are ignored).
	StepDurationsMs    map[string]int64              `json:"step_durations_ms,omitempty"`
	ConnectorCallStats map[string]*ConnectorCallStat `json:"connector_call_stats,omitempty"`
	SessionStoreStats  map[string]*SessionStoreStat  `json:"session_store_stats,omitempty"`
	IngestQuality      *IngestQualityCheckpoint      `json:"ingest_quality,omitempty"`
	Compaction         *CompactionTokenStats         `json:"compaction,omitempty"`
	Version            uint64                        `json:"version"`
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
		ActionCountsMap:                 make(map[string]ActionCount),
		Version:                         1,
	}, nil
}

// tokenParts is a decoded sync token, split into the lifetimes the syncer
// holds it in. graph is the entitlement graph the token carried inline;
// tokens written by the default writer carry none (see marshalToken), and
// production readers get the graph from GraphFromStore instead.
type tokenParts struct {
	run   *runState
	stats *runStats
	graph *expand.EntitlementGraph
}

// unmarshalToken decodes a sync token. An empty input is a sync with no
// checkpoint, and yields a run seeded with an InitOp action.
func unmarshalToken(input string) (tokenParts, error) {
	run := newRunState()
	stats := newRunStats()

	if input == "" {
		run.seedInitAction()
		return tokenParts{run: run, stats: stats}, nil
	}

	token := serializedTokenV1{}
	err := json.Unmarshal([]byte(input), &token)
	if err != nil || (token.Version != StateTokenVersion && token.Version != StateTokenVersionTypeScoped) {
		// Fall back to old serialized token format.
		token, err = unmarshalTokenV0(input)
		if err != nil {
			return tokenParts{}, err
		}
	}

	loadRunState(run, token)
	loadRunStats(stats, token)
	return tokenParts{run: run, stats: stats, graph: token.EntitlementGraph}, nil
}

func loadRunState(run *runState, token serializedTokenV1) {
	run.mu.Lock()
	defer run.mu.Unlock()

	run.actions = token.ActionsMap
	if run.actions == nil {
		run.actions = make(map[string]Action)
	}
	run.actionOrder = token.ActionOrder
	if run.actionOrder == nil {
		run.actionOrder = []string{}
	}
	run.currentActionID = token.CurrentActionID
	if token.EntitlementGraph == nil {
		// A graph-less token cannot resume a grant-expansion pagination:
		// the page token indexes a load the (now absent) graph was
		// accumulating, and continuing from it against a fresh graph would
		// silently drop the edges from earlier pages. marshalToken already
		// normalizes this, but tolerate tokens from writers that did not.
		// Tokens carrying an inline graph (written by older SDKs) keep
		// their page token and resume exactly as before.
		for id, a := range run.actions {
			if a.Op == SyncGrantExpansionOp && a.PageToken != "" {
				a.PageToken = ""
				run.actions[id] = a
			}
		}
	}
	if token.NeedsExpansion {
		run.facts.set(factNeedsExpansion)
	}
	if token.HasExternalResourceGrants {
		run.facts.set(factHasExternalResourceGrants)
	}
	if token.ShouldFetchRelatedResources {
		run.facts.set(factShouldFetchRelatedResources)
	}
	if token.ShouldSkipEntitlementsAndGrants {
		run.facts.set(factShouldSkipEntitlementsAndGrants)
	}
	if token.ShouldSkipGrants {
		run.facts.set(factShouldSkipGrants)
	}
	run.completedActions = token.CompletedActionsCount
	run.actionCounts = token.ActionCountsMap
	if run.actionCounts == nil {
		run.actionCounts = make(map[string]ActionCount)
	}
	// Rebuild the I10 drain-evidence set from the checkpointed
	// actions: a spawned cursor restored from a token was admitted
	// by a previous process and must still drain in the process
	// that completes the sync. The re-mention guard set rebuilds
	// from the same scan: only surviving identities are known —
	// crash amnesia means completed spawns are re-doable, which is
	// idempotent and re-accumulates the set.
	run.spawnedInFlight = make(map[string]Action)
	run.spawnedAdmitted = make(map[parallelActionKey]string)
	for _, action := range run.actions {
		run.recordSpawnedAdmissionLocked(action)
	}
}

func loadRunStats(stats *runStats, token serializedTokenV1) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.stepDurationsMs = token.StepDurationsMs
	if stats.stepDurationsMs == nil {
		stats.stepDurationsMs = make(map[string]int64)
	}
	stats.connectorCalls = token.ConnectorCallStats
	if stats.connectorCalls == nil {
		stats.connectorCalls = make(map[string]*ConnectorCallStat)
	}
	stats.sessionOps = token.SessionStoreStats
	if stats.sessionOps == nil {
		stats.sessionOps = make(map[string]*SessionStoreStat)
	}
	stats.ingest = cloneIngestQualityCheckpoint(token.IngestQuality)
	stats.compaction = token.Compaction
}

// marshalToken encodes a run's checkpoint. This is what datastores store to
// resume a sync.
//
// The entitlement graph is never serialized. It is a projection of data
// already in the store (loadEntitlementGraph rebuilds it from
// PendingExpansionPage, with no connector calls), and for large tenants its
// JSON encoding multiplied the checkpoint's memory footprint several times
// over — json map encoding + the string copy + the store's record/compression/
// batch copies each cost O(graph) live at once, which OOM-killed sync workers
// mid-checkpoint. A crash mid-expansion re-runs the load and expansion
// phases from the store on resume (the same replay property
// PrepareExpansionReplayToken relies on) instead of resuming them from the
// token.
//
// Because a resumed reader gets no graph, any in-flight SyncGrantExpansionOp
// action is serialized with its PageToken blanked: the page token indexes a
// pagination the graph was accumulating, and resuming from it with an empty
// graph would silently drop the edges from earlier pages. Blanking it at
// encode time (rather than only fixing it up at decode) keeps tokens
// safe for OLDER readers too — an old SDK resuming a graph-less token starts
// a fresh graph and must restart the load from the first page. Only the
// serialized copy is normalized; the live run keeps its page token.
//
// Tokens from pre-omission SDKs do carry a graph, and unmarshalToken still
// adopts it. That direction is the compatibility surface; this one is not.
func marshalToken(run *runState, stats *runStats) (string, error) {
	run.mu.RLock()
	defer run.mu.RUnlock()
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	actions := run.actions
	for _, action := range run.actions {
		if action.Op == SyncGrantExpansionOp && action.PageToken != "" {
			actions = make(map[string]Action, len(run.actions))
			for id, a := range run.actions {
				if a.Op == SyncGrantExpansionOp {
					a.PageToken = ""
				}
				actions[id] = a
			}
			break
		}
	}

	// Stamp the type-scoped version only when the token actually carries
	// markers an older parser would misinterpret; plain tokens keep
	// version 1 so downgrades resume seamlessly. Read from the normalized
	// copy, which is what actually ships.
	version := uint64(StateTokenVersion)
	for _, action := range actions {
		if action.TypeScoped || action.Spawned || action.TypeScopedPlanned {
			version = StateTokenVersionTypeScoped
			break
		}
	}

	data, err := json.Marshal(serializedTokenV1{
		ActionsMap:                      actions,
		ActionOrder:                     run.actionOrder,
		CurrentActionID:                 run.currentActionID,
		NeedsExpansion:                  run.facts.has(factNeedsExpansion),
		EntitlementGraph:                nil,
		HasExternalResourceGrants:       run.facts.has(factHasExternalResourceGrants),
		ShouldFetchRelatedResources:     run.facts.has(factShouldFetchRelatedResources),
		ShouldSkipEntitlementsAndGrants: run.facts.has(factShouldSkipEntitlementsAndGrants),
		ShouldSkipGrants:                run.facts.has(factShouldSkipGrants),
		CompletedActionsCount:           run.completedActions,
		ActionCountsMap:                 run.actionCounts,
		StepDurationsMs:                 stats.stepDurationsMs,
		ConnectorCallStats:              stats.connectorCalls,
		SessionStoreStats:               stats.sessionOps,
		IngestQuality:                   cloneIngestQualityCheckpoint(stats.ingest),
		Compaction:                      stats.compaction,
		Version:                         version,
	})
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// NeedsExpansion reports whether the sync a token describes recorded that it
// has grants to expand.
func NeedsExpansion(stateStr string) (bool, error) {
	parts, err := unmarshalToken(stateStr)
	if err != nil {
		return false, err
	}
	return parts.run.hasFact(factNeedsExpansion), nil
}

// PrepareExpansionReplayToken rewrites a finished sync's state token so the
// sync can be re-run through grant expansion, preserving the token's other
// recorded state rather than discarding it. It marks the sync as needing
// expansion and, when the action stack is empty, pushes an InitOp so the
// resumed syncer drives its work from the top. A finished sync's token has an
// empty action stack, so without the InitOp a resume would find nothing to do
// and exit before expanding; clearing the whole token would also drop the
// skip flags and exclusion-group bookkeeping the token carries.
//
// The rewritten token carries no entitlement graph. A graph preserved by
// WithPreserveEntitlementGraph has Loaded=true with every edge already
// marked expanded, so a replayed sync would skip graph loading and the
// expander would report done immediately — the replay would silently no-op.
// Omitting it makes the replay rebuild the graph from scratch.
func PrepareExpansionReplayToken(stateStr string) (string, error) {
	parts, err := unmarshalToken(stateStr)
	if err != nil {
		return "", err
	}
	parts.run.setFact(factNeedsExpansion)
	if parts.run.current() == nil {
		parts.run.pushAction(context.Background(), Action{Op: InitOp})
	}
	return marshalToken(parts.run, parts.stats)
}

// GraphFromToken parses a legacy sync token and returns its entitlement graph
// for compatibility tests. It returns nil if the token carried no graph.
//
// Token graphs have no grant-generation binding and must not drive incremental
// reuse. Production readers must use GraphFromStore, which verifies that the
// sidecar graph describes the store's exact sealed grant generation.
func GraphFromToken(stateStr string) (*expand.EntitlementGraph, error) {
	parts, err := unmarshalToken(stateStr)
	if err != nil {
		return nil, err
	}
	return parts.graph, nil
}
