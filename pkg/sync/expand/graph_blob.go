package expand

import (
	"encoding/json"
	"fmt"
)

// graphBlobEnvelope is the serialized form of the entitlement-graph sidecar
// stored in a c1z (instead of bloating the sync token). SyncID guards
// against reading a graph inherited from a different sync (e.g. a fold-copied
// compaction base).
type graphBlobEnvelope struct {
	SyncID string            `json:"sync_id"`
	Graph  *EntitlementGraph `json:"graph"`
}

// MarshalGraphBlob serializes a graph for the c1z sidecar, stamped with the
// sync it belongs to. Transient state is stripped first (a reload rebuilds it).
func MarshalGraphBlob(syncID string, g *EntitlementGraph) ([]byte, error) {
	if g == nil {
		return nil, fmt.Errorf("marshal graph blob: nil graph")
	}
	g.ClearTransientState()
	data, err := json.Marshal(graphBlobEnvelope{SyncID: syncID, Graph: g})
	if err != nil {
		return nil, fmt.Errorf("marshal graph blob: %w", err)
	}
	return data, nil
}

// UnmarshalGraphBlob parses a sidecar blob. Returns (nil, nil) when the blob
// belongs to a different sync than wantSyncID (stale inherited sidecar);
// pass "" to skip the guard.
func UnmarshalGraphBlob(data []byte, wantSyncID string) (*EntitlementGraph, error) {
	var env graphBlobEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshal graph blob: %w", err)
	}
	if wantSyncID != "" && env.SyncID != wantSyncID {
		return nil, nil
	}
	if env.Graph == nil {
		return nil, nil
	}
	env.Graph.reinitMaps()
	return env.Graph, nil
}
