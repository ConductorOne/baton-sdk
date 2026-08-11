package expand

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// graphBlobEnvelope is the serialized form of the entitlement-graph sidecar
// stored in a c1z (instead of bloating the sync token). SyncID guards
// against reading a graph inherited from a different sync (e.g. a fold-copied
// compaction base).
type graphBlobEnvelope struct {
	FormatVersion uint32                          `json:"format_version"`
	SyncID        string                          `json:"sync_id"`
	GrantDigest   *c1zstore.GrantGenerationDigest `json:"grant_digest,omitempty"`
	Graph         *EntitlementGraph               `json:"graph"`
}

const graphBlobFormatVersion uint32 = 2

// MarshalGraphBlob serializes a legacy, unbound graph blob for compatibility
// tests. Transient state is stripped first (a reload rebuilds it).
//
// The blob has no grant-generation digest, so sync.GraphFromStore deliberately
// rejects it for incremental reuse. Production persistence must use
// MarshalGraphBlobWithGrantDigest.
func MarshalGraphBlob(syncID string, g *EntitlementGraph) ([]byte, error) {
	return marshalGraphBlob(syncID, g, nil)
}

// MarshalGraphBlobWithGrantDigest binds the graph to the exact sealed grant
// generation. Graph reuse requires this binding.
func MarshalGraphBlobWithGrantDigest(syncID string, g *EntitlementGraph, digest c1zstore.GrantGenerationDigest) ([]byte, error) {
	if len(digest.Hash) == 0 || digest.ABIVersion == 0 {
		return nil, fmt.Errorf("marshal graph blob: incomplete grant digest")
	}
	digest.Hash = append([]byte(nil), digest.Hash...)
	return marshalGraphBlob(syncID, g, &digest)
}

func marshalGraphBlob(syncID string, g *EntitlementGraph, digest *c1zstore.GrantGenerationDigest) ([]byte, error) {
	if g == nil {
		return nil, fmt.Errorf("marshal graph blob: nil graph")
	}
	graphCopy := *g
	graphCopy.ClearTransientState()
	data, err := json.Marshal(graphBlobEnvelope{FormatVersion: graphBlobFormatVersion, SyncID: syncID, GrantDigest: digest, Graph: &graphCopy})
	if err != nil {
		return nil, fmt.Errorf("marshal graph blob: %w", err)
	}
	return data, nil
}

// UnmarshalGraphBlob parses a graph for compatibility tests while discarding
// its grant-generation binding. Returns (nil, nil) when the blob belongs to a
// different sync than wantSyncID (stale inherited sidecar); pass "" to skip
// the guard.
//
// The returned graph must not drive incremental reuse. Production readers
// must use UnmarshalGraphBlobWithGrantDigest and verify the returned digest.
func UnmarshalGraphBlob(data []byte, wantSyncID string) (*EntitlementGraph, error) {
	graph, _, err := UnmarshalGraphBlobWithGrantDigest(data, wantSyncID)
	return graph, err
}

// UnmarshalGraphBlobWithGrantDigest returns the persisted grant-generation
// binding along with the graph. A nil digest means the blob is unbound and
// must not be reused incrementally.
func UnmarshalGraphBlobWithGrantDigest(data []byte, wantSyncID string) (*EntitlementGraph, *c1zstore.GrantGenerationDigest, error) {
	var env graphBlobEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, nil, fmt.Errorf("unmarshal graph blob: %w", err)
	}
	if env.FormatVersion != graphBlobFormatVersion {
		return nil, nil, nil
	}
	if wantSyncID != "" && env.SyncID != wantSyncID {
		return nil, nil, nil
	}
	if env.Graph == nil {
		return nil, nil, nil
	}
	env.Graph.reinitMaps()
	if env.GrantDigest != nil {
		env.GrantDigest.Hash = bytes.Clone(env.GrantDigest.Hash)
	}
	return env.Graph, env.GrantDigest, nil
}
