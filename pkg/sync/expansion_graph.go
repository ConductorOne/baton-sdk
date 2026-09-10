package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"bytes"
	"context"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// expansionGraph holds the grant-expansion phase's entitlement graph.
//
// The graph is a projection of data already in the store: loadEntitlementGraph
// rebuilds it from PendingExpansionPage with no connector calls, and
// GraphFromStore reads the copy the EntitlementGraphStore sidecar preserved.
// It sat on the sync state back when the token was how it crossed a restart.
// Checkpoints no longer carry it at all, so the holder is process-local.
//
// It carries no mutex, and neither did the fields it replaces. The graph
// pointer is reached from the expansion phase, which runs after grant
// collection has quiesced, and callers of get() mutate the returned graph
// outside any lock anyway — a mutex on this holder would not guard the graph
// it hands out.
type expansionGraph struct {
	graph *expand.EntitlementGraph
}

func newExpansionGraph() *expansionGraph {
	return &expansionGraph{}
}

// restore adopts the graph a decoded token carried inline. Only tokens from
// pre-omission SDKs carry one, and this sets nil for the rest.
func (g *expansionGraph) restore(graph *expand.EntitlementGraph) {
	g.graph = graph
}

// get returns the entitlement graph, allocating an empty one when there is
// none yet.
func (g *expansionGraph) get(ctx context.Context) *expand.EntitlementGraph {
	if g.graph == nil {
		g.graph = expand.NewEntitlementGraph(ctx)
	}
	return g.graph
}

// peek returns the graph without allocating one when absent (unlike get).
// Used by the preserve path to decide whether there is a graph worth
// persisting.
func (g *expansionGraph) peek() *expand.EntitlementGraph {
	return g.graph
}

// clear drops the graph. This is meant to make the final sync token less confusing.
func (g *expansionGraph) clear() {
	g.graph = nil
}

// clearTransientState strips a preserved graph's expansion working state
// before the final checkpoint. A no-op when no graph was built —
// deliberately not get(ctx), which would allocate an empty graph into the
// final token where prior behavior serialized none.
func (g *expansionGraph) clearTransientState() {
	if g.graph != nil {
		g.graph.ClearTransientState()
	}
}

// EntitlementGraphStore is the optional store capability backing graph
// persistence in the c1z (Pebble implements it; SQLite does not). The blob
// format is owned by pkg/sync/expand.
type EntitlementGraphStore interface {
	PutEntitlementGraphBlob(ctx context.Context, data []byte) error
	GetEntitlementGraphBlob(ctx context.Context) ([]byte, error)
	DeleteEntitlementGraphBlob(ctx context.Context) error
}

// GraphFromStore loads the entitlement graph persisted in the c1z sidecar for
// syncID. Returns nil (no error) when the store lacks the capability, no graph
// was preserved, or the stored graph belongs to a different sync.
func GraphFromStore(ctx context.Context, store c1zstore.Store, syncID string) (*expand.EntitlementGraph, error) {
	// Exported entry point: callers hand us a bare store, so this is one of
	// the two places allowed to resolve capabilities (see store_caps.go).
	caps := resolveReaderCaps(store)
	if caps.entitlementGraph == nil {
		return nil, nil
	}
	data, err := caps.entitlementGraph.GetEntitlementGraphBlob(ctx)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	graph, boundDigest, err := expand.UnmarshalGraphBlobWithGrantDigest(data, syncID)
	if err != nil || graph == nil {
		return graph, err
	}
	if boundDigest == nil {
		return nil, nil
	}
	if caps.grantDigest == nil {
		return nil, nil
	}
	currentDigest, found, err := caps.grantDigest.GrantGenerationDigest(ctx)
	if err != nil {
		return nil, err
	}
	if !found ||
		boundDigest.Count != currentDigest.Count ||
		boundDigest.ABIVersion != currentDigest.ABIVersion ||
		!bytes.Equal(boundDigest.Hash, currentDigest.Hash) {
		return nil, nil
	}
	return graph, nil
}
