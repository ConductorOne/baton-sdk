package expand

import (
	"context"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"google.golang.org/protobuf/types/known/anypb"
)

// immutableAnnotationAny is the GrantImmutable annotation every expanded
// grant carries. The payload is a constant empty GrantImmutable, so it is
// marshaled once here instead of per grant — anypb.New per call was ~1.5GB
// of allocations on a large expansion. The *anypb.Any is treated as
// read-only; each grant's annotation slice references the shared pointer.
var immutableAnnotationAny = func() *anypb.Any {
	a, err := anypb.New(&v2.GrantImmutable{})
	if err != nil {
		panic(fmt.Errorf("expand: marshal GrantImmutable annotation: %w", err))
	}
	return a
}()

// Deprecated: no expansion path returns this; kept so importers compile.
var ErrMaxDepthExceeded = errors.New("max depth exceeded")

// ExpanderStore defines the minimal store interface needed for grant expansion.
// Implementations:
//   - *dotc1z.C1File (via c1zstore.Store) for production syncs
//   - mocks for unit tests
//
// StoreExpandedGrants writes a batch of expanded grants back to storage,
// preserving existing expansion metadata columns on the underlying rows.
// See c1zstore.GrantStore.StoreExpandedGrants for the full contract.
type ExpanderStore interface {
	GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error)
	ListGrantsForEntitlement(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error)
	StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error
}

// newExpandedGrantStorer is an optional fast path for stores that can persist
// caller-proven-new expanded grants without read-before-write. Pebble implements
// it; SQLite and tests can omit it and fall back to StoreExpandedGrants.
type newExpandedGrantStorer interface {
	StoreNewExpandedGrants(ctx context.Context, grants ...*v2.Grant) error
}

// synthesizedContributionStorer is an optional store fast path for
// caller-proven-new synthesized grants expressed as raw contributions
// (destination + principal refs + sources) rather than materialized
// v2.Grants, skipping both the grant construction and the read-before-write.
// Pebble implements it; other stores fall back to StoreExpandedGrants via
// the caller.
type synthesizedContributionStorer interface {
	StoreNewExpandedGrantContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error
}

// synthesizedContributionLayerStorer is an optional store fast path that
// accumulates one topological layer's synthesized grants into sorted bulk
// writes (SST ingests on Pebble) instead of committing each destination's
// rows out of key order as they are produced.
//
// Begin returning false routes the caller to the
// StoreNewExpandedGrantContributions fallback. Today the Pebble engine
// always returns true when a sync is open (the by_principal index is
// unconditionally rebuilt at EndSync, so a layer session never skips index
// maintenance); the boolean exists so a future store can decline. Rows
// streamed via Add become readable in bulk publishes no later than Finish
// returning — callers must not rely on visibility before Finish, nor on
// invisibility before it (large layers publish interior segments early).
// Abort discards an in-flight session.
type synthesizedContributionLayerStorer interface {
	BeginExpandedGrantLayer(ctx context.Context) (bool, error)
	AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error
	FinishExpandedGrantLayer(ctx context.Context) error
	AbortExpandedGrantLayer(ctx context.Context) error
}

// Expander handles the grant expansion algorithm.
// It can be used standalone for testing or called from the syncer.
type Expander struct {
	store ExpanderStore
	graph *EntitlementGraph
	// dropStats, when set, aggregates dropped-edge reporting for the
	// whole sync (the syncer owns it across step-wise Expander
	// reconstruction). Nil is safe: recording no-ops.
	dropStats *DroppedEdgeStats
}

// SetDropStats installs the sync-scoped dropped-edge aggregator.
func (e *Expander) SetDropStats(s *DroppedEdgeStats) {
	e.dropStats = s
}

// NewExpander creates a new Expander with the given store and graph.
func NewExpander(store ExpanderStore, graph *EntitlementGraph) *Expander {
	return &Expander{
		store: store,
		graph: graph,
	}
}

// Graph returns the entitlement graph.
func (e *Expander) Graph() *EntitlementGraph {
	return e.graph
}

// Run executes the complete expansion algorithm until the graph is fully expanded.
// This is useful for testing where you want to run the entire expansion in one call.
func (e *Expander) Run(ctx context.Context) error {
	for {
		err := e.RunSingleStep(ctx)
		if err != nil {
			return err
		}
		if e.IsDone(ctx) {
			return nil
		}
	}
}

func (e *Expander) RunSingleStep(ctx context.Context) error {
	if e.IsDone(ctx) {
		return nil
	}
	return e.RunTopologicalMergeProjection(ctx)
}

func (e *Expander) IsDone(ctx context.Context) bool {
	return e.graph.IsExpanded()
}
