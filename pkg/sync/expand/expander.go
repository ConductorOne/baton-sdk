package expand

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var tracer = otel.Tracer("baton-sdk/sync.expand")

const defaultMaxDepth int64 = 20

var maxDepth, _ = strconv.ParseInt(os.Getenv("BATON_GRAPH_EXPAND_MAX_DEPTH"), 10, 64)

// ErrMaxDepthExceeded is returned when the expansion graph exceeds the maximum allowed depth.
var ErrMaxDepthExceeded = errors.New("max depth exceeded")

// ExpanderStore defines the minimal store interface needed for grant expansion.
// Implementations:
//   - *dotc1z.C1File (via dotc1z.C1ZStore) for production syncs
//   - mocks for unit tests
//
// StoreExpandedGrants writes a batch of expanded grants back to storage,
// preserving existing expansion metadata columns on the underlying rows.
// See dotc1z.GrantStore.StoreExpandedGrants for the full contract.
type ExpanderStore interface {
	GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error)
	ListGrantsForEntitlement(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error)
	StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error
}

// entitlementGrantPrincipalKeyLister is an optional fast path for stores that
// can list only descendant principal identities without materializing full
// grants. Returned keys must use descendantGrantKey(resourceType, resource).
type entitlementGrantPrincipalKeyLister interface {
	ListGrantPrincipalKeysForEntitlement(
		ctx context.Context,
		entitlement *v2.Entitlement,
		pageToken string,
		pageSize uint32,
	) ([]string, string, error)
}

// Expander handles the grant expansion algorithm.
// It can be used standalone for testing or called from the syncer.
type Expander struct {
	store ExpanderStore
	graph *EntitlementGraph
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

// RunSingleStep executes one step of the expansion algorithm.
// Returns true when the graph is fully expanded, false if more work is needed.
// This matches the syncer's step-by-step execution model.
func (e *Expander) RunSingleStep(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	l = l.With(zap.Int("depth", e.graph.Depth))
	l.Debug("expander: starting step")

	// Process current action if any
	if len(e.graph.Actions) > 0 {
		action := e.graph.Actions[0]
		nextPageToken, err := e.runAction(ctx, action)
		if err != nil {
			l.Error("expander: error running graph action", zap.Error(err), zap.Any("action", action))
			_ = e.graph.DeleteEdge(ctx, action.SourceEntitlementID, action.DescendantEntitlementID)
			if status.Code(err) == codes.NotFound {
				// Skip action and delete the edge that caused the error.
				e.graph.Actions = e.graph.Actions[1:]
				return nil
			}
			return err
		}

		if nextPageToken != "" {
			// More pages to process
			action.PageToken = nextPageToken
		} else {
			// Action is complete - mark edge expanded and remove from queue
			e.graph.MarkEdgeExpanded(action.SourceEntitlementID, action.DescendantEntitlementID)
			e.graph.Actions = e.graph.Actions[1:]
		}
	}

	// If there are still actions remaining, continue processing
	if len(e.graph.Actions) > 0 {
		return nil
	}

	// Check max depth
	depth := maxDepth
	if depth == 0 {
		depth = defaultMaxDepth
	}

	if int64(e.graph.Depth) > depth {
		l.Error("expander: exceeded max depth", zap.Int64("max_depth", depth))
		return fmt.Errorf("expander: %w (%d)", ErrMaxDepthExceeded, depth)
	}

	// Generate new actions from expandable entitlements
	for sourceEntitlementID := range e.graph.GetExpandableEntitlements(ctx) {
		for descendantEntitlementID, grantInfo := range e.graph.GetExpandableDescendantEntitlements(ctx, sourceEntitlementID) {
			e.graph.Actions = append(e.graph.Actions, &EntitlementGraphAction{
				SourceEntitlementID:     sourceEntitlementID,
				DescendantEntitlementID: descendantEntitlementID,
				PageToken:               "",
				Shallow:                 grantInfo.IsShallow,
				ResourceTypeIDs:         grantInfo.ResourceTypeIDs,
			})
		}
	}

	e.graph.Depth++
	l.Debug("expander: graph is not expanded, incrementing depth")
	return nil
}

func (e *Expander) IsDone(ctx context.Context) bool {
	return e.graph.IsExpanded()
}

// maxPrefetchPages bounds the streaming scan that builds the principal-key
// set for the descendant entitlement. Keys are ~80 bytes each, so 100 pages
// of 10000 rows is a ~80MB ceiling per action. Exceeding the cap is not an
// error — the caller drops the partial set and falls back to per-principal
// queries (slower, but bounded and correct).
const maxPrefetchPages = 100
const descendantPrincipalPageSize = 16

func descendantGrantKey(resourceTypeID, resourceID string) string {
	return resourceTypeID + "\x00" + resourceID
}

// prefetchDescendantPrincipals streams the descendant entitlement's grants
// and returns the set of principal keys that have at least one grant. We
// keep only keys (not full grants) so memory scales with the number of
// distinct principals on the descendant, not the total grant payload size.
//
// The bool return reports whether the scan completed:
//   - true: the set is exhaustive; absence from the set means the descendant
//     definitely has no grant for that principal, so runAction can skip the
//     per-principal query and create a new grant directly.
//   - false: the page cap was exceeded. The partial set is unsafe as a
//     negative oracle — a missing key could be a true absence or just past
//     the cap — so runAction must fall back to per-principal queries.
func prefetchDescendantPrincipals(
	ctx context.Context,
	store ExpanderStore,
	entitlement *v2.Entitlement,
) (map[string]struct{}, bool, error) {
	set := make(map[string]struct{})
	pageToken := ""
	for page := 0; page < maxPrefetchPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if fastStore, ok := store.(entitlementGrantPrincipalKeyLister); ok {
			keys, nextPageToken, err := fastStore.ListGrantPrincipalKeysForEntitlement(ctx, entitlement, pageToken, 0)
			if err != nil {
				return nil, false, fmt.Errorf("prefetchDescendantPrincipals: %w", err)
			}
			for _, key := range keys {
				set[key] = struct{}{}
			}
			pageToken = nextPageToken
			if pageToken == "" {
				return set, true, nil
			}
			continue
		}
		resp, err := store.ListGrantsForEntitlement(ctx,
			reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: entitlement,
				PageToken:   pageToken,
			}.Build())
		if err != nil {
			return nil, false, fmt.Errorf("prefetchDescendantPrincipals: %w", err)
		}
		for _, g := range resp.GetList() {
			// Skip malformed grants with no principal: they would otherwise
			// add an empty key to the set, polluting the negative oracle the
			// caller relies on. Consistent with the nil-principal guard in
			// runAction. (Opaque getters are nil-safe, so this is correctness,
			// not panic-avoidance.)
			if g.GetPrincipal() == nil {
				continue
			}
			pid := g.GetPrincipal().GetId()
			set[descendantGrantKey(pid.GetResourceType(), pid.GetResource())] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return set, true, nil
		}
	}
	return set, false, nil
}

// listDescendantGrantsForPrincipal pages through ListGrantsForEntitlement
// scoped to a single principal. Used by runAction when the key set says
// (or doesn't know whether) the descendant has grants for this principal,
// so we need the full grants to merge sources.
func listDescendantGrantsForPrincipal(
	ctx context.Context,
	store ExpanderStore,
	entitlement *v2.Entitlement,
	principalID *v2.ResourceId,
) ([]*v2.Grant, error) {
	var out []*v2.Grant
	pageToken := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := store.ListGrantsForEntitlement(ctx,
			reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: entitlement,
				PrincipalId: principalID,
				PageToken:   pageToken,
				PageSize:    descendantPrincipalPageSize,
			}.Build())
		if err != nil {
			return nil, fmt.Errorf("listDescendantGrantsForPrincipal: %w", err)
		}
		out = append(out, resp.GetList()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return out, nil
		}
	}
}

// runAction processes a single action and returns the next page token.
// If the returned page token is empty, the action is complete.
//
// Starts a new root span linked to the calling expandGrantsForEntitlements
// span. One expansion run can process thousands of actions, each of which
// paginates ListGrantsForEntitlement many times; keeping them all under a
// single trace produced 100k+-span mega-traces. Per-action new roots split
// the work into one trace per source/descendant entitlement pair while
// preserving the link back to the originating expansion call.
//
// Named err is required so the deferred span error recorder observes the
// function's return value.
//
//nolint:nonamedreturns // see doc comment above.
func (e *Expander) runAction(ctx context.Context, action *EntitlementGraphAction) (nextPage string, err error) {
	ctx, span := uotel.StartWithLink(ctx, tracer, "expand.runAction",
		trace.WithAttributes(
			attribute.String("source_entitlement_id", action.SourceEntitlementID),
			attribute.String("descendant_entitlement_id", action.DescendantEntitlementID),
			attribute.Int("depth", e.graph.Depth),
			attribute.Bool("shallow", action.Shallow),
		),
	)
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	l = l.With(
		zap.Int("depth", e.graph.Depth),
		zap.String("source_entitlement_id", action.SourceEntitlementID),
		zap.String("descendant_entitlement_id", action.DescendantEntitlementID),
	)

	// Fetch source and descendant entitlement
	sourceEntitlement, err := e.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: action.SourceEntitlementID,
	}.Build())
	if err != nil {
		l.Error("runAction: error fetching source entitlement", zap.Error(err))
		return "", fmt.Errorf("runAction: error fetching source entitlement: %w", err)
	}

	descendantEntitlement, err := e.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: action.DescendantEntitlementID,
	}.Build())
	if err != nil {
		l.Error("runAction: error fetching descendant entitlement", zap.Error(err))
		return "", fmt.Errorf("runAction: error fetching descendant entitlement: %w", err)
	}

	// Fetch a page of source grants
	sourceGrants, err := e.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement:              sourceEntitlement.GetEntitlement(),
		PageToken:                action.PageToken,
		PrincipalResourceTypeIds: action.ResourceTypeIDs,
	}.Build())
	if err != nil {
		l.Error("runAction: error fetching source grants", zap.Error(err))
		return "", fmt.Errorf("runAction: error fetching source grants: %w", err)
	}

	existingPrincipals, complete, err := prefetchDescendantPrincipals(ctx, e.store, descendantEntitlement.GetEntitlement())
	if err != nil {
		l.Error("runAction: error prefetching descendant principals", zap.Error(err))
		return "", fmt.Errorf("runAction: error prefetching descendant principals: %w", err)
	}
	if !complete {
		// Partial set is unsafe as a negative oracle; drop it and pay the
		// per-principal cost for every source grant. Slow but bounded.
		l.Warn("runAction: descendant-grant prefetch cap exceeded; falling back to per-principal queries",
			zap.String("descendant_entitlement_id", action.DescendantEntitlementID),
			zap.Int("prefetch_page_cap", maxPrefetchPages))
		existingPrincipals = nil
	}
	span.SetAttributes(
		attribute.Int("descendant_prefetch_set_size", len(existingPrincipals)),
		attribute.Bool("descendant_prefetch_complete", complete),
	)

	// Per-page cache of descendant grants by principal key. Populated lazily
	// from the store on the first hit for a key, and seeded with newly
	// created grants so a second source grant for the same principal in the
	// same page merges into the in-memory grant instead of producing a
	// duplicate (same deterministic grant ID would otherwise upsert and lose
	// the first iteration's source attribution).
	descendantsByKey := make(map[string][]*v2.Grant)

	var newGrants = make([]*v2.Grant, 0)
	for _, sourceGrant := range sourceGrants.GetList() {
		if sourceGrant.GetPrincipal() == nil {
			// Malformed grant with no principal. main passed this through to
			// ListGrantsForEntitlement with a nil PrincipalId, which the store
			// treats as "no filter" (pkg/dotc1z/grants.go) — fetching every
			// descendant grant and adding this source to all of them,
			// corrupting unrelated principals. Skip it, but log so the upstream
			// connector bug is visible instead of silently dropped.
			l.Warn("runAction: skipping source grant with nil principal",
				zap.String("source_grant_id", sourceGrant.GetId()))
			continue
		}
		if action.Shallow {
			sourcesMap := sourceGrant.GetSources().GetSources()
			foundDirectGrant := len(sourcesMap) == 0
			if sourcesMap[action.SourceEntitlementID] != nil {
				foundDirectGrant = true
			}
			if !foundDirectGrant {
				continue
			}
		}

		sgSources := sourceGrant.GetSources().GetSources()
		isSourceDirect := len(sgSources) == 0 || sgSources[action.SourceEntitlementID] != nil

		principal := sourceGrant.GetPrincipal().GetId()
		key := descendantGrantKey(principal.GetResourceType(), principal.GetResource())

		descendantGrants, cached := descendantsByKey[key]
		if !cached {
			// Fast path: if the key-set is complete and this principal is
			// not in it, we know there is no existing descendant grant
			// without issuing a SQL query.
			needQuery := true
			if existingPrincipals != nil {
				if _, ok := existingPrincipals[key]; !ok {
					needQuery = false
				}
			}
			if needQuery {
				descendantGrants, err = listDescendantGrantsForPrincipal(ctx, e.store, descendantEntitlement.GetEntitlement(), principal)
				if err != nil {
					l.Error("runAction: error fetching descendant grants", zap.Error(err))
					return "", fmt.Errorf("runAction: error fetching descendant grants: %w", err)
				}
			}
			descendantsByKey[key] = descendantGrants
		}

		if len(descendantGrants) == 0 {
			descendantGrant, err := newExpandedGrant(descendantEntitlement.GetEntitlement(), sourceGrant.GetPrincipal(), action.SourceEntitlementID, isSourceDirect)
			if err != nil {
				l.Error("runAction: error creating new grant", zap.Error(err))
				return "", fmt.Errorf("runAction: error creating new grant: %w", err)
			}
			newGrants = append(newGrants, descendantGrant)
			// Seed the cache so a later source grant for the same principal
			// in this page merges into this grant rather than re-creating it.
			descendantsByKey[key] = []*v2.Grant{descendantGrant}
		} else {
			for _, descendantGrant := range descendantGrants {
				sourcesMap := descendantGrant.GetSources().GetSources()
				if sourcesMap == nil {
					sourcesMap = make(map[string]*v2.GrantSources_GrantSource)
				}

				updated := false
				if len(sourcesMap) == 0 {
					sourcesMap[action.DescendantEntitlementID] = &v2.GrantSources_GrantSource{IsDirect: true}
					updated = true
				}
				if existingSource := sourcesMap[action.SourceEntitlementID]; existingSource == nil {
					sourcesMap[action.SourceEntitlementID] = &v2.GrantSources_GrantSource{IsDirect: isSourceDirect}
					updated = true
				} else if isSourceDirect && !existingSource.GetIsDirect() {
					// A later source grant for the same principal is direct;
					// upgrade the recorded source from indirect to direct.
					// Direct wins over indirect. main got this via re-querying
					// the store each iteration + last-write-wins on the
					// re-created grant; the in-page cache merge path must do it
					// explicitly or the upgrade is silently dropped.
					existingSource.SetIsDirect(true)
					updated = true
				}

				if updated {
					sources := v2.GrantSources_builder{Sources: sourcesMap}.Build()
					descendantGrant.SetSources(sources)
					newGrants = append(newGrants, descendantGrant)
				}
			}
		}

		newGrants, err = PutGrantsInChunks(ctx, e.store, newGrants, 10000)
		if err != nil {
			l.Error("runAction: error updating descendant grants", zap.Error(err))
			return "", fmt.Errorf("runAction: error updating descendant grants: %w", err)
		}
	}

	_, err = PutGrantsInChunks(ctx, e.store, newGrants, 0)
	if err != nil {
		l.Error("runAction: error updating descendant grants", zap.Error(err))
		return "", fmt.Errorf("runAction: error updating descendant grants: %w", err)
	}

	return sourceGrants.GetNextPageToken(), nil
}

// PutGrantsInChunks accumulates grants until the buffer exceeds minChunkSize,
// then writes all grants to the store at once.
func PutGrantsInChunks(ctx context.Context, store ExpanderStore, grants []*v2.Grant, minChunkSize int) ([]*v2.Grant, error) {
	if len(grants) < minChunkSize {
		return grants, nil
	}

	err := store.StoreExpandedGrants(ctx, grants...)
	if err != nil {
		return nil, fmt.Errorf("PutGrantsInChunks: error putting grants: %w", err)
	}

	return make([]*v2.Grant, 0), nil
}

// newExpandedGrant creates a new grant for a principal on a descendant entitlement.
func newExpandedGrant(descEntitlement *v2.Entitlement, principal *v2.Resource, sourceEntitlementID string, isSourceDirect bool) (*v2.Grant, error) {
	enResource := descEntitlement.GetResource()
	if enResource == nil {
		return nil, fmt.Errorf("newExpandedGrant: entitlement has no resource")
	}

	if principal == nil {
		return nil, fmt.Errorf("newExpandedGrant: principal is nil")
	}

	// Add immutable annotation since this function is only called if no direct grant exists
	var annos annotations.Annotations
	annos.Update(&v2.GrantImmutable{})

	var sources *v2.GrantSources
	if sourceEntitlementID != "" {
		sources = &v2.GrantSources{
			Sources: map[string]*v2.GrantSources_GrantSource{
				sourceEntitlementID: {IsDirect: isSourceDirect},
			},
		}
	}

	grant := v2.Grant_builder{
		Id:          fmt.Sprintf("%s:%s:%s", descEntitlement.GetId(), principal.GetId().GetResourceType(), principal.GetId().GetResource()),
		Entitlement: descEntitlement,
		Principal:   principal,
		Sources:     sources,
		Annotations: annos,
	}.Build()

	return grant, nil
}
