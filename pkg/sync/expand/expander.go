package expand

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

const defaultMaxDepth int64 = 20

var maxDepth, _ = strconv.ParseInt(os.Getenv("BATON_GRAPH_EXPAND_MAX_DEPTH"), 10, 64)

// ErrMaxDepthExceeded is returned when the expansion graph exceeds the maximum allowed depth.
var ErrMaxDepthExceeded = errors.New("max depth exceeded")

// ExpanderStore defines the minimal store interface needed for grant expansion.
// This interface can be implemented by the connectorstore or by a mock for testing.
type ExpanderStore interface {
	GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error)
	ListGrantsForEntitlement(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error)
	PutGrants(ctx context.Context, grants ...*v2.Grant) error
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
			if errors.Is(err, sql.ErrNoRows) {
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

// runAction processes a single action and returns the next page token.
// If the returned page token is empty, the action is complete.
func (e *Expander) runAction(ctx context.Context, action *EntitlementGraphAction) (string, error) {
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

	var newGrants = make([]*v2.Grant, 0)
	for _, sourceGrant := range sourceGrants.GetList() {
		// If this is a shallow action, then we only want to expand grants that have no sources
		// which indicates that it was directly assigned.
		if action.Shallow {
			sourcesMap := sourceGrant.GetSources().GetSources()
			// If we have no sources, this is a direct grant
			foundDirectGrant := len(sourcesMap) == 0
			// If the source grant has sources, then we need to see if any of them are the source entitlement itself
			if sourcesMap[action.SourceEntitlementID] != nil {
				foundDirectGrant = true
			}

			// This is not a direct grant, so skip it since we are a shallow action
			if !foundDirectGrant {
				continue
			}
		}

		// Unroll all grants for the principal on the descendant entitlement.
		pageToken := ""
		for {
			req := reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: descendantEntitlement.GetEntitlement(),
				PrincipalId: sourceGrant.GetPrincipal().GetId(),
				PageToken:   pageToken,
				Annotations: nil,
			}.Build()

			resp, err := e.store.ListGrantsForEntitlement(ctx, req)
			if err != nil {
				l.Error("runAction: error fetching descendant grants", zap.Error(err))
				return "", fmt.Errorf("runAction: error fetching descendant grants: %w", err)
			}
			descendantGrants := resp.GetList()

			// If we have no grants for the principal in the descendant entitlement, make one.
			if pageToken == "" && resp.GetNextPageToken() == "" && len(descendantGrants) == 0 {
				descendantGrant, err := newExpandedGrant(descendantEntitlement.GetEntitlement(), sourceGrant.GetPrincipal(), action.SourceEntitlementID)
				if err != nil {
					l.Error("runAction: error creating new grant", zap.Error(err))
					return "", fmt.Errorf("runAction: error creating new grant: %w", err)
				}
				newGrants = append(newGrants, descendantGrant)
				newGrants, err = PutGrantsInChunks(ctx, e.store, newGrants, 10000)
				if err != nil {
					l.Error("runAction: error updating descendant grants", zap.Error(err))
					return "", fmt.Errorf("runAction: error updating descendant grants: %w", err)
				}
				break
			}

			// Add the source entitlement as a source to all descendant grants.
			grantsToUpdate := make([]*v2.Grant, 0)
			for _, descendantGrant := range descendantGrants {
				sourcesMap := descendantGrant.GetSources().GetSources()
				if sourcesMap == nil {
					sourcesMap = make(map[string]*v2.GrantSources_GrantSource)
				}

				updated := false

				if len(sourcesMap) == 0 {
					// If we are already granted this entitlement, make sure to add ourselves as a source.
					sourcesMap[action.DescendantEntitlementID] = &v2.GrantSources_GrantSource{}
					updated = true
				}
				// Include the source grant as a source.
				if sourcesMap[action.SourceEntitlementID] == nil {
					sourcesMap[action.SourceEntitlementID] = &v2.GrantSources_GrantSource{}
					updated = true
				}

				if updated {
					sources := v2.GrantSources_builder{Sources: sourcesMap}.Build()
					descendantGrant.SetSources(sources)
					grantsToUpdate = append(grantsToUpdate, descendantGrant)
				}
			}
			newGrants = append(newGrants, grantsToUpdate...)

			newGrants, err = PutGrantsInChunks(ctx, e.store, newGrants, 10000)
			if err != nil {
				l.Error("runAction: error updating descendant grants", zap.Error(err))
				return "", fmt.Errorf("runAction: error updating descendant grants: %w", err)
			}

			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
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

	err := store.PutGrants(ctx, grants...)
	if err != nil {
		return nil, fmt.Errorf("PutGrantsInChunks: error putting grants: %w", err)
	}

	return make([]*v2.Grant, 0), nil
}

// newExpandedGrant creates a new grant for a principal on a descendant entitlement.
func newExpandedGrant(descEntitlement *v2.Entitlement, principal *v2.Resource, sourceEntitlementID string) (*v2.Grant, error) {
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
				sourceEntitlementID: {},
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
