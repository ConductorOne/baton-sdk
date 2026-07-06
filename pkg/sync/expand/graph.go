package expand

import (
	"context"
	"iter"
	"sort"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/sync/expand/scc"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// JSON tags for actions, edges, and nodes are short to minimize size of serialized data when checkpointing

// ActionDescendant is one destination an action fans out to. Shallow is per
// destination because destinations sharing a source read can differ on it
// (Shallow only gates which grants qualify when writing, not the source read).
type ActionDescendant struct {
	EntitlementID string `json:"did"`
	Shallow       bool   `json:"s"`
}

// EntitlementGraphAction is one unit of expansion work: read the source's
// grants once (filtered by ResourceTypeIDs) and fan them out to every
// destination in Descendants. Reading the source once per batch instead of
// once per outgoing edge is what cuts the read-amplification.
//
// DescendantEntitlementID/Shallow are the old per-edge fields, kept only so
// checkpoints written by an older baton-sdk still load: descendants() folds
// such an action into a one-element batch. New actions leave them empty and
// use Descendants. JSON names are unchanged, so old and new checkpoints
// round-trip without a version bump.
type EntitlementGraphAction struct {
	SourceEntitlementID string             `json:"sid"`
	Descendants         []ActionDescendant `json:"dsts,omitempty"`
	ResourceTypeIDs     []string           `json:"rtids"`
	PageToken           string             `json:"pt"`

	// Legacy per-edge fields, retained for resuming pre-grouping checkpoints.
	DescendantEntitlementID string `json:"did,omitempty"`
	Shallow                 bool   `json:"s,omitempty"`
}

// descendants returns the destination batch for this action, folding a legacy
// per-edge action (DescendantEntitlementID/Shallow) into a one-element batch.
func (a *EntitlementGraphAction) descendants() []ActionDescendant {
	if len(a.Descendants) > 0 {
		return a.Descendants
	}
	if a.DescendantEntitlementID != "" {
		return []ActionDescendant{{EntitlementID: a.DescendantEntitlementID, Shallow: a.Shallow}}
	}
	return nil
}

type Edge struct {
	EdgeID          int      `json:"id"`
	SourceID        int      `json:"sid"`
	DestinationID   int      `json:"did"`
	IsExpanded      bool     `json:"e"`
	IsShallow       bool     `json:"s"`
	ResourceTypeIDs []string `json:"rtids"`
}

// Node represents a list of entitlements. It is the base element of the graph.
type Node struct {
	Id             int      `json:"id"`
	EntitlementIDs []string `json:"eids"` // List of entitlements.
}

// EntitlementGraph - a directed graph representing the relationships between
// entitlements and grants. This data structure is naïve to any business logic.
// Note that the data of each Node is actually a list or IDs, not a single ID.
// This is because the graph can have cycles, and we address them by reducing
// _all_ nodes in a cycle into a single node.
type EntitlementGraph struct {
	NextNodeID            int                       `json:"next_node_id"`            // Automatically incremented so that each node has a unique ID.
	NextEdgeID            int                       `json:"next_edge_id"`            // Automatically incremented so that each edge has a unique ID.
	Nodes                 map[int]Node              `json:"nodes"`                   // The mapping of all node IDs to nodes.
	EntitlementsToNodes   map[string]int            `json:"entitlements_to_nodes"`   // Internal mapping of entitlements to nodes for quicker lookup.
	SourcesToDestinations map[int]map[int]int       `json:"sources_to_destinations"` // Internal mapping of outgoing edges by node ID.
	DestinationsToSources map[int]map[int]int       `json:"destinations_to_sources"` // Internal mapping of incoming edges by node ID.
	Edges                 map[int]Edge              `json:"edges"`                   // Adjacency list. Source node -> descendant node
	Loaded                bool                      `json:"loaded"`
	Depth                 int                       `json:"depth"`
	Actions               []*EntitlementGraphAction `json:"actions"`
	HasNoCycles           bool                      `json:"has_no_cycles"`
	ExpansionPlan         *EntitlementGraphPlan     `json:"plan,omitempty"`
	ExpansionMetrics      *EntitlementGraphMetrics  `json:"metrics,omitempty"`
}

type EntitlementGraphMetrics struct {
	Algorithm               string `json:"algorithm"`
	ProjectionRowsBuilt     int64  `json:"projection_rows_built"`
	NodesReduced            int64  `json:"nodes_reduced"`
	DestinationEntitlements int64  `json:"destination_entitlements"`
	DirtyGrantsWritten      int64  `json:"dirty_grants_written"`
}

func NewEntitlementGraph(_ context.Context) *EntitlementGraph {
	return &EntitlementGraph{
		DestinationsToSources: make(map[int]map[int]int),
		Edges:                 make(map[int]Edge),
		EntitlementsToNodes:   make(map[string]int),
		Nodes:                 make(map[int]Node),
		SourcesToDestinations: make(map[int]map[int]int),
		HasNoCycles:           false,
	}
}

// isNodeExpanded - is every outgoing edge from this node Expanded?
func (g *EntitlementGraph) isNodeExpanded(nodeID int) bool {
	for _, edgeID := range g.SourcesToDestinations[nodeID] {
		if edge, ok := g.Edges[edgeID]; ok {
			if !edge.IsExpanded {
				return false
			}
		}
	}
	return true
}

// IsExpanded returns true if all entitlements in the graph have been expanded.
func (g *EntitlementGraph) IsExpanded() bool {
	for _, edge := range g.Edges {
		if !edge.IsExpanded {
			return false
		}
	}
	return true
}

// IsEntitlementExpanded returns true if all the outgoing edges for the given entitlement have been expanded.
func (g *EntitlementGraph) IsEntitlementExpanded(entitlementID string) bool {
	node := g.GetNode(entitlementID)
	if node == nil {
		// TODO: log error? return error?
		return false
	}
	return g.isNodeExpanded(node.Id)
}

// HasUnexpandedAncestors returns true if the given entitlement has ancestors that have not been expanded yet.
func (g *EntitlementGraph) HasUnexpandedAncestors(entitlementID string) bool {
	node := g.GetNode(entitlementID)
	if node == nil {
		return false
	}

	for _, ancestorId := range g.getParents(entitlementID) {
		if !g.isNodeExpanded(ancestorId) {
			return true
		}
	}
	return false
}

// getParents gets _all_ IDs of nodes that have this entitlement as a child.
func (g *EntitlementGraph) getParents(entitlementID string) []int {
	node := g.GetNode(entitlementID)
	if node == nil {
		panic("entitlement not found")
	}

	parents := make([]int, 0)
	if destinations, ok := g.DestinationsToSources[node.Id]; ok {
		for id := range destinations {
			parents = append(parents, id)
		}
	}
	return parents
}

// GetDescendantEntitlements given an entitlementID, return a mapping of child
// entitlementIDs to edge data.
func (g *EntitlementGraph) GetDescendantEntitlements(entitlementID string) map[string]*Edge {
	node := g.GetNode(entitlementID)
	if node == nil {
		return nil
	}
	entitlementsToEdges := make(map[string]*Edge)
	if destinations, ok := g.SourcesToDestinations[node.Id]; ok {
		for destinationID, edgeID := range destinations {
			if destination, ok := g.Nodes[destinationID]; ok {
				for _, e := range destination.EntitlementIDs {
					if edge, ok := g.Edges[edgeID]; ok {
						entitlementsToEdges[e] = &edge
					}
				}
			}
		}
	}
	return entitlementsToEdges
}

func (g *EntitlementGraph) GetExpandableDescendantEntitlements(ctx context.Context, entitlementID string) iter.Seq2[string, *Edge] {
	return func(yield func(string, *Edge) bool) {
		node := g.GetNode(entitlementID)
		if node == nil {
			return
		}
		if destinations, ok := g.SourcesToDestinations[node.Id]; ok {
			for destinationID, edgeID := range destinations {
				if destination, ok := g.Nodes[destinationID]; ok {
					for _, e := range destination.EntitlementIDs {
						if edge, ok := g.Edges[edgeID]; ok {
							if edge.IsExpanded {
								continue
							}
							if !yield(e, &edge) {
								return
							}
						}
					}
				}
			}
		}
	}
}

// filterGroup is a set of destination entitlements reachable from one source
// that share the same ResourceTypeIDs source-read filter, so they can be served
// by a single read of the source's grants.
type filterGroup struct {
	resourceTypeIDs []string
	dests           []ActionDescendant
}

// groupExpandableDescendants buckets a source's unexpanded outgoing edges by
// their ResourceTypeIDs filter (the filter changes the source read, so only
// edges with the same filter can share one read). Output is
// deterministic — groups ordered by their filter key, destinations sorted by
// entitlement ID — so action generation and checkpoint resume are stable across
// runs despite the underlying map iteration order.
func groupExpandableDescendants(ctx context.Context, g *EntitlementGraph, sourceEntitlementID string) []filterGroup {
	byKey := make(map[string]*filterGroup)
	order := make([]string, 0)
	for entID, edge := range g.GetExpandableDescendantEntitlements(ctx, sourceEntitlementID) {
		// Canonicalize the filter so ["user","group"] and ["group","user"]
		// share a read. ListGrantsForEntitlement filters by set membership, so
		// reordering the slice does not change the read.
		rtids := append([]string(nil), edge.ResourceTypeIDs...)
		sort.Strings(rtids)
		key := strings.Join(rtids, "\x00")
		grp, ok := byKey[key]
		if !ok {
			grp = &filterGroup{resourceTypeIDs: rtids}
			byKey[key] = grp
			order = append(order, key)
		}
		grp.dests = append(grp.dests, ActionDescendant{EntitlementID: entID, Shallow: edge.IsShallow})
	}

	sort.Strings(order)
	groups := make([]filterGroup, 0, len(order))
	for _, key := range order {
		grp := byKey[key]
		sort.Slice(grp.dests, func(i, j int) bool { return grp.dests[i].EntitlementID < grp.dests[j].EntitlementID })
		groups = append(groups, *grp)
	}
	return groups
}

func (g *EntitlementGraph) HasEntitlement(entitlementID string) bool {
	return g.GetNode(entitlementID) != nil
}

// AddEntitlementID adds an entitlement ID as an unconnected node in the graph.
func (g *EntitlementGraph) AddEntitlementID(entitlementID string) {
	if entitlementID == "" {
		return
	}

	// If the entitlement is already in the graph, fail silently.
	found := g.GetNode(entitlementID)
	if found != nil {
		return
	}
	g.HasNoCycles = false // Reset this since we're changing the graph.

	// Start at 1 in case we don't initialize something and try to get node 0.
	g.NextNodeID++

	node := Node{
		Id:             g.NextNodeID,
		EntitlementIDs: []string{entitlementID},
	}

	g.Nodes[node.Id] = node
	g.EntitlementsToNodes[entitlementID] = node.Id
}

// GetEntitlements returns a combined list of _all_ entitlements from all nodes.
func (g *EntitlementGraph) GetEntitlements() []string {
	var entitlements []string
	for _, node := range g.Nodes {
		entitlements = append(entitlements, node.EntitlementIDs...)
	}
	return entitlements
}

func (g *EntitlementGraph) GetExpandableEntitlements(ctx context.Context) iter.Seq[string] {
	return func(yield func(string) bool) {
		l := ctxzap.Extract(ctx)
		for _, node := range g.Nodes {
			for _, entitlementID := range node.EntitlementIDs {
				// We've already expanded this entitlement, so skip it.
				if g.IsEntitlementExpanded(entitlementID) {
					continue
				}
				// We have ancestors who have not been expanded yet, so we can't expand ourselves.
				if g.HasUnexpandedAncestors(entitlementID) {
					l.Debug("expandGrantsForEntitlements: skipping source entitlement because it has unexpanded ancestors", zap.String("source_entitlement_id", entitlementID))
					continue
				}
				if !yield(entitlementID) {
					return
				}
			}
		}
	}
}

// MarkEdgeExpanded given source and destination entitlements, mark the edge
// between them as "expanded".
func (g *EntitlementGraph) MarkEdgeExpanded(sourceEntitlementID string, descendantEntitlementID string) {
	srcNode := g.GetNode(sourceEntitlementID)
	if srcNode == nil {
		// TODO: panic?
		return
	}
	dstNode := g.GetNode(descendantEntitlementID)
	if dstNode == nil {
		// TODO: panic?
		return
	}

	if destinations, ok := g.SourcesToDestinations[srcNode.Id]; ok {
		if edgeID, ok := destinations[dstNode.Id]; ok {
			if edge, ok := g.Edges[edgeID]; ok {
				edge.IsExpanded = true
				g.Edges[edgeID] = edge
			}
		}
	}
}

// GetNode - returns the node that contains the given `entitlementID`.
func (g *EntitlementGraph) GetNode(entitlementID string) *Node {
	nodeID, ok := g.EntitlementsToNodes[entitlementID]
	if !ok {
		return nil
	}
	node, ok := g.Nodes[nodeID]
	if !ok {
		return nil
	}
	return &node
}

// AddEdge - given two entitlements, add an edge with resourceTypeIDs.
func (g *EntitlementGraph) AddEdge(
	ctx context.Context,
	srcEntitlementID string,
	dstEntitlementID string,
	isShallow bool,
	resourceTypeIDs []string,
) error {
	srcNode := g.GetNode(srcEntitlementID)
	if srcNode == nil {
		return ErrNoEntitlement
	}
	dstNode := g.GetNode(dstEntitlementID)
	if dstNode == nil {
		return ErrNoEntitlement
	}

	if destinations, ok := g.SourcesToDestinations[srcNode.Id]; ok {
		if edgeID, ok := destinations[dstNode.Id]; ok {
			// A duplicate (src, dst) edge from the datasource. Fold the
			// specs deterministically instead of keeping whichever the
			// engine-dependent pagination order delivered first — the two
			// engines can iterate pending expansions in different orders,
			// and a first-wins rule would let them build different graphs
			// from identical connector data. Union semantics match
			// fixCycle's edge merge: any deep edge makes the pair deep
			// (deep admits strictly more), and any unfiltered edge makes
			// the pair unfiltered (an empty filter means match-all, so
			// set-unioning across it would narrow it).
			existing := g.Edges[edgeID]
			changed := false
			if existing.IsShallow && !isShallow {
				existing.IsShallow = false
				changed = true
			}
			switch {
			case len(existing.ResourceTypeIDs) == 0:
				// Already unfiltered; nothing wider exists.
			case len(resourceTypeIDs) == 0:
				existing.ResourceTypeIDs = nil
				changed = true
			default:
				merged := mapset.NewThreadUnsafeSet[string](existing.ResourceTypeIDs...)
				before := merged.Cardinality()
				merged.Append(resourceTypeIDs...)
				if merged.Cardinality() != before {
					existing.ResourceTypeIDs = merged.ToSlice()
					changed = true
				}
			}
			if changed {
				g.Edges[edgeID] = existing
			}
			ctxzap.Extract(ctx).Warn(
				"duplicate edge from datasource; merged specs (deep-wins, unfiltered-wins)",
				zap.String("src_entitlement_id", srcEntitlementID),
				zap.String("dst_entitlement_id", dstEntitlementID),
				zap.Bool("shallow", isShallow),
				zap.Strings("resource_type_ids", resourceTypeIDs),
			)
			return nil
		}
	} else {
		g.SourcesToDestinations[srcNode.Id] = make(map[int]int)
	}

	if sources, ok := g.DestinationsToSources[dstNode.Id]; ok {
		if _, ok = sources[srcNode.Id]; ok {
			// Unreachable when the maps are consistent (the
			// SourcesToDestinations check above already handled the
			// duplicate), kept as a guard against map divergence.
			ctxzap.Extract(ctx).Warn(
				"duplicate edge from datasource (reverse map only)",
				zap.String("src_entitlement_id", srcEntitlementID),
				zap.String("dst_entitlement_id", dstEntitlementID),
			)
			return nil
		}
	} else {
		g.DestinationsToSources[dstNode.Id] = make(map[int]int)
	}

	g.HasNoCycles = false // Reset this since we're changing the graph.

	// Start at 1 in case we don't initialize something and try to get edge 0.
	g.NextEdgeID++
	edge := Edge{
		EdgeID:          g.NextEdgeID,
		SourceID:        srcNode.Id,
		DestinationID:   dstNode.Id,
		IsExpanded:      false,
		IsShallow:       isShallow,
		ResourceTypeIDs: resourceTypeIDs,
	}

	g.Edges[g.NextEdgeID] = edge
	g.SourcesToDestinations[srcNode.Id][dstNode.Id] = edge.EdgeID
	g.DestinationsToSources[dstNode.Id][srcNode.Id] = edge.EdgeID
	return nil
}

func (g *EntitlementGraph) DeleteEdge(ctx context.Context, srcEntitlementID string, dstEntitlementID string) error {
	srcNode := g.GetNode(srcEntitlementID)
	if srcNode == nil {
		return ErrNoEntitlement
	}
	dstNode := g.GetNode(dstEntitlementID)
	if dstNode == nil {
		return ErrNoEntitlement
	}

	if destinations, ok := g.SourcesToDestinations[srcNode.Id]; ok {
		if edgeID, ok := destinations[dstNode.Id]; ok {
			delete(destinations, dstNode.Id)
			delete(g.DestinationsToSources[dstNode.Id], srcNode.Id)
			delete(g.Edges, edgeID)
			return nil
		}
	}
	return nil
}

// toAdjacency builds an adjacency map for SCC. If nodesSubset is non-nil, only
// include those nodes (and edges between them). Always include all nodes in the
// subset as keys, even if they have zero outgoing edges.
// toAdjacency removed: use SCC via scc.Source on EntitlementGraph

var _ scc.Source = (*EntitlementGraph)(nil)

// ForEachNode implements scc.Source iteration over nodes (including isolated nodes).
// It does not import scc; matching the method names/signatures is sufficient.
func (g *EntitlementGraph) ForEachNode(fn func(id int) bool) {
	for id := range g.Nodes {
		if !fn(id) {
			return
		}
	}
}

// ForEachEdgeFrom implements scc.Source iteration of outgoing edges for src.
// It enumerates unique destination node IDs.
func (g *EntitlementGraph) ForEachEdgeFrom(src int, fn func(dst int) bool) {
	if dsts, ok := g.SourcesToDestinations[src]; ok {
		for dst := range dsts {
			if !fn(dst) {
				return
			}
		}
	}
}

// reachableFrom computes the set of node IDs reachable from start over
// SourcesToDestinations using an iterative BFS.
func (g *EntitlementGraph) reachableFrom(start int) map[int]struct{} {
	if _, ok := g.Nodes[start]; !ok {
		return nil
	}
	visited := make(map[int]struct{}, 16)
	queue := make([]int, 0, 16)
	queue = append(queue, start)
	visited[start] = struct{}{}
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		if nbrs, ok := g.SourcesToDestinations[u]; ok {
			for v := range nbrs {
				if _, seen := visited[v]; seen {
					continue
				}
				visited[v] = struct{}{}
				queue = append(queue, v)
			}
		}
	}
	return visited
}
