package expand

import (
	"context"
	"iter"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sync/expand/scc"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// JSON tags for actions, edges, and nodes are short to minimize size of serialized data when checkpointing

type EntitlementGraphAction struct {
	SourceEntitlementID     string   `json:"sid"`
	DescendantEntitlementID string   `json:"did"`
	Shallow                 bool     `json:"s"`
	ResourceTypeIDs         []string `json:"rtids"`
	PageToken               string   `json:"pt"`
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

func (g *EntitlementGraph) HasEntitlement(entitlementID string) bool {
	return g.GetNode(entitlementID) != nil
}

// AddEntitlement - add an entitlement's ID as an unconnected node in the graph.
func (g *EntitlementGraph) AddEntitlement(entitlement *v2.Entitlement) {
	// If the entitlement is already in the graph, fail silently.
	found := g.GetNode(entitlement.GetId())
	if found != nil {
		return
	}
	g.HasNoCycles = false // Reset this since we're changing the graph.

	// Start at 1 in case we don't initialize something and try to get node 0.
	g.NextNodeID++

	// Create a new node.
	node := Node{
		Id:             g.NextNodeID,
		EntitlementIDs: []string{entitlement.GetId()},
	}

	// Add the node to the data structures.
	g.Nodes[node.Id] = node
	g.EntitlementsToNodes[entitlement.GetId()] = node.Id
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
		if _, ok = destinations[dstNode.Id]; ok {
			// TODO: just do nothing? it's probably a mistake if we're adding the same edge twice
			ctxzap.Extract(ctx).Warn(
				"duplicate edge from datasource",
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
			// TODO: just do nothing? it's probably a mistake if we're adding the same edge twice
			ctxzap.Extract(ctx).Warn(
				"duplicate edge from datasource",
				zap.String("src_entitlement_id", srcEntitlementID),
				zap.String("dst_entitlement_id", dstEntitlementID),
				zap.Bool("shallow", isShallow),
				zap.Strings("resource_type_ids", resourceTypeIDs),
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
