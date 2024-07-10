package expand

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type EntitlementGraphAction struct {
	SourceEntitlementID     string   `json:"source_entitlement_id"`
	DescendantEntitlementID string   `json:"descendant_entitlement_id"`
	Shallow                 bool     `json:"shallow"`
	ResourceTypeIDs         []string `json:"resource_types_ids"`
	PageToken               string   `json:"page_token"`
}

type Edge struct {
	EdgeID          int      `json:"edge_id"`
	SourceID        int      `json:"source_id"`
	DestinationID   int      `json:"destination_id"`
	IsExpanded      bool     `json:"expanded"`
	IsShallow       bool     `json:"shallow"`
	ResourceTypeIDs []string `json:"resource_type_ids"`
}

// Node represents a list of entitlements. It is the base element of the graph.
type Node struct {
	Id             int      `json:"id"`
	EntitlementIDs []string `json:"entitlementIds"` // List of entitlements.
}

// EntitlementGraph - a directed graph representing the relationships between
// entitlements and grants. This data structure is naïve to any business logic.
// Note that the data of each Node is actually a list or IDs, not a single ID.
// This is because the graph can have cycles, and we address them by reducing
// _all_ nodes in a cycle into a single node.
type EntitlementGraph struct {
	NextNodeID            int                      `json:"next_node_id"`            // Automatically incremented so that each node has a unique ID.
	NextEdgeID            int                      `json:"next_edge_id"`            // Automatically incremented so that each edge has a unique ID.
	Nodes                 map[int]Node             `json:"nodes"`                   // The mapping of all node IDs to nodes.
	EntitlementsToNodes   map[string]int           `json:"entitlements_to_nodes"`   // Internal mapping of entitlements to nodes for quicker lookup.
	SourcesToDestinations map[int]map[int]int      `json:"sources_to_destinations"` // Internal mapping of outgoing edges by node ID.
	DestinationsToSources map[int]map[int]int      `json:"destinations_to_sources"` // Internal mapping of incoming edges by node ID.
	Edges                 map[int]Edge             `json:"edges"`                   // Adjacency list. Source node -> descendant node
	Loaded                bool                     `json:"loaded"`
	Depth                 int                      `json:"depth"`
	Actions               []EntitlementGraphAction `json:"actions"`
}

func NewEntitlementGraph(_ context.Context) *EntitlementGraph {
	return &EntitlementGraph{
		DestinationsToSources: make(map[int]map[int]int),
		Edges:                 make(map[int]Edge),
		EntitlementsToNodes:   make(map[string]int),
		Nodes:                 make(map[int]Node),
		SourcesToDestinations: make(map[int]map[int]int),
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
				for _, entitlementID := range destination.EntitlementIDs {
					if edge, ok := g.Edges[edgeID]; ok {
						entitlementsToEdges[entitlementID] = &edge
					}
				}
			}
		}
	}
	return entitlementsToEdges
}

func (g *EntitlementGraph) HasEntitlement(entitlementID string) bool {
	return g.GetNode(entitlementID) != nil
}

// AddEntitlement - add an entitlement's ID as an unconnected node in the graph.
func (g *EntitlementGraph) AddEntitlement(entitlement *v2.Entitlement) {
	// If the entitlement is already in the graph, fail silently.
	found := g.GetNode(entitlement.Id)
	if found != nil {
		return
	}

	// Start at 1 in case we don't initialize something and try to get node 0.
	g.NextNodeID++

	// Create a new node.
	node := Node{
		Id:             g.NextNodeID,
		EntitlementIDs: []string{entitlement.Id},
	}

	// Add the node to the data structures.
	g.Nodes[node.Id] = node
	g.EntitlementsToNodes[entitlement.Id] = node.Id
}

// GetEntitlements returns a combined list of _all_ entitlements from all nodes.
func (g *EntitlementGraph) GetEntitlements() []string {
	var entitlements []string
	for _, node := range g.Nodes {
		entitlements = append(entitlements, node.EntitlementIDs...)
	}
	return entitlements
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
