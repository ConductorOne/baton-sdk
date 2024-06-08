package entitlements_graph

import (
	"context"
	"errors"
	"fmt"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var (
	ErrNoEntitlement = errors.New("no entitlement found")
)

// EntitlementGraph - a directed graph representing the relationships between
// entitlements and grants. This data structure is naïve to any business logic.
// Note that the data of each Node is actually a list or IDs, not a single ID.
// This is because the graph can have cycles.
type EntitlementGraph struct {
	NextNodeID          int                        `json:"node_count"`            // Automatically incremented so that each node has a unique ID.
	Nodes               map[int]Node               `json:"nodes"`                 // The mapping of all node IDs to nodes.
	EntitlementsToNodes map[string]int             `json:"entitlements_to_nodes"` // Mapping of entitlements to nodes for quicker lookup.
	Edges               map[int]map[int]*GrantInfo `json:"edges"`                 // Adjacency list. Source node -> descendant node
	Loaded              bool                       `json:"loaded"`
	Depth               int                        `json:"depth"`
	Actions             []EntitlementGraphAction   `json:"actions"`
}

// Node represents a list of entitlements. It is the base element of the graph.
type Node struct {
	Id             int      `json:"id"`
	EntitlementIDs []string `json:"entitlementIds"` // List of entitlements.
}

// GrantInfo is the edge between two sets of entitlements.
type GrantInfo struct {
	Expanded        bool     `json:"expanded"`
	Shallow         bool     `json:"shallow"`
	ResourceTypeIDs []string `json:"resource_type_ids"`
}

type EntitlementGraphAction struct {
	SourceEntitlementID     string   `json:"source_entitlement_id"`
	DescendantEntitlementID string   `json:"descendant_entitlement_id"`
	Shallow                 bool     `json:"shallow"`
	ResourceTypeIDs         []string `json:"resource_types_ids"`
	PageToken               string   `json:"page_token"`
}

func New(_ context.Context) *EntitlementGraph {
	return &EntitlementGraph{
		Nodes:               make(map[int]Node),
		EntitlementsToNodes: make(map[string]int),
		Edges:               make(map[int]map[int]*GrantInfo),
	}
}

// Str lists every `node` line by line followed by every `edge`.
func (g *EntitlementGraph) Str() string {
	str := "\n"
	for id, node := range g.Nodes {
		str += fmt.Sprintf("node %v entitlement IDs: %v\n", id, node.EntitlementIDs)
	}
	str += "edges:\n"
	for src, descendants := range g.Edges {
		for descendant, grantInfo := range descendants {
			str += fmt.Sprintf("%v -> %v GrantInfo %v\n", src, descendant, grantInfo)
		}
	}
	return str
}

func (g *EntitlementGraph) getAncestors(entitlementID string) []int {
	node := g.GetNode(entitlementID)
	if node == nil {
		panic("entitlement not found")
		// return nil
	}

	ancestors := make([]int, 0)
	for src, dst := range g.Edges {
		if _, ok := dst[node.Id]; ok {
			ancestors = append(ancestors, src)
		}
	}
	return ancestors
}

func (g *EntitlementGraph) GetDescendantEntitlements(entitlementID string) map[string]*GrantInfo {
	node := g.GetNode(entitlementID)
	if node == nil {
		return nil
	}
	entsToGrants := make(map[string]*GrantInfo)
	for dstNodeId, edgeInfo := range g.Edges[node.Id] {
		dstNode := g.Nodes[dstNodeId]
		for _, entId := range dstNode.EntitlementIDs {
			entsToGrants[entId] = edgeInfo
		}
	}
	return entsToGrants
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
	shallow bool,
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

	_, ok := g.Edges[srcNode.Id]
	if !ok {
		g.Edges[srcNode.Id] = make(map[int]*GrantInfo)
	}

	_, ok = g.Edges[srcNode.Id][dstNode.Id]
	if !ok {
		g.Edges[srcNode.Id][dstNode.Id] = &GrantInfo{
			Expanded:        false,
			Shallow:         shallow,
			ResourceTypeIDs: resourceTypeIDs,
		}
	} else {
		// TODO: just do nothing? it's probably a mistake if we're adding the same edge twice
		ctxzap.Extract(ctx).Warn(
			"duplicate edge from datasource",
			zap.String("src_entitlement_id", srcEntitlementID),
			zap.String("dst_entitlement_id", dstEntitlementID),
			zap.Bool("shallow", shallow),
			zap.Strings("resource_type_ids", resourceTypeIDs),
		)
		return nil
	}
	return nil
}

func (g *EntitlementGraph) AddAction(source string, descendant string, grantInfo *GrantInfo) {
	g.Actions = append(g.Actions, EntitlementGraphAction{
		SourceEntitlementID:     source,
		DescendantEntitlementID: descendant,
		PageToken:               "",
		Shallow:                 grantInfo.Shallow,
		ResourceTypeIDs:         grantInfo.ResourceTypeIDs,
	})
}
