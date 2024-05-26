package sync

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var (
	ErrNoEntitlement = errors.New("no entitlement found")
)

type EntitlementGraphAction struct {
	SourceEntitlementID     string   `json:"source_entitlement_id"`
	DescendantEntitlementID string   `json:"descendant_entitlement_id"`
	Shallow                 bool     `json:"shallow"`
	ResourceTypeIDs         []string `json:"resource_types_ids"`
	PageToken               string   `json:"page_token"`
}

// Edges between entitlements are grants.
type grantInfo struct {
	Expanded        bool     `json:"expanded"`
	Shallow         bool     `json:"shallow"`
	ResourceTypeIDs []string `json:"resource_type_ids"`
}

type Node struct {
	Id             int      `json:"id"`
	EntitlementIDs []string `json:"entitlementIds"` // List of entitlements.
}

type EntitlementGraph struct {
	NodeCount int                        `json:"node_count"`
	Nodes     map[int]Node               `json:"nodes"`
	Edges     map[int]map[int]*grantInfo `json:"edges"` // Adjacency list. Source node -> destination node
	Loaded    bool                       `json:"loaded"`
	Depth     int                        `json:"depth"`
	Actions   []EntitlementGraphAction   `json:"actions"`
}

func NewEntitlementGraph(ctx context.Context) *EntitlementGraph {
	return &EntitlementGraph{
		NodeCount: 1, // Start at 1 in case we don't initialize something and try to get node 0
		Nodes:     make(map[int]Node),
		// TODO: probably want this for efficiency
		// EntitlementsToNodes: make(map[string]int),
		Edges: make(map[int]map[int]*grantInfo),
	}
}

func (g *EntitlementGraph) Str() string {
	str := "\n"
	for id, node := range g.Nodes {
		str += fmt.Sprintf("node %v entitlement IDs: %v\n", id, node.EntitlementIDs)
	}
	str += "edges:\n"
	for src, dsts := range g.Edges {
		for dst, gi := range dsts {
			str += fmt.Sprintf("%v -> %v grantInfo %v\n", src, dst, gi)
		}
	}
	return str
}

func (g *EntitlementGraph) Validate() error {
	for srcNodeId, dstNodeIDs := range g.Edges {
		node, ok := g.Nodes[srcNodeId]
		if !ok {
			return ErrNoEntitlement
		}
		if len(node.EntitlementIDs) == 0 {
			return fmt.Errorf("empty node")
		}
		for dstNodeId, grantInfo := range dstNodeIDs {
			node, ok := g.Nodes[dstNodeId]
			if !ok {
				return ErrNoEntitlement
			}
			if len(node.EntitlementIDs) == 0 {
				return fmt.Errorf("empty node")
			}
			if grantInfo == nil {
				return fmt.Errorf("nil edge info. dst node %v", dstNodeId)
			}
		}
	}
	// check for entitlement ids that are in multiple nodes
	seenEntitlements := make(map[string]int)
	for nodeID, node := range g.Nodes {
		for _, entID := range node.EntitlementIDs {
			if _, ok := seenEntitlements[entID]; ok {
				return fmt.Errorf("entitlement %v is in multiple nodes: %v %v", entID, nodeID, seenEntitlements[entID])
			}
			seenEntitlements[entID] = nodeID
		}
	}
	return nil
}

func (g *EntitlementGraph) isNodeExpanded(nodeID int) bool {
	dstNodeIDs := g.Edges[nodeID]
	for _, edgeInfo := range dstNodeIDs {
		if !edgeInfo.Expanded {
			return false
		}
	}
	return true
}

// IsExpanded returns true if all entitlements in the graph have been expanded.
func (g *EntitlementGraph) IsExpanded() bool {
	for srcNodeID := range g.Edges {
		if !g.isNodeExpanded(srcNodeID) {
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
	if !g.isNodeExpanded(node.Id) {
		return false
	}
	return true
}

// HasUnexpandedAncestors returns true if the given entitlement has ancestors that have not been expanded yet.
func (g *EntitlementGraph) HasUnexpandedAncestors(entitlementID string) bool {
	node := g.GetNode(entitlementID)
	if node == nil {
		return false
	}

	for _, ancestorId := range g.getAncestors(entitlementID) {
		if !g.isNodeExpanded(ancestorId) {
			return true
		}
	}
	return false
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

// Find the direct ancestors of the given entitlement. The 'all' flag returns all ancestors regardless of 'done' state.
func (g *EntitlementGraph) GetCycles() ([][]int, bool) {
	rv := make([][]int, 0)
	for nodeID := range g.Nodes {
		edges, ok := g.Edges[nodeID]
		if !ok || len(edges) == 0 {
			continue
		}
		cycle, isCycle := g.getCycle([]int{nodeID})
		if isCycle && !isInCycle(cycle, rv) {
			rv = append(rv, cycle)
		}
	}

	return rv, len(rv) > 0
}

func isInCycle(newCycle []int, cycles [][]int) bool {
	for _, cycle := range cycles {
		if len(cycle) > 0 && reflect.DeepEqual(cycle, newCycle) {
			return true
		}
	}
	return false
}

func shift(arr []int, n int) []int {
	for i := 0; i < n; i++ {
		arr = append(arr[1:], arr[0])
	}
	return arr
}

func (g *EntitlementGraph) getCycle(visits []int) ([]int, bool) {
	if len(visits) == 0 {
		return nil, false
	}
	nodeId := visits[len(visits)-1]
	for descendantId := range g.Edges[nodeId] {
		tempVisits := make([]int, len(visits))
		copy(tempVisits, visits)
		if descendantId == visits[0] {
			// shift array so that the smallest element is first
			smallestIndex := 0
			for i := range tempVisits {
				if tempVisits[i] < tempVisits[smallestIndex] {
					smallestIndex = i
				}
			}
			tempVisits = shift(tempVisits, smallestIndex)
			return tempVisits, true
		}
		for _, visit := range visits {
			if visit == descendantId {
				return nil, false
			}
		}

		tempVisits = append(tempVisits, descendantId)
		return g.getCycle(tempVisits)
	}
	return nil, false
}

func (g *EntitlementGraph) GetDescendantEntitlements(entitlementID string) map[string]*grantInfo {
	node := g.GetNode(entitlementID)
	if node == nil {
		return nil
	}
	entsToGrants := make(map[string]*grantInfo)
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

func (g *EntitlementGraph) AddEntitlement(entitlement *v2.Entitlement) {
	node := g.GetNode(entitlement.Id)
	if node != nil {
		return
	}

	g.Nodes[g.NodeCount] = Node{
		Id:             g.NodeCount,
		EntitlementIDs: []string{entitlement.Id},
	}
	g.NodeCount++
}

func (g *EntitlementGraph) GetEntitlements() []string {
	var entitlements []string
	for _, node := range g.Nodes {
		entitlements = append(entitlements, node.EntitlementIDs...)
	}
	return entitlements
}

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
	_, ok := g.Edges[srcNode.Id][dstNode.Id]
	if !ok {
		return
	}

	g.Edges[srcNode.Id][dstNode.Id].Expanded = true
}

func (g *EntitlementGraph) GetNode(entitlementId string) *Node {
	// TODO: add an EntitlementToNode map for efficiency
	for _, node := range g.Nodes {
		for _, entId := range node.EntitlementIDs {
			if entId == entitlementId {
				return &node
			}
		}
	}
	return nil
}

func (g *EntitlementGraph) AddEdge(ctx context.Context, srcEntitlementID string, dstEntitlementID string, shallow bool, resourceTypeIDs []string) error {
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
		g.Edges[srcNode.Id] = make(map[int]*grantInfo)
	}

	_, ok = g.Edges[srcNode.Id][dstNode.Id]
	if !ok {
		g.Edges[srcNode.Id][dstNode.Id] = &grantInfo{
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

func (g *EntitlementGraph) removeNode(nodeID int) {
	delete(g.Nodes, nodeID)
	delete(g.Edges, nodeID)
	for id := range g.Edges {
		delete(g.Edges[id], nodeID)
	}
}

func (g *EntitlementGraph) mergeNodes(node1ID int, node2ID int) {
	node1 := g.Nodes[node1ID]
	node2 := g.Nodes[node2ID]

	// Put node's entitlements on first node
	node1.EntitlementIDs = append(node1.EntitlementIDs, node2.EntitlementIDs...)

	// TODO: Merge grant info?

	for dstNodeID := range g.Edges[node2ID] {
		dstNode := g.Nodes[dstNodeID]
		if dstNodeID == node1ID {
			continue
		}

		// Set outgoing edges on first node
		for id := range g.Edges[dstNodeID] {
			if id != node1.Id {
				g.Edges[node1.Id][id] = g.Edges[dstNode.Id][id]
			}
		}
	}

	// Set incoming edges on first node
	for srcID, edges := range g.Edges {
		for dstID, gi := range edges {
			if dstID == node2ID && srcID != node1ID {
				g.Edges[srcID][node1ID] = gi
				delete(g.Edges[srcID], dstID)
			}
		}
	}

	// Delete node
	g.removeNode(node2ID)
}

func (g *EntitlementGraph) FixCycles() error {
	// If we can't fix the cycles in 10 tries, just give up
	const maxTries = 10
	for i := 0; i < maxTries; i++ {
		cycles, hasCycles := g.GetCycles()
		if !hasCycles {
			return nil
		}

		// Merge all the nodes in a cycle.
		for i := 1; i < len(cycles[0]); i++ {
			g.mergeNodes(cycles[0][0], cycles[0][i])
		}
	}
	return fmt.Errorf("could not fix cycles after %v tries", maxTries)
}
