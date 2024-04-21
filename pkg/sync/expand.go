package sync

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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

// TODO: this can probably go away and just be a string
type Entitlement struct {
	Id string `json:"id"`
	// Expanded bool   `json:"expanded"`
}

type Node struct {
	Id           int           `json:"id"`
	Entitlements []Entitlement `json:"entitlements"` // List of entitlements.
	// Expanded     bool          `json:"expanded"`     // If all edges and entitlements are expanded.
}

type EntitlementGraph struct {
	NodeCount int          `json:"node_count"`
	Nodes     map[int]Node `json:"nodes"`
	// Entitlements map[string]bool           `json:"entitlements"` // Bool is whether entitlement is expanded or not
	Edges   map[int]map[int]*grantInfo `json:"edges"` // Adjacency list. Source node -> destination node
	Loaded  bool                       `json:"loaded"`
	Depth   int                        `json:"depth"`
	Actions []EntitlementGraphAction   `json:"actions"`
}

func NewEntitlementGraph(ctx context.Context) *EntitlementGraph {
	return &EntitlementGraph{
		NodeCount: 1, // Start at 1 in case we don't initialize something and try to get node 0
		Nodes:     make(map[int]Node),
		// EntitlementsToNodes: make(map[string]int),
		Edges: make(map[int]map[int]*grantInfo),
	}
}

func (g *EntitlementGraph) Validate() error {
	for srcNodeId, dstNodeIDs := range g.Edges {
		node, ok := g.Nodes[srcNodeId]
		if !ok {
			return ErrNoEntitlement
		}
		if len(node.Entitlements) == 0 {
			return fmt.Errorf("empty node")
		}
		for dstNodeId, grantInfo := range dstNodeIDs {
			node, ok := g.Nodes[dstNodeId]
			if !ok {
				return ErrNoEntitlement
			}
			if len(node.Entitlements) == 0 {
				return fmt.Errorf("empty node")
			}
			if grantInfo == nil {
				return fmt.Errorf("nil edge info. dst node %v", dstNodeId)
			}
		}
	}
	// TODO: check for entitlement ids that are in multiple nodes
	return nil
}

func (g *EntitlementGraph) IsNodeExpanded(nodeID int) bool {
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
		if !g.IsNodeExpanded(srcNodeID) {
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
	if !g.IsNodeExpanded(node.Id) {
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
		if !g.IsNodeExpanded(ancestorId) {
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
		if len(g.GetDescendants(nodeID)) == 0 {
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
	nodeId := visits[len(visits)-1]
	for _, descendant := range g.GetDescendants(nodeId) {
		tempVisits := make([]int, len(visits))
		copy(tempVisits, visits)
		if descendant.Id == visits[0] {
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
			if visit == descendant.Id {
				return nil, false
			}
		}

		tempVisits = append(tempVisits, descendant.Id)
		return g.getCycle(tempVisits) //nolint:staticcheck // false positive
	}
	return nil, false
}

func (g *EntitlementGraph) GetDescendants(nodeID int) []Node {
	node, ok := g.Nodes[nodeID]
	if !ok {
		return nil
	}
	var nodes []Node
	for dstNodeId := range g.Edges[node.Id] {
		dstNode := g.Nodes[dstNodeId]
		nodes = append(nodes, dstNode)
	}
	return nodes
}

func (g *EntitlementGraph) GetDescendantEntitlements(entitlementID string) map[string]*grantInfo {
	node := g.GetNode(entitlementID)
	if node == nil {
		return nil
	}
	entsToGrants := make(map[string]*grantInfo)
	for dstNodeId, edgeInfo := range g.Edges[node.Id] {
		dstNode := g.Nodes[dstNodeId]
		for _, ent := range dstNode.Entitlements {
			entsToGrants[ent.Id] = edgeInfo
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
		Id:           g.NodeCount,
		Entitlements: []Entitlement{{Id: entitlement.Id}},
	}
	g.NodeCount++
	// if _, ok := d.Entitlements[entitlement.Id]; !ok {
	// 	d.Entitlements[entitlement.Id] = false
	// }
}

func (g *EntitlementGraph) GetEntitlements() []string {
	var entitlements []string
	for _, node := range g.Nodes {
		for _, entitlement := range node.Entitlements {
			entitlements = append(entitlements, entitlement.Id)
		}
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
		for _, ent := range node.Entitlements {
			if ent.Id == entitlementId {
				return &node
			}
		}
	}
	return nil
}

func (g *EntitlementGraph) AddEdge(srcEntitlementID string, dstEntitlementID string, shallow bool, resourceTypeIDs []string) error {
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
		return fmt.Errorf("edge already exists")
	}
	return nil
}

func (g *EntitlementGraph) RemoveNode(nodeID int) {
	delete(g.Nodes, nodeID)
	delete(g.Edges, nodeID)
	for id := range g.Edges {
		delete(g.Edges[id], nodeID)
	}
}

// Merge all the nodes in a cycle.
func (g *EntitlementGraph) FixCycle(cycle []int) {
	if len(cycle) == 0 {
		return
	}
	firstNode := g.Nodes[cycle[0]]

	for i := 1; i < len(cycle); i++ {
		nodeID := cycle[i]
		edges := g.Edges[nodeID]
		for dstNodeID := range edges {
			if dstNodeID == firstNode.Id {
				continue
			}
			dstNode := g.Nodes[dstNodeID]
			// Put node's entitlements on first node
			firstNode.Entitlements = append(firstNode.Entitlements, dstNode.Entitlements...)
			// Set outgoing edges on first node
			for id := range g.Edges[dstNode.Id] {
				if id != firstNode.Id {
					g.Edges[firstNode.Id][id] = g.Edges[dstNode.Id][id]
				}
			}
			// Set incoming edges on first node
			for id := range g.Nodes {
				descendants := g.GetDescendants(id)
				for _, dNode := range descendants {
					if dNode.Id == nodeID {
						delete(g.Edges[id], nodeID)
					}
				}
			}
			// Delete node
			g.RemoveNode(dstNodeID)
		}
	}
}

func (g *EntitlementGraph) FixCycles() {
	for {
		cycles, hasCycles := g.GetCycles()
		if !hasCycles {
			return
		}
		g.FixCycle(cycles[0])
	}
}
