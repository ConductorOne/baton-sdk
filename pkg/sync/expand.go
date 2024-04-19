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

func (d *EntitlementGraph) Validate() error {
	for srcNodeId, dstNodeIDs := range d.Edges {
		node, ok := d.Nodes[srcNodeId]
		if !ok {
			return ErrNoEntitlement
		}
		if len(node.Entitlements) == 0 {
			return fmt.Errorf("empty node")
		}
		for dstNodeId, grantInfo := range dstNodeIDs {
			node, ok := d.Nodes[dstNodeId]
			if !ok {
				return ErrNoEntitlement
			}
			if len(node.Entitlements) == 0 {
				return fmt.Errorf("empty node")
			}
			if grantInfo == nil {
				return fmt.Errorf("nil edge info")
			}
		}
	}
	// TODO: check for entitlement ids that are in multiple nodes
	return nil
}

func (d *EntitlementGraph) IsNodeExpanded(nodeID int) bool {
	dstNodeIDs := d.Edges[nodeID]
	for _, edgeInfo := range dstNodeIDs {
		if !edgeInfo.Expanded {
			return false
		}
	}
	return true
}

// IsExpanded returns true if all entitlements in the graph have been expanded.
func (d *EntitlementGraph) IsExpanded() bool {
	for srcNodeID := range d.Edges {
		if !d.IsNodeExpanded(srcNodeID) {
			return false
		}
	}
	return true
}

// IsEntitlementExpanded returns true if all the outgoing edges for the given entitlement have been expanded.
func (d *EntitlementGraph) IsEntitlementExpanded(entitlementID string) bool {
	node := d.GetNode(entitlementID)
	if node == nil {
		// TODO: log error? return error?
		return false
	}
	if !d.IsNodeExpanded(node.Id) {
		return false
	}
	return true
}

// HasUnexpandedAncestors returns true if the given entitlement has ancestors that have not been expanded yet.
func (d *EntitlementGraph) HasUnexpandedAncestors(entitlementID string) bool {
	node := d.GetNode(entitlementID)
	if node == nil {
		return false
	}

	for _, ancestorId := range d.getAncestors(entitlementID) {
		if !d.IsNodeExpanded(ancestorId) {
			return true
		}
	}
	return false
}

func (d *EntitlementGraph) getAncestors(entitlementID string) []int {
	node := d.GetNode(entitlementID)
	if node == nil {
		panic("entitlement not found")
		// return nil
	}

	ancestors := make([]int, 0)
	for src, dst := range d.Edges {
		if _, ok := dst[node.Id]; ok {
			ancestors = append(ancestors, src)
		}
	}
	return ancestors
}

// Find the direct ancestors of the given entitlement. The 'all' flag returns all ancestors regardless of 'done' state.
func (d *EntitlementGraph) GetCycles() ([][]int, bool) {
	rv := make([][]int, 0)
	for nodeID := range d.Nodes {
		if len(d.GetDescendants(nodeID)) == 0 {
			continue
		}
		cycle, isCycle := d.getCycle([]int{nodeID})
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

func (d *EntitlementGraph) getCycle(visits []int) ([]int, bool) {
	nodeId := visits[len(visits)-1]
	for _, descendant := range d.GetDescendants(nodeId) {
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
		return d.getCycle(tempVisits)
	}
	return nil, false
}

func (d *EntitlementGraph) GetDescendants(nodeID int) []Node {
	node, ok := d.Nodes[nodeID]
	if !ok {
		return nil
	}
	var nodes []Node
	for dstNodeId := range d.Edges[node.Id] {
		dstNode := d.Nodes[dstNodeId]
		nodes = append(nodes, dstNode)
	}
	return nodes
}

func (d *EntitlementGraph) GetDescendantEntitlements(entitlementID string) map[string]*grantInfo {
	node := d.GetNode(entitlementID)
	if node == nil {
		return nil
	}
	entsToGrants := make(map[string]*grantInfo)
	for dstNodeId, edgeInfo := range d.Edges[node.Id] {
		dstNode := d.Nodes[dstNodeId]
		for _, ent := range dstNode.Entitlements {
			entsToGrants[ent.Id] = edgeInfo
		}
	}
	return entsToGrants
}

func (d *EntitlementGraph) HasEntitlement(entitlementID string) bool {
	return d.GetNode(entitlementID) != nil
}

func (d *EntitlementGraph) AddEntitlement(entitlement *v2.Entitlement) {
	node := d.GetNode(entitlement.Id)
	if node != nil {
		return
	}

	d.Nodes[d.NodeCount] = Node{
		Id:           d.NodeCount,
		Entitlements: []Entitlement{{Id: entitlement.Id}},
	}
	d.NodeCount++
	// if _, ok := d.Entitlements[entitlement.Id]; !ok {
	// 	d.Entitlements[entitlement.Id] = false
	// }
}

func (d *EntitlementGraph) GetEntitlements() []string {
	var entitlements []string
	for _, node := range d.Nodes {
		for _, entitlement := range node.Entitlements {
			entitlements = append(entitlements, entitlement.Id)
		}
	}
	return entitlements
}

func (d *EntitlementGraph) MarkEdgeExpanded(sourceEntitlementID string, descendantEntitlementID string) {
	srcNode := d.GetNode(sourceEntitlementID)
	if srcNode == nil {
		// TODO: panic?
		return
	}
	dstNode := d.GetNode(descendantEntitlementID)
	if dstNode == nil {
		// TODO: panic?
		return
	}
	_, ok := d.Edges[srcNode.Id][dstNode.Id]
	if !ok {
		return
	}

	d.Edges[srcNode.Id][dstNode.Id].Expanded = true
}

func (d *EntitlementGraph) GetNode(entitlementId string) *Node {
	// TODO: add an EntitlementToNode map for efficiency
	for _, node := range d.Nodes {
		for _, ent := range node.Entitlements {
			if ent.Id == entitlementId {
				return &node
			}
		}
	}
	return nil
}

func (d *EntitlementGraph) AddEdge(srcEntitlementID string, dstEntitlementID string, shallow bool, resourceTypeIDs []string) error {
	srcNode := d.GetNode(srcEntitlementID)
	if srcNode == nil {
		return ErrNoEntitlement
	}
	dstNode := d.GetNode(dstEntitlementID)
	if dstNode == nil {
		return ErrNoEntitlement
	}

	_, ok := d.Edges[srcNode.Id]
	if !ok {
		d.Edges[srcNode.Id] = make(map[int]*grantInfo)
	}

	_, ok = d.Edges[srcNode.Id][dstNode.Id]
	if !ok {
		d.Edges[srcNode.Id][dstNode.Id] = &grantInfo{
			Expanded:        false,
			Shallow:         shallow,
			ResourceTypeIDs: resourceTypeIDs,
		}
	} else {
		// TODO: just do nothing? it's probably a mistake if we're adding the same edge twice
		return fmt.Errorf("edge already exists")
	}
	d.FixCycles()
	return nil
}

func (d *EntitlementGraph) RemoveNode(nodeID int) {
	delete(d.Nodes, nodeID)
	delete(d.Edges, nodeID)
	for id := range d.Edges {
		delete(d.Edges[id], nodeID)
	}
}

// Merge all the nodes in a cycle
func (d *EntitlementGraph) FixCycle(cycle []int) {
	if len(cycle) == 0 {
		return
	}
	firstNode := d.Nodes[cycle[0]]
	firstEdges := d.Edges[cycle[0]]
	for _, nodeID := range cycle[1:] {
		edges := d.Edges[nodeID]
		for dstNodeID, edgeInfo := range edges {
			if dstNodeID == firstNode.Id {
				continue
			}
			dstNode := d.Nodes[dstNodeID]
			// Put node's entitlements on first node
			firstNode.Entitlements = append(firstNode.Entitlements, dstNode.Entitlements...)
			// Set outgoing edges on first node
			for id := range d.Edges[dstNode.Id] {
				d.Edges[firstNode.Id][id] = d.Edges[dstNode.Id][id]
			}
			// Set incoming edges on first node
			for node := range d.Node {
				d.GetDescendants(node)
				d.Edges[id][nodeID] = nil
			}
			// Delete node
			d.RemoveNode(dstNodeID)
		}
	}
}

func (d *EntitlementGraph) FixCycles() {
	cycles, hasCycles := d.GetCycles()
	if !hasCycles {
		return
	}
	for _, cycle := range cycles {
		d.FixCycle(cycle)
	}
}
