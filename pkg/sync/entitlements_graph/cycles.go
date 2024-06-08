package entitlements_graph

import (
	"fmt"
	"reflect"
)

// GetCycles finds the direct ancestors of the given entitlement. The 'all' flag
// returns all ancestors regardless of 'done' state.
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

func (g *EntitlementGraph) FixCycles() error {
	// If we can't fix the cycles in 10 tries, just give up
	const maxTries = 10
	prevCycleCount := 0
	for i := 0; i < maxTries; i++ {
		cycles, hasCycles := g.GetCycles()
		if !hasCycles {
			return nil
		}
		cycleCount := len(cycles)
		if cycleCount < prevCycleCount {
			// Reset the number of tries if we made progress
			i = 0
		}
		prevCycleCount = cycleCount

		// Merge all the nodes in a cycle.
		for i := 1; i < len(cycles[0]); i++ {
			g.mergeNodes(cycles[0][0], cycles[0][i])
		}
	}
	return fmt.Errorf("could not fix cycles after %v tries", maxTries)
}

// removeNode obliterates all references to a node in the graph.
func (g *EntitlementGraph) removeNode(nodeID int) {
	node, ok := g.Nodes[nodeID]
	if ok {
		for _, entitlementID := range node.EntitlementIDs {
			delete(g.EntitlementsToNodes, entitlementID)
		}
	}
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
