package expand

import (
	"reflect"

	mapset "github.com/deckarep/golang-set/v2"
)

// GetCycles given an entitlements graph, get a list of every contained cycle.
func (g *EntitlementGraph) GetCycles() ([][]int, bool) {
	rv := make([][]int, 0)
	for nodeID := range g.Nodes {
		edges, ok := g.SourcesToDestinations[nodeID]
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
	for descendantId := range g.SourcesToDestinations[nodeId] {
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

// removeNode obliterates a node and all incoming/outgoing edges.
func (g *EntitlementGraph) removeNode(nodeID int) {
	// Delete from reverse mapping.
	if node, ok := g.Nodes[nodeID]; ok {
		for _, entitlementID := range node.EntitlementIDs {
			delete(g.EntitlementsToNodes, entitlementID)
		}
	}

	// Delete from nodes list.
	delete(g.Nodes, nodeID)

	// Delete all outgoing edges.
	if destinations, ok := g.SourcesToDestinations[nodeID]; ok {
		for destinationID, edgeID := range destinations {
			delete(g.DestinationsToSources[destinationID], nodeID)
			delete(g.Edges, edgeID)
		}
	}
	delete(g.SourcesToDestinations, nodeID)

	// Delete all incoming edges.
	if sources, ok := g.DestinationsToSources[nodeID]; ok {
		for sourceID, edgeID := range sources {
			delete(g.SourcesToDestinations[sourceID], nodeID)
			delete(g.Edges, edgeID)
		}
	}
	delete(g.SourcesToDestinations, nodeID)
}

// FixCycles if any cycles of nodes exist, merge all nodes in that cycle into a
// single node and then repeat. Iteration ends when there are no more cycles.
func (g *EntitlementGraph) FixCycles() error {
	cycles, hasCycles := g.GetCycles()
	if !hasCycles {
		return nil
	}

	// After fixing the cycle, all other cycles become invalid.
	largestCycleLength, largestCycleIndex := -1, -1
	for index, nodeIDs := range cycles {
		newLength := len(nodeIDs)
		if newLength > largestCycleLength {
			largestCycleLength = newLength
			largestCycleIndex = index
		}
	}
	if err := g.fixCycle(cycles[largestCycleIndex]); err != nil {
		return err
	}

	// Recurse!
	return g.FixCycles()
}

// fixCycle takes a list of Node IDs that form a cycle and merges them into a
// single, new node.
func (g *EntitlementGraph) fixCycle(nodeIDs []int) error {
	entitlementIDs := mapset.NewSet[string]()
	outgoingEdgesToResourceTypeIDs := map[int]mapset.Set[string]{}
	incomingEdgesToResourceTypeIDs := map[int]mapset.Set[string]{}
	for _, nodeID := range nodeIDs {
		if node, ok := g.Nodes[nodeID]; ok {
			// Gather entitlements.
			for _, entitlementID := range node.EntitlementIDs {
				entitlementIDs.Add(entitlementID)
			}

			// Gather all incoming edges.
			if sources, ok := g.DestinationsToSources[nodeID]; ok {
				for sourceNodeID, edgeID := range sources {
					if edge, ok := g.Edges[edgeID]; ok {
						resourceTypeIDs, ok := incomingEdgesToResourceTypeIDs[sourceNodeID]
						if !ok {
							resourceTypeIDs = mapset.NewSet[string]()
						}
						for _, resourceTypeID := range edge.ResourceTypeIDs {
							resourceTypeIDs.Add(resourceTypeID)
						}
						incomingEdgesToResourceTypeIDs[sourceNodeID] = resourceTypeIDs
					}
				}
			}

			// Gather all outgoing edges.
			if destinations, ok := g.SourcesToDestinations[nodeID]; ok {
				for destinationNodeID, edgeID := range destinations {
					if edge, ok := g.Edges[edgeID]; ok {
						resourceTypeIDs, ok := outgoingEdgesToResourceTypeIDs[destinationNodeID]
						if !ok {
							resourceTypeIDs = mapset.NewSet[string]()
						}
						for _, resourceTypeID := range edge.ResourceTypeIDs {
							resourceTypeIDs.Add(resourceTypeID)
						}
						outgoingEdgesToResourceTypeIDs[destinationNodeID] = resourceTypeIDs
					}
				}
			}
		}
	}

	// Create a new node with the entitlements.
	g.NextNodeID++
	node := Node{
		Id:             g.NextNodeID,
		EntitlementIDs: entitlementIDs.ToSlice(),
	}
	g.Nodes[node.Id] = node
	for entitlementID := range entitlementIDs.Iter() {
		// Break the old connections and point to this node.
		g.EntitlementsToNodes[entitlementID] = node.Id
	}

	// Hook up edges
	for destinationID, resourceTypeIDs := range outgoingEdgesToResourceTypeIDs {
		g.NextEdgeID++
		edge := Edge{
			EdgeID:          g.NextEdgeID,
			SourceID:        node.Id,
			DestinationID:   destinationID,
			IsExpanded:      false,
			IsShallow:       false,
			ResourceTypeIDs: resourceTypeIDs.ToSlice(),
		}
		g.Edges[edge.EdgeID] = edge
		if _, ok := g.SourcesToDestinations[node.Id]; !ok {
			g.SourcesToDestinations[node.Id] = make(map[int]int)
		}
		g.SourcesToDestinations[node.Id][destinationID] = edge.EdgeID
		if _, ok := g.DestinationsToSources[destinationID]; !ok {
			g.DestinationsToSources[destinationID] = make(map[int]int)
		}
		g.DestinationsToSources[destinationID][node.Id] = edge.EdgeID
	}
	for sourceID, resourceTypeIDs := range incomingEdgesToResourceTypeIDs {
		g.NextEdgeID++
		edge := Edge{
			EdgeID:          g.NextEdgeID,
			SourceID:        sourceID,
			DestinationID:   node.Id,
			IsExpanded:      false,
			IsShallow:       false,
			ResourceTypeIDs: resourceTypeIDs.ToSlice(),
		}
		g.Edges[edge.EdgeID] = edge

		if _, ok := g.SourcesToDestinations[sourceID]; !ok {
			g.SourcesToDestinations[sourceID] = make(map[int]int)
		}
		g.SourcesToDestinations[sourceID][node.Id] = edge.EdgeID

		if _, ok := g.DestinationsToSources[node.Id]; !ok {
			g.DestinationsToSources[node.Id] = make(map[int]int)
		}
		g.DestinationsToSources[node.Id][sourceID] = edge.EdgeID
	}

	// Call delete to delete the node and every associated edge. This will
	// conveniently delete all edges that were internal to the cycle.
	for _, nodeID := range nodeIDs {
		g.removeNode(nodeID)
	}

	return nil
}
