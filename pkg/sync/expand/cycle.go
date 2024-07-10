package expand

import (
	mapset "github.com/deckarep/golang-set/v2"
)

// GetFirstCycle given an entitlements graph, return a cycle by node ID if it
// exists. Returns nil if no cycle exists. If there is a single
// node pointing to itself, that will count as a cycle.
func (g *EntitlementGraph) GetFirstCycle() []int {
	visited := mapset.NewSet[int]()
	for nodeID := range g.Nodes {
		cycle, hasCycle := g.cycleDetectionHelper(nodeID, visited, []int{})
		if hasCycle {
			return cycle
		}
	}

	return nil
}

func (g *EntitlementGraph) cycleDetectionHelper(
	nodeID int,
	visited mapset.Set[int],
	currentCycle []int,
) ([]int, bool) {
	visited.Add(nodeID)
	if destinations, ok := g.SourcesToDestinations[nodeID]; ok {
		for destinationID := range destinations {
			nextCycle := make([]int, len(currentCycle))
			copy(nextCycle, currentCycle)
			nextCycle = append(nextCycle, nodeID)

			if !visited.Contains(destinationID) {
				if cycle, hasCycle := g.cycleDetectionHelper(destinationID, visited, nextCycle); hasCycle {
					return cycle, true
				}
			} else {
				// Make sure to not include part of the start before the cycle.
				outputCycle := make([]int, 0)
				for i := len(nextCycle) - 1; i >= 0; i-- {
					outputCycle = append(outputCycle, nextCycle[i])
					if nextCycle[i] == destinationID {
						return outputCycle, true
					}
				}
			}
		}
	}
	return nil, false
}

// removeNode obliterates a node and all incoming/outgoing edges.
func (g *EntitlementGraph) removeNode(nodeID int) {
	// Delete from reverse mapping.
	if node, ok := g.Nodes[nodeID]; ok {
		for _, entitlementID := range node.EntitlementIDs {
			entNodeId, ok := g.EntitlementsToNodes[entitlementID]
			if ok && entNodeId == nodeID {
				delete(g.EntitlementsToNodes, entitlementID)
			}
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
	cycle := g.GetFirstCycle()
	if cycle == nil {
		return nil
	}

	if err := g.fixCycle(cycle); err != nil {
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
