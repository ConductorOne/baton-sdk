package expand

// TransitiveDescendants stores precomputed transitive descendants for each entitlement.
// Key is entitlement ID, value is slice of all descendant entitlement IDs.
type TransitiveDescendants map[string][]string

// ComputeTransitiveDescendants computes all transitive descendants for each entitlement
// in the graph. This is O(N × (N + E)) time and O(N × avg_descendants) space.
func (g *EntitlementGraph) ComputeTransitiveDescendants() TransitiveDescendants {
	result := make(TransitiveDescendants, len(g.EntitlementsToNodes))

	// For each entitlement, compute reachable nodes
	for entitlementID, nodeID := range g.EntitlementsToNodes {
		reachable := g.reachableFrom(nodeID)

		// Convert node IDs to entitlement IDs, excluding the starting node
		var descendants []string
		for reachableNodeID := range reachable {
			if reachableNodeID == nodeID {
				continue // Don't include self
			}
			if node, ok := g.Nodes[reachableNodeID]; ok {
				descendants = append(descendants, node.EntitlementIDs...)
			}
		}
		result[entitlementID] = descendants
	}

	return result
}

// TopologicalOrder returns entitlement IDs in topological order (ancestors before descendants).
// Uses Kahn's algorithm for topological sorting.
func (g *EntitlementGraph) TopologicalOrder() []string {
	// Calculate in-degree for each node
	inDegree := make(map[int]int, len(g.Nodes))
	for nodeID := range g.Nodes {
		inDegree[nodeID] = 0
	}
	for _, edge := range g.Edges {
		inDegree[edge.DestinationID]++
	}

	// Find all nodes with no incoming edges (roots)
	queue := make([]int, 0)
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}

	// Process nodes in topological order
	var result []string
	for len(queue) > 0 {
		// Dequeue
		nodeID := queue[0]
		queue = queue[1:]

		// Add all entitlements from this node
		if node, ok := g.Nodes[nodeID]; ok {
			result = append(result, node.EntitlementIDs...)
		}

		// Reduce in-degree of descendants
		if destinations, ok := g.SourcesToDestinations[nodeID]; ok {
			for destID := range destinations {
				inDegree[destID]--
				if inDegree[destID] == 0 {
					queue = append(queue, destID)
				}
			}
		}
	}

	return result
}

// GetSourceEntitlements returns all entitlement IDs that are sources (have outgoing edges)
// to the given entitlement. These are the immediate predecessors.
func (g *EntitlementGraph) GetSourceEntitlements(entitlementID string) []string {
	node := g.GetNode(entitlementID)
	if node == nil {
		return nil
	}

	var sources []string
	if srcNodes, ok := g.DestinationsToSources[node.Id]; ok {
		for srcNodeID := range srcNodes {
			if srcNode, ok := g.Nodes[srcNodeID]; ok {
				sources = append(sources, srcNode.EntitlementIDs...)
			}
		}
	}
	return sources
}

