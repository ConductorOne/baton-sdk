package expand

import (
	mapset "github.com/deckarep/golang-set/v2"
)

const (
	colorWhite uint8 = iota
	colorGray
	colorBlack
)

// cycleDetector encapsulates coloring state for cycle detection on an
// EntitlementGraph. Node IDs are dense (1..NextNodeID), so slices are used for
// O(1) access and zero per-op allocations.
type cycleDetector struct {
	g      *EntitlementGraph
	state  []uint8
	parent []int
}

func newCycleDetector(g *EntitlementGraph) *cycleDetector {
	cd := &cycleDetector{
		g:      g,
		state:  make([]uint8, g.NextNodeID+1),
		parent: make([]int, g.NextNodeID+1),
	}
	for i := range cd.parent {
		cd.parent[i] = -1
	}
	return cd
}

// dfs performs a coloring-based DFS from u, returning the first detected cycle
// as a slice of node IDs or nil if no cycle is reachable from u.
func (cd *cycleDetector) dfs(u int) ([]int, bool) {
	// Self-loop fast path.
	if nbrs, ok := cd.g.SourcesToDestinations[u]; ok {
		if _, ok := nbrs[u]; ok {
			return []int{u}, true
		}
	}

	cd.state[u] = colorGray
	if nbrs, ok := cd.g.SourcesToDestinations[u]; ok {
		for v := range nbrs {
			switch cd.state[v] {
			case colorWhite:
				cd.parent[v] = u
				if cyc, ok := cd.dfs(v); ok {
					return cyc, true
				}
			case colorGray:
				// Back-edge to a node on the current recursion stack.
				// Reconstruct cycle by walking parents from u back to v (inclusive), then reverse.
				cycle := make([]int, 0, 8)
				for x := u; ; x = cd.parent[x] {
					cycle = append(cycle, x)
					if x == v || cd.parent[x] == -1 {
						break
					}
				}
				for i, j := 0, len(cycle)-1; i < j; i, j = i+1, j-1 {
					cycle[i], cycle[j] = cycle[j], cycle[i]
				}
				return cycle, true
			}
		}
	}
	cd.state[u] = colorBlack
	return nil, false
}

// FindAny scans all nodes and returns the first detected cycle or nil if none exist.
func (cd *cycleDetector) FindAny() []int {
	for nodeID := range cd.g.Nodes {
		if cd.state[nodeID] != colorWhite {
			continue
		}
		if cyc, ok := cd.dfs(nodeID); ok {
			return cyc
		}
	}
	return nil
}

// FindFrom starts cycle detection from a specific node and returns the first
// cycle reachable from that node, or nil,false if none.
func (cd *cycleDetector) FindFrom(start int) ([]int, bool) {
	return cd.dfs(start)
}

// GetFirstCycle given an entitlements graph, return a cycle by node ID if it
// exists. Returns nil if no cycle exists. If there is a single
// node pointing to itself, that will count as a cycle.
func (g *EntitlementGraph) GetFirstCycle() []int {
	if g.HasNoCycles {
		return nil
	}
	cd := newCycleDetector(g)
	return cd.FindAny()
}

func (g *EntitlementGraph) cycleDetectionHelper(
	nodeID int,
) ([]int, bool) {
	// Thin wrapper around the coloring-based DFS, starting from a specific node.
	// The provided visited/currentCycle are ignored here; coloring provides the
	// necessary state for correctness and performance.
	cd := newCycleDetector(g)
	return cd.FindFrom(nodeID)
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
	if g.HasNoCycles {
		return nil
	}
	cycle := g.GetFirstCycle()
	if cycle == nil {
		g.HasNoCycles = true
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
