package expand

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/sync/expand/scc"
	mapset "github.com/deckarep/golang-set/v2"
)

// GetFirstCycle given an entitlements graph, return a cycle by node ID if it
// exists. Returns nil if no cycle exists. If there is a single
// node pointing to itself, that will count as a cycle.
func (g *EntitlementGraph) GetFirstCycle(ctx context.Context) []int {
	if g.HasNoCycles {
		return nil
	}
	comps, _ := g.ComputeCyclicComponents(ctx)
	if len(comps) == 0 {
		return nil
	}
	return comps[0]
}

// HasCycles returns true if the graph contains any cycle.
func (g *EntitlementGraph) HasCycles(ctx context.Context) bool {
	if g.HasNoCycles {
		return false
	}
	comps, _ := g.ComputeCyclicComponents(ctx)
	return len(comps) > 0
}

func (g *EntitlementGraph) cycleDetectionHelper(
	ctx context.Context,
	nodeID int,
) ([]int, bool) {
	reach := g.reachableFrom(nodeID)
	if len(reach) == 0 {
		return nil, false
	}
	fg := filteredGraph{g: g, include: func(id int) bool { _, ok := reach[id]; return ok }}
	groups, _ := scc.CondenseFWBW(ctx, fg, scc.DefaultOptions())
	for _, comp := range groups {
		if len(comp) > 1 || (len(comp) == 1 && g.hasSelfLoop(comp[0])) {
			return comp, true
		}
	}
	return nil, false
}

func (g *EntitlementGraph) FixCycles(ctx context.Context) error {
	comps, _ := g.ComputeCyclicComponents(ctx)
	return g.FixCyclesFromComponents(ctx, comps)
}

// ComputeCyclicComponents runs SCC once and returns only cyclic components.
// A component is cyclic if len>1 or a singleton with a self-loop.
func (g *EntitlementGraph) ComputeCyclicComponents(ctx context.Context) ([][]int, *scc.Metrics) {
	if g.HasNoCycles {
		return nil, nil
	}
	groups, metrics := scc.CondenseFWBW(ctx, g, scc.DefaultOptions())
	cyclic := make([][]int, 0)
	for _, comp := range groups {
		if len(comp) > 1 || (len(comp) == 1 && g.hasSelfLoop(comp[0])) {
			cyclic = append(cyclic, comp)
		}
	}
	return cyclic, metrics
}

// hasSelfLoop reports whether a node has a self-edge.
func (g *EntitlementGraph) hasSelfLoop(id int) bool {
	if row, ok := g.SourcesToDestinations[id]; ok {
		_, ok := row[id]
		return ok
	}
	return false
}

// filteredGraph restricts EntitlementGraph iteration to nodes for which include(id) is true.
type filteredGraph struct {
	g       *EntitlementGraph
	include func(int) bool
}

func (fg filteredGraph) ForEachNode(fn func(id int) bool) {
	for id := range fg.g.Nodes {
		if fg.include != nil && !fg.include(id) {
			continue
		}
		if !fn(id) {
			return
		}
	}
}

func (fg filteredGraph) ForEachEdgeFrom(src int, fn func(dst int) bool) {
	if fg.include != nil && !fg.include(src) {
		return
	}
	if dsts, ok := fg.g.SourcesToDestinations[src]; ok {
		for dst := range dsts {
			if fg.include != nil && !fg.include(dst) {
				continue
			}
			if !fn(dst) {
				return
			}
		}
	}
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
	delete(g.DestinationsToSources, nodeID)
}

// FixCyclesFromComponents merges all provided cyclic components in one pass.
func (g *EntitlementGraph) FixCyclesFromComponents(ctx context.Context, cyclic [][]int) error {
	if g.HasNoCycles {
		return nil
	}
	if len(cyclic) == 0 {
		g.HasNoCycles = true
		return nil
	}
	for _, comp := range cyclic {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := g.fixCycle(comp); err != nil {
			return err
		}
	}
	g.HasNoCycles = true
	return nil
}

// fixCycle takes a list of Node IDs that form a cycle and merges them into a
// single, new node.
func (g *EntitlementGraph) fixCycle(nodeIDs []int) error {
	entitlementIDs := mapset.NewThreadUnsafeSet[string]()
	outgoing := map[int]*mergedEdgeSpec{}
	incoming := map[int]*mergedEdgeSpec{}
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
						mergeEdgeSpec(incoming, sourceNodeID, edge)
					}
				}
			}

			// Gather all outgoing edges.
			if destinations, ok := g.SourcesToDestinations[nodeID]; ok {
				for destinationNodeID, edgeID := range destinations {
					if edge, ok := g.Edges[edgeID]; ok {
						mergeEdgeSpec(outgoing, destinationNodeID, edge)
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
	for destinationID, spec := range outgoing {
		g.NextEdgeID++
		edge := Edge{
			EdgeID:          g.NextEdgeID,
			SourceID:        node.Id,
			DestinationID:   destinationID,
			IsExpanded:      false,
			IsShallow:       spec.allShallow,
			ResourceTypeIDs: spec.resourceTypeIDs(),
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
	for sourceID, spec := range incoming {
		g.NextEdgeID++
		edge := Edge{
			EdgeID:          g.NextEdgeID,
			SourceID:        sourceID,
			DestinationID:   node.Id,
			IsExpanded:      false,
			IsShallow:       spec.allShallow,
			ResourceTypeIDs: spec.resourceTypeIDs(),
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

// mergedEdgeSpec accumulates the attributes of the parallel edges one
// neighbor node carries into/out of a cycle being collapsed, with the
// widest-grant union semantics:
//
//   - resource-type filters: an EMPTY filter means "match every principal
//     type", so any unfiltered edge makes the merged edge unfiltered. Only
//     when every edge is filtered is the union of their sets a faithful
//     merge — set-unioning across an unfiltered edge would NARROW it.
//   - shallow: a deep edge admits strictly more than a shallow one, so any
//     deep edge makes the merged edge deep; only when every edge is
//     shallow does the merged edge stay shallow.
type mergedEdgeSpec struct {
	unfiltered bool
	rtids      mapset.Set[string]
	allShallow bool
}

func mergeEdgeSpec(into map[int]*mergedEdgeSpec, neighborID int, edge Edge) {
	spec, ok := into[neighborID]
	if !ok {
		spec = &mergedEdgeSpec{rtids: mapset.NewThreadUnsafeSet[string](), allShallow: true}
		into[neighborID] = spec
	}
	if len(edge.ResourceTypeIDs) == 0 {
		spec.unfiltered = true
	} else {
		for _, rtid := range edge.ResourceTypeIDs {
			spec.rtids.Add(rtid)
		}
	}
	if !edge.IsShallow {
		spec.allShallow = false
	}
}

func (s *mergedEdgeSpec) resourceTypeIDs() []string {
	if s.unfiltered {
		return nil
	}
	return s.rtids.ToSlice()
}
