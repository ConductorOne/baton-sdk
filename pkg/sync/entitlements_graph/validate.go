package entitlements_graph

import "fmt"

// validateEdges validates that for every edge: both nodes actually exists in Nodes
// and that `GrantInfo` exists.
func (g *EntitlementGraph) validateEdges() error {
	for srcNodeId, dstNodeIDs := range g.Edges {
		_, ok := g.Nodes[srcNodeId]
		if !ok {
			return ErrNoEntitlement
		}
		for dstNodeId, grantInfo := range dstNodeIDs {
			_, ok := g.Nodes[dstNodeId]
			if !ok {
				return ErrNoEntitlement
			}
			if grantInfo == nil {
				return fmt.Errorf("nil edge info. dst node %v", dstNodeId)
			}
		}
	}
	return nil
}

// validateNodes validates that each node has at least one `entitlementID` and
// that each `entitlementID` only appears once in the graph.
func (g *EntitlementGraph) validateNodes() error {
	// check for entitlement ids that are in multiple nodes
	seenEntitlements := make(map[string]int)
	for nodeID, node := range g.Nodes {
		if len(node.EntitlementIDs) == 0 {
			return fmt.Errorf("empty node %v", nodeID)
		}
		for _, entID := range node.EntitlementIDs {
			if _, ok := seenEntitlements[entID]; ok {
				return fmt.Errorf("entitlement %v is in multiple nodes: %v %v", entID, nodeID, seenEntitlements[entID])
			}
			seenEntitlements[entID] = nodeID
		}
	}
	return nil
}

// Validate checks every node and edge and returns an error if the graph is not valid.
func (g *EntitlementGraph) Validate() error {
	err := g.validateEdges()
	if err != nil {
		return err
	}
	err = g.validateNodes()
	if err != nil {
		return err
	}
	return nil
}
