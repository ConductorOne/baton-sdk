package entitlements_graph

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
