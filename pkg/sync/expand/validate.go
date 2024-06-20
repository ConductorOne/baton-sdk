package expand

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNoEntitlement = errors.New("no entitlement found")
)

func (node *Node) Str() string {
	return fmt.Sprintf(
		"node %v entitlement IDs: %v",
		node.Id,
		node.EntitlementIDs,
	)
}

func (edge *Edge) Str() string {
	return fmt.Sprintf(
		"%v -> %v { expanded: %v, shallow: %v, resources: %v }",
		edge.SourceID,
		edge.DestinationID,
		edge.IsExpanded,
		edge.IsShallow,
		edge.ResourceTypeIDs,
	)
}

// Str lists every `node` line by line followed by every `edge`. Useful for debugging.
func (g *EntitlementGraph) Str() string {
	nodeHeader := ""
	edgeHeader := "edges:"
	nodesStrings := make([]string, 0, len(g.Nodes))
	edgeStrings := make([]string, 0, len(g.Edges))

	for id, node := range g.Nodes {
		nodesStrings = append(
			nodesStrings,
			node.Str(),
		)
		if destinationsMap, destinationOK := g.SourcesToDestinations[id]; destinationOK {
			for _, edgeID := range destinationsMap {
				if edge, edgeOK := g.Edges[edgeID]; edgeOK {
					edgeStrings = append(
						edgeStrings,
						edge.Str(),
					)
				}
			}
		}
	}

	return strings.Join(
		[]string{
			nodeHeader,
			strings.Join(nodesStrings, "\n"),
			edgeHeader,
			strings.Join(edgeStrings, "\n"),
		},
		"\n",
	)
}

// validateEdges validates that for every edge, both nodes actually exists.
func (g *EntitlementGraph) validateEdges() error {
	for _, edge := range g.Edges {
		if _, ok := g.Nodes[edge.SourceID]; !ok {
			return ErrNoEntitlement
		}
		if _, ok := g.Nodes[edge.DestinationID]; !ok {
			return ErrNoEntitlement
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
	if err := g.validateEdges(); err != nil {
		return err
	}
	if err := g.validateNodes(); err != nil {
		return err
	}
	return nil
}
