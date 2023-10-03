package sync

import (
	"context"
	"errors"
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

type edgeInfo struct {
	Expanded        bool     `json:"expanded"`
	Shallow         bool     `json:"shallow"`
	ResourceTypeIDs []string `json:"resource_type_ids"`
}

type EntitlementGraph struct {
	Entitlements map[string]bool                 `json:"entitlements"`
	Edges        map[string]map[string]*edgeInfo `json:"edges"`
	Loaded       bool                            `json:"loaded"`
	Depth        int                             `json:"depth"`
	Actions      []EntitlementGraphAction        `json:"actions"`
}

func NewEntitlementGraph(ctx context.Context) *EntitlementGraph {
	return &EntitlementGraph{
		Entitlements: make(map[string]bool),
		Edges:        make(map[string]map[string]*edgeInfo),
	}
}

// IsExpanded returns true if all entitlements in the graph have been expanded.
func (d *EntitlementGraph) IsExpanded() bool {
	for entilementID := range d.Entitlements {
		if !d.IsEntitlementExpanded(entilementID) {
			return false
		}
	}
	return true
}

// IsEntitlementExpanded returns true if all the outgoing edges for the given entitlement have been expanded.
func (d *EntitlementGraph) IsEntitlementExpanded(entitlementID string) bool {
	for _, edgeInfo := range d.Edges[entitlementID] {
		if !edgeInfo.Expanded {
			return false
		}
	}
	return true
}

// HasUnexpandedAncestors returns true if the given entitlement has ancestors that have not been expanded yet.
func (d *EntitlementGraph) HasUnexpandedAncestors(entitlementID string) bool {
	for _, ancestorID := range d.GetAncestors(entitlementID) {
		if !d.Entitlements[ancestorID] {
			return true
		}
	}
	return false
}

// Find the direct ancestors of the given entitlement. The 'all' flag returns all ancestors regardless of 'done' state.
func (d *EntitlementGraph) GetAncestors(entitlementID string) []string {
	_, ok := d.Entitlements[entitlementID]
	if !ok {
		return nil
	}

	ancestors := make([]string, 0)
	for src, dst := range d.Edges {
		if _, ok := dst[entitlementID]; ok {
			ancestors = append(ancestors, src)
		}
	}
	return ancestors
}

// Find the direct ancestors of the given entitlement. The 'all' flag returns all ancestors regardless of 'done' state.
func (d *EntitlementGraph) GetCycles() ([][]string, bool) {
	rv := make([][]string, 0)
	for entitlementID := range d.Entitlements {
		if len(d.GetDescendants(entitlementID)) == 0 {
			continue
		}
		cycle, isCycle := d.getCycle([]string{entitlementID})
		if isCycle && !isInCycle(cycle, rv) {
			rv = append(rv, cycle)
		}
	}

	return rv, len(rv) > 0
}

func isInCycle(newCycle []string, cycles [][]string) bool {
	for _, cycle := range cycles {
		if len(cycle) > 0 && reflect.DeepEqual(cycle, newCycle) {
			return true
		}
	}
	return false
}

func shift(arr []string, n int) []string {
	for i := 0; i < n; i++ {
		arr = append(arr[1:], arr[0])
	}
	return arr
}

func (d *EntitlementGraph) getCycle(visits []string) ([]string, bool) {
	entitlementID := visits[len(visits)-1]
	for descendantID := range d.GetDescendants(entitlementID) {
		tempVisits := make([]string, len(visits))
		copy(tempVisits, visits)
		if descendantID == visits[0] {
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
			if visit == descendantID {
				return nil, false
			}
		}

		tempVisits = append(tempVisits, descendantID)
		return d.getCycle(tempVisits)
	}
	return nil, false
}

func (d *EntitlementGraph) GetDescendants(entitlementID string) map[string]*edgeInfo {
	return d.Edges[entitlementID]
}

func (d *EntitlementGraph) HasEntitlement(entitlementID string) bool {
	_, ok := d.Entitlements[entitlementID]
	return ok
}

func (d *EntitlementGraph) AddEntitlement(entitlement *v2.Entitlement) {
	if _, ok := d.Entitlements[entitlement.Id]; !ok {
		d.Entitlements[entitlement.Id] = false
	}
}

func (d *EntitlementGraph) MarkEdgeExpanded(sourceEntitlementID string, descendantEntitlementID string) {
	_, ok := d.Edges[sourceEntitlementID]
	if !ok {
		return
	}
	_, ok = d.Edges[sourceEntitlementID][descendantEntitlementID]
	if !ok {
		return
	}

	d.Edges[sourceEntitlementID][descendantEntitlementID].Expanded = true

	// If all edges are expanded, mark the source entitlement as expanded.
	// We shouldn't care about this value but we'll set it for consistency.
	allExpanded := true
	for _, edgeInfo := range d.Edges[sourceEntitlementID] {
		if !edgeInfo.Expanded {
			allExpanded = false
			break
		}
	}
	if allExpanded {
		d.Entitlements[sourceEntitlementID] = true
	}
}

func (d *EntitlementGraph) AddEdge(srcEntitlementID string, dstEntitlementID string, shallow bool, resourceTypeIDs []string) error {
	if _, ok := d.Entitlements[srcEntitlementID]; !ok {
		return ErrNoEntitlement
	}
	if _, ok := d.Entitlements[dstEntitlementID]; !ok {
		return ErrNoEntitlement
	}

	_, ok := d.Edges[srcEntitlementID]
	if !ok {
		d.Edges[srcEntitlementID] = make(map[string]*edgeInfo)
	}
	d.Edges[srcEntitlementID][dstEntitlementID] = &edgeInfo{
		Expanded:        false,
		Shallow:         shallow,
		ResourceTypeIDs: resourceTypeIDs,
	}
	return nil
}
