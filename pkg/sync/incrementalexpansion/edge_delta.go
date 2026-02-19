package incrementalexpansion

import (
	"strings"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Edge represents an expansion edge from a source entitlement to a descendant entitlement,
// with expansion constraints.
type Edge struct {
	SrcEntitlementID string
	DstEntitlementID string
	Shallow          bool
	// PrincipalResourceTypeIDs is the filter applied when listing source grants to propagate.
	PrincipalResourceTypeIDs []string
}

func (e Edge) Key() string {
	// Use an unlikely separator to avoid accidental collisions.
	sep := "\x1f"
	shallow := "0"
	if e.Shallow {
		shallow = "1"
	}
	return strings.Join([]string{
		e.SrcEntitlementID,
		e.DstEntitlementID,
		shallow,
		strings.Join(e.PrincipalResourceTypeIDs, sep),
	}, sep)
}

type EdgeDelta struct {
	Added   map[string]Edge
	Removed map[string]Edge
}

// EdgeDeltaFromExpandableGrants builds an EdgeDelta from pre-computed lists of added/removed
// expandable grant definitions (typically from cross-database queries on the attached file).
func EdgeDeltaFromExpandableGrants(added, removed []*connectorstore.ExpandableGrantDef) *EdgeDelta {
	return &EdgeDelta{
		Added:   edgeSetFromDefs(added),
		Removed: edgeSetFromDefs(removed),
	}
}

func edgeSetFromDefs(defs []*connectorstore.ExpandableGrantDef) map[string]Edge {
	out := make(map[string]Edge, len(defs))
	for _, def := range defs {
		if def == nil {
			continue
		}
		for _, src := range def.SourceEntitlementIDs {
			e := Edge{
				SrcEntitlementID:         src,
				DstEntitlementID:         def.TargetEntitlementID,
				Shallow:                  def.Shallow,
				PrincipalResourceTypeIDs: def.ResourceTypeIDs,
			}
			out[e.Key()] = e
		}
	}
	return out
}
