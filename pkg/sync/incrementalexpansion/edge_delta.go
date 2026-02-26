package incrementalexpansion

import (
	"strings"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

type edge struct {
	srcEntitlementID string
	dstEntitlementID string
	shallow          bool
	// principalResourceTypeIDs is the filter applied when listing source grants to propagate.
	principalResourceTypeIDs []string
}

func (e edge) key() string {
	sep := "\x1f"
	shallow := "0"
	if e.shallow {
		shallow = "1"
	}
	return strings.Join([]string{
		e.srcEntitlementID,
		e.dstEntitlementID,
		shallow,
		strings.Join(e.principalResourceTypeIDs, sep),
	}, sep)
}

type edgeDelta struct {
	added   map[string]edge
	removed map[string]edge
}

// edgeDeltaFromExpandableGrants builds an edgeDelta from added/removed expandable grant definitions.
// The returned delta always has non-nil maps.
func edgeDeltaFromExpandableGrants(added, removed []*connectorstore.ExpandableGrantDef) *edgeDelta {
	return &edgeDelta{
		added:   edgeSetFromDefs(added),
		removed: edgeSetFromDefs(removed),
	}
}

func edgeSetFromDefs(defs []*connectorstore.ExpandableGrantDef) map[string]edge {
	out := make(map[string]edge, len(defs))
	for _, def := range defs {
		if def == nil {
			continue
		}
		for _, src := range def.SourceEntitlementIDs {
			e := edge{
				srcEntitlementID:         src,
				dstEntitlementID:         def.TargetEntitlementID,
				shallow:                  def.Shallow,
				principalResourceTypeIDs: def.ResourceTypeIDs,
			}
			out[e.key()] = e
		}
	}
	return out
}
