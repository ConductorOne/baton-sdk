package dotc1z

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// ranking is full > no_grants > resources_only > partial
func TestUnionSyncTypes(t *testing.T) {
	full := connectorstore.SyncTypeFull
	ng := connectorstore.SyncTypeNoGrants
	ro := connectorstore.SyncTypeResourcesOnly
	partial := connectorstore.SyncTypePartial

	tests := []struct {
		name string
		a, b connectorstore.SyncType
		want connectorstore.SyncType
	}{
		{"full beats no_grants", full, ng, full},
		{"no_grants beats resources_only", ng, ro, ng},
		{"no_grants beats partial", ng, partial, ng},
		{"no_grants with no_grants", ng, ng, ng},
		{"resources_only beats partial (unchanged)", ro, partial, ro},
		{"partial with partial (unchanged)", partial, partial, partial},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unionSyncTypes(tt.a, tt.b); got != tt.want {
				t.Errorf("unionSyncTypes(%q, %q) = %q, want %q", tt.a, tt.b, got, tt.want)
			}
			// Union must be order-independent.
			if got := unionSyncTypes(tt.b, tt.a); got != tt.want {
				t.Errorf("unionSyncTypes(%q, %q) [swapped] = %q, want %q", tt.b, tt.a, got, tt.want)
			}
		})
	}
}

// no_grants should prune like a partial, not stick around like a full sync
func TestSelectSyncsToDelete_NoGrantsIsPartial(t *testing.T) {
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("ng_old", connectorstore.SyncTypeNoGrants, 5, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("ng_recent", connectorstore.SyncTypeNoGrants, 15, true),
		makeCandidate("f3", connectorstore.SyncTypeFull, 20, true),
	}
	// limit 2 drops f1; kept full sync is f2 (t=10), so ng_old goes, ng_recent stays
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"f1", "ng_old"})
}
