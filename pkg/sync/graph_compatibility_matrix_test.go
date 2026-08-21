package sync //nolint:revive,nolintlint // package name kept for compatibility

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEntitlementGraphTokenCompatibilityMatrix records every token-side
// compatibility disposition. Pebble sidecar cells are exercised by
// TestGraphFromStore and synccompactor's TestCompactorGraphCompatibilityHealing.
func TestEntitlementGraphTokenCompatibilityMatrix(t *testing.T) {
	ctx := context.Background()
	// Opt in to the legacy inline-graph token shape. Current checkpoints omit
	// graphs by default, while readers remain compatible with older tokens.
	stateWithGraph := newState(withCheckpointEntitlementGraph(true))
	graph := stateWithGraph.EntitlementGraph(ctx)
	graph.AddEntitlementID("a")
	graph.Loaded = true
	graph.MarkExpansionComplete()
	legacyToken, err := stateWithGraph.Marshal()
	require.NoError(t, err)

	emptyState := newState()
	emptyToken, err := emptyState.Marshal()
	require.NoError(t, err)

	tests := []struct {
		name        string
		token       string
		prepare     bool
		wantGraph   bool
		disposition string
	}{
		{
			name: "legacy graph in final token", token: legacyToken, wantGraph: true,
			disposition: "legacy/SQLite reader may load graph from token",
		},
		{
			name: "final token without graph", token: emptyToken,
			disposition: "missing graph selects full expansion",
		},
		{
			name: "replay clears legacy graph", token: legacyToken, prepare: true,
			disposition: "replay rebuilds graph and cannot silently no-op",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			token := tc.token
			if tc.prepare {
				token, err = PrepareExpansionReplayToken(token)
				require.NoError(t, err)
			}
			got, err := GraphFromToken(token)
			require.NoError(t, err)
			require.Equal(t, tc.wantGraph, got != nil, tc.disposition)
		})
	}
}

// Cross-version dispositions that require a pinned external binary:
//
//   - old writer, new reader: old artifact has no sidecar; new code full-expands
//     and heals by writing a current sidecar (executable coverage:
//     TestCompactorGraphCompatibilityHealing/old_pebble_without_sidecar).
//   - new writer, old reader: the sidecar is an unknown engine-meta key. The old
//     reader's grant data remains readable; it cannot opt into this new feature.
//     This repository does not promise that an old SDK can perform incremental
//     expansion on a new artifact.
//   - SQLite old/new: graph-in-token remains readable, but compaction explicitly
//     selects full expansion; SQLite-to-Pebble conversion heals and the next
//     generation reuses the sidecar (executable coverage:
//     TestCompactorGraphCompatibilityHealing/sqlite_converted_to_pebble).
