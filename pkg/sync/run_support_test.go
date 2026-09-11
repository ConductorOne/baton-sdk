package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"encoding/json"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/stretchr/testify/require"
)

// newTestRun returns the three run-scoped holders a syncer builds at the top
// of Sync.
func newTestRun() (*runState, *runStats, *expansionGraph) {
	return newRunState(), newRunStats(), newExpansionGraph()
}

// decodeTestRun decodes a checkpoint the way Sync does: run and stats from the
// token, and a fresh graph holder that adopts an inline graph if the token
// carried one.
func decodeTestRun(t *testing.T, token string) (*runState, *runStats, *expansionGraph) {
	t.Helper()
	parts, err := unmarshalToken(token)
	require.NoError(t, err)
	graph := newExpansionGraph()
	graph.restore(parts.graph)
	return parts.run, parts.stats, graph
}

// encodeTestRun encodes a checkpoint and fails the test if it cannot.
func encodeTestRun(t *testing.T, run *runState, stats *runStats) string {
	t.Helper()
	token, err := marshalToken(run, stats)
	require.NoError(t, err)
	return token
}

// marshalLegacyInlineGraphToken encodes run the way pre-omission SDKs did:
// graph inline, expansion page token kept. marshalToken cannot produce this
// shape any more, so the reader's compatibility with it is only testable
// against a hand-built token.
func marshalLegacyInlineGraphToken(t *testing.T, run *runState, graph *expand.EntitlementGraph) string {
	t.Helper()
	legacy, err := json.Marshal(serializedTokenV1{
		ActionsMap:       run.actions,
		ActionOrder:      run.actionOrder,
		CurrentActionID:  run.currentActionID,
		NeedsExpansion:   run.facts.has(factNeedsExpansion),
		EntitlementGraph: graph,
		Version:          StateTokenVersion,
	})
	require.NoError(t, err)
	return string(legacy)
}
