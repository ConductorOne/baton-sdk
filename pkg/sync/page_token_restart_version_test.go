package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPageTokenRestartVersionStamp(t *testing.T) {
	for _, tc := range []struct {
		name    string
		actions []Action
		version uint64
	}{
		{name: "plain", actions: []Action{{Op: SyncGrantsOp, PageToken: "opaque"}}, version: 1},
		{name: "type-scoped", actions: []Action{{TypeScoped: true}}, version: 2},
		{name: "spawned", actions: []Action{{Spawned: true}}, version: 2},
		{name: "planned", actions: []Action{{TypeScopedPlanned: true}}, version: 2},
		{name: "restarted", actions: []Action{{Op: SyncGrantsOp, PageTokenRestarted: true}}, version: 3},
		{name: "mixed", actions: []Action{{TypeScoped: true}, {PageTokenRestarted: true}}, version: 3},
		{name: "mixed-reversed", actions: []Action{{PageTokenRestarted: true}, {Spawned: true}}, version: 3},
	} {
		for _, omitGraph := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/graph-omission=%t", tc.name, omitGraph), func(t *testing.T) {
				run, stats, _ := newTestRun()
				for _, action := range tc.actions {
					if action.PageTokenRestarted {
						action.PageToken = encodeRestartPageToken(2, restartTestGeneration, "next")
					}
					run.pushAction(t.Context(), action)
				}
				if omitGraph {
					run.pushAction(t.Context(), Action{Op: SyncGrantExpansionOp, PageToken: "graph-page"})
				}
				encoded := encodeTestRun(t, run, stats)
				var raw serializedTokenV1
				require.NoError(t, json.Unmarshal([]byte(encoded), &raw))
				require.Equal(t, tc.version, raw.Version)
				resumed, resumedStats, _ := decodeTestRun(t, encoded)
				require.Equal(t, encoded, encodeTestRun(t, resumed, resumedStats))
				for _, action := range resumed.actions {
					if action.PageTokenRestarted {
						request, err := providerPageAction(&action)
						require.NoError(t, err)
						require.Equal(t, "next", request.PageToken)
						require.EqualValues(t, 2, request.pageTokenRestarts)
					}
				}
			})
		}
	}
}
