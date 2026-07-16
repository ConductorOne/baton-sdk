package c1zstore

// UsableAsReplaySource is the metadata gate two product non-goals hang
// off of: SQLite artifacts and compacted artifacts must never serve
// source-cache replay. The predicate itself was previously only pinned
// indirectly (through syncer degrade tests); this is its direct truth
// table.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestUsableAsReplaySource(t *testing.T) {
	cases := []struct {
		name string
		run  *SyncRun
		want bool
	}{
		{"nil run", nil, false},
		{"full sync", &SyncRun{Type: connectorstore.SyncTypeFull}, true},
		{"full but compacted", &SyncRun{Type: connectorstore.SyncTypeFull, Compacted: true}, false},
		{"partial sync", &SyncRun{Type: connectorstore.SyncTypePartial}, false},
		{"resources-only sync", &SyncRun{Type: connectorstore.SyncTypeResourcesOnly}, false},
		{"empty type", &SyncRun{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.run.UsableAsReplaySource())
		})
	}
}
