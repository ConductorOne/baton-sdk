package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestSecurity_InvalidVerificationClaimReadsUnverified pins how sync-run
// reads treat an ingest-invariant verification marker that is present but
// not valid for this SDK: a mode this SDK does not know (a newer SDK may
// add one), empty coverage, or coverage bytes that do not decode. The run
// reads as unverified, so no consumer trusts the claim, and the read
// succeeds, so an artifact from a newer SDK never fails ListSyncRuns or
// LatestFinishedSyncOfAnyType here.
func TestSecurity_InvalidVerificationClaimReadsUnverified(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name         string
		gen          string
		coverage     string
		mode         string
		wantVerified bool
	}{
		{name: "mode unknown to this SDK", gen: "gen-1", coverage: `["I1","I5"]`, mode: "a_future_mode"},
		{name: "empty coverage", gen: "gen-1", coverage: `[]`, mode: string(c1zstore.IngestInvariantVerificationModeConnector)},
		{name: "coverage bytes do not decode", gen: "gen-1", coverage: `{not json`, mode: string(c1zstore.IngestInvariantVerificationModeConnector)},
		{name: "absent marker", gen: "", coverage: ``, mode: ""},
		{name: "valid marker (control)", gen: "gen-1", coverage: `["I1","I5"]`, mode: string(c1zstore.IngestInvariantVerificationModeConnector), wantVerified: true},
	}

	t.Run("pebble", func(t *testing.T) {
		for _, tc := range cases {
			if tc.coverage == `{not json` {
				continue // pebble stores coverage as a repeated proto field, not JSON
			}
			t.Run(tc.name, func(t *testing.T) {
				store, err := NewStore(ctx, filepath.Join(t.TempDir(), "claim.c1z"), WithEngine(c1zstore.EnginePebble))
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close(ctx)) }()
				id, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.NoError(t, store.EndSync(ctx))

				ps, ok := store.(*pebbleStore)
				require.True(t, ok)
				rec, err := ps.SyncRuns().GetSyncRunRecord(ctx, id)
				require.NoError(t, err)
				var coverage []string
				if tc.coverage == `["I1","I5"]` {
					coverage = []string{"I1", "I5"}
				}
				rec.SetIngestInvariantGeneration(tc.gen)
				rec.SetIngestInvariantCoverage(coverage)
				rec.SetIngestInvariantMode(tc.mode)
				require.NoError(t, ps.SyncRuns().PutSyncRunRecord(ctx, rec))

				run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
				require.NoError(t, err)
				require.NotNil(t, run)
				require.Equal(t, tc.wantVerified, run.IsVerified())
				if !tc.wantVerified {
					require.Empty(t, run.Generation, "an unverified claim must not surface its generation")
				}
			})
		}
	})
}
