package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestGrantStatsGroupByParity is a regression check for GrantStats's
// switch from a per-resource-type COUNT loop to a single GROUP BY.
//
// It compares the production GrantStats result (now backed by
// countBySyncAndResourceType / GROUP BY) against a test-local copy of
// the prior per-type loop and asserts they produce identical maps under
// realistic data shapes:
//
//   - single sync (fresh c1z)
//   - 2 historical syncs with full external_id overlap (production
//     steady state — c1zs hold "previous" + "current" sync, and each
//     re-sync emits the same external_ids with a new sync_id)
//
// Wall-clock timings are logged so any future regression in the COUNT
// path is visible. The two implementations should match exactly in
// every cell of the result map.
//
// Run with:
//
//	go test -run TestGrantStatsGroupByParity -v ./pkg/dotc1z/...
func TestGrantStatsGroupByParity(t *testing.T) {
	cases := []struct {
		name             string
		numResourceTypes int
		grantsPerSync    int
		numSyncs         int
		// overlap = true: every sync re-emits the same external_ids
		// (each just with a new sync_id). Mirrors a real re-synced c1z.
		overlap bool
	}{
		{name: "single-sync/100k/50-types", numResourceTypes: 50, grantsPerSync: 100_000, numSyncs: 1, overlap: false},
		{name: "two-syncs-overlap/100k-each/50-types", numResourceTypes: 50, grantsPerSync: 100_000, numSyncs: 2, overlap: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runGrantStatsParityCase(t, tc.numResourceTypes, tc.grantsPerSync, tc.numSyncs, tc.overlap)
		})
	}
}

func runGrantStatsParityCase(t *testing.T, numResourceTypes, grantsPerSync, numSyncs int, overlap bool) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "grant-stats-parity-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	testFilePath := filepath.Join(tempDir, "parity.c1z")
	f, err := NewC1ZFile(ctx, testFilePath,
		WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close(ctx) })

	resourceTypes := make([]*v2.ResourceType, numResourceTypes)
	entitlements := make([]*v2.Entitlement, numResourceTypes)

	var measuredSyncID string
	for s := 0; s < numSyncs; s++ {
		syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		for i := 0; i < numResourceTypes; i++ {
			rtID := fmt.Sprintf("rt-%02d", i)
			if s == 0 {
				resourceTypes[i] = &v2.ResourceType{Id: rtID, DisplayName: rtID}
			}
			require.NoError(t, f.PutResourceTypes(ctx, resourceTypes[i]))

			res := &v2.Resource{
				Id:          &v2.ResourceId{ResourceType: rtID, Resource: "r"},
				DisplayName: "r",
			}
			require.NoError(t, f.PutResources(ctx, res))

			ent := &v2.Entitlement{
				Id:       fmt.Sprintf("ent-%s", rtID),
				Resource: res,
			}
			if s == 0 {
				entitlements[i] = ent
			}
			require.NoError(t, f.PutEntitlements(ctx, ent))
		}

		const batchSize = 1000
		batch := make([]*v2.Grant, 0, batchSize)
		for i := 0; i < grantsPerSync; i++ {
			idx := i % numResourceTypes
			grantID := fmt.Sprintf("g-%d", i)
			principalID := fmt.Sprintf("p-%d", i)
			if !overlap {
				grantID = fmt.Sprintf("g-s%d-%d", s, i)
				principalID = fmt.Sprintf("p-s%d-%d", s, i)
			}
			batch = append(batch, &v2.Grant{
				Id:          grantID,
				Entitlement: entitlements[idx],
				Principal: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypes[idx].Id,
						Resource:     principalID,
					},
				},
			})
			if len(batch) == batchSize {
				require.NoError(t, f.PutGrants(ctx, batch...))
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			require.NoError(t, f.PutGrants(ctx, batch...))
		}

		require.NoError(t, f.EndSync(ctx))
		measuredSyncID = syncID
	}

	// Flush WAL + refresh planner stats so loop and GROUP BY are
	// measured against identical on-disk state.
	_, err = f.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)
	_, err = f.db.ExecContext(ctx, "ANALYZE")
	require.NoError(t, err)

	// Old behavior: per-resource-type COUNT loop, kept here as a
	// reference implementation so any future regression in GrantStats
	// (the GROUP BY path) is caught.
	loopStart := time.Now()
	loopStats := perTypeLoopGrantStats(ctx, t, f, measuredSyncID, resourceTypes)
	loopElapsed := time.Since(loopStart)

	// New behavior: production GrantStats, now using countBySyncAndResourceType.
	gbStart := time.Now()
	gbStats, err := f.grantStats(ctx, connectorstore.SyncTypeAny, measuredSyncID)
	require.NoError(t, err)
	gbElapsed := time.Since(gbStart)

	require.Equal(t, loopStats, gbStats,
		"per-type loop and GROUP BY must produce identical maps")

	totalGrants := grantsPerSync * numSyncs
	t.Logf("dataset: %d grants total (%d per sync × %d syncs), %d resource types, overlap=%t",
		totalGrants, grantsPerSync, numSyncs, numResourceTypes, overlap)
	t.Logf("counted-sync grants: %d", grantsPerSync)
	t.Logf("per-type loop (reference): %s  (%d queries)", loopElapsed, numResourceTypes)
	t.Logf("GROUP BY (production):     %s  (1 query)", gbElapsed)
	if gbElapsed > 0 {
		t.Logf("speedup: %.2fx", float64(loopElapsed)/float64(gbElapsed))
	}
}

// perTypeLoopGrantStats is a literal test-local copy of the pre-rewrite
// GrantStats body — one COUNT(*) per resource type. It exists solely so
// the parity test has a fixed reference to compare against; production
// code now uses a single GROUP BY via countBySyncAndResourceType.
func perTypeLoopGrantStats(
	ctx context.Context,
	t *testing.T,
	f *C1File,
	syncID string,
	resourceTypes []*v2.ResourceType,
) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(resourceTypes))
	for _, rt := range resourceTypes {
		n, err := f.db.From(grants.Name()).
			Where(goqu.C("sync_id").Eq(syncID)).
			Where(goqu.C("resource_type_id").Eq(rt.Id)).
			CountContext(ctx)
		require.NoError(t, err)
		out[rt.Id] = n
	}
	return out
}
