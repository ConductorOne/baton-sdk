package dotc1z

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

const (
	diffUpsertGrantCount = 500_000
	diffUpsertMaxTime    = 2 * time.Minute
)

// TestRegression_DiffUpsertIfNewerPerformance measures the throughput of the
// IfNewer upsert path, which uses a more complex ON CONFLICT clause with a
// WHERE predicate comparing discovered_at timestamps. This is the hot path
// during incremental / diff syncs.
//
// The test inserts a baseline grant set, then re-upserts the same set with
// IfNewer mode. A correctness guard verifies that stale rows are not updated.
func TestRegression_DiffUpsertIfNewerPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping regression test in short mode")
	}

	ctx := t.Context()

	tmpDir, err := os.MkdirTemp("", "regression-diff-upsert-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	c1zPath := filepath.Join(tmpDir, "diff.c1z")

	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer c1f.Close(ctx)

	_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group 1",
	}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent1))

	makeGrants := func(displayPrefix string) []*v2.Grant {
		out := make([]*v2.Grant, diffUpsertGrantCount)
		for i := range out {
			out[i] = v2.Grant_builder{
				Id:          fmt.Sprintf("grant-%d", i),
				Entitlement: ent1,
				Principal: v2.Resource_builder{
					Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
					DisplayName: fmt.Sprintf("%s %d", displayPrefix, i),
				}.Build(),
			}.Build()
		}
		return out
	}

	// Phase 1: seed baseline.
	baseline := makeGrants("baseline")
	const batchSize = 1000
	for i := 0; i < len(baseline); i += batchSize {
		end := min(i+batchSize, len(baseline))
		require.NoError(t, c1f.PutGrants(ctx, baseline[i:end]...))
	}

	// Phase 2: upsert the same volume with IfNewer. Since time.Now() advances,
	// every row will be "newer" and the UPDATE fires for each row.
	updated := makeGrants("updated")
	start := time.Now()
	for i := 0; i < len(updated); i += batchSize {
		end := min(i+batchSize, len(updated))
		require.NoError(t, c1f.PutGrantsIfNewer(ctx, updated[i:end]...))
	}
	elapsed := time.Since(start)

	t.Logf("IfNewer upsert of %d grants completed in %v", diffUpsertGrantCount, elapsed)
	require.LessOrEqual(t, elapsed, diffUpsertMaxTime,
		"IfNewer upsert took %v, limit is %v", elapsed, diffUpsertMaxTime)

	// Phase 3 — correctness: push discovered_at into the future so a subsequent
	// IfNewer upsert with time.Now() is stale and should be a no-op.
	_, err = c1f.db.ExecContext(ctx,
		"UPDATE "+grants.Name()+" SET discovered_at = datetime('now', '+1 day')")
	require.NoError(t, err)

	// Re-upsert with stale data. Since existing discovered_at is in the future,
	// the WHERE clause prevents any update.
	stale := makeGrants("stale")
	for i := 0; i < len(stale); i += batchSize {
		end := min(i+batchSize, len(stale))
		require.NoError(t, c1f.PutGrantsIfNewer(ctx, stale[i:end]...))
	}

	// Verify stale upsert was a no-op by checking that the data still contains
	// the "updated" prefix, not "stale".
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 50}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetList())
	for _, g := range resp.GetList() {
		dn := g.GetPrincipal().GetDisplayName()
		require.Contains(t, dn, "updated",
			"stale upsert should not have overwritten grant data, got DisplayName=%q", dn)
	}
}
