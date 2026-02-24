package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

const (
	migrationGrantCount  = 500_000
	migrationVerifyCount = 5_000
	migrationMaxOpen     = 3 * time.Minute
)

// TestRegression_MigrationPerformance creates a large c1z with data across all
// tables, closes it, then reopens it. The reopen runs the full InitTables path:
// schema creation, every table's Migrations(), WAL checkpoint, and pragma setup.
//
// The measured time catches regressions in any part of the c1z load pipeline.
// A correctness guard verifies data survives the round-trip across all tables.
func TestRegression_MigrationPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping regression test in short mode")
	}

	ctx := t.Context()

	tmpDir, err := os.MkdirTemp("", "regression-migration-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	c1zPath := filepath.Join(tmpDir, "migration.c1z")

	syncID := seedLargeC1Z(ctx, t, c1zPath, tmpDir, migrationGrantCount)

	// Reopen — runs full InitTables + migrations.
	start := time.Now()
	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	t.Logf("Open with %d grants completed in %v", migrationGrantCount, elapsed)
	require.LessOrEqual(t, elapsed, migrationMaxOpen,
		"open took %v, limit is %v", elapsed, migrationMaxOpen)

	// Correctness: verify data across all tables.
	require.NoError(t, c1f.ViewSync(ctx, syncID))

	var grantCount int
	err = c1f.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+grants.Name()+" WHERE sync_id=?", syncID,
	).Scan(&grantCount)
	require.NoError(t, err)
	require.Equal(t, migrationGrantCount, grantCount, "grant count mismatch")

	var resourceCount int
	err = c1f.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+resources.Name()+" WHERE sync_id=?", syncID,
	).Scan(&resourceCount)
	require.NoError(t, err)
	require.Greater(t, resourceCount, 0, "expected resources to survive round-trip")

	var entitlementCount int
	err = c1f.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+entitlements.Name()+" WHERE sync_id=?", syncID,
	).Scan(&entitlementCount)
	require.NoError(t, err)
	require.Greater(t, entitlementCount, 0, "expected entitlements to survive round-trip")

	// Verify expansion column is populated for expandable grants within a sample.
	// Every 10th grant is expandable, so in a sample of migrationVerifyCount we
	// expect migrationVerifyCount/10 to have expansion IS NOT NULL.
	var expandableCount int
	err = c1f.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(*) FROM (SELECT expansion FROM %s WHERE sync_id=? LIMIT %d) WHERE expansion IS NOT NULL",
		grants.Name(), migrationVerifyCount,
	), syncID).Scan(&expandableCount)
	require.NoError(t, err)
	expectedExpandable := migrationVerifyCount / 10
	require.Equal(t, expectedExpandable, expandableCount,
		"expected %d expandable grants in sample, got %d", expectedExpandable, expandableCount)

	// Verify GrantExpandable was stripped from grant data blobs within the sample.
	var unstrippedCount int
	err = c1f.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(*) FROM (SELECT data FROM %s WHERE sync_id=? LIMIT %d) WHERE CAST(data AS TEXT) LIKE '%%GrantExpandable%%'",
		grants.Name(), migrationVerifyCount,
	), syncID).Scan(&unstrippedCount)
	require.NoError(t, err)
	require.Equal(t, 0, unstrippedCount,
		"expected zero grants with GrantExpandable in data blob, found %d", unstrippedCount)

	// Verify migration-managed columns exist on the grants table.
	var expansionColExists int
	err = c1f.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pragma_table_info('"+grants.Name()+"') WHERE name='expansion'",
	).Scan(&expansionColExists)
	require.NoError(t, err)
	require.Equal(t, 1, expansionColExists, "expansion column should exist after migrations")

	var needsExpansionColExists int
	err = c1f.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pragma_table_info('"+grants.Name()+"') WHERE name='needs_expansion'",
	).Scan(&needsExpansionColExists)
	require.NoError(t, err)
	require.Equal(t, 1, needsExpansionColExists, "needs_expansion column should exist after migrations")

	// Verify migration-managed columns exist on the sync_runs table.
	for _, col := range []string{"sync_type", "parent_sync_id", "linked_sync_id", "supports_diff"} {
		var exists int
		err = c1f.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pragma_table_info('"+syncRuns.Name()+"') WHERE name=?", col,
		).Scan(&exists)
		require.NoError(t, err)
		require.Equal(t, 1, exists, "sync_runs column %q should exist after migrations", col)
	}
}

// seedLargeC1Z creates a c1z with data in all tables: resource types, resources,
// entitlements, grants (mix of expandable and plain), and a completed sync run.
func seedLargeC1Z(ctx context.Context, t *testing.T, c1zPath, tmpDir string, numGrants int) string {
	t.Helper()

	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	roleRT := v2.ResourceType_builder{Id: "role", DisplayName: "Role"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT, roleRT))

	const numGroups = 50
	const numEntitlements = 100

	groupResources := make([]*v2.Resource, numGroups)
	for i := range numGroups {
		groupResources[i] = v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "group", Resource: fmt.Sprintf("group-%d", i)}.Build(),
			DisplayName: fmt.Sprintf("Group %d", i),
		}.Build()
	}
	require.NoError(t, c1f.PutResources(ctx, groupResources...))

	ents := make([]*v2.Entitlement, numEntitlements)
	for i := range numEntitlements {
		ents[i] = v2.Entitlement_builder{
			Id:       fmt.Sprintf("ent-%d", i),
			Resource: groupResources[i%numGroups],
		}.Build()
	}
	require.NoError(t, c1f.PutEntitlements(ctx, ents...))

	// Insert grants — every 10th grant has a GrantExpandable annotation.
	const batchSize = 1000
	buf := make([]*v2.Grant, 0, batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		require.NoError(t, c1f.PutGrants(ctx, buf...))
		buf = buf[:0]
	}

	expandable := v2.GrantExpandable_builder{
		EntitlementIds:  []string{"ent-1"},
		Shallow:         false,
		ResourceTypeIds: []string{"user"},
	}.Build()

	for i := range numGrants {
		ent := ents[i%numEntitlements]
		gb := v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: ent,
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
			}.Build(),
		}
		if i%10 == 0 {
			gb.Annotations = annotations.New(expandable)
		}
		buf = append(buf, gb.Build())
		if len(buf) >= batchSize {
			flush()
		}
	}
	flush()

	// Insert user resources in bulk so the resources table has volume.
	const userResourceBatch = 5000
	userBuf := make([]*v2.Resource, 0, userResourceBatch)
	for i := range numGrants {
		userBuf = append(userBuf, v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
			DisplayName: fmt.Sprintf("User %d", i),
		}.Build())
		if len(userBuf) >= userResourceBatch {
			require.NoError(t, c1f.PutResources(ctx, userBuf...))
			userBuf = userBuf[:0]
		}
	}
	if len(userBuf) > 0 {
		require.NoError(t, c1f.PutResources(ctx, userBuf...))
	}

	require.NoError(t, c1f.EndSync(ctx))
	require.NoError(t, c1f.Close(ctx))

	return syncID
}
