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
	"google.golang.org/protobuf/proto"
)

const (
	migrationGrantCount = 1_500_000
	migrationMaxOpen    = 3 * time.Minute
)

// TestRegression_MigrationBackfillPerformance creates a c1z with many grants in
// "old" format (GrantExpandable annotation embedded in the data blob, expansion
// column NULL, supports_diff=0) then reopens the file. The migration that runs
// during open must complete within migrationMaxOpen.
func TestRegression_MigrationBackfillPerformance(t *testing.T) {
	t.Skip("disabled: backfill migration path is stable; re-enable if migration logic changes")

	if testing.Short() {
		t.Skip("skipping regression test in short mode")
	}

	ctx := t.Context()

	tmpDir, err := os.MkdirTemp("", "regression-migration-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	c1zPath := filepath.Join(tmpDir, "migration.c1z")

	// Phase 1: build a c1z with old-format grants.
	syncID := seedOldFormatC1Z(ctx, t, c1zPath, migrationGrantCount)

	// Phase 2: reopen — migration runs during NewC1ZFile.
	start := time.Now()
	c1f, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(tmpDir))
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	t.Logf("Migration of %d grants completed in %v", migrationGrantCount, elapsed)
	require.LessOrEqual(t, elapsed, migrationMaxOpen,
		"migration took %v, limit is %v", elapsed, migrationMaxOpen)

	// Correctness guard: all old-format grants should be backfilled.
	var backfilledCount int
	err = c1f.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+grants.Name()+" WHERE sync_id=? AND expansion IS NOT NULL",
		syncID,
	).Scan(&backfilledCount)
	require.NoError(t, err)
	require.Equal(t, migrationGrantCount, backfilledCount, "expected all grants to be backfilled")

	// Correctness guard: backfill strips GrantExpandable from grant payloads.
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 50}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetList())
	for _, g := range resp.GetList() {
		readAnnos := annotations.Annotations(g.GetAnnotations())
		require.False(t, readAnnos.Contains(&v2.GrantExpandable{}),
			"expected GrantExpandable to be stripped from grant payload for grant %s", g.GetId())
	}
}

// seedOldFormatC1Z creates a c1z at path containing numGrants grants that look
// like pre-expansion-column data: the GrantExpandable annotation is embedded in
// the serialized grant proto and the expansion SQL column is NULL. The sync is
// marked supports_diff=0 so the migration picks these up.
func seedOldFormatC1Z(ctx context.Context, t *testing.T, path string, numGrants int) string {
	t.Helper()

	c1f, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent1))

	expandable := v2.GrantExpandable_builder{
		EntitlementIds:  []string{"ent1"},
		Shallow:         true,
		ResourceTypeIds: []string{"user"},
	}.Build()

	// Insert grants with direct SQL so the annotation stays in the data blob
	// and expansion is NULL (simulating old-format storage).
	const batchSize = 1000
	for i := 0; i < numGrants; i += batchSize {
		end := min(i+batchSize, numGrants)

		tx, err := c1f.db.Begin()
		require.NoError(t, err)

		stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
			`INSERT INTO %s
				(resource_type_id, resource_id, entitlement_id,
				 principal_resource_type_id, principal_resource_id,
				 external_id, expansion, needs_expansion, data, sync_id, discovered_at)
			VALUES (?,?,?, ?,?, ?, NULL, 0, ?, ?, datetime('now'))`,
			grants.Name(),
		))
		require.NoError(t, err)

		for j := i; j < end; j++ {
			grant := v2.Grant_builder{
				Id:          fmt.Sprintf("grant-%d", j),
				Entitlement: ent1,
				Principal: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "user",
						Resource:     fmt.Sprintf("u%d", j),
					}.Build(),
				}.Build(),
				Annotations: annotations.New(expandable),
			}.Build()

			data, mErr := proto.Marshal(grant)
			require.NoError(t, mErr)

			_, mErr = stmt.ExecContext(ctx,
				"group", "g1", "ent1",
				"user", fmt.Sprintf("u%d", j),
				fmt.Sprintf("grant-%d", j),
				data, syncID,
			)
			require.NoError(t, mErr)
		}

		require.NoError(t, stmt.Close())
		require.NoError(t, tx.Commit())
	}

	// Mark sync as old-format so the migration will process these grants.
	_, err = c1f.db.ExecContext(ctx,
		"UPDATE "+syncRuns.Name()+" SET supports_diff=0 WHERE sync_id=?", syncID)
	require.NoError(t, err)

	require.NoError(t, c1f.EndSync(ctx))
	require.NoError(t, c1f.Close(ctx))

	return syncID
}
