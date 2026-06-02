package dotc1z

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestCloneSyncMigratedColumnOrder verifies that CloneSync correctly handles
// a source database whose column order differs from a freshly-created schema.
//
// When a c1z file is created by an older version of the code and then migrated,
// ALTER TABLE ADD COLUMN appends new columns to the end. The current schema
// definition places expansion and needs_expansion between external_id and data.
// A freshly-created table has them inline; a migrated table has them at the end.
//
// CloneSync uses INSERT INTO clone.t SELECT * FROM t, which matches columns by
// ordinal position. If the source and destination have different physical column
// orders, the data is silently written into the wrong columns.
func TestCloneSyncMigratedColumnOrder(t *testing.T) {
	ctx := context.Background()

	srcDir := t.TempDir()
	srcDBPath := filepath.Join(srcDir, "source.db")

	// Step 1: Create a C1File normally — this creates grants with the current
	// schema where expansion/needs_expansion are inline (between external_id and data).
	srcFile, err := NewC1File(ctx, srcDBPath, WithC1FTmpDir(srcDir))
	require.NoError(t, err)

	// Step 2: Drop the grants table and recreate it with the OLD column order
	// (no expansion/needs_expansion), then ALTER TABLE to add them at the end.
	// This simulates a database originally created before those columns existed.
	_, err = srcFile.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", grants.Name()))
	require.NoError(t, err)

	oldSchema := fmt.Sprintf(`
		CREATE TABLE %s (
			id integer primary key,
			resource_type_id text not null,
			resource_id text not null,
			entitlement_id text not null,
			principal_resource_type_id text not null,
			principal_resource_id text not null,
			external_id text not null,
			data blob not null,
			sync_id text not null,
			discovered_at datetime not null
		)`, grants.Name())
	_, err = srcFile.db.ExecContext(ctx, oldSchema)
	require.NoError(t, err)

	// Recreate indexes so queries work.
	indexSQL := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_grants_resource_type_id_resource_id_v1 ON %s (resource_type_id, resource_id);
		CREATE INDEX IF NOT EXISTS idx_grants_principal_id_v1 ON %s (principal_resource_type_id, principal_resource_id);
		CREATE INDEX IF NOT EXISTS idx_grants_entitlement_id_principal_id_v1 ON %s (entitlement_id, principal_resource_type_id, principal_resource_id);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_grants_external_sync_v1 ON %s (external_id, sync_id);
	`, grants.Name(), grants.Name(), grants.Name(), grants.Name())
	_, err = srcFile.db.ExecContext(ctx, indexSQL)
	require.NoError(t, err)

	// Add migration columns — ALTER TABLE appends them to the END in SQLite.
	_, err = srcFile.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN expansion blob", grants.Name()))
	require.NoError(t, err)
	_, err = srcFile.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN needs_expansion integer not null default 0", grants.Name()))
	require.NoError(t, err)

	// Create partial indexes that the migration would add.
	_, err = srcFile.db.ExecContext(ctx, fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS idx_grants_sync_expansion_v1 ON %s (sync_id) WHERE expansion IS NOT NULL",
		grants.Name()))
	require.NoError(t, err)
	_, err = srcFile.db.ExecContext(ctx, fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS idx_grants_sync_needs_expansion_v1 ON %s (sync_id) WHERE needs_expansion = 1",
		grants.Name()))
	require.NoError(t, err)

	// Step 3: Write objects via the normal API. goqu uses named columns, so the
	// data goes into the correct columns regardless of physical order.
	syncID, err := srcFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, srcFile.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	u1 := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	require.NoError(t, srcFile.PutResources(ctx, g1, u1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(t, srcFile.PutEntitlements(ctx, ent1))

	expandableAny, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds:  []string{"ent1"},
		Shallow:         true,
		ResourceTypeIds: []string{"user"},
	}.Build())
	require.NoError(t, err)

	expandableGrant := v2.Grant_builder{
		Id:          "grant-expandable",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: []*anypb.Any{expandableAny},
	}.Build()
	normalGrant := v2.Grant_builder{
		Id:          "grant-normal",
		Entitlement: ent1,
		Principal:   g1,
	}.Build()
	require.NoError(t, srcFile.PutGrants(ctx, expandableGrant, normalGrant))
	require.NoError(t, srcFile.EndSync(ctx))

	// Sanity check: the source file can list grants correctly.
	srcFile.currentSyncID = ""
	require.NoError(t, srcFile.ViewSync(ctx, syncID))
	srcResp, err := srcFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, srcResp.GetList(), 2, "source should have 2 grants before clone")

	// Step 4: Clone the sync into a new file.
	clonePath := filepath.Join(srcDir, "clone.c1z")
	err = srcFile.CloneSync(ctx, clonePath, syncID)
	require.NoError(t, err, "CloneSync should succeed")

	// Step 5: Open the clone and verify grants survived intact.
	cloneFile, err := NewC1ZFile(ctx, clonePath)
	require.NoError(t, err)

	cloneSyncID, err := cloneFile.LatestSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.NotEmpty(t, cloneSyncID)

	cloneFile.currentSyncID = ""
	require.NoError(t, cloneFile.ViewSync(ctx, cloneSyncID))

	cloneResp, err := cloneFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, cloneResp.GetList(), 2, "clone should have 2 grants")

	grantsByID := make(map[string]*v2.Grant)
	for _, g := range cloneResp.GetList() {
		grantsByID[g.GetId()] = g
	}

	// Verify the normal grant survived.
	normalClone := grantsByID["grant-normal"]
	require.NotNil(t, normalClone, "grant-normal should exist in clone")
	require.Equal(t, "ent1", normalClone.GetEntitlement().GetId())
	require.Equal(t, "group", normalClone.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "g1", normalClone.GetPrincipal().GetId().GetResource())

	// Verify the expandable grant survived.
	expandableClone := grantsByID["grant-expandable"]
	require.NotNil(t, expandableClone, "grant-expandable should exist in clone")
	require.Equal(t, "ent1", expandableClone.GetEntitlement().GetId())
	require.Equal(t, "user", expandableClone.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "u1", expandableClone.GetPrincipal().GetId().GetResource())

	// srcFile is a raw C1File with no outputFilePath, so Close() would fail
	// trying to saveC1z. Just release the database handle directly.
	require.NoError(t, srcFile.rawDb.Close())
	require.NoError(t, cloneFile.Close(ctx))
}
