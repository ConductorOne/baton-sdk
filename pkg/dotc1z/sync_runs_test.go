package dotc1z

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

const (
	resourceTypeCount = 3
	resourceCount     = 10
	userCount         = 10
	entitlementCount  = 10
	grantCount        = 25
)

func createData(ctx context.Context, t *testing.T, f *C1File) (string, error) {
	// Add a sync
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	for i := range resourceTypeCount {
		err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
			Id:          fmt.Sprintf("rt-%d", i),
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		}.Build())
		require.NoError(t, err)
	}

	for i := range resourceCount {
		err = f.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: fmt.Sprintf("rt-%d", i%resourceTypeCount),
				Resource:     fmt.Sprintf("resource-%d", i),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	for i := range userCount {
		err = f.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     fmt.Sprintf("user-%d", i),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	for i := range entitlementCount {
		err = f.PutEntitlements(ctx, v2.Entitlement_builder{
			Id: fmt.Sprintf("ent-%d", i),
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: fmt.Sprintf("rt-%d", i%resourceTypeCount),
					Resource:     fmt.Sprintf("resource-%d", i%resourceCount),
				}.Build(),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	for i := range grantCount {
		err = f.PutGrants(ctx, v2.Grant_builder{
			Id: fmt.Sprintf("grant-%d", i),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     fmt.Sprintf("user-%d", i%userCount),
				}.Build(),
			}.Build(),
			Entitlement: v2.Entitlement_builder{
				Id: fmt.Sprintf("ent-%d", i%entitlementCount),
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: fmt.Sprintf("rt-%d", i%resourceTypeCount),
						Resource:     fmt.Sprintf("resource-%d", i%resourceCount),
					}.Build(),
				}.Build(),
			}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	// Delete 25% of grants.
	for i := 0; i < grantCount; i += 4 {
		err = f.DeleteGrant(ctx, fmt.Sprintf("grant-%d", i))
		require.NoError(t, err)
	}

	err = f.EndSync(ctx)
	require.NoError(t, err)

	return syncID, nil
}

func TestCleanupVacuum(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	// Create some data.
	_, err = createData(ctx, t, f)
	require.NoError(t, err)

	var pageCount int
	row := f.rawDb.QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&pageCount))
	require.Greater(t, pageCount, 0)

	var freelistCount int
	row = f.rawDb.QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&freelistCount))
	require.Greater(t, freelistCount, 0)

	err = f.Cleanup(ctx)
	require.NoError(t, err)

	// Vacuum should have run, so page_count should be lower and freelist_count should be zero.
	var cleanupPageCount int
	row = f.rawDb.QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&cleanupPageCount))
	require.Less(t, cleanupPageCount, pageCount, "page_count should be lower")

	var cleanupFreelistCount int
	row = f.rawDb.QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&cleanupFreelistCount))
	require.Equal(t, 0, cleanupFreelistCount, "freelist_count should be zero")

	// Close the file.
	err = f.Close(ctx)
	require.NoError(t, err)
}

func TestCleanupVacuumWAL(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	// Create some data.
	_, err = createData(ctx, t, f)
	require.NoError(t, err)

	var pageCount int
	row := f.rawDb.QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&pageCount))
	require.Greater(t, pageCount, 0)

	var freelistCount int
	row = f.rawDb.QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&freelistCount))
	require.Greater(t, freelistCount, 0)

	err = f.Cleanup(ctx)
	require.NoError(t, err)

	// Vacuum should not have run, so page_count and freelist_count should be the same.
	var cleanupPageCount int
	row = f.rawDb.QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&cleanupPageCount))
	require.Equal(t, pageCount, cleanupPageCount, "page_count should be the same")

	var cleanupFreelistCount int
	row = f.rawDb.QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&cleanupFreelistCount))
	require.Equal(t, freelistCount, cleanupFreelistCount, "freelist_count should be the same")

	// Close the file.
	err = f.Close(ctx)
	require.NoError(t, err)
}
