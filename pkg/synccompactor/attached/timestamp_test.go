package attached

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// TestDiscoveredAtMergeLogic specifically tests the discovered_at timestamp comparison
// by creating two separate scenarios with controlled timing.
func TestDiscoveredAtMergeLogic(t *testing.T) {
	ctx := context.Background()

	// Test Case 1: Applied is newer (natural case)
	t.Run("AppliedNewer", func(t *testing.T) {
		tmpDir := t.TempDir()
		baseFile := filepath.Join(tmpDir, "base.c1z")
		appliedFile := filepath.Join(tmpDir, "applied.c1z")

		opts := []dotc1z.C1ZOption{
			dotc1z.WithPragma("journal_mode", "WAL"),
			dotc1z.WithTmpDir(tmpDir),
		}

		// Create base database first (older timestamps)
		baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
		require.NoError(t, err)
		defer baseDB.Close()

		_, err = baseDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
		err = baseDB.PutResourceTypes(ctx, userRT)
		require.NoError(t, err)

		// Base resource (will be older)
		baseResource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "overlapping-user",
			}.Build(),
			DisplayName: "Base Version (Should Lose)",
		}.Build()
		err = baseDB.PutResources(ctx, baseResource)
		require.NoError(t, err)

		err = baseDB.EndSync(ctx)
		require.NoError(t, err)

		// Small delay to ensure different timestamps
		time.Sleep(10 * time.Millisecond)

		// Create applied database (newer timestamps)
		appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
		require.NoError(t, err)
		defer appliedDB.Close()

		_, err = appliedDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		err = appliedDB.PutResourceTypes(ctx, userRT)
		require.NoError(t, err)

		// Applied resource (will be newer)
		appliedResource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "overlapping-user",
			}.Build(),
			DisplayName: "Applied Version (Should Win)",
		}.Build()
		err = appliedDB.PutResources(ctx, appliedResource)
		require.NoError(t, err)

		err = appliedDB.EndSync(ctx)
		require.NoError(t, err)

		compactor := NewAttachedCompactor(baseDB, appliedDB)
		err = compactor.Compact(ctx)
		require.NoError(t, err)

		// Verify applied version won
		resp, err := baseDB.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: appliedResource.GetId(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, "Applied Version (Should Win)", resp.GetResource().GetDisplayName())
	})

	// Test Case 2: Base is newer (reverse timing)
	t.Run("BaseNewer", func(t *testing.T) {
		tmpDir := t.TempDir()
		baseFile := filepath.Join(tmpDir, "base.c1z")
		appliedFile := filepath.Join(tmpDir, "applied.c1z")

		opts := []dotc1z.C1ZOption{
			dotc1z.WithPragma("journal_mode", "WAL"),
			dotc1z.WithTmpDir(tmpDir),
		}

		// Create applied database first (will have older timestamps)
		appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
		require.NoError(t, err)
		defer appliedDB.Close()

		_, err = appliedDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
		err = appliedDB.PutResourceTypes(ctx, userRT)
		require.NoError(t, err)

		// Applied resource (will be older due to creation order)
		appliedResource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "overlapping-user",
			}.Build(),
			DisplayName: "Applied Version (Should Lose)",
		}.Build()
		err = appliedDB.PutResources(ctx, appliedResource)
		require.NoError(t, err)

		err = appliedDB.EndSync(ctx)
		require.NoError(t, err)

		// Small delay to ensure different timestamps
		time.Sleep(10 * time.Millisecond)

		// Create base database after applied (newer timestamps)
		baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
		require.NoError(t, err)
		defer baseDB.Close()

		_, err = baseDB.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		err = baseDB.PutResourceTypes(ctx, userRT)
		require.NoError(t, err)

		// Base resource (will be newer)
		baseResource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "overlapping-user",
			}.Build(),
			DisplayName: "Base Version (Should Win)",
		}.Build()
		err = baseDB.PutResources(ctx, baseResource)
		require.NoError(t, err)

		err = baseDB.EndSync(ctx)
		require.NoError(t, err)

		compactor := NewAttachedCompactor(baseDB, appliedDB)
		err = compactor.Compact(ctx)
		require.NoError(t, err)

		// Verify base version won (because it was created later and has newer discovered_at)
		resp, err := baseDB.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
			ResourceId: baseResource.GetId(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, "Base Version (Should Win)", resp.GetResource().GetDisplayName())
	})
}
