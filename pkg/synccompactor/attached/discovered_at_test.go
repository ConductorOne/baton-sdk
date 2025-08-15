package attached

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func TestAttachedCompactorDiscoveredAtComparison(t *testing.T) {
	ctx := context.Background()

	// Create temporary files for base, applied, and dest databases
	tmpDir := t.TempDir()
	baseFile := filepath.Join(tmpDir, "base.c1z")
	appliedFile := filepath.Join(tmpDir, "applied.c1z")
	destFile := filepath.Join(tmpDir, "dest.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithTmpDir(tmpDir),
	}

	// Create base database (older timestamps)
	baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
	require.NoError(t, err)
	defer baseDB.Close()

	_, _, err = baseDB.StartSync(ctx)
	require.NoError(t, err)

	// Create resource types
	userRT := &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
	}
	err = baseDB.PutResourceTypes(ctx, userRT)
	require.NoError(t, err)

	// Resources in base sync
	baseResource1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-overlapping",
		},
		DisplayName: "User Overlapping (Base Version)",
	}

	baseOnlyResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-base-only",
		},
		DisplayName: "User Base Only",
	}

	err = baseDB.PutResources(ctx, baseResource1, baseOnlyResource)
	require.NoError(t, err)

	err = baseDB.EndSync(ctx)
	require.NoError(t, err)

	// Create applied database (newer timestamps since it's created after base)
	appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
	require.NoError(t, err)
	defer appliedDB.Close()

	_, _, err = appliedDB.StartSync(ctx)
	require.NoError(t, err)

	// Add same resource type to applied
	err = appliedDB.PutResourceTypes(ctx, userRT)
	require.NoError(t, err)

	// Same resource ID as base, but will have newer discovered_at timestamp
	appliedResource1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-overlapping",
		},
		DisplayName: "User Overlapping (Applied Version - Should Win)",
	}

	// Resource only in applied
	appliedOnlyResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-applied-only",
		},
		DisplayName: "User Applied Only",
	}

	err = appliedDB.PutResources(ctx, appliedResource1, appliedOnlyResource)
	require.NoError(t, err)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)

	// Create destination database
	destDB, err := dotc1z.NewC1ZFile(ctx, destFile, opts...)
	require.NoError(t, err)
	defer destDB.Close()

	// Create compactor and run compaction
	compactor := NewAttachedCompactor(baseDB, appliedDB, destDB)
	err = compactor.Compact(ctx)
	require.NoError(t, err)

	// Verify the compacted results
	// Overlapping resource: Should have the applied version (newer discovered_at)
	overlappingResp, err := destDB.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-overlapping",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "User Overlapping (Applied Version - Should Win)", overlappingResp.Resource.DisplayName)

	// Base-only resource: Should exist from base
	baseOnlyResp, err := destDB.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-base-only",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "User Base Only", baseOnlyResp.Resource.DisplayName)

	// Applied-only resource: Should exist from applied
	appliedOnlyResp, err := destDB.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-applied-only",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "User Applied Only", appliedOnlyResp.Resource.DisplayName)
}
