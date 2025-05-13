package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func TestCompactor(t *testing.T) {
	ctx := context.Background()

	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "compactor-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create output directory for compacted file
	outputDir, err := os.MkdirTemp("", "compactor-output")
	require.NoError(t, err)
	defer os.RemoveAll(outputDir)

	// Create temporary directory for intermediate files
	tmpDir, err := os.MkdirTemp("", "compactor-tmp")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create the first sync file
	firstSyncPath := filepath.Join(tempDir, "first-sync.c1z")
	firstSync, err := dotc1z.NewC1ZFile(ctx, firstSyncPath)
	require.NoError(t, err)

	// Start a new sync
	firstSyncID, _, err := firstSync.StartSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, firstSyncID)

	// Create a resource type
	resourceTypeID := "test-resource-type"
	err = firstSync.PutResourceTypes(ctx, &v2.ResourceType{Id: resourceTypeID})
	require.NoError(t, err)

	// Create resources for the first sync
	// 1. Resources that will only be in the first sync
	onlyInFirstResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "only-in-first",
		},
	}
	err = firstSync.PutResources(ctx, onlyInFirstResource)
	require.NoError(t, err)

	// 2. Resources that will be in both syncs
	inBothResource1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-1",
		},
	}
	err = firstSync.PutResources(ctx, inBothResource1)
	require.NoError(t, err)

	// 3. Another resource that will be in both syncs
	inBothResource2 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-2",
		},
	}
	err = firstSync.PutResources(ctx, inBothResource2)
	require.NoError(t, err)

	// End the first sync
	err = firstSync.EndSync(ctx)
	require.NoError(t, err)
	err = firstSync.Close()
	require.NoError(t, err)

	// Create the second sync file
	secondSyncPath := filepath.Join(tempDir, "second-sync.c1z")
	secondSync, err := dotc1z.NewC1ZFile(ctx, secondSyncPath)
	require.NoError(t, err)

	// Start a new sync
	secondSyncID, _, err := secondSync.StartSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, secondSyncID)

	// Create the same resource type
	err = secondSync.PutResourceTypes(ctx, &v2.ResourceType{Id: resourceTypeID})
	require.NoError(t, err)

	// Create resources for the second sync
	// 1. Resources that will only be in the second sync
	onlyInSecondResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "only-in-second",
		},
	}
	err = secondSync.PutResources(ctx, onlyInSecondResource)
	require.NoError(t, err)

	// 2. Resources that will be in both syncs
	inBothResource1InSecond := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-1",
		},
	}
	err = secondSync.PutResources(ctx, inBothResource1InSecond)
	require.NoError(t, err)

	// 3. Another resource that will be in both syncs
	inBothResource2InSecond := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-2",
		},
	}
	err = secondSync.PutResources(ctx, inBothResource2InSecond)
	require.NoError(t, err)

	// End the second sync
	err = secondSync.EndSync(ctx)
	require.NoError(t, err)
	err = secondSync.Close()
	require.NoError(t, err)

	// Create compactable syncs
	firstCompactableSync := &CompactableSync{
		FilePath: firstSyncPath,
		SyncID:   firstSyncID,
	}
	secondCompactableSync := &CompactableSync{
		FilePath: secondSyncPath,
		SyncID:   secondSyncID,
	}

	// Create compactor
	compactableSyncs := []*CompactableSync{firstCompactableSync, secondCompactableSync}
	compactor, err := NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(tmpDir))
	require.NoError(t, err)

	// Compact the syncs
	compactedSync, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, compactedSync)

	// Open the compacted file
	compactedFile, err := dotc1z.NewC1ZFile(ctx, compactedSync.FilePath)
	require.NoError(t, err)
	defer compactedFile.Close()

	// Verify the compacted file contains the expected resources
	// 1. Resource that was only in the first sync should be in the compacted file
	onlyInFirstResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInFirstResource.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInFirstResource.Id.Resource, onlyInFirstResp.Resource.Id.Resource)

	// 2. Resource that was only in the second sync should be in the compacted file
	onlyInSecondResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInSecondResource.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInSecondResource.Id.Resource, onlyInSecondResp.Resource.Id.Resource)

	// 3. Resource that was in both syncs (first one) should be in the compacted file
	inBothResource1Resp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: inBothResource1.Id,
	})
	require.NoError(t, err)
	require.Equal(t, inBothResource1.Id.Resource, inBothResource1Resp.Resource.Id.Resource)

	// 4. Resource that was in both syncs (second one) should be in the compacted file
	inBothResource2Resp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: inBothResource2.Id,
	})
	require.NoError(t, err)
	require.Equal(t, inBothResource2.Id.Resource, inBothResource2Resp.Resource.Id.Resource)
}
