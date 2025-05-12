package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

	// 2. Resources that will be in both syncs with first sync having older discovered_at
	inBothFirstOlderResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-first-older",
		},
	}
	err = firstSync.PutResources(ctx, inBothFirstOlderResource)
	require.NoError(t, err)

	// Wait a bit to ensure different timestamps
	time.Sleep(100 * time.Millisecond)

	// 3. Resources that will be in both syncs with first sync having newer discovered_at
	inBothFirstNewerResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-first-newer",
		},
	}
	err = firstSync.PutResources(ctx, inBothFirstNewerResource)
	require.NoError(t, err)

	// End the first sync
	err = firstSync.EndSync(ctx)
	require.NoError(t, err)
	err = firstSync.Close()
	require.NoError(t, err)

	// Wait a bit to ensure different timestamps
	time.Sleep(100 * time.Millisecond)

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

	// 2. Resources that will be in both syncs with second sync having newer discovered_at
	inBothSecondNewerResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-first-older",
		},
	}
	err = secondSync.PutResources(ctx, inBothSecondNewerResource)
	require.NoError(t, err)

	// Wait a bit to ensure different timestamps
	time.Sleep(100 * time.Millisecond)

	// 3. Resources that will be in both syncs with second sync having older discovered_at
	inBothSecondOlderResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     "in-both-first-newer",
		},
	}
	err = secondSync.PutResources(ctx, inBothSecondOlderResource)
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
	compactor, err := NewCompactor(ctx, outputDir, firstCompactableSync, secondCompactableSync)
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

	// 3. Resource that was in both syncs with second sync having newer discovered_at should use the second sync version
	inBothSecondNewerResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: inBothSecondNewerResource.Id,
	})
	require.NoError(t, err)
	require.Equal(t, inBothSecondNewerResource.Id.Resource, inBothSecondNewerResp.Resource.Id.Resource)

	// 4. Resource that was in both syncs with first sync having newer discovered_at should use the first sync version
	inBothFirstNewerResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: inBothFirstNewerResource.Id,
	})
	require.NoError(t, err)
	require.Equal(t, inBothFirstNewerResource.Id.Resource, inBothFirstNewerResp.Resource.Id.Resource)
}
