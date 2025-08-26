package synccompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/synccompactor/attached"
	"github.com/conductorone/baton-sdk/pkg/synccompactor/naive"
	"github.com/stretchr/testify/require"
)

// BenchmarkData represents the scale of data to generate for benchmarks.
type BenchmarkData struct {
	ResourceTypes int
	Resources     int
	Entitlements  int
	Grants        int
}

// Small dataset for quick benchmarks.
var SmallDataset = BenchmarkData{
	ResourceTypes: 5,
	Resources:     100,
	Entitlements:  50,
	Grants:        100,
}

// Medium dataset for realistic scenarios.
var MediumDataset = BenchmarkData{
	ResourceTypes: 10,
	Resources:     1000,
	Entitlements:  500,
	Grants:        1000,
}

// Large dataset for stress testing.
var LargeDataset = BenchmarkData{
	ResourceTypes: 20,
	Resources:     10000,
	Entitlements:  5000,
	Grants:        10000,
}

// generateTestData creates test databases with the specified amount of data.
func generateTestData(ctx context.Context, t *testing.B, tmpDir string, dataset BenchmarkData) (string, string, string, string) {
	baseFile := filepath.Join(tmpDir, "base.c1z")
	appliedFile := filepath.Join(tmpDir, "applied.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithTmpDir(tmpDir),
	}

	// Create base sync file
	baseSync, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
	require.NoError(t, err)

	baseSyncID, _, err := baseSync.StartSync(ctx)
	require.NoError(t, err)

	// Generate resource types
	resourceTypeIDs := make([]string, dataset.ResourceTypes)
	for i := 0; i < dataset.ResourceTypes; i++ {
		rtId := fmt.Sprintf("resource-type-%d", i)
		resourceTypeIDs[i] = rtId
		err = baseSync.PutResourceTypes(ctx, &v2.ResourceType{
			Id:          rtId,
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		})
		require.NoError(t, err)
	}

	// Generate resources for base sync (about 60% of total)
	baseResourceCount := int(float64(dataset.Resources) * 0.6)
	baseResources := make([]*v2.Resource, baseResourceCount)
	for i := 0; i < baseResourceCount; i++ {
		resource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: resourceTypeIDs[i%len(resourceTypeIDs)],
				Resource:     fmt.Sprintf("base-resource-%d", i),
			},
			DisplayName: fmt.Sprintf("Base Resource %d", i),
		}
		baseResources[i] = resource
		err = baseSync.PutResources(ctx, resource)
		require.NoError(t, err)
	}

	// Generate entitlements for base sync
	baseEntitlementCount := int(float64(dataset.Entitlements) * 0.6)
	baseEntitlements := make([]*v2.Entitlement, baseEntitlementCount)
	for i := 0; i < baseEntitlementCount; i++ {
		entitlement := &v2.Entitlement{
			Id:          fmt.Sprintf("base-entitlement-%d", i),
			DisplayName: fmt.Sprintf("Base Entitlement %d", i),
			Resource:    baseResources[i%len(baseResources)],
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}
		baseEntitlements[i] = entitlement
		err = baseSync.PutEntitlements(ctx, entitlement)
		require.NoError(t, err)
	}

	// Generate grants for base sync
	baseGrantCount := int(float64(dataset.Grants) * 0.6)
	for i := 0; i < baseGrantCount; i++ {
		grant := &v2.Grant{
			Id:          fmt.Sprintf("base-grant-%d", i),
			Principal:   baseResources[i%len(baseResources)],
			Entitlement: baseEntitlements[i%len(baseEntitlements)],
		}
		err = baseSync.PutGrants(ctx, grant)
		require.NoError(t, err)
	}

	err = baseSync.EndSync(ctx)
	require.NoError(t, err)
	err = baseSync.Close()
	require.NoError(t, err)

	// Create applied sync file
	appliedSync, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
	require.NoError(t, err)

	appliedSyncID, _, err := appliedSync.StartSync(ctx)
	require.NoError(t, err)

	// Reuse same resource types
	for _, rtId := range resourceTypeIDs {
		err = appliedSync.PutResourceTypes(ctx, &v2.ResourceType{
			Id:          rtId,
			DisplayName: fmt.Sprintf("Resource Type %s", rtId),
		})
		require.NoError(t, err)
	}

	// Generate resources for applied sync (remaining 40% + some overlapping)
	appliedResourceCount := dataset.Resources - baseResourceCount + int(float64(baseResourceCount)*0.2) // 20% overlap
	appliedResources := make([]*v2.Resource, 0, appliedResourceCount)

	// Add some overlapping resources from base
	overlapCount := int(float64(baseResourceCount) * 0.2)
	for i := 0; i < overlapCount; i++ {
		resource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: resourceTypeIDs[i%len(resourceTypeIDs)],
				Resource:     fmt.Sprintf("base-resource-%d", i), // Same ID as base
			},
			DisplayName: fmt.Sprintf("Updated Base Resource %d", i), // Different display name
		}
		appliedResources = append(appliedResources, resource)
		err = appliedSync.PutResources(ctx, resource)
		require.NoError(t, err)
	}

	// Add new resources only in applied
	newResourceCount := appliedResourceCount - overlapCount
	for i := 0; i < newResourceCount; i++ {
		resource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: resourceTypeIDs[i%len(resourceTypeIDs)],
				Resource:     fmt.Sprintf("applied-resource-%d", i),
			},
			DisplayName: fmt.Sprintf("Applied Resource %d", i),
		}
		appliedResources = append(appliedResources, resource)
		err = appliedSync.PutResources(ctx, resource)
		require.NoError(t, err)
	}

	// Generate entitlements for applied sync
	appliedEntitlementCount := dataset.Entitlements - baseEntitlementCount
	appliedEntitlements := make([]*v2.Entitlement, appliedEntitlementCount)
	for i := 0; i < appliedEntitlementCount; i++ {
		entitlement := &v2.Entitlement{
			Id:          fmt.Sprintf("applied-entitlement-%d", i),
			DisplayName: fmt.Sprintf("Applied Entitlement %d", i),
			Resource:    appliedResources[i%len(appliedResources)],
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}
		appliedEntitlements[i] = entitlement
		err = appliedSync.PutEntitlements(ctx, entitlement)
		require.NoError(t, err)
	}

	// Generate grants for applied sync
	appliedGrantCount := dataset.Grants - baseGrantCount
	for i := 0; i < appliedGrantCount; i++ {
		grant := &v2.Grant{
			Id:          fmt.Sprintf("applied-grant-%d", i),
			Principal:   appliedResources[i%len(appliedResources)],
			Entitlement: appliedEntitlements[i%len(appliedEntitlements)],
		}
		err = appliedSync.PutGrants(ctx, grant)
		require.NoError(t, err)
	}

	err = appliedSync.EndSync(ctx)
	require.NoError(t, err)
	err = appliedSync.Close()
	require.NoError(t, err)

	return baseFile, baseSyncID, appliedFile, appliedSyncID
}

// benchmarkNaiveCompactor runs a benchmark using the naive compactor.
func benchmarkNaiveCompactor(b *testing.B, dataset BenchmarkData) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create temporary directories
		tmpDir, err := os.MkdirTemp("", "benchmark-naive")
		require.NoError(b, err)
		defer os.RemoveAll(tmpDir)

		outputDir, err := os.MkdirTemp("", "benchmark-output")
		require.NoError(b, err)
		defer os.RemoveAll(outputDir)

		// Generate test data
		baseFile, _, appliedFile, _ := generateTestData(ctx, b, tmpDir, dataset)

		opts := []dotc1z.C1ZOption{
			dotc1z.WithPragma("journal_mode", "WAL"),
		}

		// Use naive compactor
		baseC1Z, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
		require.NoError(b, err)
		defer baseC1Z.Close()

		appliedC1Z, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
		require.NoError(b, err)
		defer appliedC1Z.Close()

		destFile := filepath.Join(tmpDir, "naive-dest.c1z")
		destOpts := []dotc1z.C1ZOption{
			dotc1z.WithTmpDir(tmpDir),
		}
		destOpts = append(destOpts, opts...)
		destC1Z, err := dotc1z.NewC1ZFile(ctx, destFile, destOpts...)
		require.NoError(b, err)
		defer destC1Z.Close()

		// Start a sync in the destination file
		_, err = destC1Z.StartNewSync(ctx)
		require.NoError(b, err)

		b.StartTimer()

		// Benchmark the naive compaction
		naiveCompactor := naive.NewNaiveCompactor(baseC1Z, appliedC1Z, destC1Z)
		err = naiveCompactor.Compact(ctx)
		require.NoError(b, err)

		b.StopTimer()

		// End the sync
		err = destC1Z.EndSync(ctx)
		require.NoError(b, err)
	}
}

// benchmarkAttachedCompactor runs a benchmark using the attached compactor.
func benchmarkAttachedCompactor(b *testing.B, dataset BenchmarkData) {
	ctx := context.Background()

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create temporary directories
		tmpDir, err := os.MkdirTemp("", "benchmark-attached")
		require.NoError(b, err)
		defer os.RemoveAll(tmpDir)

		outputDir, err := os.MkdirTemp("", "benchmark-output")
		require.NoError(b, err)
		defer os.RemoveAll(outputDir)

		// Generate test data
		baseFile, _, appliedFile, _ := generateTestData(ctx, b, tmpDir, dataset)

		// Use attached compactor
		baseC1Z, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
		require.NoError(b, err)
		defer baseC1Z.Close()

		appliedC1Z, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
		require.NoError(b, err)
		defer appliedC1Z.Close()

		destFile := filepath.Join(tmpDir, "attached-dest.c1z")
		destOpts := []dotc1z.C1ZOption{
			dotc1z.WithTmpDir(tmpDir),
		}
		destOpts = append(destOpts, opts...)
		destC1Z, err := dotc1z.NewC1ZFile(ctx, destFile, destOpts...)
		require.NoError(b, err)
		defer destC1Z.Close()

		b.StartTimer()

		// Start sync in destination
		destSyncID, err := destC1Z.StartNewSync(ctx)
		require.NoError(b, err)

		// Benchmark the attached compaction
		attachedCompactor := attached.NewAttachedCompactor(baseC1Z, appliedC1Z, destC1Z)
		err = attachedCompactor.CompactWithSyncID(ctx, destSyncID)
		require.NoError(b, err)

		err = destC1Z.EndSync(ctx)
		require.NoError(b, err)
	}
}

// Benchmark functions for different data sizes and approaches

func BenchmarkNaiveCompactor_Small(b *testing.B) {
	benchmarkNaiveCompactor(b, SmallDataset)
}

func BenchmarkAttachedCompactor_Small(b *testing.B) {
	benchmarkAttachedCompactor(b, SmallDataset)
}

func BenchmarkNaiveCompactor_Medium(b *testing.B) {
	benchmarkNaiveCompactor(b, MediumDataset)
}

func BenchmarkAttachedCompactor_Medium(b *testing.B) {
	benchmarkAttachedCompactor(b, MediumDataset)
}

func BenchmarkNaiveCompactor_Large(b *testing.B) {
	benchmarkNaiveCompactor(b, LargeDataset)
}

func BenchmarkAttachedCompactor_Large(b *testing.B) {
	benchmarkAttachedCompactor(b, LargeDataset)
}
