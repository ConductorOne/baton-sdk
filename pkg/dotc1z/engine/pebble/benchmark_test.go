package pebble

import (
	"context"
	"fmt"
	"os"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

// BenchmarkResourceOperations benchmarks basic resource operations
func BenchmarkResourceOperations(b *testing.B) {
	ctx := context.Background()
	
	b.Run("PutResources", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_put")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(64<<20),
			WithBatchSizeLimit(1000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkPutResources(b, ctx, pe)
	})
	
	b.Run("ListResources", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_list")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(64<<20),
			WithBatchSizeLimit(1000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkListResources(b, ctx, pe)
	})
	
	b.Run("GetResource", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_get")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(64<<20),
			WithBatchSizeLimit(1000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkGetResource(b, ctx, pe)
	})
}

// BenchmarkGrantOperations benchmarks grant-related operations
func BenchmarkGrantOperations(b *testing.B) {
	ctx := context.Background()
	
	b.Run("PutGrants", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_grants_put")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(64<<20),
			WithBatchSizeLimit(1000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkPutGrants(b, ctx, pe)
	})
	
	b.Run("ListGrants", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_grants_list")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(64<<20),
			WithBatchSizeLimit(1000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkListGrants(b, ctx, pe)
	})
	
	b.Run("ListGrantsForPrincipal", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_grants_principal")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(64<<20),
			WithBatchSizeLimit(1000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkListGrantsForPrincipal(b, ctx, pe)
	})
}

// BenchmarkSyncOperations benchmarks sync lifecycle operations
func BenchmarkSyncOperations(b *testing.B) {
	ctx := context.Background()
	
	b.Run("FullSyncCycle", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_sync_full")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(128<<20),
			WithBatchSizeLimit(5000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkFullSyncCycle(b, ctx, pe)
	})
	
	b.Run("ConditionalUpserts", func(b *testing.B) {
		tempDir, err := os.MkdirTemp("", "pebble_benchmark_sync_conditional")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(tempDir)
		
		pe, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(128<<20),
			WithBatchSizeLimit(5000),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pe.Close()
		
		benchmarkConditionalUpserts(b, ctx, pe)
	})
}

// BenchmarkBatchSizes tests different batch sizes for optimal performance
func BenchmarkBatchSizes(b *testing.B) {
	ctx := context.Background()
	
	batchSizes := []int{10, 50, 100, 500, 1000, 2500, 5000}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			benchmarkWithBatchSize(b, ctx, batchSize)
		})
	}
}

func benchmarkPutResources(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	resources := make([]*v2.Resource, b.N)
	for i := 0; i < b.N; i++ {
		resources[i] = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "benchmark-type",
				Resource:     fmt.Sprintf("benchmark-resource-%d", i),
			},
			DisplayName: fmt.Sprintf("Benchmark Resource %d", i),
		}
	}
	
	_, err := pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	
	err = pe.PutResources(ctx, resources...)
	if err != nil {
		b.Fatal(err)
	}
	
	b.StopTimer()
	pe.EndSync(ctx)
}

func benchmarkListResources(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	// Setup: create some resources first
	const setupCount = 10000
	resources := make([]*v2.Resource, setupCount)
	for i := 0; i < setupCount; i++ {
		resources[i] = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "benchmark-list-type",
				Resource:     fmt.Sprintf("list-resource-%d", i),
			},
			DisplayName: fmt.Sprintf("List Resource %d", i),
		}
	}
	
	_, err := pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.PutResources(ctx, resources...)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.EndSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	// Benchmark the listing operation
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		req := &v2.ResourcesServiceListResourcesRequest{
			PageSize: 1000,
		}
		
		_, err := pe.ListResources(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGetResource(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	// Setup: create resources for getting
	const setupCount = 1000
	resources := make([]*v2.Resource, setupCount)
	for i := 0; i < setupCount; i++ {
		resources[i] = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "benchmark-get-type",
				Resource:     fmt.Sprintf("get-resource-%d", i),
			},
			DisplayName: fmt.Sprintf("Get Resource %d", i),
		}
	}
	
	_, err := pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.PutResources(ctx, resources...)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.EndSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	// Benchmark the get operation
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		resourceIndex := i % setupCount
		req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
			ResourceId: &v2.ResourceId{
				ResourceType: "benchmark-get-type",
				Resource:     fmt.Sprintf("get-resource-%d", resourceIndex),
			},
		}
		
		_, err := pe.GetResource(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkPutGrants(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	grants := make([]*v2.Grant, b.N)
	for i := 0; i < b.N; i++ {
		grants[i] = &v2.Grant{
			Id: fmt.Sprintf("benchmark-grant-%d", i),
			Entitlement: &v2.Entitlement{
				Id: "benchmark-entitlement",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     fmt.Sprintf("user-%d", i%1000),
				},
			},
		}
	}
	
	_, err := pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	
	err = pe.PutGrants(ctx, grants...)
	if err != nil {
		b.Fatal(err)
	}
	
	b.StopTimer()
	pe.EndSync(ctx)
}

func benchmarkListGrants(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	// Setup: create grants
	const setupCount = 10000
	grants := make([]*v2.Grant, setupCount)
	for i := 0; i < setupCount; i++ {
		grants[i] = &v2.Grant{
			Id: fmt.Sprintf("list-grant-%d", i),
			Entitlement: &v2.Entitlement{
				Id: "list-entitlement",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     fmt.Sprintf("list-user-%d", i%1000),
				},
			},
		}
	}
	
	_, err := pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.PutGrants(ctx, grants...)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.EndSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	// Benchmark listing
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		req := &v2.GrantsServiceListGrantsRequest{
			PageSize: 1000,
		}
		
		_, err := pe.ListGrants(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkListGrantsForPrincipal(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	// Setup: create grants for specific principals
	const setupCount = 10000
	const numPrincipals = 100
	
	grants := make([]*v2.Grant, setupCount)
	for i := 0; i < setupCount; i++ {
		grants[i] = &v2.Grant{
			Id: fmt.Sprintf("principal-grant-%d", i),
			Entitlement: &v2.Entitlement{
				Id: "principal-entitlement",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     fmt.Sprintf("principal-user-%d", i%numPrincipals),
				},
			},
		}
	}
	
	_, err := pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.PutGrants(ctx, grants...)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.EndSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	// Benchmark listing grants for specific principal
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		principalIndex := i % numPrincipals
		req := &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
			PrincipalId: &v2.ResourceId{
				ResourceType: "user",
				Resource:     fmt.Sprintf("principal-user-%d", principalIndex),
			},
			PageSize: 1000,
		}
		
		_, err := pe.ListGrantsForPrincipal(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkFullSyncCycle(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	const itemsPerSync = 1000
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		
		_, err := pe.StartNewSync(ctx)
		if err != nil {
			b.Fatal(err)
		}
		
		// Add some data
		resources := make([]*v2.Resource, itemsPerSync)
		for j := 0; j < itemsPerSync; j++ {
			resources[j] = &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "sync-type",
					Resource:     fmt.Sprintf("sync-%d-resource-%d", i, j),
				},
				DisplayName: fmt.Sprintf("Sync %d Resource %d", i, j),
			}
		}
		
		err = pe.PutResources(ctx, resources...)
		if err != nil {
			b.Fatal(err)
		}
		
		err = pe.EndSync(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkConditionalUpserts(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	// Setup: create initial resources
	const setupCount = 1000
	
	_, err := pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	resources := make([]*v2.Resource, setupCount)
	for i := 0; i < setupCount; i++ {
		resources[i] = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "conditional-type",
				Resource:     fmt.Sprintf("conditional-resource-%d", i),
			},
			DisplayName: fmt.Sprintf("Conditional Resource %d", i),
		}
	}
	
	err = pe.PutResources(ctx, resources...)
	if err != nil {
		b.Fatal(err)
	}
	
	err = pe.EndSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	// Start a new sync for conditional upserts
	_, err = pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	// Benchmark conditional upserts
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		resourceIndex := i % setupCount
		updatedResource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "conditional-type",
				Resource:     fmt.Sprintf("conditional-resource-%d", resourceIndex),
			},
			DisplayName: fmt.Sprintf("Updated Conditional Resource %d - %d", resourceIndex, i),
		}
		
		err := pe.PutResourcesIfNewer(ctx, updatedResource)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkWithBatchSize(b *testing.B, ctx context.Context, batchSize int) {
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("pebble_benchmark_batch_%d", batchSize))
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	pe, err := NewPebbleEngine(ctx, tempDir,
		WithBatchSizeLimit(batchSize),
		WithCacheSize(64<<20),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer pe.Close()
	
	// Create enough resources to trigger multiple batches
	const totalItems = 10000
	resources := make([]*v2.Resource, totalItems)
	
	for i := 0; i < totalItems; i++ {
		resources[i] = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "batch-type",
				Resource:     fmt.Sprintf("batch-resource-%d", i),
			},
			DisplayName: fmt.Sprintf("Batch Resource %d", i),
		}
	}
	
	_, err = pe.StartNewSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		err = pe.PutResources(ctx, resources...)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	b.StopTimer()
	pe.EndSync(ctx)
}

// BenchmarkMemoryUsage benchmarks memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	ctx := context.Background()
	
	tempDir, err := os.MkdirTemp("", "pebble_memory_benchmark")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	pe, err := NewPebbleEngine(ctx, tempDir,
		WithCacheSize(32<<20), // Smaller cache to test memory pressure
		WithBatchSizeLimit(1000),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer pe.Close()
	
	b.Run("LargeDatasetMemory", func(b *testing.B) {
		benchmarkLargeDatasetMemory(b, ctx, pe)
	})
}

func benchmarkLargeDatasetMemory(b *testing.B, ctx context.Context, pe *PebbleEngine) {
	const itemsPerIteration = 10000
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := pe.StartNewSync(ctx)
		if err != nil {
			b.Fatal(err)
		}
		
		// Create large resources with substantial data
		resources := make([]*v2.Resource, itemsPerIteration)
		for j := 0; j < itemsPerIteration; j++ {
			// Add some bulk to test memory usage
			description := fmt.Sprintf("This is a large description for resource %d in iteration %d. ", j, i)
			description = description + description + description // Make it larger
			
			resources[j] = &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "memory-type",
					Resource:     fmt.Sprintf("memory-resource-%d-%d", i, j),
				},
				DisplayName: fmt.Sprintf("Memory Resource %d", j),
				Description: description,
			}
		}
		
		err = pe.PutResources(ctx, resources...)
		if err != nil {
			b.Fatal(err)
		}
		
		// Force some reads to test memory usage during reads
		listReq := &v2.ResourcesServiceListResourcesRequest{
			PageSize: 1000,
		}
		
		_, err = pe.ListResources(ctx, listReq)
		if err != nil {
			b.Fatal(err)
		}
		
		err = pe.EndSync(ctx)
		if err != nil {
			b.Fatal(err)
		}
		
		// Force cleanup to test memory cleanup
		if i%10 == 0 {
			err = pe.Cleanup(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}