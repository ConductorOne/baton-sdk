package dotc1z

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

var grantCounts = []int{100, 1000, 10000, 100000}

// setupBenchmarkDB creates a test database with the specified number of grants.
func setupBenchmarkDB(b *testing.B, numGrants int) (*C1File, string, func()) {
	b.Helper()

	ctx := b.Context()
	tempDir, err := os.MkdirTemp("", "grants-bench-*")
	require.NoError(b, err)

	testFilePath := filepath.Join(tempDir, "bench.c1z")

	opts := []C1ZOption{
		WithTmpDir(tempDir),
		WithEncoderConcurrency(0),
		WithDecoderOptions(WithDecoderConcurrency(0)),
	}

	f, err := NewC1ZFile(ctx, testFilePath, opts...)
	require.NoError(b, err)

	// Start a sync
	syncID, _, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)
	require.NotEmpty(b, syncID)

	// Create a resource type
	err = f.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          "test-type",
		DisplayName: "Test Type",
	})
	require.NoError(b, err)

	// Create a resource for the entitlement
	testResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}
	err = f.PutResources(ctx, testResource)
	require.NoError(b, err)

	// Create an entitlement
	testEntitlement := &v2.Entitlement{
		Id:       "test-entitlement",
		Resource: testResource,
	}
	err = f.PutEntitlements(ctx, testEntitlement)
	require.NoError(b, err)

	// Create grants
	grants := make([]*v2.Grant, numGrants)
	for i := range numGrants {
		grants[i] = &v2.Grant{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: testEntitlement,
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "test-type",
					Resource:     fmt.Sprintf("principal-%d", i),
				},
				DisplayName: fmt.Sprintf("Principal %d", i),
			},
		}
	}

	// Insert grants in batches
	batchSize := 1000
	for i := 0; i < len(grants); i += batchSize {
		end := min(i+batchSize, len(grants))
		err = f.PutGrants(ctx, grants[i:end]...)
		require.NoError(b, err)
	}

	cleanup := func() {
		_ = f.Close(ctx)
		_ = os.RemoveAll(tempDir)
	}

	return f, testEntitlement.Id, cleanup
}

// BenchmarkListGrantsForEntitlement benchmarks ListGrantsForEntitlement filtered by an entitlement.
func BenchmarkListGrantsForEntitlement(b *testing.B) {
	for _, numGrants := range grantCounts {
		b.Run(fmt.Sprintf("grants=%d", numGrants), func(b *testing.B) {
			f, entitlementID, cleanup := setupBenchmarkDB(b, numGrants)
			defer cleanup()

			ctx := b.Context()

			// Get the entitlement for the request
			entResp, err := f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
				EntitlementId: entitlementID,
			}.Build())
			require.NoError(b, err)

			req := reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement: entResp.GetEntitlement(),
			}.Build()

			b.ReportAllocs()

			for b.Loop() {
				pageToken := ""
				totalGrants := 0
				for {
					req.SetPageToken(pageToken)
					resp, err := f.ListGrantsForEntitlement(ctx, req)
					if err != nil {
						b.Fatal(err)
					}
					totalGrants += len(resp.GetList())
					pageToken = resp.GetNextPageToken()
					if pageToken == "" {
						break
					}
				}
				if totalGrants != numGrants {
					b.Fatalf("expected %d grants, got %d", numGrants, totalGrants)
				}
			}
		})
	}
}
