package dotc1z

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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

// setupExternalMatchBenchmarkDB seeds a c1z with numGrants total grants, of which
// approximately matchPercent percent carry an ExternalResourceMatchID annotation.
// The syncer's processGrantsWithExternalPrincipals iterates these rows via the
// PayloadWithExpansion internal listing mode.
func setupExternalMatchBenchmarkDB(b *testing.B, numGrants int, matchPercent int) (*C1File, string, func()) {
	b.Helper()

	ctx := b.Context()
	tempDir, err := os.MkdirTemp("", "grants-extmatch-bench-*")
	require.NoError(b, err)

	testFilePath := filepath.Join(tempDir, "bench.c1z")

	opts := []C1ZOption{
		WithTmpDir(tempDir),
		WithEncoderConcurrency(0),
		WithDecoderOptions(WithDecoderConcurrency(0)),
	}

	f, err := NewC1ZFile(ctx, testFilePath, opts...)
	require.NoError(b, err)

	syncID, _, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)
	require.NotEmpty(b, syncID)

	require.NoError(b, f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "test-type", DisplayName: "Test Type"}.Build()))
	testResource := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "test-type", Resource: "test-resource"}.Build(),
		DisplayName: "Test Resource",
	}.Build()
	require.NoError(b, f.PutResources(ctx, testResource))

	testEntitlement := v2.Entitlement_builder{Id: "test-entitlement", Resource: testResource}.Build()
	require.NoError(b, f.PutEntitlements(ctx, testEntitlement))

	// Use a deterministic stride so matchPercent% of rows get the annotation.
	// stride = 100/matchPercent (e.g. 5% -> every 20th grant).
	var stride int
	if matchPercent > 0 {
		stride = 100 / matchPercent
		if stride < 1 {
			stride = 1
		}
	}

	grants := make([]*v2.Grant, numGrants)
	for i := range numGrants {
		gb := v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: testEntitlement,
			Principal: v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "test-type", Resource: fmt.Sprintf("principal-%d", i)}.Build(),
				DisplayName: fmt.Sprintf("Principal %d", i),
			}.Build(),
		}
		if stride > 0 && i%stride == 0 {
			gb.Annotations = annotations.New(v2.ExternalResourceMatchID_builder{
				Id: fmt.Sprintf("ext-%d", i),
			}.Build())
		}
		grants[i] = gb.Build()
	}

	batchSize := 1000
	for i := 0; i < len(grants); i += batchSize {
		end := min(i+batchSize, len(grants))
		require.NoError(b, f.PutGrants(ctx, grants[i:end]...))
	}

	cleanup := func() {
		_ = f.Close(ctx)
		_ = os.RemoveAll(tempDir)
	}

	return f, syncID, cleanup
}

// BenchmarkListGrantsWithExpansion_ExternalMatchOnly measures the alloc/op
// reduction from pushing the ExternalResourceMatch annotation filter down to
// SQL. Two sub-benchmarks per size/density: the filtered path (what production
// now runs) and the kill-switch path (pre-fix behavior, for comparison).
func BenchmarkListGrantsWithExpansion_ExternalMatchOnly(b *testing.B) {
	// Paired with processGrantsWithExternalPrincipals — realistic mixes are
	// small-fraction matches amid large grant totals. 1%/5%/20% spans the
	// expected range; 100% pins the upper bound where filter offers no win.
	densities := []int{1, 5, 20, 100}
	sizes := []int{10_000, 50_000}

	for _, n := range sizes {
		for _, pct := range densities {
			b.Run(fmt.Sprintf("n=%d/match=%d%%/filtered", n, pct), func(b *testing.B) {
				f, _, cleanup := setupExternalMatchBenchmarkDB(b, n, pct)
				defer cleanup()
				ctx := b.Context()

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					pageToken := ""
					total := 0
					for {
						resp, err := f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
							Mode:              connectorstore.GrantListModePayloadWithExpansion,
							ExternalMatchOnly: true,
							PageToken:         pageToken,
						})
						if err != nil {
							b.Fatal(err)
						}
						total += len(resp.Rows)
						pageToken = resp.NextPageToken
						if pageToken == "" {
							break
						}
					}
					_ = total
				}
			})

			b.Run(fmt.Sprintf("n=%d/match=%d%%/killswitch", n, pct), func(b *testing.B) {
				f, _, cleanup := setupExternalMatchBenchmarkDB(b, n, pct)
				defer cleanup()
				ctx := b.Context()

				orig := externalMatchFilterDisabled
				externalMatchFilterDisabled = true
				defer func() { externalMatchFilterDisabled = orig }()

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					pageToken := ""
					total := 0
					for {
						resp, err := f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
							Mode:              connectorstore.GrantListModePayloadWithExpansion,
							ExternalMatchOnly: true,
							PageToken:         pageToken,
						})
						if err != nil {
							b.Fatal(err)
						}
						total += len(resp.Rows)
						pageToken = resp.NextPageToken
						if pageToken == "" {
							break
						}
					}
					_ = total
				}
			})
		}
	}
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
