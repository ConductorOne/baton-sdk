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

// listGrantsFixture bundles everything the BenchmarkListGrants* benches
// need from setupListGrantsBench, so each bench can construct its
// request without re-fetching the entitlement/resource.
type listGrantsFixture struct {
	F               *C1File
	EntitlementID   string
	Resource        *v2.Resource // resource the entitlement is on, used for the resource-filtered ListGrants
	PrincipalSample *v2.Resource // one of the N principals, used for ListGrantsForPrincipal
	ResourceTypeID  string       // "test-type"
}

// setupListGrantsBench constructs a fresh c1z with numGrants grants,
// 1 entitlement, and N unique principals — same fixture shape as
// setupBenchmarkDB, but returns additional fields the new benches
// need (the resource for filtered ListGrants, a sample principal for
// ListGrantsForPrincipal, the resource-type id).
//
// extraOpts are appended to the default C1ZOptions and let callers
// flip writer modes (e.g. WithV2GrantsWriter(true) for the slim
// sub-benches). The default — no extra opts — is the full-blob
// writer, equivalent to the bench code on origin/main.
func setupListGrantsBench(b *testing.B, numGrants int, extraOpts ...C1ZOption) (*listGrantsFixture, func()) {
	b.Helper()

	ctx := b.Context()
	tempDir, err := os.MkdirTemp("", "grants-listing-bench-*")
	require.NoError(b, err)

	testFilePath := filepath.Join(tempDir, "bench.c1z")

	opts := []C1ZOption{
		WithTmpDir(tempDir),
		WithEncoderConcurrency(0),
		WithDecoderOptions(WithDecoderConcurrency(0)),
	}
	opts = append(opts, extraOpts...)

	f, err := newC1ZFile(ctx, testFilePath, opts...)
	require.NoError(b, err)

	syncID, _, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)
	require.NotEmpty(b, syncID)

	require.NoError(b, f.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          "test-type",
		DisplayName: "Test Type",
	}))

	testResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}
	require.NoError(b, f.PutResources(ctx, testResource))

	testEntitlement := &v2.Entitlement{
		Id:       "test-entitlement",
		Resource: testResource,
	}
	require.NoError(b, f.PutEntitlements(ctx, testEntitlement))

	grants := make([]*v2.Grant, numGrants)
	var principalSample *v2.Resource
	for i := range numGrants {
		principal := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "test-type",
				Resource:     fmt.Sprintf("principal-%d", i),
			},
			DisplayName: fmt.Sprintf("Principal %d", i),
		}
		grants[i] = &v2.Grant{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: testEntitlement,
			Principal:   principal,
		}
		if i == 0 {
			principalSample = principal
		}
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

	return &listGrantsFixture{
		F:               f,
		EntitlementID:   testEntitlement.Id,
		Resource:        testResource,
		PrincipalSample: principalSample,
		ResourceTypeID:  "test-type",
	}, cleanup
}

// drainPaginatedListGrants paginates ListGrants until exhaustion and
// returns the total row count. Used by the unfiltered + resource-
// filtered ListGrants benches.
func drainPaginatedListGrants(b *testing.B, fix *listGrantsFixture, withResourceFilter bool) int {
	b.Helper()
	ctx := b.Context()

	total := 0
	pageToken := ""
	for {
		req := v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}
		if withResourceFilter {
			req.Resource = fix.Resource
		}
		resp, err := fix.F.ListGrants(ctx, req.Build())
		if err != nil {
			b.Fatal(err)
		}
		total += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return total
}

// BenchmarkListGrants benchmarks the unfiltered public ListGrants
// reader — the path baton CLI's default `baton grants` invocation
// hits, and the c1 uplift fallback path when the store does not
// implement InternalWriter.
func BenchmarkListGrants(b *testing.B) {
	for _, numGrants := range grantCounts {
		b.Run(fmt.Sprintf("grants=%d", numGrants), func(b *testing.B) {
			b.Run("writer=full", func(b *testing.B) {
				fix, cleanup := setupListGrantsBench(b, numGrants)
				defer cleanup()

				b.ReportAllocs()
				for b.Loop() {
					if got := drainPaginatedListGrants(b, fix, false); got != numGrants {
						b.Fatalf("expected %d grants, got %d", numGrants, got)
					}
				}
			})
			b.Run("writer=slim", func(b *testing.B) {
				fix, cleanup := setupListGrantsBench(b, numGrants, WithV2GrantsWriter(true))
				defer cleanup()

				b.ReportAllocs()
				for b.Loop() {
					if got := drainPaginatedListGrants(b, fix, false); got != numGrants {
						b.Fatalf("expected %d grants, got %d", numGrants, got)
					}
				}
			})
		})
	}
}

// BenchmarkListGrants_ResourceFilter exercises ListGrants with the
// Resource filter set — the UI drill-down path for "grants on this
// resource". Same SQL backbone, different filter.
func BenchmarkListGrants_ResourceFilter(b *testing.B) {
	for _, numGrants := range grantCounts {
		b.Run(fmt.Sprintf("grants=%d", numGrants), func(b *testing.B) {
			b.Run("writer=full", func(b *testing.B) {
				fix, cleanup := setupListGrantsBench(b, numGrants)
				defer cleanup()

				b.ReportAllocs()
				for b.Loop() {
					if got := drainPaginatedListGrants(b, fix, true); got != numGrants {
						b.Fatalf("expected %d grants, got %d", numGrants, got)
					}
				}
			})
			b.Run("writer=slim", func(b *testing.B) {
				fix, cleanup := setupListGrantsBench(b, numGrants, WithV2GrantsWriter(true))
				defer cleanup()

				b.ReportAllocs()
				for b.Loop() {
					if got := drainPaginatedListGrants(b, fix, true); got != numGrants {
						b.Fatalf("expected %d grants, got %d", numGrants, got)
					}
				}
			})
		})
	}
}

// BenchmarkListGrantsForPrincipal benchmarks the principal-filtered
// reader path used by UI "what does this user have?" lookups. Note
// the request type is GrantsReaderServiceListGrantsForEntitlementRequest
// with the PrincipalId field set — not a separately-named
// principal-request struct. The fixture has unique principals, so
// each call returns exactly 1 row.
func BenchmarkListGrantsForPrincipal(b *testing.B) {
	runForPrincipal := func(b *testing.B, numGrants int, extraOpts ...C1ZOption) {
		fix, cleanup := setupListGrantsBench(b, numGrants, extraOpts...)
		defer cleanup()

		ctx := b.Context()

		req := reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			PrincipalId: fix.PrincipalSample.Id,
		}.Build()

		b.ReportAllocs()
		for b.Loop() {
			pageToken := ""
			total := 0
			for {
				req.SetPageToken(pageToken)
				resp, err := fix.F.ListGrantsForPrincipal(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				total += len(resp.GetList())
				pageToken = resp.GetNextPageToken()
				if pageToken == "" {
					break
				}
			}
			if total != 1 {
				b.Fatalf("expected 1 grant for sample principal, got %d", total)
			}
		}
	}

	for _, numGrants := range grantCounts {
		b.Run(fmt.Sprintf("grants=%d", numGrants), func(b *testing.B) {
			b.Run("writer=full", func(b *testing.B) {
				runForPrincipal(b, numGrants)
			})
			b.Run("writer=slim", func(b *testing.B) {
				runForPrincipal(b, numGrants, WithV2GrantsWriter(true))
			})
		})
	}
}

// BenchmarkListGrantsForResourceType benchmarks the resource-type-
// filtered reader path used by admin/audit views ("all grants on
// resources of type X"). The fixture's entitlement and all
// principals share resource-type "test-type", so the filter matches
// every row.
func BenchmarkListGrantsForResourceType(b *testing.B) {
	runForResourceType := func(b *testing.B, numGrants int, extraOpts ...C1ZOption) {
		fix, cleanup := setupListGrantsBench(b, numGrants, extraOpts...)
		defer cleanup()

		ctx := b.Context()

		req := reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
			ResourceTypeId: fix.ResourceTypeID,
		}.Build()

		b.ReportAllocs()
		for b.Loop() {
			pageToken := ""
			total := 0
			for {
				req.SetPageToken(pageToken)
				resp, err := fix.F.ListGrantsForResourceType(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				total += len(resp.GetList())
				pageToken = resp.GetNextPageToken()
				if pageToken == "" {
					break
				}
			}
			if total != numGrants {
				b.Fatalf("expected %d grants, got %d", numGrants, total)
			}
		}
	}

	for _, numGrants := range grantCounts {
		b.Run(fmt.Sprintf("grants=%d", numGrants), func(b *testing.B) {
			b.Run("writer=full", func(b *testing.B) {
				runForResourceType(b, numGrants)
			})
			b.Run("writer=slim", func(b *testing.B) {
				runForResourceType(b, numGrants, WithV2GrantsWriter(true))
			})
		})
	}
}

// BenchmarkListGrants_Payload benchmarks the gRPC ListGrants surface
// (the closest remaining payload-shaped reader after RFC 0002 removed
// the internal payload-only mode). It is a sibling to
// BenchmarkListGrantsInternal_PayloadWithExpansion in grants_bench_test.go;
// ListGrants routes through listGrantsGeneric, a narrower SELECT than
// the with-expansion path because it skips the expansion column
// projection and the per-row expansion unmarshal.
func BenchmarkListGrants_Payload(b *testing.B) {
	runPayload := func(b *testing.B, numGrants int, extraOpts ...C1ZOption) {
		fix, cleanup := setupListGrantsBench(b, numGrants, extraOpts...)
		defer cleanup()

		ctx := b.Context()

		b.ReportAllocs()
		for b.Loop() {
			pageToken := ""
			total := 0
			for {
				resp, err := fix.F.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
					PageToken: pageToken,
				}.Build())
				if err != nil {
					b.Fatal(err)
				}
				total += len(resp.GetList())
				pageToken = resp.GetNextPageToken()
				if pageToken == "" {
					break
				}
			}
			if total != numGrants {
				b.Fatalf("expected %d grants, got %d", numGrants, total)
			}
		}
	}

	for _, numGrants := range grantCounts {
		b.Run(fmt.Sprintf("grants=%d", numGrants), func(b *testing.B) {
			b.Run("writer=full", func(b *testing.B) {
				runPayload(b, numGrants)
			})
			b.Run("writer=slim", func(b *testing.B) {
				runPayload(b, numGrants, WithV2GrantsWriter(true))
			})
		})
	}
}
