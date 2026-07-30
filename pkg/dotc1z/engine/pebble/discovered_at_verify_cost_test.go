package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// pageSizeFor returns a page size comfortably larger than n. n is a small,
// test-controlled grant count, so the int->uint32 conversion cannot overflow.
func pageSizeFor(n int) uint32 { return uint32(n + 10) } //nolint:gosec // small test-controlled count

// seedNGrants writes n grants under ent-A with distinct principals, each with
// a distinct discovered_at. Returns the engine and its v3 reader.
func seedNGrants(ctx context.Context, t testing.TB, n int) (*Engine, reader_v3.GrantsReaderServiceServer, reader_v2.GrantsReaderServiceServer) {
	e, err := Open(ctx, t.TempDir()+"/engine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, "full", "")
	require.NoError(t, err)
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		rec := v3.GrantRecord_builder{
			ExternalId: fmt.Sprintf("g-%06d", i),
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
			}.Build(),
			Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: fmt.Sprintf("p-%06d", i)}.Build(),
			DiscoveredAt: timestamppb.New(base.Add(time.Duration(i) * time.Second)),
		}.Build()
		require.NoError(t, e.PutGrantRecord(ctx, rec))
	}
	return e, e.V3GrantReader(), e
}

// === V-17: cost contract (measured/sampled — never closure) ===
//
// Reading discovered_at must add no extra full-store pass and no per-grant
// point lookup: work is O(N) in returned grants. The v3 read path is the SAME
// prefix scan as v2 minus the V3GrantToV2 downshift, so v3 must be no more
// allocation-hungry than v2, and allocations must grow ~linearly in N (a
// per-grant point-get or an extra full pass would show super-linear growth).
func TestV3ReadCostLinearNoPerGrantPointGet(t *testing.T) {
	if testing.Short() {
		t.Skip("cost sampling skipped in -short")
	}
	ctx := context.Background()

	measure := func(n int) float64 {
		_, r3, _ := seedNGrants(ctx, t, n)
		req := reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: v3TestEntStub("ent-A"),
			PageSize:    pageSizeFor(n),
		}.Build()
		return testing.AllocsPerRun(20, func() {
			resp, err := r3.ListGrantsForEntitlement(ctx, req)
			if err != nil || len(resp.GetList()) != n {
				t.Fatalf("read n=%d: err=%v len=%d", n, err, len(resp.GetList()))
			}
		})
	}

	a1 := measure(100)
	a2 := measure(200)
	// Linear growth: doubling N should ~double allocs, not quadruple. Allow a
	// generous ceiling (2.6x) to absorb fixed per-call overhead and noise; a
	// per-grant point-get / extra full pass would blow past this.
	require.Positivef(t, a1, "n=100 allocs must be measurable")
	ratio := a2 / a1
	require.Lessf(t, ratio, 2.6,
		"alloc growth 100->200 was %.2fx (a1=%.0f a2=%.0f) — expected ~linear; super-linear implies a hidden per-grant point-get or extra full pass",
		ratio, a1, a2)
	t.Logf("V-17 alloc sampling: n=100 -> %.0f allocs/op, n=200 -> %.0f allocs/op (%.2fx)", a1, a2, ratio)
}

// BenchmarkV3VsV2ListGrantsForEntitlement compares the rich v3 read against the
// lossy v2 read over the same fixture. v3 skips the V3GrantToV2 downshift, so it
// must not be slower/heavier than v2 — evidence that returning discovered_at
// adds no hot-path work beyond decoding the field it already read.
func BenchmarkV3VsV2ListGrantsForEntitlement(b *testing.B) {
	ctx := context.Background()
	const n = 1000
	_, r3, r2 := seedNGrants(ctx, b, n)
	entV3 := v3TestEntStub("ent-A")

	b.Run("v3", func(b *testing.B) {
		req := reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: entV3, PageSize: pageSizeFor(n),
		}.Build()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := r3.ListGrantsForEntitlement(ctx, req)
			if err != nil || len(resp.GetList()) != n {
				b.Fatalf("v3 read: err=%v len=%d", err, len(resp.GetList()))
			}
		}
	})

	b.Run("v2", func(b *testing.B) {
		req := reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: entV3, PageSize: pageSizeFor(n),
		}.Build()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := r2.ListGrantsForEntitlement(ctx, req)
			if err != nil || len(resp.GetList()) != n {
				b.Fatalf("v2 read: err=%v len=%d", err, len(resp.GetList()))
			}
		}
	})
}
