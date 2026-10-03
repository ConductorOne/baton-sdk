package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// BenchmarkImportValidation times validateImportedGrantDigestStateLocked
// alone, the cost every Open of digest-bearing state pays.
// It is O(grant primaries + index/digest nodes): ns/op should grow
// linearly with total grants and be flat in partition fan-out.
func BenchmarkImportValidation(b *testing.B) {
	cases := []struct {
		partitions int
		grants     int
	}{
		{1, 1_000},
		{1, 10_000},
		{1, 100_000},
		{100, 1_000},
		{1_000, 100},
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("partitions=%d,grants=%d", tc.partitions, tc.grants), func(b *testing.B) {
			ctx := context.Background()
			e, _ := newTestEngine(b)
			a := NewAdapter(e)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(b, err)
			for p := range tc.partitions {
				entID := fmt.Sprintf("ent-%04d", p)
				putEnt(b, e, ctx, entID)
				require.NoError(b, e.PutGrantRecords(ctx, makeTestGrants(entID, tc.grants)...))
			}
			require.NoError(b, a.EndSync(ctx))
			require.True(b, e.db.GrantDigestsPresent(), "precondition: the seal must build digest state to validate")

			b.ReportAllocs()
			for b.Loop() {
				if err := e.withWriteMu(func() error { return e.validateImportedGrantDigestStateLocked(ctx) }); err != nil {
					b.Fatalf("validate: %v", err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(tc.partitions*tc.grants), "ns/grant")
		})
	}
}

// BenchmarkEndSyncFastPath pins the EndSync repair fast path over an
// already-digested engine (RepairMissingGrantDigests): one point Get,
// so ns/op stays flat as grants grow.
func BenchmarkEndSyncFastPath(b *testing.B) {
	for _, grants := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("grants=%d", grants), func(b *testing.B) {
			ctx := context.Background()
			e, _ := newTestEngine(b)
			a := NewAdapter(e)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(b, err)
			putEnt(b, e, ctx, "ent-A")
			require.NoError(b, e.PutGrantRecords(ctx, makeTestGrants("ent-A", grants)...))
			require.NoError(b, a.EndSync(ctx))

			b.ReportAllocs()
			for b.Loop() {
				if err := e.RepairMissingGrantDigests(ctx); err != nil {
					b.Fatalf("RepairMissingGrantDigests: %v", err)
				}
			}
		})
	}
}
