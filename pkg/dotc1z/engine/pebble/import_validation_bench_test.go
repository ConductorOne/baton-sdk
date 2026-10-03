package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// BenchmarkImportValidation measures the open-time imported-digest
// validation (WithImportValidation, grant_digest_import_validation.go):
// O(grant primaries + index/digest nodes), paid ONCE per imported-artifact
// open. Vary partitions x grants to expose the linear curve — the cost the
// PR body states so operators know what a hostile-input-safe open adds.
func BenchmarkImportValidation(b *testing.B) {
	cases := []struct {
		partitions int
		grants     int
	}{
		{1, 100},
		{1, 10_000},
		{10, 1_000},
		{100, 1_000},
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("partitions=%d,grants=%d", tc.partitions, tc.grants), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			e, dir := newTestEngine(b)
			a := NewAdapter(e)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(b, err)
			for p := 0; p < tc.partitions; p++ {
				entID := fmt.Sprintf("ent-%03d", p)
				putEnt(b, e, ctx, entID)
				require.NoError(b, e.PutGrantRecords(ctx, makeTestGrants(entID, tc.grants)...))
			}
			require.NoError(b, a.EndSync(ctx))
			require.NoError(b, e.Close())
			b.StartTimer()

			b.ReportAllocs()
			for b.Loop() {
				e2, err := Open(ctx, dir)
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				if err := e2.Close(); err != nil {
					b.Fatalf("close: %v", err)
				}
			}
		})
	}
}

// BenchmarkEndSyncFastPath pins the trusted-seal fast path's cost curve
// (thread 4170953216): repeated calls over an already-digested engine
// (the EndSync repair fast path — RepairMissingGrantDigests) must stay
// a SINGLE point-Get — O(1) in digest nodes, not the O(keyspace) scan
// the PR's interim fold-consistency check paid. Scaled grant counts
// show the flat curve; the removed fold check made this linear.
func BenchmarkEndSyncFastPath(b *testing.B) {
	for _, grants := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("grants=%d", grants), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			e, _ := newTestEngine(b)
			a := NewAdapter(e)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(b, err)
			putEnt(b, e, ctx, "ent-A")
			require.NoError(b, e.PutGrantRecords(ctx, makeTestGrants("ent-A", grants)...))
			require.NoError(b, a.EndSync(ctx))
			b.StartTimer()

			b.ReportAllocs()
			for b.Loop() {
				if err := e.RepairMissingGrantDigests(ctx); err != nil {
					b.Fatalf("RepairMissingGrantDigests: %v", err)
				}
			}
		})
	}
}
