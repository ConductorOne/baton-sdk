package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// BenchmarkRollbackExpansionPebble times the rollback write over a finished
// sync of 100k grants, nine in ten expander-derived. Run with:
//
//	go test ./pkg/dotc1z/engine/pebble/ -run x -bench BenchmarkRollbackExpansionPebble -benchtime 3x -benchmem
func BenchmarkRollbackExpansionPebble(b *testing.B) {
	const grantCount = 100_000
	const derivedEvery = 10
	ctx := context.Background()
	b.ReportAllocs()
	var elapsed time.Duration
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e, err := Open(ctx, b.TempDir())
		require.NoError(b, err)
		syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(b, err)
		const chunk = 5_000
		batch := make([]*v3.GrantRecord, 0, chunk)
		for g := 0; g < grantCount; g++ {
			principal := fmt.Sprintf("u%07d", g)
			if g%derivedEvery == 0 {
				batch = append(batch, rollbackTestGrant("ent-B", principal, false, "ent-B"))
			} else {
				batch = append(batch, rollbackTestGrant("ent-A", principal, true, "ent-B"))
			}
			if len(batch) == chunk {
				require.NoError(b, e.PutGrantRecords(ctx, batch...))
				batch = batch[:0]
			}
		}
		require.NoError(b, e.EndSync(ctx))
		b.StartTimer()

		start := time.Now()
		res, err := e.RollbackExpansion(ctx, syncID, false, false)
		elapsed += time.Since(start)

		b.StopTimer()
		require.NoError(b, err)
		require.Equal(b, grantCount-grantCount/derivedEvery, res.GrantsDeleted)
		require.Equal(b, grantCount/derivedEvery, res.SourcesCleared)
		require.NoError(b, e.Close())
		b.StartTimer()
	}
	b.ReportMetric(float64(grantCount*b.N)/elapsed.Seconds(), "rows/s")
}
