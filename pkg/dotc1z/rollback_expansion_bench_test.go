package dotc1z

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// buildLargeExpandedC1Z writes a finished, expansion-marked c1z holding
// grantCount expander-derived grants (Sources keyed only to a foreign
// entitlement, GrantImmutable set) so a rollback deletes every one of them —
// the heaviest write path. Returns the saved .c1z path and the sync id.
func buildLargeExpandedC1Z(tb testing.TB, ctx context.Context, dir string, grantCount int) (string, string) {
	tb.Helper()
	path := filepath.Join(dir, "large.c1z")
	f, err := NewC1ZFile(ctx, path)
	require.NoError(tb, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(tb, err)

	require.NoError(tb, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
	))
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	require.NoError(tb, f.PutResources(ctx, g1))
	g1Member := v2.Entitlement_builder{Id: "group:g1:member", DisplayName: "g1 member", Resource: g1, Slug: "member"}.Build()
	require.NoError(tb, f.PutEntitlements(ctx, g1Member))

	immutableAnno, err := anypb.New(v2.GrantImmutable_builder{}.Build())
	require.NoError(tb, err)

	const chunk = 2000
	batch := make([]*v2.Grant, 0, chunk)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		require.NoError(tb, f.PutGrants(ctx, batch...))
		batch = batch[:0]
	}
	for i := 0; i < grantCount; i++ {
		principal := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
		}.Build()
		batch = append(batch, v2.Grant_builder{
			Id:          fmt.Sprintf("group:g1:member:user:u%d", i),
			Entitlement: g1Member,
			Principal:   principal,
			Annotations: []*anypb.Any{immutableAnno},
			Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
				"group:g2:member": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
			}}.Build(),
		}.Build())
		if len(batch) == chunk {
			flush()
		}
	}
	flush()

	require.NoError(tb, f.SetSupportsDiff(ctx, syncID))
	require.NoError(tb, f.EndSync(ctx))
	require.NoError(tb, f.Close(ctx))
	return path, syncID
}

func copyFileBench(tb testing.TB, src, dst string) {
	tb.Helper()
	in, err := os.Open(src)
	require.NoError(tb, err)
	defer in.Close()
	out, err := os.Create(dst)
	require.NoError(tb, err)
	_, err = io.Copy(out, in)
	require.NoError(tb, err)
	require.NoError(tb, out.Close())
}

// rollbackJournalBytes opens a fresh copy of srcPath, deletes pageSize grant
// rows inside one open transaction, and returns the size of the sqlite
// rollback journal that transaction produced before it commits. Because each
// page commits in its own transaction, this is the journal high-water mark the
// page size governs.
func rollbackJournalBytes(tb testing.TB, ctx context.Context, srcPath, dir string, pageSize int) int64 {
	tb.Helper()
	work := filepath.Join(dir, fmt.Sprintf("journal-%d.c1z", pageSize))
	copyFileBench(tb, srcPath, work)
	store, err := NewC1ZFile(ctx, work)
	require.NoError(tb, err)
	defer func() { _ = store.Close(ctx) }()

	tx, err := store.db.BeginTx(ctx, nil)
	require.NoError(tb, err)
	tableName := grants.Name()
	for i := 0; i < pageSize; i++ {
		_, err := tx.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE external_id = ?", tableName),
			fmt.Sprintf("group:g1:member:user:u%d", i))
		require.NoError(tb, err)
	}
	fi, err := os.Stat(store.dbFilePath + "-journal")
	require.NoError(tb, err)
	size := fi.Size()
	require.NoError(tb, tx.Rollback())
	return size
}

// BenchmarkRollbackExpansionPageSize measures the rollback write path at the
// production page size against a 10x larger page, reporting wall time and the
// rollback-journal size each page transaction produces. Run with:
//
//	go test ./pkg/dotc1z/ -run x -bench BenchmarkRollbackExpansionPageSize -benchtime 3x
func BenchmarkRollbackExpansionPageSize(b *testing.B) {
	ctx := context.Background()
	const grantCount = 40000
	srcDir := b.TempDir()
	srcPath, syncID := buildLargeExpandedC1Z(b, ctx, srcDir, grantCount)

	original := rollbackPageSize
	b.Cleanup(func() { rollbackPageSize = original })

	for _, pageSize := range []int{1000, 10000} {
		journalKiB := float64(rollbackJournalBytes(b, ctx, srcPath, b.TempDir(), pageSize)) / 1024.0
		b.Run(fmt.Sprintf("pageSize=%d", pageSize), func(b *testing.B) {
			rollbackPageSize = pageSize
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				workPath := filepath.Join(b.TempDir(), fmt.Sprintf("work-%d.c1z", i))
				copyFileBench(b, srcPath, workPath)
				store, err := NewC1ZFile(ctx, workPath)
				require.NoError(b, err)
				b.StartTimer()

				res, err := store.RollbackExpansion(ctx, syncID, false)

				b.StopTimer()
				require.NoError(b, err)
				require.Equal(b, grantCount, res.GrantsDeleted)
				_ = store.Close(ctx)
				b.StartTimer()
			}
			b.ReportMetric(journalKiB, "journal_KiB/page-tx")
			b.ReportMetric(float64(grantCount), "grants")
		})
	}
}
