package dotc1z

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

type diffBenchConfig struct {
	totalGrants int
	addedPct    int // % of grants that are new in the applied sync
	removedPct  int // % of grants that were removed from the applied sync
	modifiedPct int // % of grants that were modified in the applied sync
	expandable  bool
}

func (c diffBenchConfig) name() string {
	tag := ""
	if c.expandable {
		tag = "/expandable"
	}
	return fmt.Sprintf("grants=%d/add=%d%%/del=%d%%/mod=%d%%%s",
		c.totalGrants, c.addedPct, c.removedPct, c.modifiedPct, tag)
}

// buildDiffFixture creates two c1z files (old and new) that share a baseline of grants with the
// specified add/remove/modify ratios. It returns both files, the old sync ID, the new sync ID, and
// a cleanup function. The old file is opened with normal locking so it can be attached.
func buildDiffFixture(b *testing.B, cfg diffBenchConfig) (*C1File, *C1File, string, string, func()) {
	b.Helper()
	ctx := b.Context()

	tempDir, err := os.MkdirTemp("", "diff-bench-*")
	require.NoError(b, err)

	opts := []C1ZOption{
		WithTmpDir(tempDir),
		WithPragma("journal_mode", "WAL"),
		WithEncoderConcurrency(0),
		WithDecoderOptions(WithDecoderConcurrency(0)),
	}
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))

	oldPath := filepath.Join(tempDir, "old.c1z")
	newPath := filepath.Join(tempDir, "new.c1z")

	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(b, err)

	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(b, err)

	rt := &v2.ResourceType{Id: "user", DisplayName: "User"}
	ent := &v2.Entitlement{
		Id: "group:g1:member",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{ResourceType: "group", Resource: "g1"},
		},
	}
	ent2 := &v2.Entitlement{
		Id: "group:g2:member",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{ResourceType: "group", Resource: "g2"},
		},
	}

	total := cfg.totalGrants
	nAdded := total * cfg.addedPct / 100
	nRemoved := total * cfg.removedPct / 100
	nModified := total * cfg.modifiedPct / 100
	nUnchanged := total - nAdded - nRemoved - nModified

	// OLD sync
	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)
	require.NoError(b, oldFile.PutResourceTypes(ctx, rt))
	require.NoError(b, oldFile.PutEntitlements(ctx, ent, ent2))

	batchSize := 1000
	batch := make([]*v2.Grant, 0, batchSize)
	flush := func(f *C1File) {
		if len(batch) > 0 {
			require.NoError(b, f.PutGrants(ctx, batch...))
			batch = batch[:0]
		}
	}

	makeGrant := func(idx int, entitlement *v2.Entitlement, exp bool) *v2.Grant {
		g := v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", idx),
			Entitlement: entitlement,
			Principal: &v2.Resource{
				Id: &v2.ResourceId{ResourceType: "user", Resource: fmt.Sprintf("u%d", idx)},
			},
		}
		if exp {
			g.Annotations = annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{ent2.GetId()},
				Shallow:         false,
				ResourceTypeIds: []string{"user"},
			}.Build())
		}
		return g.Build()
	}

	id := 0

	// Grants that exist in both syncs unchanged
	for range nUnchanged {
		batch = append(batch, makeGrant(id, ent, cfg.expandable))
		id++
		if len(batch) >= batchSize {
			flush(oldFile)
		}
	}
	unchangedEnd := id

	// Grants that exist in old but are removed from new
	for range nRemoved {
		batch = append(batch, makeGrant(id, ent, cfg.expandable))
		id++
		if len(batch) >= batchSize {
			flush(oldFile)
		}
	}
	removedEnd := id

	// Grants that will be modified: old version uses ent, new version uses ent2
	for range nModified {
		batch = append(batch, makeGrant(id, ent, cfg.expandable))
		id++
		if len(batch) >= batchSize {
			flush(oldFile)
		}
	}
	modifiedEnd := id

	flush(oldFile)
	require.NoError(b, oldFile.SetSupportsDiff(ctx, oldSyncID))
	require.NoError(b, oldFile.EndSync(ctx))

	// NEW sync
	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)
	require.NoError(b, newFile.PutResourceTypes(ctx, rt))
	require.NoError(b, newFile.PutEntitlements(ctx, ent, ent2))

	// Unchanged grants (same as old)
	for i := range unchangedEnd {
		batch = append(batch, makeGrant(i, ent, cfg.expandable))
		if len(batch) >= batchSize {
			flush(newFile)
		}
	}

	// Skip removed grants (removedEnd - unchangedEnd grants absent)

	// Modified grants — change entitlement to ent2
	for i := removedEnd; i < modifiedEnd; i++ {
		batch = append(batch, makeGrant(i, ent2, cfg.expandable))
		if len(batch) >= batchSize {
			flush(newFile)
		}
	}

	// Added grants
	for range nAdded {
		batch = append(batch, makeGrant(id, ent, cfg.expandable))
		id++
		if len(batch) >= batchSize {
			flush(newFile)
		}
	}
	flush(newFile)

	require.NoError(b, newFile.EndSync(ctx))

	cleanup := func() {
		_ = oldFile.Close(ctx)
		_ = newFile.Close(ctx)
		_ = os.RemoveAll(tempDir)
	}

	return oldFile, newFile, oldSyncID, newSyncID, cleanup
}

func BenchmarkGenerateSyncDiffFromFile(b *testing.B) {
	configs := []diffBenchConfig{
		// Steady state: mostly unchanged
		{totalGrants: 1_000, addedPct: 1, removedPct: 1, modifiedPct: 1},
		{totalGrants: 10_000, addedPct: 1, removedPct: 1, modifiedPct: 1},
		{totalGrants: 100_000, addedPct: 1, removedPct: 1, modifiedPct: 1},
		{totalGrants: 1_000_000, addedPct: 1, removedPct: 1, modifiedPct: 1},

		// High churn
		{totalGrants: 10_000, addedPct: 20, removedPct: 20, modifiedPct: 10},
		{totalGrants: 100_000, addedPct: 20, removedPct: 20, modifiedPct: 10},

		// With expandable grants (expansion column comparison)
		{totalGrants: 10_000, addedPct: 1, removedPct: 1, modifiedPct: 1, expandable: true},
		{totalGrants: 100_000, addedPct: 1, removedPct: 1, modifiedPct: 1, expandable: true},
	}

	for _, cfg := range configs {
		b.Run(cfg.name(), func(b *testing.B) {
			oldFile, newFile, oldSyncID, newSyncID, cleanup := buildDiffFixture(b, cfg)
			defer cleanup()

			ctx := b.Context()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				attached, err := newFile.AttachFile(oldFile, "attached")
				require.NoError(b, err)

				b.StartTimer()

				upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
				require.NoError(b, err)
				require.NotEmpty(b, upsertsSyncID)
				require.NotEmpty(b, deletionsSyncID)

				b.StopTimer()

				for _, table := range []string{
					resourceTypes.Name(), resources.Name(), entitlements.Name(), grants.Name(), syncRuns.Name(),
				} {
					_, err := newFile.db.ExecContext(ctx,
						fmt.Sprintf("DELETE FROM %s WHERE sync_id IN (?, ?)", table),
						upsertsSyncID, deletionsSyncID,
					)
					require.NoError(b, err)
				}

				_, err = attached.DetachFile("attached")
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkGenerateSyncDiff(b *testing.B) {
	configs := []diffBenchConfig{
		{totalGrants: 1_000, addedPct: 1, removedPct: 1, modifiedPct: 0},
		{totalGrants: 10_000, addedPct: 1, removedPct: 1, modifiedPct: 0},
		{totalGrants: 100_000, addedPct: 1, removedPct: 1, modifiedPct: 0},
		{totalGrants: 10_000, addedPct: 20, removedPct: 20, modifiedPct: 0},
	}

	for _, cfg := range configs {
		b.Run(cfg.name(), func(b *testing.B) {
			ctx := b.Context()

			tempDir, err := os.MkdirTemp("", "diff-bench-same-*")
			require.NoError(b, err)
			defer os.RemoveAll(tempDir)

			fPath := filepath.Join(tempDir, "diff.c1z")
			opts := []C1ZOption{
				WithTmpDir(tempDir),
				WithPragma("journal_mode", "WAL"),
				WithEncoderConcurrency(0),
				WithDecoderOptions(WithDecoderConcurrency(0)),
			}

			f, err := NewC1ZFile(ctx, fPath, opts...)
			require.NoError(b, err)
			defer f.Close(ctx)

			total := cfg.totalGrants
			nAdded := total * cfg.addedPct / 100
			nRemoved := total * cfg.removedPct / 100
			nUnchanged := total - nAdded - nRemoved

			rt := &v2.ResourceType{Id: "user", DisplayName: "User"}
			ent := &v2.Entitlement{
				Id: "group:g1:member",
				Resource: &v2.Resource{
					Id: &v2.ResourceId{ResourceType: "group", Resource: "g1"},
				},
			}

			makeGrant := func(idx int) *v2.Grant {
				return v2.Grant_builder{
					Id:          fmt.Sprintf("grant-%d", idx),
					Entitlement: ent,
					Principal: &v2.Resource{
						Id: &v2.ResourceId{ResourceType: "user", Resource: fmt.Sprintf("u%d", idx)},
					},
				}.Build()
			}

			batchSize := 1000
			batch := make([]*v2.Grant, 0, batchSize)
			flush := func() {
				if len(batch) > 0 {
					require.NoError(b, f.PutGrants(ctx, batch...))
					batch = batch[:0]
				}
			}

			// Base sync
			baseSyncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(b, err)
			require.NoError(b, f.PutResourceTypes(ctx, rt))
			require.NoError(b, f.PutEntitlements(ctx, ent))

			id := 0
			for range nUnchanged {
				batch = append(batch, makeGrant(id))
				id++
				if len(batch) >= batchSize {
					flush()
				}
			}
			for range nRemoved {
				batch = append(batch, makeGrant(id))
				id++
				if len(batch) >= batchSize {
					flush()
				}
			}
			flush()
			require.NoError(b, f.EndSync(ctx))

			// Applied sync
			appliedSyncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(b, err)
			require.NoError(b, f.PutResourceTypes(ctx, rt))
			require.NoError(b, f.PutEntitlements(ctx, ent))

			for i := range nUnchanged {
				batch = append(batch, makeGrant(i))
				if len(batch) >= batchSize {
					flush()
				}
			}
			for range nAdded {
				batch = append(batch, makeGrant(id))
				id++
				if len(batch) >= batchSize {
					flush()
				}
			}
			flush()
			require.NoError(b, f.EndSync(ctx))

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				diffSyncID, err := f.GenerateSyncDiff(ctx, baseSyncID, appliedSyncID)
				require.NoError(b, err)
				require.NotEmpty(b, diffSyncID)

				b.StopTimer()
				for _, table := range []string{
					resourceTypes.Name(), resources.Name(), entitlements.Name(), grants.Name(), syncRuns.Name(),
				} {
					_, err := f.db.ExecContext(ctx,
						fmt.Sprintf("DELETE FROM %s WHERE sync_id = ?", table), diffSyncID,
					)
					require.NoError(b, err)
				}
				b.StartTimer()
			}
		})
	}
}
