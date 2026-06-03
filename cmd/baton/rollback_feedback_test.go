package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// Item 2 (kans): the suspect-connector-sourced protective behavior is behind
// a default-off flag. Default deletes the suspect grant (TestRollbackExpansion-
// Classification covers that); with WithPreserveSuspectGrants it is kept.
func TestRollbackPreserveSuspectGrants(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.c1z")
	syncID := buildSyntheticExpandedC1Z(t, ctx, path)

	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	res, err := store.RollbackExpansion(ctx, syncID, false, dotc1z.WithPreserveSuspectGrants())
	require.NoError(t, err)
	require.NoError(t, store.Close(ctx))

	require.Equal(t, 1, res.GrantsDeleted, "only the GrantImmutable-marked derived grant is deleted")
	require.Equal(t, 1, res.SourcesCleared, "directExpanded still cleared")
	require.Equal(t, 1, res.SuspectConnectorSourced, "suspect still detected + counted")
	require.Equal(t, 1, res.SuspectPreserved, "suspect kept, not deleted")

	requireGrantPresence(t, ctx, path, syncID, "group:g1:member:user:alice", false) // derived → deleted
	requireGrantPresence(t, ctx, path, syncID, "group:g1:member:user:dave", true)   // suspect → PRESERVED
	g, found := loadGrant(t, ctx, path, syncID, "group:g1:member:user:dave")
	require.True(t, found)
	require.NotEmpty(t, g.GetSources().GetSources(), "preserved suspect grant keeps its connector Sources intact")
}

// Item 3 (kans): rollback must not leave stale sync_runs.stats. After a
// rollback-without-replay the recomputed stats must reflect the deleted
// grants, not the cached pre-rollback totals.
func TestRollbackClearsStaleStats(t *testing.T) {
	ctx := context.Background()
	inPath := filepath.Join(t.TempDir(), "src.c1z")
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	syncID, deletes := buildExpandedFixture(t, ctx, inPath)
	require.Positive(t, deletes)

	before := sumStats(t, ctx, inPath, syncID)

	// No --replay: the expanded grants are removed and not re-derived.
	require.NoError(t, runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID))

	after := sumStats(t, ctx, outPath, syncID)
	require.Less(t, after, before, "Stats() must recompute against the rolled-back rows, not return the cached pre-rollback totals")
}

func sumStats(t *testing.T, ctx context.Context, path, syncID string) int64 {
	t.Helper()
	ro, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer ro.Close(ctx)
	m, err := ro.Stats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	var total int64
	for _, v := range m {
		total += v
	}
	return total
}

// Item 1 (kans): --validate compares each grant's Sources before rollback and
// after replay, failing on divergence. It requires --replay.
func TestRollbackValidateRequiresReplay(t *testing.T) {
	ctx := context.Background()
	inPath := filepath.Join(t.TempDir(), "src.c1z")
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	syncID, _ := buildExpandedFixture(t, ctx, inPath)

	err := runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--validate")
	require.ErrorContains(t, err, "--validate requires --replay")
	require.NoFileExists(t, outPath)
}

// The expanded fixture carries a connector-set, self-sourced grant
// (group:g2:member:group:g2) whose Sources rollback clears and an
// empty-connector replay cannot reproduce — so --validate must DETECT the
// divergence and fail without finalizing the output.
func TestRollbackValidateDetectsDivergence(t *testing.T) {
	ctx := context.Background()
	inPath := filepath.Join(t.TempDir(), "src.c1z")
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	syncID, _ := buildExpandedFixture(t, ctx, inPath)

	err := runRollback(ctx, inPath, "--out", outPath, "--sync-id", syncID, "--replay", "--validate")
	require.Error(t, err)
	require.Contains(t, err.Error(), "changed grant sources")
	require.NoFileExists(t, outPath, "a failed validation must not leave a finalized output")
}
