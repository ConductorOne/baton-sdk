package synccompactor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
)

// TestIncrementalFoldReusesCapturedBaseGraph: an opted-in fold must carry the
// base's graph inputs forward itself. Without the capture, loadIncrementalBaseGraph
// re-extracts the whole base c1z to read one blob the fold already held — on a
// large base that can cost more than the full expansion it is avoiding.
func TestIncrementalFoldReusesCapturedBaseGraph(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())

	// Force fold mode explicitly: the capture only exists on the fold path, and
	// auto mode picks a rebuild for fixtures this small.
	compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeFold),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	_, err = compactor.Compact(ctx)
	require.NoError(t, err)

	require.True(t, compactor.incrementalExpansionRan,
		"fixture should take the incremental path")
	require.NotNil(t, compactor.foldBaseGraph,
		"the fold must capture the base graph inputs rather than leaving a reopen")
	require.NotNil(t, compactor.foldBaseGraph.blob,
		"the base carried a preserved graph, so the captured blob must be populated")
	require.NotNil(t, compactor.foldBaseGraph.run,
		"the verification run must be captured before the rename overwrites it")
	require.True(t, compactor.foldBaseGraph.digestFound,
		"the grant digest must be captured before the merge rebuilds it")
}

// TestIncrementalFoldSurvivesUncapturableBase: the capture is an optimization,
// not a requirement. A base whose graph inputs cannot be read must still
// compact — falling back to the reopen path and then to full expansion —
// rather than failing the run. Enabling a performance flag must never turn a
// compaction that previously succeeded into one that errors.
func TestIncrementalFoldSurvivesUncapturableBase(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())

	// Strip the base's graph sidecar: the capture reads nothing useful, and the
	// reopen path finds nothing either, so expansion must take the full route.
	store, err := dotc1z.NewStore(ctx, entries[0].FilePath, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	gs, ok := store.(sdksync.EntitlementGraphStore)
	require.True(t, ok)
	require.NoError(t, gs.DeleteEntitlementGraphBlob(ctx))
	require.NoError(t, store.Close(ctx))

	compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithPebbleCompactorMode(PebbleCompactorModeFold),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	out, err := compactor.Compact(ctx)
	require.NoError(t, err, "a base without a usable graph must still compact")
	require.NotNil(t, out)
	require.False(t, compactor.incrementalExpansionRan,
		"no base graph means full expansion, not the fast path")
}

// TestFoldBaseGraphCaptureDeclines: the captured path must apply the same
// checks as the reopen path it replaces. Every branch here declines rather than
// handing back a graph that might not describe the artifact beside it.
func TestFoldBaseGraphCaptureDeclines(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())
	baseSyncID := entries[0].SyncID

	// A real capture, taken the way compactPebbleFold takes one.
	good := captureFromFixture(t, ctx, entries)
	require.NotNil(t, good.blob, "fixture base must carry a preserved graph")

	t.Run("happy path returns the graph", func(t *testing.T) {
		graph, err := good.baseGraph(baseSyncID)
		require.NoError(t, err)
		require.NotNil(t, graph)
		require.NoError(t, graph.ValidateCompleted())
	})

	t.Run("no finished sync", func(t *testing.T) {
		c := *good
		c.run = nil
		_, err := c.baseGraph(baseSyncID)
		require.ErrorContains(t, err, "no finished sync")
	})

	t.Run("sync id mismatch", func(t *testing.T) {
		_, err := good.baseGraph("some-other-sync")
		require.ErrorContains(t, err, "not verified")
	})

	t.Run("unverified generation", func(t *testing.T) {
		c := *good
		run := *good.run
		run.Generation = "not-" + sdksync.IngestInvariantGeneration
		c.run = &run
		_, err := c.baseGraph(baseSyncID)
		require.ErrorContains(t, err, "not verified")
	})

	t.Run("no preserved graph declines cleanly", func(t *testing.T) {
		c := *good
		c.blob = nil
		graph, err := c.baseGraph(baseSyncID)
		require.NoError(t, err, "a missing graph is a decline, not an error")
		require.Nil(t, graph)
	})

	t.Run("digest mismatch declines cleanly", func(t *testing.T) {
		c := *good
		c.digest.Count = good.digest.Count + 1
		graph, err := c.baseGraph(baseSyncID)
		require.NoError(t, err)
		require.Nil(t, graph, "a graph that does not describe these grants must not be reused")
	})

	t.Run("digest unavailable declines cleanly", func(t *testing.T) {
		c := *good
		c.digestFound = false
		graph, err := c.baseGraph(baseSyncID)
		require.NoError(t, err)
		require.Nil(t, graph)
	})
}

// captureFromFixture builds a foldBaseGraphCapture from the base artifact the
// same way compactPebbleFold does, so the assertions above exercise real data.
func captureFromFixture(t *testing.T, ctx context.Context, entries []*CompactableSync) *foldBaseGraphCapture {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, entries[0].FilePath,
		dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)

	gs, ok := store.(sdksync.EntitlementGraphStore)
	require.True(t, ok)
	blob, err := gs.GetEntitlementGraphBlob(ctx)
	require.NoError(t, err)

	reader, ok := store.(c1zstore.GrantGenerationDigestReader)
	require.True(t, ok)
	digest, found, err := reader.GrantGenerationDigest(ctx)
	require.NoError(t, err)

	return &foldBaseGraphCapture{run: run, blob: blob, digest: digest, digestFound: found}
}
