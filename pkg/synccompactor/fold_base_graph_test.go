package synccompactor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
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
	require.NotNil(t, compactor.foldBaseGraph.run,
		"the verification run must be captured before the rename overwrites it")
	require.True(t, compactor.foldBaseGraph.digestFound,
		"the grant digest must be captured before the merge rebuilds it")
	// blob is deliberately released once decoded; that it was captured at all is
	// what incrementalExpansionRan above proves, since the fast path cannot run
	// without a decoded graph. captureFromFixture asserts the blob directly.
	require.Nil(t, compactor.foldBaseGraph.blob,
		"the decoded blob must be released rather than held through expansion")
}

// TestIncrementalFoldSurvivesBaseWithoutGraph: a base carrying no preserved
// graph must still compact, via full expansion. The capture succeeds here with
// an empty blob — this pins the artifact-level outcome, not the capture's
// error handling, which TestLoadIncrementalBaseGraphReopensWithoutCapture covers.
func TestIncrementalFoldSurvivesBaseWithoutGraph(t *testing.T) {
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

// TestLoadIncrementalBaseGraphReopensWithoutCapture pins the fallback the
// capture's doc comment promises: when captureFoldBaseGraph left nothing behind
// — a failed read, or a rebuild-mode run that never folded — the base is
// reopened and the graph still loads. This is the guarantee that keeps a failed
// capture from costing a compaction.
func TestLoadIncrementalBaseGraphReopensWithoutCapture(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())

	compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	require.Nil(t, compactor.foldBaseGraph, "no fold has run yet")

	graph, err := compactor.loadIncrementalBaseGraph(ctx)
	require.NoError(t, err, "an absent capture must reopen the base, not fail")
	require.NotNil(t, graph, "the base's preserved graph must load via the reopen path")
	require.NoError(t, graph.ValidateCompleted())
}

// TestCaptureFoldBaseGraphLeavesNothingOnReadFailure pins the capture's error
// handling itself: a failed read must leave c.foldBaseGraph nil so
// loadIncrementalBaseGraph reopens the base, rather than storing a half-built
// capture or failing the fold. A cancelled context stands in for any read error
// — the sidecar and digest reads both check ctx.Err() first.
func TestCaptureFoldBaseGraphLeavesNothingOnReadFailure(t *testing.T) {
	ctx := context.Background()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())

	store, err := dotc1z.NewStore(ctx, entries[0].FilePath,
		dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	destEng, ok := enginepkg.AsEngine(store)
	require.True(t, ok)

	c := &Compactor{compactedC1z: store, entries: entries, incrementalExpansion: true}

	// Sanity: the same call on a live context does populate the capture, so the
	// assertion below is about the failure, not a broken fixture.
	c.captureFoldBaseGraph(ctx, destEng)
	require.NotNil(t, c.foldBaseGraph)

	c.foldBaseGraph = nil
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	c.captureFoldBaseGraph(cancelled, destEng)
	require.Nil(t, c.foldBaseGraph,
		"a failed read must leave no capture, so the reopen path takes over")
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
