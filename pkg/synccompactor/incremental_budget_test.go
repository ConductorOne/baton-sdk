package synccompactor

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestIncrementalClassificationContextReservesFallbackBudget(t *testing.T) {
	ctx := t.Context()
	const total = time.Hour
	start := time.Now()
	c := &Compactor{runDuration: total}

	classificationCtx, cancel, ok := c.incrementalClassificationContext(ctx, start)
	require.True(t, ok)
	defer cancel()

	deadline, ok := classificationCtx.Deadline()
	require.True(t, ok)
	got := time.Until(deadline)
	want := total * incrementalClassificationBudgetPercent / 100
	require.InDelta(t, float64(want), float64(got), float64(250*time.Millisecond))
}

func TestExpandGrantsSkipsIncrementalAttemptWhenRunDurationExhausted(t *testing.T) {
	var logs bytes.Buffer
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&logs), zap.InfoLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))

	c := &Compactor{
		incrementalExpansion: true,
		engine:               c1zstore.EnginePebble,
		runDuration:          time.Minute,
		entries: []*CompactableSync{{
			FilePath: "/path/that/must/not/be/opened.c1z",
			SyncID:   "base",
		}},
	}

	err := c.expandGrants(ctx, ctx, "new-sync", time.Now().Add(-2*time.Minute))
	require.ErrorContains(t, err, "unable to finish compaction sync in run duration")
	require.Contains(t, logs.String(), `"incremental_expansion_outcome":"not_attempted"`)
	require.Contains(t, logs.String(), `"incremental_expansion_reason":"run_duration_exhausted"`)
	require.NotContains(t, logs.String(), "base_graph_error",
		"an exhausted run must not touch the base artifact")
}

func TestIncrementalDeclineFallsBackWithRunDuration(t *testing.T) {
	ctx := t.Context()
	entries := buildSpecChangeFixtures(t, ctx, t.TempDir(), false, true)
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
		WithRunDuration(10*time.Second),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.False(t, c.incrementalExpansionRan, "narrowed edge must decline to full expansion")
	require.NotNil(t, artifactGraph(t, ctx, out.FilePath, out.SyncID),
		"the fallback must finish and persist the fresh full-expansion graph")
}

func TestIncrementalClassificationTimeoutFallsBackWithRunDuration(t *testing.T) {
	ctx := t.Context()
	entries := buildIncrementalFixtures(t, ctx, t.TempDir())
	c, cleanup, err := NewCompactor(ctx, t.TempDir(), entries,
		WithTmpDir(t.TempDir()),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion(),
		WithRunDuration(10*time.Second),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	classificationExpired := false
	c.incrementalClassificationTestContext = func(parent context.Context) context.Context {
		classificationExpired = true
		expired, cancel := context.WithDeadline(parent, time.Now().Add(-time.Second))
		t.Cleanup(cancel)
		return expired
	}

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.True(t, classificationExpired, "test must expire classification after ResumeSync")
	require.NotNil(t, out)
	require.False(t, c.incrementalExpansionRan, "expired classification must fall back to full expansion")
	require.NotNil(t, artifactGraph(t, ctx, out.FilePath, out.SyncID),
		"full expansion must finish after the classification deadline expires")
}

func TestRestoreEndedSyncIgnoresExpiredAttemptContext(t *testing.T) {
	ctx := t.Context()
	store, err := dotc1z.NewStore(ctx, t.TempDir()+"/restore.c1z",
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.EndSync(ctx))
	_, err = store.ResumeSync(ctx, connectorstore.SyncTypeFull, syncID)
	require.NoError(t, err)

	expiredCtx, cancel := context.WithCancel(ctx)
	cancel()
	c := &Compactor{compactedC1z: store}
	require.NoError(t, c.restoreEndedSync(expiredCtx))

	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Equal(t, syncID, run.ID)
}
