package synccompactor

import (
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
)

func TestCompactorRejectsSampledInputs(t *testing.T) {
	ctx, dir := t.Context(), t.TempDir()
	basePath, partialPath := filepath.Join(dir, "base.c1z"), filepath.Join(dir, "partial.c1z")
	baseID := buildPebbleInput(t, ctx, basePath, connectorstore.SyncTypeFull, "base")
	partialID := buildPebbleInput(t, ctx, partialPath, connectorstore.SyncTypePartial, "partial")
	store, err := dotc1z.NewStore(ctx, partialPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentSync(ctx, partialID))
	engine, ok := enginepkg.AsEngine(store)
	require.True(t, ok)
	sealer, ok := store.(c1zstore.LedgerLifecycle)
	require.True(t, ok)
	require.NoError(t, sealer.Seal(ctx, c1zstore.SyncStats{Run: c1zstore.RunStats{
		Counters: map[string]uint64{c1zstore.ListPageWindowCounterPrefix + "sampled": 1},
	}}))
	record, err := engine.SyncRuns().GetSyncRunRecord(ctx, partialID)
	require.NoError(t, err)
	require.True(t, record.GetSampled())
	require.NoError(t, store.Close(ctx))
	compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{
		{FilePath: basePath, SyncID: baseID}, {FilePath: partialPath, SyncID: partialID},
	}, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion(), WithPebbleCompactorMode(PebbleCompactorModeKWay))
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	output, err := compactor.Compact(ctx)
	require.ErrorContains(t, err, "sampled")
	require.Nil(t, output)
}
