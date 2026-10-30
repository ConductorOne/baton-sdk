package pebble

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestFinishedSyncMetadataIgnoresCorruptStatsSidecar(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, sealWithStats(t, e, ctx, c1zstore.SyncStats{
		Run: c1zstore.RunStats{Counters: map[string]uint64{"list_page_window.sampled": 1}},
	}))
	lookups := []struct {
		name   string
		lookup func(context.Context) (*c1zstore.SyncRun, error)
	}{
		{"full", e.SyncMeta().LatestFullSync},
		{"any", e.SyncMeta().LatestFinishedSyncOfAnyType},
	}
	for _, lookup := range lookups {
		run, err := lookup.lookup(ctx)
		require.NoError(t, err, lookup.name)
		require.NotNil(t, run, lookup.name)
		require.NotNil(t, run.Stats, lookup.name)
		require.EqualValues(t, 1, run.Stats.GetCounters()["list_page_window.sampled"], lookup.name)
	}

	require.NoError(t, e.db.UnsafeForTesting().Set(encodeSyncStatsKey(), []byte{0xff}, nil))
	_, err = e.readSyncStats(ctx, syncID)
	require.Error(t, err)
	core, logs := observer.New(zap.WarnLevel)
	ctx = ctxzap.ToContext(ctx, zap.New(core))
	for _, lookup := range lookups {
		run, err := lookup.lookup(ctx)
		require.NoError(t, err, lookup.name)
		require.NotNil(t, run, lookup.name)
		require.Equal(t, syncID, run.ID, lookup.name)
		require.NotNil(t, run.EndedAt, lookup.name)
		require.Equal(t, connectorstore.SyncTypeFull, run.Type, lookup.name)
		require.Nil(t, run.Stats, lookup.name)
	}
	require.Equal(t, len(lookups), logs.FilterMessage("finished sync stats unavailable").Len())
}
