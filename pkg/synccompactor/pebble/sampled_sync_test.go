package pebble

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func TestSampledInputsRejectedByMergeEntrypoints(t *testing.T) {
	for _, tc := range []struct {
		name               string
		entrypoint         string
		destinationSampled bool
	}{
		{name: "compact", entrypoint: "compact"},
		{name: "fold", entrypoint: "fold"},
		{name: "kway", entrypoint: "kway"},
		{name: "overlay", entrypoint: "overlay"},
		{name: "fold sampled destination", entrypoint: "fold", destinationSampled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			source, _ := newEngine(t, "sampled-source")
			syncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			sampledCounters := map[string]uint64{c1zstore.ListPageWindowCounterPrefix + "sampled": 1}
			sourceCounters := sampledCounters
			if tc.destinationSampled {
				sourceCounters = nil
			}
			require.NoError(t, source.EndSyncWithStats(ctx, c1zstore.SyncStats{Run: c1zstore.RunStats{Counters: sourceCounters}}))
			destination, _ := newEngine(t, "destination")
			destinationID := ksuid.New().String()
			if tc.destinationSampled {
				destinationID, err = destination.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
			}
			require.NoError(t, destination.SetCurrentSync(ctx, destinationID))
			require.NoError(t, destination.Records().PutGrantRecord(ctx, grant(destinationID, "keep", "member", "user")))
			if tc.destinationSampled {
				require.NoError(t, destination.EndSyncWithStats(ctx, c1zstore.SyncStats{Run: c1zstore.RunStats{Counters: sampledCounters}}))
			}
			switch tc.entrypoint {
			case "compact":
				compactor, createErr := NewCompactor(destination, t.TempDir())
				require.NoError(t, createErr)
				err = compactor.Compact(ctx, source, syncID)
			case "fold":
				_, err = MergeInto(ctx, destination, []SourceSync{{Engine: source, SyncID: syncID}}, destinationID)
			case "kway":
				_, err = MergeFilesInto(ctx, destination, []SourceFile{{Engine: source, SyncID: syncID}}, destinationID, t.TempDir())
			case "overlay":
				_, err = MergeFilesIntoOverlay(ctx, destination, []SourceFile{{Engine: source, SyncID: syncID}}, destinationID, t.TempDir())
			}
			require.ErrorContains(t, err, "sampled")
			require.Equal(t, 1, countGrants(t, destination, destinationID))
		})
	}
}

func TestSampledGuardAllowsNewSelectedCompleteSync(t *testing.T) {
	ctx := t.Context()
	source, _ := newEngine(t, "selected-source")
	sampledID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, source.EndSyncWithStats(ctx, c1zstore.SyncStats{Run: c1zstore.RunStats{
		Counters: map[string]uint64{c1zstore.ListPageWindowCounterPrefix + "sampled": 1},
	}}))
	require.ErrorContains(t, requireUnsampledSource(ctx, source, sampledID), "sampled")
	completeID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEqual(t, sampledID, completeID)
	require.NoError(t, source.EndSync(ctx))
	selected, err := source.SyncRuns().GetSyncRunRecord(ctx, completeID)
	require.NoError(t, err)
	require.False(t, selected.GetSampled())
	require.NoError(t, requireUnsampledSource(ctx, source, completeID))
}
