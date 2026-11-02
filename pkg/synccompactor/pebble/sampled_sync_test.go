package pebble

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func TestSampledInputsRejectedByMergeEntrypoints(t *testing.T) {
	for _, entrypoint := range []string{"compact", "fold", "kway", "overlay"} {
		t.Run(entrypoint, func(t *testing.T) {
			ctx := t.Context()
			source, _ := newEngine(t, "sampled-source")
			syncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, source.CheckpointSync(ctx, `{"counters":{"list_page_window.sampled":1}}`))
			require.NoError(t, source.EndSync(ctx))
			destination, _ := newEngine(t, "destination")
			destinationID := ksuid.New().String()
			require.NoError(t, destination.SetCurrentSync(ctx, destinationID))
			require.NoError(t, destination.PutGrantRecord(ctx, grant(destinationID, "keep", "member", "user")))
			switch entrypoint {
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
	require.NoError(t, source.CheckpointSync(ctx, `{"counters":{"list_page_window.sampled":1}}`))
	require.NoError(t, source.EndSync(ctx))
	require.ErrorContains(t, requireUnsampledSource(ctx, source, sampledID), "sampled")
	completeID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEqual(t, sampledID, completeID)
	require.NoError(t, source.EndSync(ctx))
	selected, err := source.GetSyncRunRecord(ctx, completeID)
	require.NoError(t, err)
	require.False(t, selected.GetSampled())
	require.NoError(t, requireUnsampledSource(ctx, source, completeID))
}
