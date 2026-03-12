//go:build !windows

package sync

import (
	"path/filepath"
	"testing"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1ztest"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/stretchr/testify/require"
)

func TestCleanupContextDeadlineExceeded(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	// Set up zap logger in context to capture logs.
	ctx, err := logging.Init(
		ctx,
		logging.WithLogFormat(logging.LogFormatConsole),
		logging.WithLogLevel("debug"),
	)
	require.NoError(t, err)

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	// Create and end a bunch of syncs. We should delete all but 2 of them in Cleanup().
	// Keep the last sync to check that it is not deleted after cleanup.
	syncID := ""
	for range 100 {
		syncID, err = c1ztest.CreateTestSync(ctx, t, f, c1ztest.C1ZCounts{
			ResourceTypeCount: 10,
			ResourceCount:     100,
			UserCount:         100,
			EntitlementCount:  10,
			GrantCount:        250,
		})
		require.NoError(t, err)
	}

	// Start a sync with an empty mock connector. This will call cleanup at the end of the sync.
	syncer, err := NewSyncer(ctx, newMockConnector(), WithRunDuration(200*time.Millisecond), WithConnectorStore(f))
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)

	err = syncer.Close(ctx)
	require.NoError(t, err)

	// Reopen the file and start sync again, which should succeed.
	f, err = dotc1z.NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	syncer, err = NewSyncer(ctx, newMockConnector(), WithConnectorStore(f))
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.NoError(t, err)

	// Check that we only have two syncs left.
	syncs, _, err := f.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	for _, sync := range syncs {
		t.Logf("sync: %s, ended at: %s\n", sync.ID, sync.EndedAt.Format(time.RFC3339))
	}
	require.Len(t, syncs, 2, "cleanup should keep 2 syncs")

	// Check that the last sync we created before running syncer is kept.
	resp, err := f.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: syncID,
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, resp.GetSync())
	require.Equal(t, syncID, resp.GetSync().GetId())

	err = syncer.Close(ctx)
	require.NoError(t, err)
}
