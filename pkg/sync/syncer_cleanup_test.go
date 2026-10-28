//go:build !windows

//revive:disable-next-line:var-naming Package name matches the existing sync package under test.
package sync

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1ztest"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/stretchr/testify/require"
)

// TestCleanupPebbleSingleSyncLifecycle is the default-engine sibling of
// TestCleanupContextDeadlineExceeded (which is pinned to SQLite because
// its deadline assertion depends on SQLite's slow row-by-row deletes).
// On pebble — now the default — end-of-sync cleanup is a no-op by
// design: each StartNewSync replaces the prior sync in place
// (single-sync contract), so a backlog of old syncs can never
// accumulate and the run budget cannot expire during cleanup. The
// pebble analog of "old syncs dropped within budget" is therefore:
// the syncer run completes well inside a budget, the seeded sync was
// replaced rather than retained, exactly one sealed sync remains, and
// the artifact is v3 on disk.
func TestCleanupPebbleSingleSyncLifecycle(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	ctx, err := logging.Init(
		ctx,
		logging.WithLogFormat(logging.LogFormatConsole),
		logging.WithLogLevel("debug"),
	)
	require.NoError(t, err)

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	// Engine-less NewStore: the default (pebble) path under test.
	f, err := dotc1z.NewStore(ctx, testFilePath)
	require.NoError(t, err)
	// Fail-safe only: a mid-test require failure must not leak the open
	// pebble store for the rest of the test binary. On the happy path
	// syncer.Close closes the store and this is a no-op (Close is
	// idempotent).
	defer func() { _ = f.Close(ctx) }()

	// Seed synced data the same way the SQLite test does. Under the
	// single-sync contract each iteration replaces the previous sync, so
	// unlike SQLite this can never build up a cleanup backlog.
	seededSyncID := ""
	for range 3 {
		seededSyncID, err = c1ztest.CreateTestSync(ctx, t, f, c1ztest.C1ZCounts{
			ResourceTypeCount: 10,
			ResourceCount:     100,
			UserCount:         100,
			EntitlementCount:  10,
			GrantCount:        250,
		})
		require.NoError(t, err)
	}

	// The budget is intentionally generous: with no-op cleanup the only
	// thing that could burn it is the sync itself, and a tight bound
	// would test machine speed, not engine behavior. What matters is
	// that ErrSyncNotComplete — the SQLite test's expected outcome —
	// cannot happen here.
	syncer, err := NewSyncer(ctx, newMockConnector(), WithRunDuration(30*time.Second), WithConnectorStore(f))
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.NoError(t, err, "pebble sync must complete within budget; cleanup is a no-op")

	// Exactly one sync remains, it is sealed, and it is the syncer's run,
	// not the seeded one (replacement, the pebble analog of cleanup).
	syncsResp, err := f.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{
		PageSize: 100,
	}.Build())
	require.NoError(t, err)
	syncs := syncsResp.GetSyncs()
	require.Len(t, syncs, 1, "single-sync contract: exactly one sync after the run")
	require.NotNil(t, syncs[0].GetEndedAt(), "the remaining sync must be sealed")
	require.NotEqual(t, seededSyncID, syncs[0].GetId(), "the seeded sync must have been replaced")

	err = syncer.Close(ctx)
	require.NoError(t, err)

	// The default-engine artifact must be v3 on disk.
	file, err := os.Open(testFilePath)
	require.NoError(t, err)
	defer file.Close()
	format, err := dotc1z.ReadHeaderFormat(file)
	require.NoError(t, err)
	require.Equal(t, dotc1z.C1ZFormatV3, format, "default-engine artifact must be a v3 c1z on disk")
}
