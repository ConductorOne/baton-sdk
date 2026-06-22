package dotc1z_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestResumeCheckpointParity locks in that both storage engines resume
// an interrupted sync across a process restart instead of restarting it
// from scratch. This is the contract the syncer depends on for
// time-boxed (activity-window) syncs: when a window expires mid-sync,
// the next window must pick up at the persisted checkpoint.
//
// It exercises the exact call the syncer makes on resume —
// StartOrResumeSync(ctx, syncType, "") with an EMPTY syncID
// (pkg/sync/syncer.go startOrResumeSync) — and asserts:
//   - the interrupted sync is resumed (started_new == false), not
//     replaced by a fresh sync, and
//   - CurrentSyncStep returns the checkpointed token, so the syncer's
//     state.Unmarshal sees a real FSM cursor rather than "" (which
//     would reset the FSM to InitOp and re-run the whole sync).
//
// Pebble previously failed both: StartOrResumeSync("") always started a
// new sync (wiping prior data), and even on the direct ResumeSync path
// it dropped the in-memory step. SQLite has always satisfied this via
// getLatestUnfinishedSync + reading sync_token from the row.
func TestResumeCheckpointParity(t *testing.T) {
	ctx := context.Background()
	const fsmCursor = "fsm-page-cursor-42"

	for _, engine := range []dotc1z.Engine{dotc1z.EngineSQLite, dotc1z.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "sync.c1z")

			// Activity window 1: start a sync, write a record, checkpoint
			// a non-empty FSM token mid-sync, then close WITHOUT EndSync
			// (the window expired before the sync finished).
			store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
			require.NoError(t, err)
			syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
			require.NoError(t, store.CheckpointSync(ctx, fsmCursor))
			require.NoError(t, store.Close(ctx))

			// Activity window 2: a fresh process reopens the same c1z and
			// resumes via the empty-syncID path the syncer drives.
			store2, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
			require.NoError(t, err)
			defer func() { _ = store2.Close(ctx) }()

			resumedID, startedNew, err := store2.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.False(t, startedNew, "must resume the interrupted sync, not start a new one")
			require.Equal(t, syncID, resumedID, "must resume the same sync_id")

			step, err := store2.CurrentSyncStep(ctx)
			require.NoError(t, err)
			require.Equal(t, fsmCursor, step, "resumed sync must retain its checkpoint token")
		})
	}
}
