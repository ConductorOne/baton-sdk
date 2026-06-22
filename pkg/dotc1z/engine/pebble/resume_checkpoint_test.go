package pebble

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// reopenEngine closes the engine and reopens a fresh one against the
// same on-disk directory, returning the new engine. It models a new
// activity window / process: the in-memory adapter cache (a.current) is
// gone, but the persisted sync_run record survives a graceful Close
// (which flushes + fsyncs).
func reopenEngine(t testing.TB, e *Engine, dir string) *Engine {
	t.Helper()
	require.NoError(t, e.Close(), "close before reopen")
	reopened, err := Open(context.Background(), filepath.Join(dir, "db"))
	require.NoError(t, err, "reopen")
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

// TestResumeSyncRestoresCheckpointStep is the core regression for the
// Pebble resume bug: ResumeSync must rehydrate the in-memory step from
// the persisted SyncToken so CurrentSyncStep returns the checkpointed
// token (not "") after a new-process resume. A lost step makes the
// syncer's state.Unmarshal("") restart the FSM from InitOp — i.e. a
// full sync on every activity window.
func TestResumeSyncRestoresCheckpointStep(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	a := NewAdapter(e)

	const fsmCursor = "page-cursor-abc123"

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, a.CheckpointSync(ctx, fsmCursor))
	// No EndSync: the sync is still in progress when the window expires.

	// New activity window / process: reopen from disk, fresh adapter.
	e2 := reopenEngine(t, e, dir)
	a2 := NewAdapter(e2)

	resumedID, err := a2.ResumeSync(ctx, connectorstore.SyncTypeFull, syncID)
	require.NoError(t, err)
	require.Equal(t, syncID, resumedID)

	step, err := a2.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, fsmCursor, step, "ResumeSync must restore the checkpointed step from the persisted SyncToken")
}

// TestStartOrResumeSyncRestoresCheckpointStep exercises the production
// resume path the syncer actually drives: startOrResumeSync calls
// store.StartOrResumeSync(ctx, syncType, "") with an EMPTY syncID. It
// must resume the existing in-progress sync (started==false) and keep
// its checkpoint — NOT start a fresh sync that wipes prior data and
// resets the FSM to InitOp.
func TestStartOrResumeSyncRestoresCheckpointStep(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	a := NewAdapter(e)

	const fsmCursor = "page-cursor-xyz789"

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, a.CheckpointSync(ctx, fsmCursor))

	// New activity window / process.
	e2 := reopenEngine(t, e, dir)
	a2 := NewAdapter(e2)

	resumedID, startedNew, err := a2.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, startedNew, "StartOrResumeSync with empty syncID must resume the in-progress sync, not start a new one")
	require.Equal(t, syncID, resumedID, "must resume the same sync_id")

	step, err := a2.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, fsmCursor, step, "resumed sync must retain its checkpointed step")
}
