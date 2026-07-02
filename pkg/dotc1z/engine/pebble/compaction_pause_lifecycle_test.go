package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func pauseLifecycleGrant(principal string) *v2.Grant {
	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()
	return v2.Grant_builder{
		Id:          "app:github:read:user:" + principal,
		Entitlement: v2.Entitlement_builder{Id: "app:github:read", Resource: app}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: principal}.Build(),
		}.Build(),
	}.Build()
}

// TestCompactionPauseLifecycle pins the compaction scheduler's pause/resume
// contract across the sync lifecycle:
//
//   - a successful EndSync leaves compactions paused (the intended
//     EndSync-to-close window);
//   - binding a sync again (SetCurrentSync / StartNewSync) resumes;
//   - a FAILED EndSync must resume on its own — the sync stays bound and
//     the caller may keep writing or retry, and with nothing rebinding the
//     sync a stuck pause would stall writes at L0StopWritesThreshold;
//   - a reopen always starts unpaused (scheduler state is per-instance).
func TestCompactionPauseLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	e, err := Open(ctx, dir)
	require.NoError(t, err)
	require.NotNil(t, e.compactionScheduler)
	require.False(t, e.compactionScheduler.paused.Load(), "fresh engine starts unpaused")

	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.Grants().StoreExpandedGrants(ctx, pauseLifecycleGrant("alice")))

	// Failed EndSync: cancel the context so the deferred index build (armed
	// by the expanded write above) aborts after the pause.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, a.EndSync(canceled), "EndSync with canceled ctx must fail in the deferred build")
	require.False(t, e.compactionScheduler.paused.Load(),
		"a failed EndSync must resume compactions: the sync stays bound and nothing else would ever resume")

	// Successful EndSync: paused for the save/close window.
	require.NoError(t, a.EndSync(ctx))
	require.True(t, e.compactionScheduler.paused.Load(), "successful EndSync leaves compactions paused for the save window")

	// Rebinding resumes.
	require.NoError(t, a.SetCurrentSync(ctx, syncID))
	require.False(t, e.compactionScheduler.paused.Load(), "binding a sync must resume compactions")

	// Pause again via EndSync, then prove a reopen starts unpaused.
	require.NoError(t, a.EndSync(ctx))
	require.True(t, e.compactionScheduler.paused.Load())
	require.NoError(t, e.Close())

	e2, err := Open(ctx, dir)
	require.NoError(t, err)
	defer e2.Close()
	require.False(t, e2.compactionScheduler.paused.Load(), "reopened engine must start unpaused")
}
