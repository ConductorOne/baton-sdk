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

// TestCompactionPauseLifecycle pins the engine's explicit sealed state and
// the compaction scheduler's pause/resume contract across the sync
// lifecycle:
//
//   - a successful EndSync SEALS the engine: compactions paused AND record
//     writes refused with ErrEngineSealed (the EndSync-to-close window);
//   - sync-run metadata writes stay allowed while sealed (ended_at
//     overrides, diff links, supports_diff are stamped on finished syncs);
//   - CompactAllRanges refuses while sealed — manual compactions share the
//     paused scheduler and would otherwise block forever, deadlocking Close;
//   - binding a sync again (SetCurrentSync / StartNewSync) unseals and
//     resumes;
//   - a FAILED EndSync must resume on its own — the sync stays bound and
//     the caller may keep writing or retry, and with nothing rebinding the
//     sync a stuck pause would stall writes at L0StopWritesThreshold;
//   - a reopen always starts unsealed (the state is per-instance).
func TestCompactionPauseLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	e, err := Open(ctx, dir)
	require.NoError(t, err)
	require.NotNil(t, e.compactionScheduler)
	require.False(t, e.compactionScheduler.paused.Load(), "fresh engine starts unpaused")
	require.False(t, e.IsSealed(), "fresh engine starts unsealed")

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
	require.False(t, e.IsSealed(), "a failed EndSync must not seal: the sync stays bound")

	// Successful EndSync: sealed (paused + writes refused) for the
	// save/close window.
	require.NoError(t, a.EndSync(ctx))
	require.True(t, e.compactionScheduler.paused.Load(), "successful EndSync leaves compactions paused for the save window")
	require.True(t, e.IsSealed(), "successful EndSync seals the engine")

	// Record writes are refused while sealed — loudly, not by silently
	// running on a paused scheduler.
	err = e.withWrite(func() error { return nil })
	require.ErrorIs(t, err, ErrEngineSealed, "record writes while sealed must fail with ErrEngineSealed")
	require.ErrorIs(t, e.CompactAllRanges(ctx), ErrEngineSealed,
		"manual compaction while sealed must refuse: it shares the paused scheduler and would block forever")

	// Sync-run metadata writes stay allowed: ToPebble stamps the source
	// ended_at, the sanitizer applies diff links, the compactor renames the
	// folded sync — all after EndSync.
	rec, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.NoError(t, e.PutSyncRunRecord(ctx, rec), "sync-run metadata writes must be allowed while sealed")

	// Rebinding unseals and resumes.
	require.NoError(t, a.SetCurrentSync(ctx, syncID))
	require.False(t, e.compactionScheduler.paused.Load(), "binding a sync must resume compactions")
	require.False(t, e.IsSealed(), "binding a sync must unseal")

	// Seal again via EndSync, then prove StartNewSync (which must wipe the
	// sealed prior sync via ResetForNewSync) also unseals.
	require.NoError(t, a.EndSync(ctx))
	require.True(t, e.IsSealed())
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err, "StartNewSync over a sealed finished sync must work (ResetForNewSync is seal-exempt)")
	require.False(t, e.IsSealed(), "StartNewSync must unseal")
	require.False(t, e.compactionScheduler.paused.Load())

	// Seal once more and prove a reopen starts unsealed.
	require.NoError(t, a.EndSync(ctx))
	require.True(t, e.compactionScheduler.paused.Load())
	require.True(t, e.IsSealed())
	require.NoError(t, e.Close())

	e2, err := Open(ctx, dir)
	require.NoError(t, err)
	defer e2.Close()
	require.False(t, e2.compactionScheduler.paused.Load(), "reopened engine must start unpaused")
	require.False(t, e2.IsSealed(), "reopened engine must start unsealed")
}
