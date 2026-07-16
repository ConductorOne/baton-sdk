package pebble

// Pins the seal-failure contract: EndSync's finalize stamps ended_at on
// the sync-run record, and the durability flush (EndFreshSync) turns the
// sync's NoSync writes into on-disk state. If the flush FAILS, the sync
// must remain visibly UNFINISHED — LatestUnfinishedSyncRecord skips any
// record with ended_at, so a premature stamp costs resumability (the
// next StartOrResumeSync starts a new sync instead of resuming) and,
// after a crash, can surface a partially-durable artifact as a FINISHED
// sync to replay/uplift consumers.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestEndSyncFlushFailureKeepsSyncResumable(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "group:g1:member", "user", "alice")))

	injected := errors.New("injected flush failure at the durability boundary")
	e.testEndSyncFlushHook = func() error { return injected }
	err = a.EndSync(ctx)
	require.ErrorIs(t, err, injected, "the failed flush must fail EndSync")
	e.testEndSyncFlushHook = nil

	// THE CONTRACT: a sync whose durability flush failed is NOT finished.
	// It must still be discoverable as unfinished (resumable), and must
	// not carry ended_at (a stamped record with maybe-undurable data is
	// a finished-looking lie).
	unfinished, err := e.LatestUnfinishedSyncRecord(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, unfinished,
		"a failed seal must leave the sync resumable; a premature ended_at stamp orphans the work")
	require.Equal(t, syncID, unfinished.GetSyncId())

	rec, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.Nil(t, rec.GetEndedAt(),
		"ended_at must only be stamped after the durability flush succeeds")

	// Retry converges: the second EndSync finishes the sync for real.
	require.NoError(t, a.EndSync(ctx))
	rec, err = e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.NotNil(t, rec.GetEndedAt(), "the retried seal must stamp the sync finished")
	unfinished, err = e.LatestUnfinishedSyncRecord(ctx, func(v3.SyncType) bool { return true })
	require.NoError(t, err)
	require.Nil(t, unfinished, "no unfinished run may remain after a successful seal")
}

// The stamp-commit analog of the flush test above (H1 residual): the
// durability flush SUCCEEDS, then the ended_at stamp's commit fails.
// endSyncFinalize has already mutated the in-memory record
// (existing.SetEndedAt) by then — that stamp must leak nowhere, and a
// retried EndSync must converge. State assertions on the engine live in
// the obligations harness row (endsync-stamp-commit-failure); this test
// owns the adapter-level retry contract.
func TestEndSyncStampFailureKeepsSyncResumable(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("g1", "group:g1:member", "user", "alice")))

	injected := errors.New("injected stamp-commit failure after the durability flush")
	e.testEndSyncStampHook = func() error { return injected }
	err = a.EndSync(ctx)
	require.ErrorIs(t, err, injected, "the failed stamp commit must fail EndSync")
	e.testEndSyncStampHook = nil

	rec, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.Nil(t, rec.GetEndedAt(),
		"the in-memory stamp must not reach the store when its commit fails")
	unfinished, err := e.LatestUnfinishedSyncRecord(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, unfinished, "a failed stamp commit must leave the sync resumable")
	require.Equal(t, syncID, unfinished.GetSyncId())

	// Retry converges: the second EndSync reloads the (unstamped) stored
	// record and finishes the sync for real.
	require.NoError(t, a.EndSync(ctx))
	rec, err = e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.NotNil(t, rec.GetEndedAt(), "the retried seal must stamp the sync finished")
	unfinished, err = e.LatestUnfinishedSyncRecord(ctx, func(v3.SyncType) bool { return true })
	require.NoError(t, err)
	require.Nil(t, unfinished, "no unfinished run may remain after a successful seal")
}
