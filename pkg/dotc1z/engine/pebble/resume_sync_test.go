package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// The compactor's incremental expansion reopens a sync it just merged into, so
// "resume failed" and "no such sync, start fresh" must never be conflated:
// starting fresh wipes the merged records. These two tests pin the difference
// between the verbs, which is why pkg/synccompactor calls ResumeSync directly.

// TestResumeSyncUnknownIDFailsClosed: an unresolvable sync id must produce an
// error and leave the destination's records intact.
func TestResumeSyncUnknownIDFailsClosed(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	require.NoError(t, a.EndSync(ctx))
	require.NotEmpty(t, syncID)

	_, err = a.ResumeSync(ctx, connectorstore.SyncTypeFull, "no-such-sync")
	require.Error(t, err, "ResumeSync must reject an unresolvable sync id")

	// The failure must not have cost us the data.
	require.NoError(t, a.SetCurrentSync(ctx, syncID))
	resp, err := a.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "ResumeSync failure must leave records intact")
}

// TestResumeSyncOnEndedSyncAllowsWrites pins the contract the compactor's
// incremental expansion actually depends on: the merge leaves the sync ENDED,
// and expansion must reopen it and write grants into it. Resuming an ended sync
// therefore has to unseal the engine, not just rebind the current sync id.
// Without this, a change separating rebind from unseal would surface as
// ErrEngineSealed on the first grant write, caught only by the much slower
// pkg/synccompactor integration tests.
func TestResumeSyncOnEndedSyncAllowsWrites(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	require.NoError(t, a.EndSync(ctx))

	resumed, err := a.ResumeSync(ctx, connectorstore.SyncTypeFull, syncID)
	require.NoError(t, err, "resuming an ended sync must succeed")
	require.Equal(t, syncID, resumed)

	// The write is the point: a rebind that left the engine sealed would fail here.
	require.NoError(t, a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()),
		"resumed ended sync must accept writes")

	resp, err := a.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 2, "the resumed write must land alongside the originals")
}

// TestStartOrResumeSyncUnknownIDWipesRecords documents the hazard the compactor
// avoids: StartOrResumeSync treats an unresolvable id as "nothing to resume"
// and starts a new sync, whose ResetForNewSync excises the record range. Correct
// for the syncer, which passes an empty id; destructive for a caller that knows
// the sync exists.
//
// This documents current behavior, not desired behavior. SQLite's C1File
// (sync_runs.go:704) returns NotFound instead of starting a new sync when an
// explicit id fails to resolve, so the two engines diverge here despite
// adapter.go's claim to mirror the SQLite cascade. If Pebble is ever aligned,
// invert the assertions below — a failure here is that alignment landing, not a
// regression.
func TestStartOrResumeSyncUnknownIDWipesRecords(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	require.NoError(t, a.EndSync(ctx))

	newID, startedNew, err := a.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "no-such-sync")
	require.NoError(t, err)
	require.True(t, startedNew, "unresolvable id falls through to StartNewSync")
	require.NotEqual(t, syncID, newID)

	resp, err := a.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, resp.GetList(), "StartNewSync's reset drops the prior records")
}
