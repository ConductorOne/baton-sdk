package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestResolveActiveSyncForReader_UnfinishedFallback verifies the
// read-path fallback to the latest in-progress sync, matching
// SQLite's resolveSyncIDForRead cascade. A c1z whose only sync was
// interrupted before EndSync must still resolve for reads instead of
// returning ErrNoCurrentSync.
func TestResolveActiveSyncForReader_UnfinishedFallback(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, a.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()))
	// Intentionally NO EndSync — the sync stays in-progress.

	// A fresh adapter over the same engine has no current sync set and
	// there is no finished sync, so the reader must fall back to the
	// in-progress sync.
	reader := NewAdapter(e)

	got, err := reader.resolveActiveSyncForReader(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, syncID, got, "reader must fall back to the latest in-progress sync")

	resp, err := reader.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "group",
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "g1", resp.GetList()[0].GetId().GetResource())
}

// TestResolveActiveSyncForReader_UnfinishedCutoff verifies the
// one-week cutoff: an in-progress sync older than a week is treated
// as abandoned and NOT used as the read fallback, matching SQLite's
// getLatestUnfinishedSync.
func TestResolveActiveSyncForReader_UnfinishedCutoff(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	// Write an in-progress (ended_at nil) sync record started 8 days
	// ago, directly — past the one-week cutoff.
	staleID := ksuid.New().String()
	require.NoError(t, e.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId:    staleID,
		Type:      v3.SyncType_SYNC_TYPE_FULL,
		StartedAt: timestamppb.New(time.Now().AddDate(0, 0, -8)),
	}.Build()))

	reader := NewAdapter(e)
	got, err := reader.resolveActiveSyncForReader(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got, "an in-progress sync older than a week must not be used as the read fallback")
}
