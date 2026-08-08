package pebble

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestGetGrantResolvesSyncAfterEndSync locks in the fix for the sync
// resolution kans flagged: after EndSync clears the in-memory current-sync
// binding, GetGrant (v2 and v3) must still resolve the grant via the
// persisted SyncRunRecord (resolveActiveSyncForReader → latest-finished),
// exactly like the List readers. Before the fix, GetGrant gated on
// CurrentSyncID() alone and returned ErrNoCurrentSync here.
func TestGetGrantResolvesSyncAfterEndSync(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	rec := v3.GrantRecord_builder{
		ExternalId: "g-1",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
		}.Build(),
		Principal: v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "alice"}.Build(),
	}.Build()
	require.NoError(t, e.PutGrantRecord(ctx, rec))
	require.NoError(t, e.EndSync(ctx))

	// The binding is cleared by EndSync; the sync_id is still on disk.
	require.Empty(t, e.currentSyncID(), "EndSync should clear the in-memory binding")

	// v2 GetGrant resolves from the persisted sync record.
	v2Resp, err := e.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-1"}.Build())
	require.NoError(t, err, "v2 GetGrant must resolve the sync from the persisted record after EndSync")
	require.Equal(t, "g-1", v2Resp.GetGrant().GetId())

	// v3 GetGrant does the same, returning the rich record.
	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok)
	v3Resp, err := provider.V3GrantReader().GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-1"}.Build())
	require.NoError(t, err, "v3 GetGrant must resolve the sync from the persisted record after EndSync")
	require.Equal(t, "g-1", v3Resp.GetGrant().GetExternalId())
}
