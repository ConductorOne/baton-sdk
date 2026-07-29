package dotc1z

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestGetGrantAfterReopenNoRebind is the e2e reopen path prod actually
// takes: write a Pebble c1z, seal it (EndSync), close, then reopen
// read-only and read a grant WITHOUT calling SetCurrentSync. This is the
// case no existing test covers — TestRegisteredPebbleNewStoreRoundtrip
// re-binds the sync before GetGrant, masking the gap, and
// TestV2GetGrant_HydratesAfterCloseReopen only exercises SQLite (whose
// GetGrant already falls back to the latest-finished sync).
//
// Both the v2 and the v3 GetGrant must hydrate from the latest-finished
// sync on a sealed file, matching ListGrants and the SQLite contract.
func TestGetGrantAfterReopenNoRebind(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sync.c1z")

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	wantID := mkV2GrantID("ent", "user", "alice")

	// Reopen read-only — NO SetCurrentSync. This is the prod read path.
	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { _ = reopened.Close(ctx) }()

	// v2 GetGrant must hydrate from the latest-finished sync.
	v2Resp, err := reopened.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: wantID,
	}.Build())
	require.NoError(t, err, "v2 GetGrant after reopen without SetCurrentSync")
	require.Equal(t, wantID, v2Resp.GetGrant().GetId())

	// v3 GetGrant must do the same, returning the rich record.
	provider, ok := reopened.(connectorstore.V3GrantReaderProvider)
	require.True(t, ok, "reopened Pebble store must expose V3GrantReaderProvider")
	v3Resp, err := provider.V3GrantReader().GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: wantID,
	}.Build())
	require.NoError(t, err, "v3 GetGrant after reopen without SetCurrentSync")
	require.Equal(t, wantID, v3Resp.GetGrant().GetExternalId())
}
