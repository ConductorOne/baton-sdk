package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// TestPebbleFullSyncThroughSyncer drives pkg/sync.NewSyncer end-to-end
// against the Pebble engine via WithConnectorStore. Validates that the
// pebble.registeredStore satisfies dotc1z.C1ZStore for real (the
// var-decl compile-time guard catches missing methods; this test
// catches signature drift and runtime errors).
//
// Workload: small mockConnector with a couple of groups + members.
// Goal: full sync runs without error and the resulting c1z contains
// the expected grant count when read back.
func TestPebbleFullSyncThroughSyncer(t *testing.T) {
	ctx := t.Context()
	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	require.NoError(t, pebble.Register())

	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "pebble-sync.c1z")

	store, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(tempDir),
	)
	require.NoError(t, err)
	pebbleStore, ok := store.(dotc1z.C1ZStore)
	require.True(t, ok, "Pebble registered store must satisfy dotc1z.C1ZStore")

	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	group1, _, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	group2, _, err := mc.AddGroup(ctx, "g2")
	require.NoError(t, err)
	u1, err := mc.AddUser(ctx, "u1")
	require.NoError(t, err)
	u2, err := mc.AddUser(ctx, "u2")
	require.NoError(t, err)
	_ = mc.AddGroupMember(ctx, group1, u1)
	_ = mc.AddGroupMember(ctx, group1, u2)
	_ = mc.AddGroupMember(ctx, group2, u2)

	syncer, err := NewSyncer(ctx, mc,
		WithConnectorStore(pebbleStore),
		WithTmpDir(tempDir),
	)
	require.NoError(t, err)

	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	// Re-open the produced c1z and confirm grants round-trip.
	reopen, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer reopen.Close(ctx)

	// Resolve the sync we just wrote and set it active before
	// listing — re-opened Pebble stores don't auto-select a
	// current sync (matches SQLite's contract).
	latest, ok := reopen.(interface {
		LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error)
	})
	require.True(t, ok, "reopened store should implement LatestFinishedSyncIDFetcher")
	syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.NotEmpty(t, syncID, "expected at least one finished sync after Syncer.Sync")
	require.NoError(t, reopen.SetCurrentSync(ctx, syncID))

	resp, err := reopen.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	// Two member entitlements × 3 group-memberships = 3 grants total.
	require.Len(t, resp.GetList(), 3, "expected 3 grants total")
}
