package pebble_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// TestManifestSyncRunProjection locks in the envelope's sync-run
// projection: after a sync is written and the store saved, the v3
// manifest header alone (no payload unpack) must carry the sync run
// and its stats sidecar. This is what lets compaction source selection
// and overlay bucket planning skip unpacking sources entirely.
func TestManifestSyncRunProjection(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "projection.c1z")
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(t, w.PutResourceTypes(ctx, userRT, groupRT))
	user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	require.NoError(t, w.PutResources(ctx, user, group))
	ent := v2.Entitlement_builder{Id: "ent-1", Resource: group}.Build()
	require.NoError(t, w.PutEntitlements(ctx, ent))
	grant := v2.Grant_builder{Id: "grant-1", Entitlement: ent, Principal: user}.Build()
	require.NoError(t, w.PutGrants(ctx, grant))
	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	require.NoErrorf(t, err, "ReadManifestHeader")
	runs := m.GetSyncRuns()
	require.Len(t, runs, 1, "manifest sync_runs")
	run := runs[0]
	require.Equal(t, syncID, run.GetSyncId(), "summary sync_id")
	require.NotNil(t, run.GetEndedAt(), "summary ended_at is nil for a finished sync")
	stats := run.GetStats()
	require.NotNil(t, stats, "summary stats is nil; expected stats sidecar projection")
	require.Equal(t, int64(2), stats.GetResourceTypes(), "stats resource_types")
	require.Equal(t, int64(2), stats.GetResources(), "stats resources")
	require.Equal(t, int64(1), stats.GetEntitlements(), "stats entitlements")
	require.Equal(t, int64(1), stats.GetGrants(), "stats grants")

	// The engine-dispatch header path must surface the same projection.
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	env, err := formatv3.ReadEnvelopeHeader(f)
	require.NoErrorf(t, err, "ReadEnvelopeHeader")
	defer env.Close()
	require.Len(t, env.Manifest.GetSyncRuns(), 1, "ReadEnvelopeHeader sync_runs")
}
