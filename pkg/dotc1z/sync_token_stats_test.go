package dotc1z_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestEndSyncPersistsSyncTokenStats locks in that EndSync lifts
// step_durations_ms / connector_call_stats / session_store_stats from
// the syncer token into SyncStats (StatsV2 / GetSync).
func TestEndSyncPersistsSyncTokenStats(t *testing.T) {
	ctx := context.Background()

	token := map[string]any{
		"version": 1,
		"step_durations_ms": map[string]int64{
			"rate_limit_wait":      1500,
			"rate_limit_wait:user": 1500,
			"list-grants":          4000,
		},
		"connector_call_stats": map[string]any{
			"list-grants": map[string]int64{
				"count":    3,
				"total_ms": 4000,
				"max_ms":   2000,
			},
		},
		"session_store_stats": map[string]any{
			"get": map[string]int64{
				"count":    10,
				"errors":   1,
				"timeouts": 1,
				"total_ms": 50,
				"max_ms":   20,
			},
		},
	}
	tokenJSON, err := json.Marshal(token)
	require.NoError(t, err)

	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), string(engine)+".c1z")
			store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
			require.NoError(t, err)
			defer func() { _ = store.Close(ctx) }()

			syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, store.PutResourceTypes(ctx,
				v2.ResourceType_builder{Id: "user"}.Build(),
			))
			require.NoError(t, store.CheckpointSync(ctx, string(tokenJSON)))
			require.NoError(t, store.EndSync(ctx))

			stats, err := store.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, syncID)
			require.NoError(t, err)
			require.NotNil(t, stats)

			require.Equal(t, int64(1500), stats.GetStepDurationsMs()["rate_limit_wait"])
			require.Equal(t, int64(1500), stats.GetStepDurationsMs()["rate_limit_wait:user"])
			require.Equal(t, int64(4000), stats.GetStepDurationsMs()["list-grants"])

			call := stats.GetConnectorCallStats()["list-grants"]
			require.NotNil(t, call)
			require.Equal(t, int64(3), call.GetCount())
			require.Equal(t, int64(4000), call.GetTotalMs())
			require.Equal(t, int64(2000), call.GetMaxMs())

			session := stats.GetSessionStoreStats()["get"]
			require.NotNil(t, session)
			require.Equal(t, int64(10), session.GetCount())
			require.Equal(t, int64(1), session.GetErrors())
			require.Equal(t, int64(1), session.GetTimeouts())
			require.Equal(t, int64(50), session.GetTotalMs())
			require.Equal(t, int64(20), session.GetMaxMs())

			// Row counts still populated alongside token stats.
			require.Equal(t, int64(1), stats.GetResourceTypes())
		})
	}
}

func TestApplySyncTokenStatsIgnoresGarbage(t *testing.T) {
	stats := &reader_v2.SyncStats{}
	c1zstore.ApplySyncTokenStats(stats, "not-json")
	require.Empty(t, stats.GetStepDurationsMs())
	c1zstore.ApplySyncTokenStats(stats, "")
	require.Empty(t, stats.GetStepDurationsMs())
	c1zstore.ApplySyncTokenStats(nil, `{"step_durations_ms":{"rate_limit_wait":1}}`)
}
