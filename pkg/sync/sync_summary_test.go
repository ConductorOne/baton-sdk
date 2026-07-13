package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"fmt"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestPartitionStepDurationsForLog(t *testing.T) {
	ops, waits, other := partitionStepDurationsForLog(map[string]int64{
		"list-grants":              1000,
		"list-resources":           0, // omitted
		"checkpoint":               29,
		"rate_limit_wait":          50,
		"rate_limit_wait:group":    50,
		"retry_wait":               10,
		"source_cache_tombstones":  0, // omitted
		"source_cache_tombstones2": 3,
	})
	require.Equal(t, map[string]int64{"list-grants": 1000}, ops)
	require.Equal(t, map[string]int64{
		"rate_limit_wait":       50,
		"rate_limit_wait:group": 50,
		"retry_wait":            10,
	}, waits)
	require.Equal(t, map[string]int64{
		"checkpoint":               29,
		"source_cache_tombstones2": 3,
	}, other)
}

func TestPartitionConnectorCallStatsForLog(t *testing.T) {
	flat, byRT := partitionConnectorCallStatsForLog(map[string]ConnectorCallStat{
		"list-grants":                        {Count: 626, TotalMs: 89988, MaxMs: 8042},
		"list-grants:enterprise_application": {Count: 454, TotalMs: 78241, MaxMs: 1174},
		"list-grants:group":                  {Count: 14, TotalMs: 10131, MaxMs: 8042},
		"list-grants:user":                   {Count: 150, TotalMs: 262, MaxMs: 10},
		"list-static-entitlements":           {Count: 7, TotalMs: 2, MaxMs: 2},
		"list-static-entitlements:user":      {Count: 1, TotalMs: 0, MaxMs: 0}, // noise
		"list-resources":                     {Count: 0, TotalMs: 0, MaxMs: 0}, // omitted
	})
	require.Equal(t, map[string]ConnectorCallStat{
		"list-grants":              {Count: 626, TotalMs: 89988, MaxMs: 8042},
		"list-static-entitlements": {Count: 7, TotalMs: 2, MaxMs: 2},
	}, flat)
	require.NotContains(t, byRT, "list-static-entitlements:user")
	require.Equal(t, ConnectorCallStat{Count: 454, TotalMs: 78241, MaxMs: 1174}, byRT["list-grants:enterprise_application"])
	require.Equal(t, ConnectorCallStat{Count: 14, TotalMs: 10131, MaxMs: 8042}, byRT["list-grants:group"])
	require.Equal(t, ConnectorCallStat{Count: 150, TotalMs: 262, MaxMs: 10}, byRT["list-grants:user"])
}

func TestPartitionConnectorCallStatsForLogTopN(t *testing.T) {
	all := map[string]ConnectorCallStat{
		"list-grants": {Count: 1, TotalMs: 100, MaxMs: 100},
	}
	for i := 0; i < topConnectorCallStatsByResourceType+5; i++ {
		all[fmt.Sprintf("list-grants:rt%02d", i)] = ConnectorCallStat{
			Count: 1, TotalMs: int64(100 - i), MaxMs: int64(100 - i),
		}
	}
	_, byRT := partitionConnectorCallStatsForLog(all)
	require.Len(t, byRT, topConnectorCallStatsByResourceType)
	require.Contains(t, byRT, "list-grants:rt00")
	require.NotContains(t, byRT, fmt.Sprintf("list-grants:rt%02d", topConnectorCallStatsByResourceType))
}

func TestFilterSessionStatsForLog(t *testing.T) {
	_, ok := filterSessionStatsForLog(nil)
	require.False(t, ok)

	_, ok = filterSessionStatsForLog(map[string]SessionStoreStat{
		"connector.get": {Count: 3, TotalMs: 1},
	})
	require.False(t, ok, "sub-threshold idle session stats should be omitted")

	out, ok := filterSessionStatsForLog(map[string]SessionStoreStat{
		"connector.get": {Count: 3, TotalMs: 30},
		"store.set":     {Count: 6, TotalMs: 40},
	})
	require.True(t, ok)
	require.Len(t, out, 2)

	out, ok = filterSessionStatsForLog(map[string]SessionStoreStat{
		"connector.get": {Count: 1, Errors: 1, Timeouts: 1, TotalMs: 1, MaxMs: 1},
	})
	require.True(t, ok, "timeouts always surface")
	require.EqualValues(t, 1, out["connector.get"].Timeouts)

	out, ok = filterSessionStatsForLog(map[string]SessionStoreStat{
		"connector.cache.get": {Count: 3, TotalMs: 100},
		"connector.get":       {Count: 3, TotalMs: 10},
	})
	require.True(t, ok, "cache vs backend divergence should surface")
	require.Len(t, out, 2)
}

func TestAggregateSessionStats(t *testing.T) {
	count, errors, timeouts, totalMs, maxMs := aggregateSessionStats(map[string]SessionStoreStat{
		"a": {Count: 2, Errors: 1, Timeouts: 1, TotalMs: 10, MaxMs: 8},
		"b": {Count: 3, TotalMs: 5, MaxMs: 4},
	})
	require.EqualValues(t, 5, count)
	require.EqualValues(t, 1, errors)
	require.EqualValues(t, 1, timeouts)
	require.EqualValues(t, 15, totalMs)
	require.EqualValues(t, 8, maxMs)
}

func TestSyncSummaryFieldsShape(t *testing.T) {
	s := &syncer{
		recordStats: true,
		syncID:      "sync-1",
		syncType:    connectorstore.SyncTypeFull,
		workerCount: 20,
		state:       newState(),
	}
	s.state.AddStepDuration("list-grants", time.Second)
	s.state.AddStepDuration("checkpoint", 29*time.Millisecond)
	s.state.AddStepDuration("rate_limit_wait", 50*time.Millisecond)
	s.state.AddStepDuration("source_cache_tombstones", 3*time.Millisecond)
	s.state.RecordConnectorCall("list-grants", 100*time.Millisecond)
	s.state.RecordConnectorCall("list-grants:group", 80*time.Millisecond)
	// Sub-ms static entitlement labeled entry should not appear in by-RT log output.
	s.state.RecordConnectorCall("list-static-entitlements:user", 0)
	s.state.MergeSessionStat("connector.get", SessionStoreStat{Count: 1, TotalMs: 1})

	fields := s.syncSummaryFields(trace.SpanFromContext(t.Context()))
	byKey := zapFieldsByKey(t, fields)

	require.Equal(t, map[string]int64{"list-grants": 1000}, byKey["sync_step_durations_ms"])
	require.Equal(t, map[string]int64{"rate_limit_wait": 50}, byKey["sync_step_wait_ms"])
	require.Equal(t, map[string]int64{"checkpoint": 29, "source_cache_tombstones": 3}, byKey["sync_step_other_ms"])
	require.EqualValues(t, int64(1000), byKey["sync_steps_total_ms"])

	flat, ok := byKey["connector_call_stats"].(map[string]ConnectorCallStat)
	require.True(t, ok)
	require.EqualValues(t, 1, flat["list-grants"].Count)
	require.NotContains(t, flat, "list-grants:group")

	byRT, ok := byKey["connector_call_stats_by_resource_type"].(map[string]ConnectorCallStat)
	require.True(t, ok)
	require.Contains(t, byRT, "list-grants:group")
	require.NotContains(t, byRT, "list-static-entitlements:user")

	_, hasSession := byKey["session_store_stats"]
	require.False(t, hasSession, "idle session stats should not appear on the sync-complete log")
}

func zapFieldsByKey(t *testing.T, fields []zap.Field) map[string]any {
	t.Helper()
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		switch f.Type {
		case zapcore.StringType:
			out[f.Key] = f.String
		case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type:
			out[f.Key] = f.Integer
		case zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type:
			out[f.Key] = uint64(f.Integer) //nolint:gosec // test helper
		case zapcore.BoolType:
			out[f.Key] = f.Integer == 1
		case zapcore.ReflectType, zapcore.ErrorType:
			out[f.Key] = f.Interface
		default:
			// zap.Any / zap.Int use mixed encodings; fall back to Interface then Integer.
			if f.Interface != nil {
				out[f.Key] = f.Interface
			} else {
				out[f.Key] = f.Integer
			}
		}
	}
	return out
}
