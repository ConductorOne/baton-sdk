package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type ledgerCostConnector struct {
	*mockConnector
	pages, records, streams int
	calls                   atomic.Int64
}

func (c *ledgerCostConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	stream, err := strconv.Atoi(strings.TrimPrefix(req.GetResourceTypeId(), "cost-"))
	if err != nil || stream < 0 || stream >= c.streams {
		return nil, fmt.Errorf("cost connector: invalid stream %q", req.GetResourceTypeId())
	}
	page := 0
	if req.GetPageToken() != "" {
		parsed, err := strconv.Atoi(req.GetPageToken())
		if err != nil {
			return nil, err
		}
		page = parsed
	}
	globalPage := stream + page*c.streams
	if page < 0 || globalPage >= c.pages {
		return nil, fmt.Errorf("cost connector: page %d outside [0,%d)", page, c.pages)
	}
	records := make([]*v2.Resource, c.records)
	for i := range records {
		records[i] = v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: req.GetResourceTypeId(), Resource: fmt.Sprintf("%012d", globalPage*c.records+i)}.Build(),
			DisplayName: "fixed-resource-payload",
		}.Build()
	}
	next := ""
	if globalPage+c.streams < c.pages {
		next = strconv.Itoa(page + 1)
	}
	c.calls.Add(1)
	return v2.ResourcesServiceListResourcesResponse_builder{List: records, NextPageToken: next}.Build(), nil
}

func ledgerCostInput(t *testing.T, name string, fallback int) int {
	t.Helper()
	if value := os.Getenv(name); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		require.Positive(t, parsed)
		return parsed
	}
	return fallback
}

func TestLedgerCostBaseline(t *testing.T) {
	if os.Getenv("BATON_LEDGER_COST") != "1" {
		t.Skip("opt-in cost driver; use a separate process per cell")
	}
	pages := ledgerCostInput(t, "BATON_LEDGER_COST_PAGES", 10)
	records := ledgerCostInput(t, "BATON_LEDGER_COST_RECORDS", 100)
	workers := ledgerCostInput(t, "BATON_LEDGER_COST_WORKERS", 1)
	require.GreaterOrEqual(t, pages, workers)
	connector := &ledgerCostConnector{mockConnector: newMockConnector(), pages: pages, records: records, streams: workers}
	for stream := range workers {
		connector.rtDB = append(connector.rtDB, v2.ResourceType_builder{
			Id: fmt.Sprintf("cost-%d", stream), DisplayName: "Cost resources", Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
		}.Build())
	}
	path := filepath.Join(t.TempDir(), "cost.c1z")
	ctx := t.Context()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	raw, ok := engine.AsEngine(store)
	require.True(t, ok)
	runner, err := NewSyncer(ctx, connector, WithConnectorStore(store), WithWorkerCount(workers), WithDontExpandGrants())
	require.NoError(t, err)
	var checkpoints atomic.Int64
	runner.(*syncer).testHooks.checkpointHook = func(string) { checkpoints.Add(1) }
	runner.(*syncer).testHooks.ingestHaltHook = ledgerCostMetricsHook(func() string { return raw.Metrics().String() })
	started := time.Now()
	require.NoError(t, runner.Sync(ctx))
	elapsed := time.Since(started)
	require.Positive(t, checkpoints.Load(), "baseline must execute checkpoint path")
	require.EqualValues(t, pages, connector.calls.Load())
	count := 0
	token := ""
	for {
		resp, listErr := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: token}.Build())
		require.NoError(t, listErr)
		count += len(resp.GetList())
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}
	require.Equal(t, pages*records, count)
	metrics := raw.Metrics()
	var flushed, compacted uint64
	for _, level := range metrics.Levels {
		flushed += level.TableBytesFlushed + level.BlobBytesFlushed
		compacted += level.TableBytesCompacted + level.BlobBytesCompacted
	}
	require.NoError(t, runner.Close(ctx))
	info, err := os.Stat(path)
	require.NoError(t, err)
	result := map[string]any{
		"arm": "token-path", "pages": pages, "records_per_page": records, "workers": workers,
		"sync_wall_ns": elapsed.Nanoseconds(), "wal_bytes_before_close": metrics.WAL.BytesWritten,
		"flush_bytes_before_close": flushed, "compaction_bytes_before_close": compacted,
		"c1z_bytes": info.Size(), "resources_verified": count, "scope": "baseline smoke; not C49 measurement evidence",
	}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	if output := os.Getenv("BATON_LEDGER_COST_OUTPUT"); output != "" {
		require.NoError(t, writeLedgerTestFile(output, append(encoded, '\n'), 0600))
	}
	t.Log(string(encoded))
}

func ledgerCostMetricsHook(metrics func() string) func(string) error {
	path := os.Getenv("BATON_LEDGER_COST_PRESEAL_METRICS")
	if path == "" {
		return nil
	}
	return func(stage string) error {
		if stage != haltStageInvariantsComplete {
			return nil
		}
		return writeLedgerTestFile(path, []byte(metrics()), 0600)
	}
}

func writeLedgerTestFile(path string, data []byte, mode os.FileMode) error {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	return root.WriteFile(filepath.Base(path), data, mode)
}
