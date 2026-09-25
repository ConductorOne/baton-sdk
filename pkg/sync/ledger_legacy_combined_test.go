package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	stdsync "sync"
	"testing"
	"testing/synctest"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type legacyCombinedConnector struct {
	*ledgerFamilyConnector
	mu        stdsync.Mutex
	remaining map[string]bool
}

func (*legacyCombinedConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return nil, fmt.Errorf("repeated legacy resource types")
}
func (*legacyCombinedConnector) ListResources(context.Context, *v2.ResourcesServiceListResourcesRequest, ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	return nil, fmt.Errorf("repeated legacy resources")
}
func (*legacyCombinedConnector) ListEntitlements(context.Context, *v2.EntitlementsServiceListEntitlementsRequest, ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return nil, fmt.Errorf("repeated legacy entitlements")
}
func (*legacyCombinedConnector) ListStaticEntitlements(
	context.Context, *v2.EntitlementsServiceListStaticEntitlementsRequest, ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return nil, fmt.Errorf("repeated legacy static entitlements")
}
func (c *legacyCombinedConnector) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	key := req.GetResource().GetId().GetResource() + "/" + req.GetPageToken()
	c.mu.Lock()
	allowed := c.remaining[key]
	delete(c.remaining, key)
	c.mu.Unlock()
	if !allowed {
		return nil, fmt.Errorf("repeated or unexpected grant request: %s", key)
	}
	return c.ledgerFamilyConnector.ListGrants(ctx, req, opts...)
}

type legacyCombinedStore struct{ ledgerPublicCrashStore }

func (s legacyCombinedStore) BeginPage() c1zstore.PageWriter {
	return ledgerFamilyWriter{PageWriter: s.PageLedgerStore.BeginPage(), cut: func(id c1zstore.LedgerActionIdentity, after bool) {
		phase := ""
		if id.Op == SyncGrantsOp.String() && id.ResourceID != "" {
			phase = "grant"
		}
		if id.Op == ledgerTerminalOp {
			phase = "terminal"
		}
		if phase != "" {
			if after {
				s.cut(phase + "-after")
			} else {
				s.cut(phase + "-before")
			}
		}
	}}
}

type legacyCombinedResult struct {
	data     []ledgerKV
	stats    *v3.SyncStatsRecord
	counters c1zstore.LedgerCounters
}

func readLegacyCombinedResult(t *testing.T, path string, current bool) legacyCombinedResult {
	t.Helper()
	f := openLedgerFixtureAt(t, path, false)
	run, err := f.store.SyncMeta().LatestFinishedSyncOfAnyType(t.Context())
	require.NoError(t, err)
	require.NotNil(t, run)
	result := legacyCombinedResult{}
	result.stats, err = engine.ReadSyncStatsRecord(t.Context(), f.engine, run.ID)
	require.NoError(t, err)
	require.NotNil(t, result.stats)
	require.EqualValues(t, 2, result.stats.GetResourceTypes())
	require.EqualValues(t, 4, result.stats.GetResources())
	require.EqualValues(t, 4, result.stats.GetEntitlements())
	require.EqualValues(t, 4, result.stats.GetGrants())
	result.stats.SetWrittenAt(nil)
	result.stats.SetStepDurationsMs(nil)
	for _, call := range result.stats.GetConnectorCallStats() {
		call.SetTotalMs(0)
		call.SetMaxMs(0)
	}
	for _, call := range result.stats.GetSessionStoreStats() {
		call.SetTotalMs(0)
		call.SetMaxMs(0)
	}
	for _, kv := range ledgerRawSnapshot(t, f.engine) {
		if len(kv.key) > 1 && (kv.key[1] >= 1 && kv.key[1] <= 5 || kv.key[1] == 7 || kv.key[1] == 8 || kv.key[1] == 10) {
			result.data = append(result.data, kv)
		}
		if bytes.Equal(kv.key, append([]byte{3, 255}, []byte("ledger-archive")...)) {
			var archive struct {
				Counters c1zstore.LedgerCounters `json:"counters"`
			}
			require.NoError(t, json.Unmarshal(kv.value, &archive))
			result.counters = archive.Counters
			result.counters.StepDurationsMs = nil
			for key, call := range result.counters.ConnectorCalls {
				call.TotalMs, call.MaxMs = 0, 0
				result.counters.ConnectorCalls[key] = call
			}
			for key, call := range result.counters.SessionCalls {
				call.TotalMs, call.MaxMs = 0, 0
				result.counters.SessionCalls[key] = call
			}
		}
	}
	require.NotEmpty(t, result.data)
	if current {
		record, err := f.engine.GetSyncRunRecord(t.Context(), run.ID)
		require.NoError(t, err)
		require.Empty(t, record.GetSyncToken())
		facts, err := f.ledger.LedgerFacts(t.Context())
		require.NoError(t, err)
		require.Empty(t, facts)
		report, err := f.ledger.GetArchivedLedgerReport(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, report)
		require.Positive(t, result.counters.Counters[ledgerCompletedActions])
	}
	require.NoError(t, f.store.Close(t.Context()))
	return result
}

func bindLegacyCombinedSync(t *testing.T, f *ledgerFixture) {
	t.Helper()
	var ids []string
	require.NoError(t, f.engine.IterateAllSyncRuns(t.Context(), func(record *v3.SyncRunRecord) bool {
		ids = append(ids, record.GetSyncId())
		return true
	}))
	require.Len(t, ids, 1)
	require.NoError(t, f.store.SetCurrentSync(t.Context(), ids[0]))
}

func runLegacyCombinedMigration(t *testing.T, path string, workers int, cut string, flush bool) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		f := openLedgerFixtureAt(t, path, false)
		bindLegacyCombinedSync(t, f)
		c := &legacyCombinedConnector{ledgerFamilyConnector: newLedgerFamilyConnector(t), remaining: map[string]bool{}}
		grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
		require.NoError(t, err)
		present := map[string]bool{}
		for _, g := range grants.GetList() {
			present[g.GetId()] = true
		}
		for group := range 2 {
			resource := fmt.Sprintf("group-%d", group)
			id := c1zstore.LedgerActionIdentity{Op: SyncGrantsOp.String(), ResourceTypeID: groupResourceType.GetId(), ResourceID: resource, PageToken: "1"}
			_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, found, present[c.grantDB[resource][1].GetId()], "page record and committed row disagree")
			if !found {
				c.remaining[resource+"/1"] = true
			}
		}
		created, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSyncID(f.engine.CurrentSyncID()), WithWorkerCount(workers), WithDontExpandGrants())
		require.NoError(t, err)
		s := created.(*syncer)
		observeLedgerRestore(t, s, f)
		s.testHooks.checkpointHook = func(string) { t.Error("migration wrote a checkpoint") }
		s.caps.pageLedger = legacyCombinedStore{ledgerPublicCrashStore{PageLedgerStore: f.ledger, cut: func(reached string) {
			if reached != cut {
				return
			}
			if flush {
				require.NoError(t, f.engine.Flush(t.Context()))
			}
			marker, err := json.Marshal(ledgerCrashMarker{Cut: cut, SyncID: s.syncID})
			require.NoError(t, err)
			require.NoError(t, writeLedgerTestFile(path+".cut", marker, 0600))
			os.Exit(79)
		}}}
		require.NoError(t, s.Sync(t.Context()))
		require.Empty(t, cut, "crash point was not reached")
		require.Empty(t, c.remaining, "unfinished connector pages were skipped")
		require.NoError(t, f.store.Close(t.Context()))
	})
}

func TestLedgerHistoricalMigrationCombinedCrashes(t *testing.T) {
	if cut := os.Getenv("BATON_LEGACY_COMBINED_CUT"); cut != "" {
		workers, err := strconv.Atoi(os.Getenv("BATON_LEGACY_COMBINED_WORKERS"))
		require.NoError(t, err)
		runLegacyCombinedMigration(t, os.Getenv("BATON_LEGACY_COMBINED_FILE"), workers, cut, os.Getenv("BATON_LEGACY_COMBINED_FLUSH") == "true")
		t.Fatal("crash point not reached")
	}
	source := os.Getenv("BATON_LEGACY_RICH_ARTIFACT")
	if source == "" {
		t.Skip("requires the baseline SDK rich checkpoint and completed control")
	}
	data, err := os.ReadFile(source)
	require.NoError(t, err)
	copyInput := func() string {
		path := filepath.Join(t.TempDir(), "legacy.c1z")
		require.NoError(t, writeLedgerTestFile(path, data, 0600))
		return path
	}
	f := openLedgerFixtureAt(t, copyInput(), false)
	bindLegacyCombinedSync(t, f)
	token, err := f.store.CurrentSyncStep(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, token)
	var checkpoint struct {
		Completed uint64 `json:"completed_actions_count"`
		Calls     map[string]struct {
			Count int64 `json:"count"`
		} `json:"connector_call_stats"`
		Actions map[string]struct {
			Op       string `json:"operation"`
			Resource string `json:"resource_id"`
			Token    string `json:"page_token"`
		} `json:"actions_map"`
	}
	require.NoError(t, json.Unmarshal([]byte(token), &checkpoint))
	pending := map[string]bool{}
	for _, a := range checkpoint.Actions {
		if a.Op == SyncGrantsOp.String() && a.Resource != "" {
			require.Equal(t, "1", a.Token)
			pending[a.Resource] = true
		}
	}
	require.Equal(t, map[string]bool{"group-0": true, "group-1": true}, pending)
	require.Len(t, checkpoint.Actions, 3)
	require.NoError(t, f.store.Close(t.Context()))
	oldComplete := readLegacyCombinedResult(t, source+".complete.c1z", false)
	baselinePath := copyInput()
	runLegacyCombinedMigration(t, baselinePath, 4, "", false)
	baseline := readLegacyCombinedResult(t, baselinePath, true)
	require.Equal(t, oldComplete.data, baseline.data, "migration differs from completing the same checkpoint on the old SDK")
	require.True(t, proto.Equal(oldComplete.stats, baseline.stats), "old SDK=%s new SDK=%s", oldComplete.stats, baseline.stats)
	require.Equal(t, checkpoint.Completed+3, baseline.counters.Counters[ledgerCompletedActions])
	for method, call := range checkpoint.Calls {
		expected := call.Count
		if method == "list-grants" || method == "list-grants:"+groupResourceType.GetId() {
			expected += 2
		}
		require.Equal(t, expected, baseline.stats.GetConnectorCallStats()[method].GetCount(), method)
	}
	chains := [][]string{{"takeover-before"}, {"takeover-after"}, {"grant-before"}, {"grant-after"}, {"terminal-before"}, {"terminal-after"}, {"takeover-after", "grant-after", "terminal-before"}}
	for _, workers := range []int{1, 4} {
		for _, flush := range []bool{false, true} {
			for _, chain := range chains {
				t.Run(fmt.Sprintf("workers-%d/flush-%t/%s", workers, flush, strings.Join(chain, "+")), func(t *testing.T) {
					path := copyInput()
					for _, cut := range chain {
						runLedgerCrashChild(t, "^TestLedgerHistoricalMigrationCombinedCrashes$", 79,
							"BATON_LEGACY_COMBINED_CUT="+cut, "BATON_LEGACY_COMBINED_FILE="+path,
							"BATON_LEGACY_COMBINED_WORKERS="+strconv.Itoa(workers), "BATON_LEGACY_COMBINED_FLUSH="+strconv.FormatBool(flush))
						marker, err := os.ReadFile(path + ".cut")
						require.NoError(t, err)
						var reached ledgerCrashMarker
						require.NoError(t, json.Unmarshal(marker, &reached))
						require.Equal(t, cut, reached.Cut)
						path = recoverLedgerFamilyFile(t, path)
						probe := openLedgerFixtureAt(t, path, false)
						bindLegacyCombinedSync(t, probe)
						state, err := probe.store.CurrentSyncStep(t.Context())
						require.NoError(t, err)
						frontier, found, err := probe.ledger.LedgerFrontier(t.Context())
						require.NoError(t, err)
						if cut == "takeover-before" {
							require.Equal(t, token, state)
							require.False(t, found)
						} else {
							require.Empty(t, state)
							require.True(t, found)
							require.Equal(t, token, frontier.State)
						}
						require.NoError(t, probe.store.Close(t.Context()))
					}
					runLegacyCombinedMigration(t, path, 5-workers, "", false)
					actual := readLegacyCombinedResult(t, path, true)
					require.Equal(t, baseline.data, actual.data)
					require.True(t, proto.Equal(baseline.stats, actual.stats), "baseline=%s resumed=%s", baseline.stats, actual.stats)
					require.Equal(t, baseline.counters, actual.counters)
				})
			}
		}
	}
}
