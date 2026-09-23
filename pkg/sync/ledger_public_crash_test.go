package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	stdsync "sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type ledgerPublicCrashStore struct {
	c1zstore.PageLedgerStore
	cut func(string)
}

func (s ledgerPublicCrashStore) TakeoverPendingWork(
	ctx context.Context, runID, expected string, facts []string, counters c1zstore.LedgerCounters, work []c1zstore.LedgerWork,
) (string, error) {
	s.cut("takeover-before")
	token, err := s.PageLedgerStore.TakeoverPendingWork(ctx, runID, expected, facts, counters, work)
	if err == nil {
		s.cut("takeover-after")
	}
	return token, err
}

func ledgerPublicLegacyToken(version int) string {
	if version == 0 {
		return `{"actions":[{"operation":"list-resources","resource_type_id":"cost-0","page_token":"1"},` +
			`{"operation":"list-resources","resource_type_id":"cost-1","page_token":"1"}],"completed_actions_count":2}`
	}
	return fmt.Sprintf(`{"version":%d,"actions_map":{"a":{"id":"a","operation":"list-resources","resource_type_id":"cost-0","page_token":"1"},`+
		`"b":{"id":"b","operation":"list-resources","resource_type_id":"cost-1","page_token":"1"}},`+
		`"action_order":["a","b"],"current_action_id":2,"completed_actions_count":2,"ingest_quality":{}}`, version)
}

func seedLedgerPublicLegacy(t *testing.T, f *ledgerFixture, c *ledgerPublicCrashConnector, version int) {
	t.Helper()
	_, err := f.store.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.store.PutResourceTypes(t.Context(), c.rtDB...))
	for stream := range 2 {
		response, err := c.ledgerCostConnector.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: fmt.Sprintf("cost-%d", stream)}.Build())
		require.NoError(t, err)
		require.Equal(t, "1", response.GetNextPageToken())
		require.NoError(t, f.store.PutResources(t.Context(), response.GetList()...))
	}
	require.NoError(t, f.store.CheckpointSync(t.Context(), ledgerPublicLegacyToken(version)))
	require.NoError(t, f.engine.Flush(t.Context()))
}

func (s ledgerPublicCrashStore) BeginPage() c1zstore.PageWriter {
	return ledgerPublicCrashWriter{PageWriter: s.PageLedgerStore.BeginPage(), cut: s.cut}
}
func (s ledgerPublicCrashStore) EndSyncWithStats(ctx context.Context, stats c1zstore.SyncStats) error {
	if err := s.PageLedgerStore.EndSyncWithStats(ctx, stats); err != nil {
		return err
	}
	s.cut("sealed")
	return nil
}
func (s ledgerPublicCrashStore) ArchiveLedgerReport(ctx context.Context) ([]byte, error) {
	report, err := s.PageLedgerStore.ArchiveLedgerReport(ctx)
	if err == nil {
		s.cut("archived")
	}
	return report, err
}

type ledgerPublicCrashWriter struct {
	c1zstore.PageWriter
	cut func(string)
}

func (w ledgerPublicCrashWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	phase := ""
	if id.Op == SyncResourcesOp.String() && id.ResourceTypeID != "" {
		phase = "resource"
	}
	if id.Op == ledgerTerminalOp {
		phase = "terminal"
	}
	if phase != "" {
		w.cut(phase + "-before")
	}
	if err := w.PageWriter.Commit(ctx, id, row); err != nil {
		return err
	}
	if phase != "" {
		w.cut(phase + "-after")
	}
	return nil
}

type ledgerPublicCrashConnector struct {
	*ledgerCostConnector
	mu        stdsync.Mutex
	seen      map[c1zstore.LedgerActionIdentity]bool
	committed map[c1zstore.LedgerActionIdentity]bool
	finished  bool
}

func newLedgerPublicCrashConnector() *ledgerPublicCrashConnector {
	c := &ledgerPublicCrashConnector{ledgerCostConnector: &ledgerCostConnector{mockConnector: newMockConnector(), pages: 6, records: 2, streams: 2}, seen: make(map[c1zstore.LedgerActionIdentity]bool)}
	for stream := range 2 {
		c.rtDB = append(c.rtDB, v2.ResourceType_builder{Id: fmt.Sprintf("cost-%d", stream), Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{})}.Build())
	}
	return c
}
func (c *ledgerPublicCrashConnector) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	id := c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: req.GetResourceTypeId(), PageToken: req.GetPageToken()}
	c.mu.Lock()
	invalid := c.finished || c.committed[id] || c.seen[id]
	c.seen[id] = true
	c.mu.Unlock()
	if invalid {
		return nil, fmt.Errorf("connector called for completed or repeated page: %v", id)
	}
	return c.ledgerCostConnector.ListResources(ctx, req, opts...)
}

func TestLedgerPublicCrashResume(t *testing.T) {
	if cut := os.Getenv("BATON_LEDGER_PUBLIC_CRASH_CUT"); cut != "" {
		path := os.Getenv("BATON_LEDGER_PUBLIC_CRASH_FILE")
		workers, err := strconv.Atoi(os.Getenv("BATON_LEDGER_PUBLIC_CRASH_WORKERS"))
		require.NoError(t, err)
		f := openLedgerFixtureAt(t, path, false)
		version, err := strconv.Atoi(os.Getenv("BATON_LEDGER_PUBLIC_CRASH_VERSION"))
		require.NoError(t, err)
		connector := newLedgerPublicCrashConnector()
		if version >= 0 {
			seedLedgerPublicLegacy(t, f, connector, version)
		}
		created, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store), WithWorkerCount(workers), WithDontExpandGrants())
		require.NoError(t, err)
		s := created.(*syncer)
		s.caps.pageLedger = ledgerPublicCrashStore{PageLedgerStore: f.ledger, cut: func(reached string) {
			if reached != cut {
				return
			}
			if os.Getenv("BATON_LEDGER_PUBLIC_CRASH_IMAGE") == "flushed" {
				require.NoError(t, f.engine.Flush(t.Context()))
			}
			marker, err := json.Marshal(ledgerCrashMarker{Cut: cut, SyncID: s.syncID})
			require.NoError(t, err)
			require.NoError(t, writeLedgerTestFile(path+".cut", marker, 0600))
			os.Exit(75)
		}}
		require.NoError(t, s.Sync(t.Context()))
		t.Fatal("crash cut was not reached")
	}
	for _, version := range []int{-1, 0, 1, 2} {
		for _, image := range []string{"wal", "flushed"} {
			if version >= 0 && image != "flushed" {
				continue
			}
			for _, workers := range []int{1, 4} {
				cuts := []string{"resource-before", "resource-after", "terminal-before", "terminal-after", "sealed", "archived"}
				if version >= 0 {
					cuts = []string{"takeover-before", "takeover-after", "resource-after"}
				}
				for _, cut := range cuts {
					if image == "flushed" && (cut == "sealed" || cut == "archived") {
						continue
					}
					t.Run(fmt.Sprintf("version-%d/%s/workers-%d/%s", version, image, workers, cut), func(t *testing.T) {
						path := filepath.Join(t.TempDir(), "crash.c1z")
						runLedgerCrashChild(t, "^TestLedgerPublicCrashResume$", 75, "BATON_LEDGER_PUBLIC_CRASH_VERSION="+strconv.Itoa(version), "BATON_LEDGER_PUBLIC_CRASH_IMAGE="+image, "BATON_LEDGER_PUBLIC_CRASH_CUT="+cut,
							"BATON_LEDGER_PUBLIC_CRASH_FILE="+path, "BATON_LEDGER_PUBLIC_CRASH_WORKERS="+strconv.Itoa(workers))
						data, err := os.ReadFile(path + ".cut")
						require.NoError(t, err)
						var marker ledgerCrashMarker
						require.NoError(t, json.Unmarshal(data, &marker))
						require.Equal(t, cut, marker.Cut)
						require.NotEmpty(t, marker.SyncID)
						dirs, err := filepath.Glob(filepath.Join(filepath.Dir(path), "c1z-pebble*", "db"))
						require.NoError(t, err)
						require.Len(t, dirs, 1)
						recovered, err := engine.Open(t.Context(), dirs[0])
						require.NoError(t, err)
						defer func() { require.NoError(t, recovered.Close()) }()
						before := ledgerRawSnapshot(t, recovered)
						checkpoint := filepath.Join(t.TempDir(), "checkpoint")
						require.NoError(t, recovered.CheckpointTo(t.Context(), checkpoint))
						manifest, err := engine.BuildManifestWithSyncRuns(t.Context(), recovered, c1zstore.PayloadEncodingTarZstd)
						require.NoError(t, err)
						transported := filepath.Join(t.TempDir(), "recovered.c1z")
						out, err := os.Create(transported)
						require.NoError(t, err)
						_, err = formatv3.WriteEnvelopeWithReuse(out, manifest, checkpoint, nil)
						require.NoError(t, err)
						require.NoError(t, out.Close())
						f := openLedgerFixtureAt(t, transported, false)
						require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)), "recovery transport changed keys")
						require.NoError(t, f.store.SetCurrentSync(t.Context(), marker.SyncID))
						connector := newLedgerPublicCrashConnector()
						connector.committed = make(map[c1zstore.LedgerActionIdentity]bool)
						connector.finished, err = f.ledger.BoundSyncFinished(t.Context())
						require.NoError(t, err)
						if version < 0 && image == "flushed" && !connector.finished {
							_, initFound, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: InitOp.String()})
							require.NoError(t, err)
							require.True(t, initFound, "flushed image must exercise committed history")
						}
						if version >= 0 {
							token, err := f.store.CurrentSyncStep(t.Context())
							require.NoError(t, err)
							frontier, found, err := f.ledger.LedgerFrontier(t.Context())
							require.NoError(t, err)
							if cut == "takeover-before" {
								require.Equal(t, ledgerPublicLegacyToken(version), token)
								require.False(t, found)
							} else {
								require.Empty(t, token)
								require.True(t, found)
								require.Equal(t, ledgerPublicLegacyToken(version), frontier.State)
							}
						}
						missing := 0
						for global := range 6 {
							token := ""
							if global/2 > 0 {
								token = strconv.Itoa(global / 2)
							}
							id := c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: fmt.Sprintf("cost-%d", global%2), PageToken: token}
							_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
							require.NoError(t, err)
							legacyPage := version >= 0 && global/2 == 0
							connector.committed[id] = found || legacyPage
							if !found && !legacyPage {
								missing++
							}
							for record := range 2 {
								value, err := f.engine.GetResourceRecord(t.Context(), id.ResourceTypeID, fmt.Sprintf("%012d", global*2+record))
								if found || connector.finished || legacyPage {
									require.NoError(t, err)
									require.NotNil(t, value)
								} else {
									require.ErrorIs(t, err, pebble.ErrNotFound)
								}
							}
						}
						t.Logf("completed resource pages (including legacy seed): %d; finished: %t", 6-missing, connector.finished)
						if image == "flushed" && cut == "resource-after" {
							limit := 6
							if version >= 0 {
								limit = 4
							}
							require.Less(t, missing, limit)
						}
						if image == "flushed" && (cut == "terminal-before" || cut == "terminal-after") {
							require.Zero(t, missing)
						}
						opts := []SyncOpt{WithConnectorStore(f.store), WithSyncID(marker.SyncID), WithWorkerCount(workers)}
						if connector.finished {
							opts = append(opts, WithOnlyExpandGrants())
						} else {
							opts = append(opts, WithDontExpandGrants())
						}
						created, err := NewSyncer(t.Context(), connector, opts...)
						require.NoError(t, err)
						var walked bool
						var walkImage []ledgerKV
						created.(*syncer).testHooks.ledgerWalk = func(entering bool) {
							if entering {
								walked = true
								walkImage = ledgerRawSnapshot(t, f.engine)
								f.audit.enter(ledgerWalk)
							} else {
								require.True(t, equalLedgerSnapshot(walkImage, ledgerRawSnapshot(t, f.engine)))
								f.audit.enter(ledgerLifecycle)
							}
						}
						require.NoError(t, created.Sync(t.Context()))
						require.True(t, walked)
						if version >= 0 {
							require.EqualValues(t, 4, created.(*syncer).run.completedActionsCount())
						}
						if connector.finished {
							missing = 0
						}
						require.Len(t, connector.seen, missing)
						require.NoError(t, f.store.SetCurrentSync(t.Context(), marker.SyncID))
						token, err := f.store.CurrentSyncStep(t.Context())
						require.NoError(t, err)
						require.Empty(t, token)
						finished, err := f.ledger.BoundSyncFinished(t.Context())
						require.NoError(t, err)
						require.True(t, finished)
						response, err := f.store.ListResources(t.Context(), &v2.ResourcesServiceListResourcesRequest{})
						require.NoError(t, err)
						require.Len(t, response.GetList(), 12)
						var actual, expected []string
						for _, resource := range response.GetList() {
							require.Equal(t, "fixed-resource-payload", resource.GetDisplayName())
							actual = append(actual, resource.GetId().GetResourceType()+"/"+resource.GetId().GetResource())
						}
						for global := range 6 {
							for record := range 2 {
								expected = append(expected, fmt.Sprintf("cost-%d/%012d", global%2, global*2+record))
							}
						}
						require.ElementsMatch(t, expected, actual)
						report, err := f.ledger.GetArchivedLedgerReport(t.Context())
						require.NoError(t, err)
						require.NotEmpty(t, report)
						facts, err := f.ledger.LedgerFacts(t.Context())
						require.NoError(t, err)
						require.Empty(t, facts)
					})
				}
			}
		}
	}
}
