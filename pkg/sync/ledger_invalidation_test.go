package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// invalidatingConnector pages each resource type through "" → "T1" → "T2"
// with one resource per page. fail decides, per call, whether the call
// fails instead of answering.
type invalidatingConnector struct {
	*mockConnector
	types []string
	fail  func(rt, token string, nth int) error

	mu    native_sync.Mutex
	calls map[string][]string
}

func newInvalidatingConnector(types []string, fail func(rt, token string, nth int) error) *invalidatingConnector {
	return &invalidatingConnector{mockConnector: newMockConnector(), types: types, fail: fail, calls: map[string][]string{}}
}

func (c *invalidatingConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	list := make([]*v2.ResourceType, 0, len(c.types))
	for _, rt := range c.types {
		list = append(list, v2.ResourceType_builder{Id: rt, DisplayName: rt}.Build())
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: list}.Build(), nil
}

func (c *invalidatingConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	rt, token := req.GetResourceTypeId(), req.GetPageToken()
	c.mu.Lock()
	c.calls[rt] = append(c.calls[rt], token)
	nth := 0
	for _, seen := range c.calls[rt] {
		if seen == token {
			nth++
		}
	}
	c.mu.Unlock()
	if c.fail != nil {
		if err := c.fail(rt, token, nth); err != nil {
			return nil, err
		}
	}
	page, next := 0, "T1"
	switch token {
	case "T1":
		page, next = 1, "T2"
	case "T2":
		page, next = 2, ""
	}
	resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: rt, Resource: fmt.Sprintf("%s-p%d", rt, page)}.Build(), DisplayName: rt}.Build()
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{resource}, NextPageToken: next}.Build(), nil
}

func (c *invalidatingConnector) callsFor(rt string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls[rt])
}

func invalidateOnce(rt, restartToken string) func(string, string, int) error {
	return func(gotRT, token string, nth int) error {
		if gotRT == rt && token == "T1" && nth == 1 {
			return pagination.NewErrInvalidateAction(restartToken, "page token expired")
		}
		return nil
	}
}

func invalidateAlways(restartToken string) func(string, string, int) error {
	return func(_, token string, _ int) error {
		if token == "T1" {
			return pagination.NewErrInvalidateAction(restartToken, "page token expired")
		}
		return nil
	}
}

func storedResourceIDs(t *testing.T, store c1zstore.Store, rt string) []string {
	t.Helper()
	resp, err := store.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: rt, PageSize: 100}.Build())
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.GetList()))
	for _, r := range resp.GetList() {
		ids = append(ids, r.GetId().GetResource())
	}
	slices.Sort(ids)
	return ids
}

func newInvalidationSyncer(t *testing.T, f *ledgerFixture, c *invalidatingConnector, opts ...SyncOpt) *syncer {
	t.Helper()
	opts = append([]SyncOpt{WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true), WithWorkerCount(1)}, opts...)
	s, err := NewSyncer(t.Context(), c, opts...)
	require.NoError(t, err)
	return s.(*syncer)
}

func invalidationCounter(t *testing.T, f *ledgerFixture) uint64 {
	t.Helper()
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	return counters.Counters[ledgerActionInvalidations]
}

// C11, C13, C19: the next call carries the restart token, the sync
// finishes with the same resources as an uninterrupted run, and the
// restart leaves the replay block and the counters in the ledger.
func TestActionInvalidationRestartsAction(t *testing.T) {
	for _, tc := range []struct {
		restart string
		calls   []string
	}{
		{restart: "", calls: []string{"", "T1", "", "T1", "T2"}},
		{restart: "T1", calls: []string{"", "T1", "T1", "T2"}},
		{restart: "T2", calls: []string{"", "T1", "T2"}},
	} {
		t.Run(fmt.Sprintf("restart=%q", tc.restart), func(t *testing.T) {
			clean := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "clean.c1z"), false)
			cleanSyncer := newInvalidationSyncer(t, clean, newInvalidatingConnector([]string{"group"}, nil))
			require.NoError(t, cleanSyncer.Sync(t.Context()))

			f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "restart.c1z"), false)
			c := newInvalidatingConnector([]string{"group"}, invalidateOnce("group", tc.restart))
			s := newInvalidationSyncer(t, f, c)
			require.NoError(t, s.Sync(t.Context()))
			require.Equal(t, tc.calls, c.callsFor("group"))

			want := storedResourceIDs(t, clean.store, "group")
			if tc.restart == "T2" {
				want = []string{"group-p0", "group-p2"}
			}
			require.Equal(t, want, storedResourceIDs(t, f.store, "group"))

			byOp, total := s.run.actionInvalidationCounts()
			require.EqualValues(t, 1, total)
			require.EqualValues(t, 1, byOp[SyncResourcesOp.String()])
			require.True(t, s.ingestFilterStats.replayBlocked.Load())
			require.Equal(t, cleanSyncer.listResourceActionsCompletedThisRun.Load(), s.listResourceActionsCompletedThisRun.Load(),
				"a restart is not a finish of the list-resources action")
			report, err := f.engine.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			var decoded struct {
				Latest struct {
					RestartPages uint64 `json:"restart_pages"`
					Pages        uint64 `json:"pages"`
				} `json:"latest"`
			}
			require.NoError(t, json.Unmarshal(report, &decoded))
			require.EqualValues(t, 1, decoded.Latest.RestartPages)
			require.Positive(t, decoded.Latest.Pages)
		})
	}
}

// The restart's facts and counters are durable before the restarted
// chain runs: cut the sync right after the restart page and read them back.
func TestActionInvalidationRestartPageIsDurable(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "cut.c1z"), false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := newInvalidatingConnector([]string{"group"}, func(rt, token string, nth int) error {
		if token == "T1" && nth == 1 {
			return pagination.NewErrInvalidateAction("", "page token expired")
		}
		if token == "" && nth == 2 {
			cancel()
			return ctx.Err()
		}
		return nil
	})
	s := newInvalidationSyncer(t, f, c)
	require.ErrorIs(t, s.Sync(ctx), context.Canceled)
	require.Equal(t, []string{"", "T1", ""}, c.callsFor("group"))
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Contains(t, facts, ledgerFactIngestKnown)
	require.Contains(t, facts, ledgerFactIngestBlocked)
	require.EqualValues(t, 1, invalidationCounter(t, f))
	pending, _, err := f.ledger.PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	var item *c1zstore.LedgerWork
	for i := range pending {
		if pending[i].Action.Identity.Op == SyncResourcesOp.String() && pending[i].Action.Identity.ResourceTypeID == "group" {
			item = &pending[i]
		}
	}
	require.NotNil(t, item)
	require.Equal(t, "", item.Action.Identity.PageToken)
	require.Equal(t, []uint64{1}, item.Invalidations)

	resumed := newInvalidationSyncer(t, f, newInvalidatingConnector([]string{"group"}, nil))
	require.NoError(t, resumed.Sync(t.Context()))
	_, total := resumed.run.actionInvalidationCounts()
	require.EqualValues(t, 1, total, "the resumed run restores the count from the ledger")
	require.Equal(t, []string{"group-p0", "group-p1", "group-p2"}, storedResourceIDs(t, f.store, "group"))
}

// S6 per item: the cap is per sync, not per attempt. After the cap fails
// the sync, a resume makes exactly one connector call for the item and
// fails the same way.
func TestActionInvalidationPerItemCapFailsSync(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "cap.c1z"), false)
	c := newInvalidatingConnector([]string{"group"}, invalidateAlways(""))
	s := newInvalidationSyncer(t, f, c, WithActionInvalidationCaps(2, 100))
	err := s.Sync(t.Context())
	require.ErrorIs(t, err, ErrActionInvalidationCapExceeded)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the connector's status survives the wrap")
	var capErr *ActionInvalidationCapError
	require.ErrorAs(t, err, &capErr)
	require.True(t, capErr.Cap.PerItem)
	require.Equal(t, []uint64{1, 3}, capErr.Cap.Invalidations)
	require.Equal(t, []string{"", "T1", "", "T1", "", "T1"}, c.callsFor("group"))
	require.EqualValues(t, 2, invalidationCounter(t, f))
	require.NoError(t, f.store.Close(t.Context()))

	f = openLedgerFixtureAt(t, f.path, false)
	c = newInvalidatingConnector([]string{"group"}, invalidateAlways(""))
	resumed := newInvalidationSyncer(t, f, c, WithActionInvalidationCaps(2, 100))
	err = resumed.Sync(t.Context())
	require.ErrorIs(t, err, ErrActionInvalidationCapExceeded)
	require.Equal(t, []string{"T1"}, c.callsFor("group"))
	_, total := resumed.run.actionInvalidationCounts()
	require.EqualValues(t, 2, total)
	require.EqualValues(t, 2, invalidationCounter(t, f))
}

// S6 per sync: a budget of one across two items that both expire.
func TestActionInvalidationPerSyncBudgetFailsSync(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "budget.c1z"), false)
	c := newInvalidatingConnector([]string{"group", "team"}, invalidateAlways(""))
	s := newInvalidationSyncer(t, f, c, WithActionInvalidationCaps(100, 1))
	err := s.Sync(t.Context())
	require.ErrorIs(t, err, ErrActionInvalidationCapExceeded)
	var capErr *ActionInvalidationCapError
	require.ErrorAs(t, err, &capErr)
	require.False(t, capErr.Cap.PerItem)
	require.EqualValues(t, 1, capErr.Cap.Cap)
	require.EqualValues(t, 1, invalidationCounter(t, f))
}

// C04: a detail on a call that cannot honor it fails the sync without a
// restart and names the cell.
func TestActionInvalidationMisclassifiedCodeFailsSync(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "misclassified.c1z"), false)
	c := newInvalidatingConnector([]string{"group"}, func(_, token string, _ int) error {
		if token != "T1" {
			return nil
		}
		detail, err := status.New(codes.InvalidArgument, "expired").WithDetails(v2.InvalidateAction_builder{RestartPageToken: ""}.Build())
		if err != nil {
			return err
		}
		return detail.Err()
	})
	s := newInvalidationSyncer(t, f, c)
	err := s.Sync(t.Context())
	require.ErrorIs(t, err, ErrActionInvalidationMisclassified)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, []string{"", "T1"}, c.callsFor("group"))
	require.Zero(t, invalidationCounter(t, f))
}

// I1: every (op, root or per-type, code, spawned, detail form, own list
// call) cell. Honored iff the detail is present, the code is
// FailedPrecondition or Aborted, the action's own connector call is a
// paginated list (resource types, or a per-type action of the other list
// ops), the error came from that call, and the cursor was not spawned.
func TestClassifyActionInvalidationTable(t *testing.T) {
	ops := []ActionOp{UnknownOp, InitOp, SyncResourceTypesOp, SyncResourcesOp, SyncEntitlementsOp, ListResourcesForEntitlementsOp, SyncGrantsOp,
		SyncExternalResourcesOp, SyncAssetsOp, SyncGrantExpansionOp, SyncTargetedResourceOp, SyncStaticEntitlementsOp, MaterializeStaticEntitlementsOp}
	listOps := []ActionOp{SyncResourceTypesOp, SyncResourcesOp, SyncEntitlementsOp, SyncGrantsOp, SyncStaticEntitlementsOp}
	forms := map[string]func(code codes.Code) (error, bool){
		"absent": func(code codes.Code) (error, bool) { return status.Error(code, "plain"), false },
		"typed": func(code codes.Code) (error, bool) {
			err := pagination.NewErrInvalidateAction("R", "typed")
			if code == codes.FailedPrecondition {
				return err, true
			}
			detail, _ := status.New(code, "typed").WithDetails(v2.InvalidateAction_builder{RestartPageToken: "R"}.Build())
			return detail.Err(), true
		},
		"wrapped": func(code codes.Code) (error, bool) {
			detail, _ := status.New(code, "wrapped").WithDetails(v2.InvalidateAction_builder{RestartPageToken: "R"}.Build())
			return fmt.Errorf("list failed: %w", detail.Err()), true
		},
	}
	cells := 0
	for _, op := range ops {
		for _, resourceType := range []string{"", "group"} {
			for code := codes.Canceled; code <= codes.Unauthenticated; code++ {
				for _, spawned := range []bool{false, true} {
					for form, build := range forms {
						for _, ownCall := range []bool{true, false} {
							err, present := build(code)
							if ownCall {
								err = markActionListCall(err)
							}
							cells++
							action := &Action{Op: op, Spawned: spawned, ResourceTypeID: resourceType}
							detail, gotPresent, classErr := classifyActionInvalidation(action, err)
							name := fmt.Sprintf("%s/rt=%q/%s/spawned=%t/%s/own_call=%t", op, resourceType, code, spawned, form, ownCall)
							require.Equal(t, present, gotPresent, name)
							if !present {
								require.Nil(t, detail, name)
								require.NoError(t, classErr, name)
								continue
							}
							require.Equal(t, "R", detail.GetRestartPageToken(), name)
							ownPaginatedOp := op == SyncResourceTypesOp || (slices.Contains(listOps, op) && resourceType != "")
							honored := (code == codes.FailedPrecondition || code == codes.Aborted) && ownPaginatedOp && !spawned && ownCall
							if honored {
								require.NoError(t, classErr, name)
							} else {
								require.ErrorIs(t, classErr, ErrActionInvalidationMisclassified, name)
								require.ErrorIs(t, classErr, err, name)
								require.Equal(t, code, status.Code(classErr), name)
							}
						}
					}
				}
			}
		}
	}
	require.Equal(t, 13*2*16*2*3*2, cells)
}

// nestedGetResourceConnector answers GetResource for the unsynced group
// "g2" with the detail. The grants page for "g1" reaches that call after
// ListGrants succeeded and before any grant is staged, so without the
// list-call mark the grants action would restart on a token it never paged
// on, and keep restarting until the per-item cap.
type nestedGetResourceConnector struct {
	*mockConnector
	getResourceCalls int
}

func (c *nestedGetResourceConnector) GetResource(ctx context.Context, in *v2.ResourceGetterServiceGetResourceRequest, opts ...grpc.CallOption) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	if in.GetResourceId().GetResource() == "g2" {
		c.getResourceCalls++
		return nil, pagination.NewErrInvalidateAction("", "get-resource is not paginated")
	}
	return c.mockConnector.GetResource(ctx, in, opts...)
}

func TestActionInvalidationNestedConnectorCallMisclassified(t *testing.T) {
	c := &nestedGetResourceConnector{mockConnector: newMockConnector()}
	c.rtDB = append(c.rtDB, groupResourceType)
	g1, _, err := c.AddGroup(t.Context(), "g1")
	require.NoError(t, err)
	g2, err := rs.NewGroupResource("g2", groupResourceType, "g2", nil)
	require.NoError(t, err)
	c.grantDB["g1"] = []*v2.Grant{gt.NewGrant(g2, "member", g1.GetId())}

	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "nested.c1z"), false)
	s, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithWorkerCount(1), WithTargetedSyncResources([]*v2.Resource{g1}))
	require.NoError(t, err)
	err = s.Sync(t.Context())
	require.ErrorIs(t, err, ErrActionInvalidationMisclassified)
	require.NotErrorIs(t, err, ErrActionInvalidationCapExceeded)
	require.Contains(t, err.Error(), "own_list_call=false")
	require.Equal(t, 1, c.getResourceCalls, "one failing call, no restart")
	require.Zero(t, invalidationCounter(t, f))
}

type ledgerRestartCrashStore struct {
	c1zstore.PageLedgerStore
	cut func(string)
}

func (s ledgerRestartCrashStore) BeginPage() c1zstore.PageWriter {
	return ledgerRestartCrashWriter{PageWriter: s.PageLedgerStore.BeginPage(), cut: s.cut}
}

type ledgerRestartCrashWriter struct {
	c1zstore.PageWriter
	cut func(string)
}

func (w ledgerRestartCrashWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if row != nil && row.Restarted {
		w.cut("restart-before")
	}
	if err := w.PageWriter.Commit(ctx, id, row); err != nil {
		return err
	}
	if row != nil && row.Restarted {
		w.cut("restart-after")
	}
	return nil
}

// C10 on the public path, in a separate process: the process dies around
// the restart commit. After recovery the image is one of exactly two, and
// a resumed sync with a connector that no longer expires the token finishes
// with the same records either way. If the restart landed, the resume
// starts the item at the restart token and the count is one; if it did
// not, the resume starts at the expired token and the count is zero.
func TestActionInvalidationCrashProcess(t *testing.T) {
	if cut := os.Getenv("BATON_LEDGER_RESTART_CRASH_CUT"); cut != "" {
		path := os.Getenv("BATON_LEDGER_RESTART_CRASH_FILE")
		f := openLedgerFixtureAt(t, path, false)
		c := newInvalidatingConnector([]string{"group"}, invalidateOnce("group", ""))
		s := newInvalidationSyncer(t, f, c)
		var crash native_sync.Once
		s.caps.pageLedger = ledgerRestartCrashStore{PageLedgerStore: f.ledger, cut: func(reached string) {
			if reached != cut {
				return
			}
			crash.Do(func() {
				if os.Getenv("BATON_LEDGER_RESTART_CRASH_IMAGE") == "flushed" {
					require.NoError(t, f.engine.Flush(t.Context()))
				}
				marker, err := json.Marshal(ledgerCrashMarker{Cut: cut, SyncID: s.syncID})
				require.NoError(t, err)
				require.NoError(t, writeLedgerTestFile(path+".cut", marker, 0600))
				os.Exit(76)
			})
			select {}
		}}
		require.NoError(t, s.Sync(t.Context()))
		t.Fatal("crash cut was not reached")
	}
	for _, image := range []string{"wal", "flushed"} {
		for _, cut := range []string{"restart-before", "restart-after"} {
			t.Run(image+"/"+cut, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "restart-crash.c1z")
				runLedgerCrashChild(t, "^TestActionInvalidationCrashProcess$", 76,
					"BATON_LEDGER_RESTART_CRASH_IMAGE="+image, "BATON_LEDGER_RESTART_CRASH_CUT="+cut, "BATON_LEDGER_RESTART_CRASH_FILE="+path)
				data, err := os.ReadFile(path + ".cut")
				require.NoError(t, err)
				var marker ledgerCrashMarker
				require.NoError(t, json.Unmarshal(data, &marker))
				dirs, err := filepath.Glob(filepath.Join(filepath.Dir(path), "c1z-pebble*", "db"))
				require.NoError(t, err)
				require.Len(t, dirs, 1)
				recovered, err := engine.Open(t.Context(), dirs[0])
				require.NoError(t, err)
				defer func() { require.NoError(t, recovered.Close()) }()
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
				require.NoError(t, f.store.SetCurrentSync(t.Context(), marker.SyncID))

				pending, _, err := f.ledger.PendingWork(t.Context(), 0, 100)
				require.NoError(t, err)
				var item *c1zstore.LedgerWork
				for i := range pending {
					if pending[i].Action.Identity.Op == SyncResourcesOp.String() && pending[i].Action.Identity.ResourceTypeID == "group" {
						item = &pending[i]
					}
				}
				// An unsynced WAL image may predate the whole collecting state;
				// that is the pre-restart side of the dichotomy too.
				landed := item != nil && item.Action.Identity.PageToken == ""
				counter := invalidationCounter(t, f)
				switch {
				case landed:
					require.Equal(t, []uint64{1}, item.Invalidations)
					require.EqualValues(t, 2, item.Revision)
					require.EqualValues(t, 1, counter)
					require.Equal(t, []string{"group-p0"}, storedResourceIDs(t, f.store, "group"), "the restart page writes no records")
					facts, err := f.ledger.LedgerFacts(t.Context())
					require.NoError(t, err)
					require.Contains(t, facts, ledgerFactIngestBlocked, "the replay block rides the restart batch")
				case item != nil:
					require.Equal(t, "T1", item.Action.Identity.PageToken)
					require.Empty(t, item.Invalidations)
					require.EqualValues(t, 1, item.Revision)
					require.Zero(t, counter)
				default:
					require.Equal(t, "wal", image)
					require.Zero(t, counter)
				}
				if cut == "restart-before" {
					require.False(t, landed)
				}
				if cut == "restart-after" && image == "flushed" {
					require.True(t, landed)
				}

				c := newInvalidatingConnector([]string{"group"}, nil)
				resumed := newInvalidationSyncer(t, f, c, WithSyncID(marker.SyncID))
				require.NoError(t, resumed.Sync(t.Context()))
				switch {
				case landed:
					require.Equal(t, []string{"", "T1", "T2"}, c.callsFor("group"))
				case item != nil:
					require.Equal(t, []string{"T1", "T2"}, c.callsFor("group"))
				default:
					require.Equal(t, []string{"", "T1", "T2"}, c.callsFor("group"))
				}
				_, total := resumed.run.actionInvalidationCounts()
				require.EqualValues(t, counter, total)
				require.Equal(t, []string{"group-p0", "group-p1", "group-p2"}, storedResourceIDs(t, f.store, "group"))
			})
		}
	}
}
