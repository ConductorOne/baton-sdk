package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	nativeSync "sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type windowConnector struct {
	*mockConnector
	pages int
	mu    nativeSync.Mutex
	calls map[string][]string
}

func newWindowConnector(pages int) *windowConnector {
	return &windowConnector{mockConnector: newMockConnector(), pages: pages, calls: make(map[string][]string)}
}

func windowResource(rt, id string) *v2.Resource {
	return v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(), DisplayName: id}.Build()
}

func (c *windowConnector) page(key, token string) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[key] = append(c.calls[key], token)
	stream, raw := "", token
	if before, after, ok := strings.Cut(token, ":"); ok {
		stream, raw = before, after
	}
	page, _ := strconv.Atoi(raw)
	next := ""
	if page+1 < c.pages {
		next = strconv.Itoa(page + 1)
		if stream != "" {
			next = stream + ":" + next
		}
	}
	return page, next
}

func (c *windowConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	var list []*v2.ResourceType
	for _, rt := range []string{"parent-a", "parent-b", "resource", "child", "owner", "scoped"} {
		annos := annotations.New()
		if rt != "owner" && rt != "scoped" {
			annos.Append(&v2.SkipEntitlementsAndGrants{})
		}
		if rt == "scoped" {
			annos.Append(&v2.TypeScopedGrants{})
		}
		list = append(list, v2.ResourceType_builder{Id: rt, DisplayName: rt, Annotations: annos}.Build())
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: list}.Build(), nil
}

func (c *windowConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	rt := req.GetResourceTypeId()
	if rt == "parent-a" || rt == "parent-b" {
		r := windowResource(rt, "shared")
		r.SetAnnotations(annotations.New(v2.ChildResourceType_builder{ResourceTypeId: "child"}.Build()))
		return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{r}}.Build(), nil
	}
	if rt == "owner" || rt == "scoped" {
		return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{windowResource(rt, rt)}}.Build(), nil
	}
	if rt == "child" && req.GetParentResourceId() == nil {
		return &v2.ResourcesServiceListResourcesResponse{}, nil
	}
	key := "resources:" + rt
	if parent := req.GetParentResourceId(); parent != nil {
		key += ":" + parent.GetResourceType() + ":" + parent.GetResource()
	}
	page, next := c.page(key, req.GetPageToken())
	r := windowResource(rt, fmt.Sprintf("%s-%d", key, page))
	r.SetParentResourceId(req.GetParentResourceId())
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{r}, NextPageToken: next}.Build(), nil
}

func (c *windowConnector) ListEntitlements(_ context.Context, req *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	r := req.GetResource()
	page, next := 0, ""
	if r.GetId().GetResourceType() == "owner" {
		page, next = c.page("entitlements:owner", req.GetPageToken())
	}
	slug := "member"
	if page > 0 {
		slug = fmt.Sprintf("member-%d", page)
	}
	ent := et.NewAssignmentEntitlement(r, slug)
	return v2.EntitlementsServiceListEntitlementsResponse_builder{List: []*v2.Entitlement{ent}, NextPageToken: next}.Build(), nil
}

func (c *windowConnector) ListGrants(_ context.Context, req *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	rt := req.GetResource().GetId().GetResourceType()
	if rt == "" {
		rt = "scoped"
	}
	token, stream := req.GetPageToken(), "origin"
	if before, _, ok := strings.Cut(token, ":"); ok {
		stream = before
	}
	page, next := c.page("grants:"+rt+":"+stream, token)
	if rt == "scoped" && next != "" && !strings.Contains(next, ":") {
		next = stream + ":" + next
	}
	r := windowResource(rt, rt)
	grant := gt.NewGrant(r, "member", r.GetId())
	grant.SetId(fmt.Sprintf("%s:%s:%d", grant.GetId(), stream, page))
	annos := annotations.New()
	if rt == "scoped" && token == "" {
		annos.Append(v2.EnqueuePageTokens_builder{PageTokens: []string{"left:0", "right:0"}}.Build())
	}
	return v2.GrantsServiceListGrantsResponse_builder{List: []*v2.Grant{grant}, NextPageToken: next, Annotations: annos}.Build(), nil
}

func (c *windowConnector) ListStaticEntitlements(
	_ context.Context, req *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	_, next := c.page("static:"+req.GetResourceTypeId(), req.GetPageToken())
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{NextPageToken: next}.Build(), nil
}

func TestListPageWindowSyncMatrix(t *testing.T) {
	for _, mode := range []string{"serial", "parallel", "ledger"} {
		for _, scenario := range []struct {
			name   string
			pages  int
			window uint32
		}{
			{"sampled", 5, 2}, {"terminated-at-window", 2, 2}, {"terminated-before-window", 1, 2}, {"disabled", 5, 0},
		} {
			t.Run(mode+"/"+scenario.name, func(t *testing.T) {
				ctx, dir := t.Context(), t.TempDir()
				store, err := dotc1z.NewStore(ctx, filepath.Join(dir, "window.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
				require.NoError(t, err)
				c := newWindowConnector(scenario.pages)
				var reports []ListPageWindowReport
				var mu nativeSync.Mutex
				opts := []SyncOpt{WithTmpDir(dir), WithDontExpandGrants(), WithListPageWindow(scenario.window, func(report ListPageWindowReport) {
					mu.Lock()
					defer mu.Unlock()
					reports = append(reports, report)
				})}
				if mode == "ledger" {
					opts = append(opts, WithConnectorStore(store))
				} else {
					opts = append(opts, WithConnectorStore(ledgerMetadataStore{Store: store}))
					if mode == "parallel" {
						opts = append(opts, WithWorkerCount(2))
					}
				}
				created, err := NewSyncer(ctx, c, opts...)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, created.Close(context.Background())) })
				require.Equal(t, mode == "ledger", created.(*syncer).ledgered)
				require.NoError(t, created.Sync(ctx))
				expectedPages := scenario.pages
				if scenario.window > 0 {
					expectedPages = min(expectedPages, int(scenario.window))
				}
				keys := []string{
					"resources:resource", "resources:child:parent-a:shared", "resources:child:parent-b:shared",
					"entitlements:owner", "grants:owner:origin", "grants:scoped:origin", "grants:scoped:left", "grants:scoped:right",
				}
				for _, key := range keys {
					require.Len(t, c.calls[key], expectedPages, key)
				}
				staticLists := 0
				for key, calls := range c.calls {
					if strings.HasPrefix(key, "static:") {
						require.Len(t, calls, scenario.pages, key)
						staticLists++
					}
				}
				require.Positive(t, staticLists)
				reported := scenario.window > 0 && scenario.pages >= int(scenario.window)
				sampled := scenario.window > 0 && scenario.pages > int(scenario.window)
				if reported {
					require.Len(t, reports, len(keys))
					var children, spawned int
					for _, r := range reports {
						require.Equal(t, scenario.window, r.PagesReturned)
						require.Equal(t, scenario.window, r.Window)
						require.Equal(t, !sampled, r.VendorTerminated)
						switch r.Op {
						case SyncResourcesOp:
							require.Empty(t, r.ResourceID)
							require.False(t, r.TypeScoped)
						case SyncEntitlementsOp:
							require.Equal(t, "owner", r.ResourceTypeID)
							require.Equal(t, "owner", r.ResourceID)
							require.Empty(t, r.ParentResourceID)
						case SyncGrantsOp:
							require.Equal(t, r.ResourceTypeID == "scoped", r.TypeScoped)
							if r.TypeScoped {
								require.Empty(t, r.ResourceID)
							} else {
								require.Equal(t, "owner", r.ResourceID)
							}
						default:
							t.Errorf("unexpected report operation %v", r.Op)
						}
						if r.ParentResourceID != "" {
							require.Contains(t, []string{"parent-a", "parent-b"}, r.ParentResourceTypeID)
							require.Equal(t, "shared", r.ParentResourceID)
							children++
						}
						if r.Spawned {
							require.True(t, r.TypeScoped)
							require.Equal(t, SyncGrantsOp, r.Op)
							spawned++
						}
					}
					require.Equal(t, 2, children)
					require.Equal(t, 2, spawned)
				} else {
					require.Empty(t, reports)
				}
				sr, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
				require.NoError(t, err)
				require.NotNil(t, sr)
				require.NotNil(t, sr.Stats)
				wantSampled := int64(0)
				if sampled {
					wantSampled = int64(len(keys))
				}
				require.Equal(t, wantSampled, sr.Stats.GetCounters()["list_page_window.sampled"])
				if sampled {
					require.EqualValues(t, 1, sr.Stats.GetCounters()["list_page_window.sampled:resource"])
					require.EqualValues(t, 2, sr.Stats.GetCounters()["list_page_window.sampled:owner"])
					require.EqualValues(t, 2, sr.Stats.GetCounters()["list_page_window.sampled:child"])
					require.EqualValues(t, 3, sr.Stats.GetCounters()["list_page_window.sampled:scoped"])
				}
				rows, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: "resource"}.Build())
				require.NoError(t, err)
				require.Len(t, rows.GetList(), expectedPages)
				if expectedPages >= 2 {
					var ids []string
					for _, r := range rows.GetList() {
						ids = append(ids, r.GetId().GetResource())
					}
					require.Contains(t, ids, "resources:resource-1")
					entitlements, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{Resource: windowResource("owner", "owner")}.Build())
					require.NoError(t, err)
					var slugs []string
					for _, ent := range entitlements.GetList() {
						slugs = append(slugs, ent.GetSlug())
					}
					require.Contains(t, slugs, "member-1")
					grants, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{Resource: windowResource("owner", "owner")}.Build())
					require.NoError(t, err)
					var grantIDs []string
					for _, grant := range grants.GetList() {
						grantIDs = append(grantIDs, grant.GetId())
					}
					owner := windowResource("owner", "owner")
					expectedGrant := gt.NewGrant(owner, "member", owner.GetId())
					require.Contains(t, grantIDs, expectedGrant.GetId()+fmt.Sprintf(":origin:%d", expectedPages-1))
				}
			})
		}
	}
}

func TestListPageWindowActionCheckpointAndRewalk(t *testing.T) {
	ctx := t.Context()
	s := &syncer{run: newRunState(), stats: newRunStats()}
	var reports []ListPageWindowReport
	WithListPageWindow(2, func(r ListPageWindowReport) {
		require.True(t, s.run.mu.TryLock())
		s.run.mu.Unlock()
		require.True(t, s.parallelTransitionMu.TryLock())
		s.parallelTransitionMu.Unlock()
		reports = append(reports, r)
	})(s)
	for range 2 {
		action := s.run.pushAction(ctx, Action{Op: SyncResourcesOp, ResourceTypeID: "resource"})
		require.NoError(t, s.nextPageOrFinishAction(ctx, action, "page-2"))
		token, err := marshalToken(s.run, s.stats)
		require.NoError(t, err)
		parts, err := unmarshalToken(token)
		require.NoError(t, err)
		s.run, s.stats = parts.run, parts.stats
		action = s.run.current()
		require.EqualValues(t, 1, action.WindowPages)
		require.NoError(t, s.nextPageOrFinishAction(ctx, action, "page-3"))
	}
	require.Len(t, reports, 2)
	require.EqualValues(t, 2, s.stats.counters["list_page_window.sampled"])
}

func TestListPageWindowDisabledTokenAndExcludedActions(t *testing.T) {
	for _, op := range []ActionOp{SyncResourcesOp, SyncEntitlementsOp, SyncGrantsOp, SyncStaticEntitlementsOp, InitOp} {
		a := Action{Op: op, ResourceTypeID: "type", PageToken: "cursor-17", Spawned: true}
		before, err := json.Marshal(a)
		require.NoError(t, err)
		s := &syncer{}
		WithListPageWindow(0, func(ListPageWindowReport) { t.Fatal("disabled handler called") })(s)
		next, report := s.applyListPageWindow(&a, "next")
		require.Equal(t, "next", next)
		require.Nil(t, report)
		after, err := json.Marshal(a)
		require.NoError(t, err)
		require.Equal(t, string(before), string(after))
		if op == SyncStaticEntitlementsOp || op == InitOp {
			WithListPageWindow(1, nil)(s)
			next, report = s.applyListPageWindow(&a, "next")
			require.Equal(t, "next", next)
			require.Nil(t, report)
			require.Zero(t, a.WindowPages)
		}
	}
	s := &syncer{}
	WithListPageWindow(1, nil)(s)
	for _, op := range []ActionOp{SyncResourcesOp, SyncEntitlementsOp, SyncGrantsOp} {
		a := Action{Op: op}
		next, report := s.applyListPageWindow(&a, "planner-page")
		require.Equal(t, "planner-page", next)
		require.Nil(t, report)
	}
}

func TestListPageWindowFailedTransitionDoesNotReport(t *testing.T) {
	s := &syncer{run: newRunState(), stats: newRunStats()}
	WithListPageWindow(1, func(ListPageWindowReport) { t.Fatal("failed transition reported") })(s)
	s.parallelActionTransitioner = func(context.Context, *Action, string, []Action) error { return errors.New("commit failed") }
	a := Action{Op: SyncGrantsOp, ResourceTypeID: "resource"}
	require.ErrorContains(t, s.nextPageOrFinishAction(t.Context(), &a, "next"), "commit failed")
	require.Zero(t, a.WindowPages)
	require.Empty(t, s.stats.counters)
}

func TestListPageWindowZeroPreservesSyncTokenBytes(t *testing.T) {
	var tokens []string
	for _, explicitZero := range []bool{false, true} {
		s := &syncer{run: newRunState(), stats: newRunStats()}
		if explicitZero {
			WithListPageWindow(0, nil)(s)
		}
		action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "resource"})
		for _, next := range []string{"one", "two", ""} {
			require.NoError(t, s.nextPageOrFinishAction(t.Context(), action, next))
			token, err := marshalToken(s.run, s.stats)
			require.NoError(t, err)
			if explicitZero {
				require.Equal(t, tokens[0], token)
				tokens = tokens[1:]
			} else {
				tokens = append(tokens, token)
			}
		}
	}
}

func TestListPageWindowLedgerReconstructionStartsNewWindow(t *testing.T) {
	action := Action{Op: SyncResourcesOp, ResourceTypeID: "resource", PageToken: "page-2", WindowPages: 1}
	reconstructed := ledgerActionFromIdentity(ledgerIdentity(&action))
	require.Equal(t, action.PageToken, reconstructed.PageToken)
	require.Zero(t, reconstructed.WindowPages)
	s := &syncer{}
	WithListPageWindow(2, nil)(s)
	next, report := s.applyListPageWindow(&reconstructed, "page-3")
	require.Equal(t, "page-3", next)
	require.Nil(t, report)
	next, report = s.applyListPageWindow(&reconstructed, "page-4")
	require.Empty(t, next)
	require.NotNil(t, report)
	require.False(t, report.VendorTerminated)
}

func TestListPageWindowLedgerFailedPageDoesNotReport(t *testing.T) {
	ctx := t.Context()
	f := newLedgerFixture(t)
	roots := []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "resource"}}}
	require.NoError(t, f.ledger.BeginCollecting(ctx, pendingSeeds(roots), nil))
	runtime, err := newTestLedgerRuntime(ctx, f.ledger, "window-failed-page")
	require.NoError(t, err)
	s := &syncer{ledgered: true, ledger: runtime, run: newRunState(), stats: newRunStats(), syncID: f.engine.CurrentSyncID()}
	s.store = f.store
	s.caps = resolveStoreCaps(f.store)
	WithListPageWindow(1, func(ListPageWindowReport) { t.Fatal("uncommitted page reported") })(s)
	require.NoError(t, s.refreshPendingWindow(ctx))
	action := s.run.current()
	require.NotNil(t, action)
	stopped := errors.New("failed after staging")
	err = s.invokeActionPage(ctx, action, func(ctx context.Context, action *Action) error {
		require.NoError(t, s.nextPageOrFinishAction(ctx, action, "next"))
		return stopped
	}, false)
	require.ErrorIs(t, err, stopped)
	require.Zero(t, action.WindowPages)
	counters, err := f.ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	require.Zero(t, counters.Counters["list_page_window.sampled"])
}

func TestListPageWindowGoldenCheckpoint(t *testing.T) {
	run, stats := newRunState(), newRunStats()
	run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "group", PageToken: "page-2", WindowPages: 1})
	stats.addCounter("list_page_window.sampled", 2)
	stats.addCounter("list_page_window.sampled:group", 2)
	encoded, err := marshalToken(run, stats)
	require.NoError(t, err)
	require.Equal(t, readGoldenToken(t, "v1_list_page_window.json"), encoded)
}
