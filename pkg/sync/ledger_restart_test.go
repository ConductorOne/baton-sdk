package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/retry"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// invalidatingConnector pages each resource type through "" → "T1" → "T2"
// with one resource per page. fail decides, per call, whether the call
// fails instead of answering. Resources of a type in childOf carry a
// ChildResourceType annotation; a list scoped to a parent answers one page
// holding "<child>-of-<parent>" and is recorded, and offered to fail, under
// "<type>@<parent>".
// Resources named in omit are left out of their page (an upstream deletion).
type invalidatingConnector struct {
	*mockConnector
	types   []string
	childOf map[string]string
	fail    func(rt, token string, nth int) error

	mu    native_sync.Mutex
	calls map[string][]string
	omit  map[string]bool
}

func newInvalidatingConnector(types []string, fail func(rt, token string, nth int) error) *invalidatingConnector {
	return &invalidatingConnector{mockConnector: newMockConnector(), types: types, fail: fail, calls: map[string][]string{}, omit: map[string]bool{}}
}

func (c *invalidatingConnector) omitResource(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.omit[id] = true
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
	if parent := req.GetParentResourceId(); parent != nil {
		key := rt + "@" + parent.GetResource()
		c.mu.Lock()
		c.calls[key] = append(c.calls[key], token)
		nth := len(c.calls[key])
		c.mu.Unlock()
		if c.fail != nil {
			if err := c.fail(key, token, nth); err != nil {
				return nil, err
			}
		}
		child := v2.Resource_builder{
			Id:               v2.ResourceId_builder{ResourceType: rt, Resource: rt + "-of-" + parent.GetResource()}.Build(),
			ParentResourceId: parent,
			DisplayName:      rt,
		}.Build()
		return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{child}}.Build(), nil
	}
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
	id := fmt.Sprintf("%s-p%d", rt, page)
	c.mu.Lock()
	omitted := c.omit[id]
	c.mu.Unlock()
	if omitted {
		return v2.ResourcesServiceListResourcesResponse_builder{NextPageToken: next}.Build(), nil
	}
	resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(), DisplayName: rt}.Build()
	if childRT, ok := c.childOf[rt]; ok {
		anno, err := anypb.New(v2.ChildResourceType_builder{ResourceTypeId: childRT}.Build())
		if err != nil {
			return nil, err
		}
		resource.SetAnnotations([]*anypb.Any{anno})
	}
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
			return pagination.NewRestartActionError(restartToken, "page token expired")
		}
		return nil
	}
}

func invalidateAlways(restartToken string) func(string, string, int) error {
	return func(_, token string, _ int) error {
		if token == "T1" {
			return pagination.NewRestartActionError(restartToken, "page token expired")
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

func restartCounter(t *testing.T, f *ledgerFixture) uint64 {
	t.Helper()
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	return counters.Counters[ledgerActionRestarts]
}

// C11, C19: the next call carries the restart token, the sync finishes
// with the same resources as an uninterrupted run when the restart token
// is "" or the expired token (a restart to the page after the expired one
// skips that page's resource, which is the connector's instruction), and
// the restart leaves the replay block and the counters in the ledger. The
// fixture has no child work, so C13 (a restarted chain re-emitting a
// claimed child) is not exercised here.
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

			byOp, total := s.run.restartCounts()
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
			return pagination.NewRestartActionError("", "page token expired")
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
	require.EqualValues(t, 1, restartCounter(t, f))
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
	require.Equal(t, []uint64{1}, item.Restarts)

	resumed := newInvalidationSyncer(t, f, newInvalidatingConnector([]string{"group"}, nil))
	require.NoError(t, resumed.Sync(t.Context()))
	_, total := resumed.run.restartCounts()
	require.EqualValues(t, 1, total, "the resumed run restores the count from the ledger")
	require.Equal(t, []string{"group-p0", "group-p1", "group-p2"}, storedResourceIDs(t, f.store, "group"))
}

// Stop inside the restarted chain, after its first page committed, and
// resume: the item carries the restart's invalidation list into the resumed
// attempt and continues from the page it reached, not from the restart.
func TestActionInvalidationResumeMidRestartedChain(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "mid.c1z"), false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := newInvalidatingConnector([]string{"group"}, func(_, token string, nth int) error {
		switch {
		case token == "T1" && nth == 1:
			return pagination.NewRestartActionError("", "page token expired")
		case token == "T1" && nth == 2:
			cancel()
			return ctx.Err()
		}
		return nil
	})
	s := newInvalidationSyncer(t, f, c)
	require.ErrorIs(t, s.Sync(ctx), context.Canceled)
	require.Equal(t, []string{"", "T1", "", "T1"}, c.callsFor("group"))
	item := pendingGroupItem(t, f)
	require.Equal(t, "T1", item.Action.Identity.PageToken, "the rerun's first page committed")
	require.Equal(t, []uint64{1}, item.Restarts)
	require.EqualValues(t, 3, item.Revision)

	c = newInvalidatingConnector([]string{"group"}, nil)
	resumed := newInvalidationSyncer(t, f, c)
	require.NoError(t, resumed.Sync(t.Context()))
	require.Equal(t, []string{"T1", "T2"}, c.callsFor("group"))
	_, total := resumed.run.restartCounts()
	require.EqualValues(t, 1, total)
	require.Equal(t, []string{"group-p0", "group-p1", "group-p2"}, storedResourceIDs(t, f.store, "group"))
}

// S6 per sync across a resume: one restart in the first attempt uses the
// whole budget of one; the next restart, in a second attempt on the same
// file, is refused on the count restored from the ledger.
func TestActionInvalidationPerSyncBudgetAcrossResume(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "budget-resume.c1z"), false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := newInvalidatingConnector([]string{"group", "user"}, func(rt, token string, nth int) error {
		switch {
		case rt == "group" && token == "T1" && nth == 1:
			return pagination.NewRestartActionError("", "page token expired")
		case rt == "group" && token == "" && nth == 2:
			cancel()
			return ctx.Err()
		}
		return nil
	})
	s := newInvalidationSyncer(t, f, c, WithRestartLimits(100, 1))
	require.ErrorIs(t, s.Sync(ctx), context.Canceled)
	require.EqualValues(t, 1, restartCounter(t, f))

	c = newInvalidatingConnector([]string{"group", "user"}, invalidateAlways(""))
	resumed := newInvalidationSyncer(t, f, c, WithRestartLimits(100, 1))
	err := resumed.Sync(t.Context())
	require.ErrorIs(t, err, ErrRestartLimitExceeded)
	var capErr *RestartLimitError
	require.ErrorAs(t, err, &capErr)
	require.Equal(t, c1zstore.RestartScopeSync, capErr.Limit.Scope, "the per-sync limit, not the per-action one")
	require.EqualValues(t, 1, restartCounter(t, f), "the refused restart recorded nothing")
	_, total := resumed.run.restartCounts()
	require.EqualValues(t, 1, total)
}

// C13: a restarted chain re-lists a parent whose child it already pushed.
// The child runs once, in the same attempt and across a resume. The ledger's
// scheduling claim is the only dedupe on this path (collectLedgerResources
// does not record into childSchedule); disabling the HasScheduledWork check
// fails both cases.
func TestActionInvalidationRestartedChainPushesChildrenOnce(t *testing.T) {
	for _, resume := range []bool{false, true} {
		name := "same-attempt"
		if resume {
			name = "across-resume"
		}
		t.Run(name, func(t *testing.T) {
			f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "children.c1z"), false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			c := newInvalidatingConnector([]string{"group", "project"}, func(rt, token string, nth int) error {
				switch {
				case rt == "group" && token == "T1" && nth == 1:
					return pagination.NewRestartActionError("", "page token expired")
				case resume && rt == "group" && token == "" && nth == 2:
					cancel()
					return ctx.Err()
				}
				return nil
			})
			c.childOf = map[string]string{"group": "project"}
			s := newInvalidationSyncer(t, f, c)
			err := s.Sync(ctx)
			if resume {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, "", pendingGroupItem(t, f).Action.Identity.PageToken, "stopped before the rerun's first page")
				resumed := newInvalidatingConnector([]string{"group", "project"}, nil)
				resumed.childOf = c.childOf
				require.NoError(t, newInvalidationSyncer(t, f, resumed).Sync(t.Context()))
				c.mu.Lock()
				for key, calls := range resumed.calls {
					c.calls[key] = append(c.calls[key], calls...)
				}
				c.mu.Unlock()
			} else {
				require.NoError(t, err)
			}
			for _, parent := range []string{"group-p0", "group-p1", "group-p2"} {
				require.Equal(t, []string{""}, c.callsFor("project@"+parent), "child of %s listed exactly once", parent)
			}
			require.Equal(t, []string{"group-p0", "group-p1", "group-p2"}, storedResourceIDs(t, f.store, "group"))
			require.Equal(t, []string{"project-of-group-p0", "project-of-group-p1", "project-of-group-p2", "project-p0", "project-p1", "project-p2"}, storedResourceIDs(t, f.store, "project"))
			pending, _, err := f.ledger.PendingWork(t.Context(), 0, 100)
			require.NoError(t, err)
			require.Empty(t, pending)
		})
	}
}

// The child-under-parent variant of SyncResources restarts: the expiry is
// on the child list itself, whose identity carries the parent, and the
// restart token "" makes the r+1 identity equal the first-page identity.
func TestActionInvalidationRestartsChildUnderParent(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "child.c1z"), false)
	c := newInvalidatingConnector([]string{"group", "project"}, func(key, token string, nth int) error {
		if key == "project@group-p1" && token == "" && nth == 1 {
			return pagination.NewRestartActionError("", "page token expired")
		}
		return nil
	})
	c.childOf = map[string]string{"group": "project"}
	s := newInvalidationSyncer(t, f, c)
	require.NoError(t, s.Sync(t.Context()))
	require.Equal(t, []string{"", ""}, c.callsFor("project@group-p1"), "the expired child list runs again from the start")
	require.Equal(t, []string{""}, c.callsFor("project@group-p0"))
	require.Equal(t, []string{""}, c.callsFor("project@group-p2"))
	byOp, total := s.run.restartCounts()
	require.EqualValues(t, 1, total)
	require.EqualValues(t, 1, byOp[SyncResourcesOp.String()])
	require.Equal(t, []string{"project-of-group-p0", "project-of-group-p1", "project-of-group-p2", "project-p0", "project-p1", "project-p2"}, storedResourceIDs(t, f.store, "project"))
	pending, _, err := f.ledger.PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	require.Empty(t, pending)
}

// C15: the accepted over-retention. A resource listed before the expiry and
// deleted upstream before the restarted chain re-lists stays in the store;
// the control run, which never saw it, does not have it.
func TestActionInvalidationOverRetention(t *testing.T) {
	invalidating := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "retained.c1z"), false)
	var c *invalidatingConnector
	c = newInvalidatingConnector([]string{"group"}, func(_, token string, nth int) error {
		if token == "T1" && nth == 1 {
			c.omitResource("group-p0")
			return pagination.NewRestartActionError("", "page token expired")
		}
		return nil
	})
	require.NoError(t, newInvalidationSyncer(t, invalidating, c).Sync(t.Context()))
	require.Equal(t, []string{"", "T1", "", "T1", "T2"}, c.callsFor("group"))
	require.Equal(t, []string{"group-p0", "group-p1", "group-p2"}, storedResourceIDs(t, invalidating.store, "group"), "group-p0 was ingested before the deletion and the restart does not unwrite it")

	control := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "control.c1z"), false)
	cc := newInvalidatingConnector([]string{"group"}, nil)
	cc.omitResource("group-p0")
	require.NoError(t, newInvalidationSyncer(t, control, cc).Sync(t.Context()))
	require.Equal(t, []string{"group-p1", "group-p2"}, storedResourceIDs(t, control.store, "group"))
}

func pendingGroupItem(t *testing.T, f *ledgerFixture) c1zstore.LedgerWork {
	t.Helper()
	pending, _, err := f.ledger.PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	for _, item := range pending {
		if item.Action.Identity.Op == SyncResourcesOp.String() && item.Action.Identity.ResourceTypeID == "group" && item.Action.Identity.ParentResourceID == "" {
			return item
		}
	}
	t.Fatal("no pending group item")
	return c1zstore.LedgerWork{}
}

// S6 per item: the cap is per sync, not per attempt. After the cap fails
// the sync, a resume makes exactly one connector call for the item and
// fails the same way.
func TestActionInvalidationPerItemCapFailsSync(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "cap.c1z"), false)
	c := newInvalidatingConnector([]string{"group"}, invalidateAlways(""))
	s := newInvalidationSyncer(t, f, c, WithRestartLimits(2, 100))
	err := s.Sync(t.Context())
	require.ErrorIs(t, err, ErrRestartLimitExceeded)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the connector's status survives the wrap")
	var capErr *RestartLimitError
	require.ErrorAs(t, err, &capErr)
	require.Equal(t, c1zstore.RestartScopeAction, capErr.Limit.Scope)
	require.Equal(t, []uint64{1, 3}, capErr.Limit.Restarts)
	require.Equal(t, []string{"", "T1", "", "T1", "", "T1"}, c.callsFor("group"))
	require.EqualValues(t, 2, restartCounter(t, f))
	require.NoError(t, f.store.Close(t.Context()))

	f = openLedgerFixtureAt(t, f.path, false)
	c = newInvalidatingConnector([]string{"group"}, invalidateAlways(""))
	resumed := newInvalidationSyncer(t, f, c, WithRestartLimits(2, 100))
	err = resumed.Sync(t.Context())
	require.ErrorIs(t, err, ErrRestartLimitExceeded)
	require.Equal(t, []string{"T1"}, c.callsFor("group"))
	_, total := resumed.run.restartCounts()
	require.EqualValues(t, 2, total)
	require.EqualValues(t, 2, restartCounter(t, f))
}

// S6 per sync: a budget of one across two items that both expire.
func TestActionInvalidationPerSyncBudgetFailsSync(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "budget.c1z"), false)
	c := newInvalidatingConnector([]string{"group", "team"}, invalidateAlways(""))
	s := newInvalidationSyncer(t, f, c, WithRestartLimits(100, 1))
	err := s.Sync(t.Context())
	require.ErrorIs(t, err, ErrRestartLimitExceeded)
	var capErr *RestartLimitError
	require.ErrorAs(t, err, &capErr)
	require.Equal(t, c1zstore.RestartScopeSync, capErr.Limit.Scope)
	require.EqualValues(t, 1, capErr.Limit.Limit)
	require.EqualValues(t, 1, restartCounter(t, f))
}

// C04: a detail on a call that cannot honor it fails the sync without a
// restart and names the cell.
func TestActionInvalidationMisclassifiedCodeFailsSync(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "misclassified.c1z"), false)
	c := newInvalidatingConnector([]string{"group"}, func(_, token string, _ int) error {
		if token != "T1" {
			return nil
		}
		detail, err := status.New(codes.InvalidArgument, "expired").WithDetails(v2.RestartAction_builder{RestartPageToken: ""}.Build())
		if err != nil {
			return err
		}
		return detail.Err()
	})
	s := newInvalidationSyncer(t, f, c)
	err := s.Sync(t.Context())
	require.ErrorIs(t, err, ErrRestartRejected)
	require.Equal(t, codes.Internal, status.Code(err), "the connector's code does not reach the callers of invokeActionPage")
	require.Contains(t, err.Error(), "code=InvalidArgument")
	require.Equal(t, []string{"", "T1"}, c.callsFor("group"))
	require.Zero(t, restartCounter(t, f))
}

// pagedOpsConnector pages every list op "" → "T1" → "T2" with one element
// per page and fails the op named by failOp once, at "T1", with the typed
// error. Resource types page [group], [], []; the group resources g-p0..2
// each list one entitlement and one grant per page; static entitlements
// list one template per page.
type pagedOpsConnector struct {
	*mockConnector
	failOp  string
	restart string

	mu     native_sync.Mutex
	calls  map[string][]string
	failed bool
}

func newPagedOpsConnector(failOp, restart string) *pagedOpsConnector {
	return &pagedOpsConnector{mockConnector: newMockConnector(), failOp: failOp, restart: restart, calls: map[string][]string{}}
}

func (c *pagedOpsConnector) page(op, token string) (int, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[op] = append(c.calls[op], token)
	if op == c.failOp && token == "T1" && !c.failed {
		c.failed = true
		return 0, "", pagination.NewRestartActionError(c.restart, "page token expired")
	}
	switch token {
	case "":
		return 0, "T1", nil
	case "T1":
		return 1, "T2", nil
	default:
		return 2, "", nil
	}
}

func (c *pagedOpsConnector) callsFor(op string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls[op])
}

func (c *pagedOpsConnector) ListResourceTypes(
	_ context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	n, next, err := c.page("list-resource-types", req.GetPageToken())
	if err != nil {
		return nil, err
	}
	list := []*v2.ResourceType{}
	if n == 0 {
		list = append(list, groupResourceType)
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: list, NextPageToken: next}.Build(), nil
}

func (c *pagedOpsConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	n, next, err := c.page("list-resources", req.GetPageToken())
	if err != nil {
		return nil, err
	}
	group, err := rs.NewGroupResource(fmt.Sprintf("g-p%d", n), groupResourceType, fmt.Sprintf("g-p%d", n), nil)
	if err != nil {
		return nil, err
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{group}, NextPageToken: next}.Build(), nil
}

func (c *pagedOpsConnector) ListEntitlements(_ context.Context, req *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	n, next, err := c.page("list-entitlements", req.GetPageToken())
	if err != nil {
		return nil, err
	}
	ent := et.NewAssignmentEntitlement(req.GetResource(), fmt.Sprintf("e%d", n))
	return v2.EntitlementsServiceListEntitlementsResponse_builder{List: []*v2.Entitlement{ent}, NextPageToken: next}.Build(), nil
}

func (c *pagedOpsConnector) ListGrants(_ context.Context, req *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	n, next, err := c.page("list-grants", req.GetPageToken())
	if err != nil {
		return nil, err
	}
	grant := gt.NewGrant(req.GetResource(), fmt.Sprintf("e%d", n), req.GetResource().GetId())
	return v2.GrantsServiceListGrantsResponse_builder{List: []*v2.Grant{grant}, NextPageToken: next}.Build(), nil
}

func (c *pagedOpsConnector) ListStaticEntitlements(
	_ context.Context, req *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	n, next, err := c.page("list-static-entitlements", req.GetPageToken())
	if err != nil {
		return nil, err
	}
	template := v2.Entitlement_builder{Slug: fmt.Sprintf("static-%d", n), DisplayName: fmt.Sprintf("static-%d", n)}.Build()
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{List: []*v2.Entitlement{template}, NextPageToken: next}.Build(), nil
}

// storedShape is every record family in the store, as sorted identities.
type storedShape struct {
	resourceTypes, resources, entitlements, grants []string
}

func storeShape(t *testing.T, store c1zstore.Store) storedShape {
	t.Helper()
	var shape storedShape
	rts, err := store.ListResourceTypes(t.Context(), v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	for _, rt := range rts.GetList() {
		shape.resourceTypes = append(shape.resourceTypes, rt.GetId())
		shape.resources = append(shape.resources, storedResourceIDs(t, store, rt.GetId())...)
	}
	ents, err := store.ListEntitlements(t.Context(), v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	for _, e := range ents.GetList() {
		shape.entitlements = append(shape.entitlements, e.GetId())
	}
	grants, err := store.ListGrants(t.Context(), v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	for _, g := range grants.GetList() {
		shape.grants = append(shape.grants, g.GetId())
	}
	for _, list := range []*[]string{&shape.resourceTypes, &shape.resources, &shape.entitlements, &shape.grants} {
		slices.Sort(*list)
	}
	require.NotEmpty(t, shape.entitlements)
	require.NotEmpty(t, shape.grants)
	return shape
}

// Every honoring op restarts end to end through its own handler and list
// call site: the op's call sequence shows the restart, the store equals an
// uninterrupted run's, and the restart is counted under that op. Deleting
// the markActionListCall at any one call site fails that op's case as a
// misclassification.
func TestActionInvalidationRestartsEveryHonoringOp(t *testing.T) {
	ops := map[string]ActionOp{
		"list-resource-types":      SyncResourceTypesOp,
		"list-resources":           SyncResourcesOp,
		"list-entitlements":        SyncEntitlementsOp,
		"list-grants":              SyncGrantsOp,
		"list-static-entitlements": SyncStaticEntitlementsOp,
	}
	clean := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "clean.c1z"), false)
	cleanSyncer, err := NewSyncer(t.Context(), newPagedOpsConnector("", ""), WithConnectorStore(clean.store), WithWorkerCount(1))
	require.NoError(t, err)
	require.NoError(t, cleanSyncer.Sync(t.Context()))
	want := storeShape(t, clean.store)

	for op, actionOp := range ops {
		t.Run(op, func(t *testing.T) {
			f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), op+".c1z"), false)
			c := newPagedOpsConnector(op, "T1")
			s, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithWorkerCount(1))
			require.NoError(t, err)
			require.NoError(t, s.Sync(t.Context()))
			calls := c.callsFor(op)
			require.Contains(t, strings.Join(calls, ","), ",T1,T1,", "the call after the expiry carries the restart token: %v", calls)
			require.Equal(t, want, storeShape(t, f.store))
			byOp, total := s.(*syncer).run.restartCounts()
			require.EqualValues(t, 1, total)
			require.EqualValues(t, 1, byOp[actionOp.String()])
			report, err := f.engine.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			var decoded struct {
				Latest struct {
					RestartPages uint64 `json:"restart_pages"`
				} `json:"latest"`
			}
			require.NoError(t, json.Unmarshal(report, &decoded))
			require.EqualValues(t, 1, decoded.Latest.RestartPages)
		})
	}
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
			err := pagination.NewRestartActionError("R", "typed")
			if code == codes.FailedPrecondition {
				return err, true
			}
			detail, _ := status.New(code, "typed").WithDetails(v2.RestartAction_builder{RestartPageToken: "R"}.Build())
			return detail.Err(), true
		},
		"wrapped": func(code codes.Code) (error, bool) {
			detail, _ := status.New(code, "wrapped").WithDetails(v2.RestartAction_builder{RestartPageToken: "R"}.Build())
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
							detail, gotPresent, classErr := classifyRestartRequest(action, err)
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
								require.ErrorIs(t, classErr, ErrRestartRejected, name)
								require.ErrorIs(t, classErr, err, name)
								require.Equal(t, codes.Internal, status.Code(classErr), name)
								require.False(t, isWarning(t.Context(), classErr), name)
								require.False(t, retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 3}).ShouldWaitAndRetry(t.Context(), classErr), name)
								require.Contains(t, classErr.Error(), code.String(), name)
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
	fail             error
	getResourceCalls int
}

func (c *nestedGetResourceConnector) GetResource(ctx context.Context, in *v2.ResourceGetterServiceGetResourceRequest, opts ...grpc.CallOption) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	if in.GetResourceId().GetResource() == "g2" {
		c.getResourceCalls++
		return nil, c.fail
	}
	return c.mockConnector.GetResource(ctx, in, opts...)
}

// The detail on the codes getLedgerResourceFromConnector otherwise swallows
// (NotFound, Unimplemented: the resource is skipped) must not be swallowed.
// notFoundDetailGrantsConnector answers ListGrants with the detail on
// NotFound, the code isWarning treats as "finish the action with a
// warning" when the caller allows warnings (grants does).
type notFoundDetailGrantsConnector struct {
	*mockConnector
	listGrantsCalls int
}

func (c *notFoundDetailGrantsConnector) ListGrants(context.Context, *v2.GrantsServiceListGrantsRequest, ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	c.listGrantsCalls++
	st, err := status.New(codes.NotFound, "gone").WithDetails(v2.RestartAction_builder{RestartPageToken: ""}.Build())
	if err != nil {
		return nil, err
	}
	return nil, st.Err()
}

func TestActionInvalidationMisclassifiedNotFoundFailsNotWarns(t *testing.T) {
	c := &notFoundDetailGrantsConnector{mockConnector: newMockConnector()}
	c.rtDB = append(c.rtDB, groupResourceType)
	_, _, err := c.AddGroup(t.Context(), "g1")
	require.NoError(t, err)

	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "notfound.c1z"), false)
	s, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithWorkerCount(1))
	require.NoError(t, err)
	err = s.Sync(t.Context())
	require.ErrorIs(t, err, ErrRestartRejected, "a detail on NotFound fails the sync instead of finishing the action with a warning")
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, 1, c.listGrantsCalls)
	require.Zero(t, restartCounter(t, f))
}

func TestActionInvalidationNestedConnectorCallMisclassified(t *testing.T) {
	withDetail := func(code codes.Code) error {
		st, err := status.New(code, "from get-resource").WithDetails(v2.RestartAction_builder{RestartPageToken: ""}.Build())
		if err != nil {
			panic(err)
		}
		return st.Err()
	}
	for name, fail := range map[string]error{
		"typed":         pagination.NewRestartActionError("", "get-resource is not paginated"),
		"not-found":     withDetail(codes.NotFound),
		"unimplemented": withDetail(codes.Unimplemented),
	} {
		t.Run(name, func(t *testing.T) {
			c := &nestedGetResourceConnector{mockConnector: newMockConnector(), fail: fail}
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
			require.ErrorIs(t, err, ErrRestartRejected)
			require.NotErrorIs(t, err, ErrRestartLimitExceeded)
			require.Contains(t, err.Error(), "own_list_call=false")
			require.Equal(t, 1, c.getResourceCalls, "one failing call, no restart")
			require.Zero(t, restartCounter(t, f))
		})
	}
}

// ledgerRestartCrashStore calls cut around every restart page's commit. A
// non-nil error from the "restart-before" cut stands in for the commit
// itself failing: the real Commit is not called.
type ledgerRestartCrashStore struct {
	c1zstore.PageLedgerStore
	cut func(string) error
}

func (s ledgerRestartCrashStore) BeginPage() c1zstore.PageWriter {
	return ledgerRestartCrashWriter{PageWriter: s.PageLedgerStore.BeginPage(), cut: s.cut}
}

type ledgerRestartCrashWriter struct {
	c1zstore.PageWriter
	cut func(string) error
}

func (w ledgerRestartCrashWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if row != nil && row.Restarted {
		if err := w.cut("restart-before"); err != nil {
			return err
		}
	}
	if err := w.PageWriter.Commit(ctx, id, row); err != nil {
		return err
	}
	if row != nil && row.Restarted {
		return w.cut("restart-after")
	}
	return nil
}

// I2 at the syncer level: every honoring op × restart token × the sync
// ending at the restart commit, either because the commit failed or right
// after it landed. A resumed attempt on the same file finishes with the
// store an uninterrupted run produces. If the restart landed the resume
// starts the item at the restart token and the count is one; if it did
// not, the resume starts at the expired token and the count is zero.
func TestActionInvalidationAbortAtRestartSweep(t *testing.T) {
	ops := []string{"list-resource-types", "list-resources", "list-entitlements", "list-grants", "list-static-entitlements"}
	clean := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "clean.c1z"), false)
	cleanSyncer, err := NewSyncer(t.Context(), newPagedOpsConnector("", ""), WithConnectorStore(clean.store), WithWorkerCount(1))
	require.NoError(t, err)
	require.NoError(t, cleanSyncer.Sync(t.Context()))
	want := storeShape(t, clean.store)
	errCommit := errors.New("restart commit failed")

	for _, op := range ops {
		for _, restart := range []string{"", "T1"} {
			for _, cut := range []string{"restart-before", "restart-after"} {
				t.Run(fmt.Sprintf("%s/restart=%q/%s", op, restart, cut), func(t *testing.T) {
					f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "abort.c1z"), false)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					c := newPagedOpsConnector(op, restart)
					s, err := NewSyncer(ctx, c, WithConnectorStore(f.store), WithWorkerCount(1))
					require.NoError(t, err)
					s.(*syncer).caps.pageLedger = ledgerRestartCrashStore{PageLedgerStore: f.ledger, cut: func(reached string) error {
						if reached != cut {
							return nil
						}
						if cut == "restart-before" {
							return errCommit
						}
						cancel()
						return nil
					}}
					err = s.Sync(ctx)
					landed := cut == "restart-after"
					if landed {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.ErrorIs(t, err, errCommit)
					}
					counter := restartCounter(t, f)
					if landed {
						require.EqualValues(t, 1, counter)
					} else {
						require.Zero(t, counter)
					}

					resumed, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithWorkerCount(1))
					require.NoError(t, err)
					require.NoError(t, resumed.Sync(t.Context()))
					require.Equal(t, want, storeShape(t, f.store))
					_, total := resumed.(*syncer).run.restartCounts()
					require.EqualValues(t, counter, total, "the resumed run restores the count from the ledger")
					next := "T1"
					if landed {
						next = restart
					}
					calls := strings.Join(c.callsFor(op), ",")
					require.Contains(t, calls, ",T1,"+next+",", "the call after the expiry: %s", calls)
				})
			}
		}
	}
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
		s.caps.pageLedger = ledgerRestartCrashStore{PageLedgerStore: f.ledger, cut: func(reached string) error {
			if reached != cut {
				return nil
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
				counter := restartCounter(t, f)
				switch {
				case landed:
					require.Equal(t, []uint64{1}, item.Restarts)
					require.EqualValues(t, 2, item.Revision)
					require.EqualValues(t, 1, counter)
					require.Equal(t, []string{"group-p0"}, storedResourceIDs(t, f.store, "group"), "the restart page writes no records")
					facts, err := f.ledger.LedgerFacts(t.Context())
					require.NoError(t, err)
					require.Contains(t, facts, ledgerFactIngestBlocked, "the replay block rides the restart batch")
				case item != nil:
					require.Equal(t, "T1", item.Action.Identity.PageToken)
					require.Empty(t, item.Restarts)
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
				_, total := resumed.run.restartCounts()
				require.EqualValues(t, counter, total)
				require.Equal(t, []string{"group-p0", "group-p1", "group-p2"}, storedResourceIDs(t, f.store, "group"))
			})
		}
	}
}
