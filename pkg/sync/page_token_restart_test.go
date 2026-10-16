package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errExpiredTestPageToken = errors.New("provider cursor expired")

func isExpiredTestPageToken(err error) bool { return errors.Is(err, errExpiredTestPageToken) }

func restartTestPolicy() *pageTokenRestartPolicy {
	return &pageTokenRestartPolicy{limit: 2, isExpired: isExpiredTestPageToken}
}

const restartTestGeneration = "abcdefghijklmnopqrstuvwxyz"

type restartTestConnector struct {
	*mockConnector
	mu             sync.Mutex
	pages          map[string][]string
	starts         map[string]int
	resources      map[string]int
	expiringGroups map[string]int
	expiries       int
	failStart      int
	startErr       error
	u1, u2, u3     *v2.Resource
}

func newRestartTestConnector(t *testing.T) *restartTestConnector {
	t.Helper()
	m := newMockConnector()
	m.rtDB = append(m.rtDB, groupResourceType, userResourceType)
	u1, err := m.AddUser(t.Context(), "first-user")
	require.NoError(t, err)
	u2, err := m.AddUser(t.Context(), "second-user")
	require.NoError(t, err)
	u3, err := m.AddUser(t.Context(), "replacement-user")
	require.NoError(t, err)
	_, _, err = m.AddGroup(t.Context(), "failing-group")
	require.NoError(t, err)
	_, _, err = m.AddGroup(t.Context(), "healthy-group")
	require.NoError(t, err)
	return &restartTestConnector{mockConnector: m, pages: map[string][]string{}, starts: map[string]int{}, resources: map[string]int{}, expiries: 1, u1: u1, u2: u2, u3: u3}
}

func (c *restartTestConnector) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	c.mu.Lock()
	c.resources[req.GetResourceTypeId()]++
	c.mu.Unlock()
	return c.mockConnector.ListResources(ctx, req, opts...)
}

func (c *restartTestConnector) ListGrants(
	_ context.Context, req *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	if req.GetResource().GetId().GetResourceType() != groupResourceType.GetId() {
		return &v2.GrantsServiceListGrantsResponse{}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id := req.GetResource().GetId().GetResource()
	c.pages[id] = append(c.pages[id], req.GetPageToken())
	if req.GetPageToken() == "" {
		c.starts[id]++
		if id == "failing-group" && c.starts[id] == c.failStart {
			return nil, c.startErr
		}
	}
	expiries := c.expiries
	if c.expiringGroups != nil {
		expiries = c.expiringGroups[id]
	}
	if id == "failing-group" || c.expiringGroups[id] > 0 {
		switch req.GetPageToken() {
		case "":
			principal := c.u1
			if c.starts[id] > 1 {
				principal = c.u3
			}
			return &v2.GrantsServiceListGrantsResponse{List: []*v2.Grant{c.grant(req.GetResource(), principal)}, NextPageToken: "next"}, nil
		case "next":
			if c.starts[id] <= expiries {
				return nil, errExpiredTestPageToken
			}
		default:
			return nil, fmt.Errorf("provider received an invalid token")
		}
	}
	return &v2.GrantsServiceListGrantsResponse{List: []*v2.Grant{c.grant(req.GetResource(), c.u2)}}, nil
}

func TestPageTokenRestartBudgetSurvivesResume(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		for _, workers := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/workers=%d", engine, workers), func(t *testing.T) {
				ctx := t.Context()
				dir := t.TempDir()
				path := filepath.Join(dir, "budget.c1z")
				conn := newRestartTestConnector(t)
				conn.expiries = 100
				var syncID string
				var checkpoint []byte
				for attempt := range 3 {
					if attempt > 0 {
						path = filepath.Join(t.TempDir(), "cold-resume.c1z")
						require.NoError(t, writeLedgerTestFile(path, checkpoint, 0o600))
						conn = newRestartTestConnector(t)
						conn.expiries = 100
					}
					store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
					require.NoError(t, err)
					id, fresh, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					if attempt == 0 {
						syncID = id
						require.True(t, fresh)
					} else {
						require.False(t, fresh)
						require.Equal(t, syncID, id)
					}
					workerCount := workers
					if attempt == 1 {
						workerCount = 4 - workers
					}
					s, err := NewSyncer(ctx, conn, WithConnectorStore(store), WithTmpDir(dir), WithSyncID(syncID), WithDontExpandGrants(),
						WithWorkerCount(workerCount), WithPageTokenRestarts(2, isExpiredTestPageToken))
					require.NoError(t, err)
					err = s.Sync(ctx)
					require.ErrorIs(t, err, ErrPageTokenRestartLimit)
					require.ErrorIs(t, err, errExpiredTestPageToken)
					require.True(t, IsSyncPreservable(err))
					if attempt == 0 {
						require.Equal(t, 3, conn.starts["failing-group"])
					} else {
						require.Zero(t, conn.starts["failing-group"], "resuming the identical checkpoint must not replenish the restart budget")
					}
					require.False(t, ShouldDiscardSyncArtifact(errors.Join(err, s.Close(ctx))))
					if attempt == 0 {
						checkpoint, err = os.ReadFile(path)
						require.NoError(t, err)
						require.NotEmpty(t, checkpoint)
					}
					if attempt == 2 {
						runLedgerCrashChild(t, "^TestPageTokenRestartColdProcessResume$", 73,
							"BATON_PAGE_RESTART_CHECKPOINT="+path, "BATON_PAGE_RESTART_ENGINE="+string(engine), "BATON_PAGE_RESTART_SYNC="+syncID)
					}
				}
			})
		}
	}
}

func TestPageTokenRestartColdProcessResume(t *testing.T) {
	path := os.Getenv("BATON_PAGE_RESTART_CHECKPOINT")
	if path == "" {
		t.Skip("invoked by the budget resume matrix")
	}
	store, err := dotc1z.NewStore(t.Context(), path, dotc1z.WithEngine(c1zstore.Engine(os.Getenv("BATON_PAGE_RESTART_ENGINE"))), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	syncID, fresh, err := store.StartOrResumeSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, fresh)
	require.Equal(t, os.Getenv("BATON_PAGE_RESTART_SYNC"), syncID)
	conn := newRestartTestConnector(t)
	conn.expiries = 100
	s, err := NewSyncer(t.Context(), conn, WithConnectorStore(store), WithTmpDir(t.TempDir()), WithSyncID(syncID), WithDontExpandGrants(), WithWorkerCount(3),
		WithPageTokenRestarts(2, isExpiredTestPageToken))
	require.NoError(t, err)
	require.ErrorIs(t, s.Sync(t.Context()), ErrPageTokenRestartLimit)
	require.Zero(t, conn.starts["failing-group"])
	require.NoError(t, s.Close(t.Context()))
	os.Exit(73)
}

func TestPageTokenRestartLegacyTakeover(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "takeover.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	syncID, _, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	s := &syncer{syncID: syncID, run: newRunState(), stats: newRunStats()}
	s.setStore(store)
	require.NoError(t, store.CheckpointSync(ctx, readGoldenToken(t, "v3_page_token_restart.json")))
	resume, err := loadLedgerResume(ctx, store, s.caps.pageLedger, "takeover-attempt", "")
	require.NoError(t, err)
	require.True(t, resume.hasPendingWork)
	state, err := store.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Empty(t, state)
	require.NoError(t, store.Close(ctx))
	store, err = dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
	require.NoError(t, store.SetCurrentSync(ctx, syncID))
	s = &syncer{syncID: syncID}
	s.setStore(store)
	require.NoError(t, s.restoreLedgerState(ctx, s.caps.pageLedger, "cold-takeover", false))
	request, err := providerPageAction(s.run.current())
	require.NoError(t, err)
	require.Equal(t, "next", request.PageToken)
	require.EqualValues(t, 1, request.pageTokenRestarts)
}

type cancelRestartPageStore struct {
	c1zstore.PageLedgerStore
	cancel context.CancelFunc
	armed  *bool
}

func (s cancelRestartPageStore) BeginPage() c1zstore.PageWriter {
	return cancelRestartPageWriter{PageWriter: s.PageLedgerStore.BeginPage(), cancel: s.cancel, armed: s.armed}
}

type cancelRestartPageWriter struct {
	c1zstore.PageWriter
	cancel context.CancelFunc
	armed  *bool
}

func (w cancelRestartPageWriter) SetPendingWork(work c1zstore.LedgerWork, keys ...string) error {
	if work.PageTokenRestarted {
		*w.armed = true
		w.cancel()
	}
	return w.PageWriter.SetPendingWork(work, keys...)
}

func TestPageTokenRestartCanceledBeforeCommit(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.cfg.pageTokenRestartPolicy = restartTestPolicy()
	action := s.run.pushAction(t.Context(), Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "one", PageToken: "expired"})
	seedLedgerTestRun(t, s, action)
	before := ledgerRawSnapshot(t, f.engine)
	s.testHooks.ledgerHandler = func(context.Context, *Action, *ledgerPage) error {
		return providerPageError{cause: errExpiredTestPageToken}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	armed := false
	s.ledger.store = cancelRestartPageStore{PageLedgerStore: f.ledger, cancel: cancel, armed: &armed}
	f.audit.enter(ledgerHandler)
	err := invokeLedgerTestPage(t, s, ctx, action, s.SyncGrants, true)
	f.audit.enter(ledgerLifecycle)
	require.True(t, armed)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, errExpiredTestPageToken)
	require.Equal(t, codes.Canceled, status.Code(err))
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Zero(t, s.run.completedActionsCount())
	require.Zero(t, f.audit.writers)
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncGrants, true))
	f.audit.enter(ledgerLifecycle)
	request, err := providerPageAction(s.run.current())
	require.NoError(t, err)
	require.EqualValues(t, 1, request.pageTokenRestarts)
	require.Zero(t, f.audit.writers)
}

func TestPageTokenRestartInterruptedFirstPage(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		for _, disable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disabled-on-resume=%t", engine, disable), func(t *testing.T) {
				ctx := t.Context()
				dir := t.TempDir()
				path := filepath.Join(dir, "interrupted.c1z")
				conn := newRestartTestConnector(t)
				conn.failStart, conn.startErr = 2, context.Canceled
				store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
				require.NoError(t, err)
				syncID, _, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				s, err := NewSyncer(ctx, conn, WithConnectorStore(store), WithTmpDir(dir), WithSyncID(syncID), WithDontExpandGrants(), WithPageTokenRestarts(2, isExpiredTestPageToken))
				require.NoError(t, err)
				require.ErrorIs(t, s.Sync(ctx), context.Canceled)
				require.Equal(t, 2, conn.starts["failing-group"])
				require.NoError(t, s.Close(ctx))
				store, err = dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
				require.NoError(t, err)
				resumed, fresh, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.False(t, fresh)
				require.Equal(t, syncID, resumed)
				var limit uint32 = 2
				if disable {
					limit = 0
				}
				s, err = NewSyncer(ctx, conn, WithConnectorStore(store), WithTmpDir(dir), WithSyncID(syncID), WithDontExpandGrants(),
					WithWorkerCount(3), WithPageTokenRestarts(limit, isExpiredTestPageToken))
				require.NoError(t, err)
				require.NoError(t, s.Sync(ctx))
				require.NoError(t, s.Close(ctx))
				require.Equal(t, []string{"", "next", "", "", "next"}, conn.pages["failing-group"])
				store, err = dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
				require.NoError(t, store.SetCurrentSync(ctx, syncID))
				grants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{PageSize: 100})
				require.NoError(t, err)
				require.ElementsMatch(t, []string{
					"failing-group:first-user", "failing-group:replacement-user", "failing-group:second-user", "healthy-group:second-user",
				}, grantIDs(grants.GetList()))
			})
		}
	}
}

func TestPageTokenRestartCodec(t *testing.T) {
	for _, raw := range []string{"", "next", "https://provider.invalid/?token=secret", paginationRestartPrefix + "provider-opaque"} {
		action := &Action{ID: "work", Op: SyncGrantsOp, ResourceID: "group", PageToken: encodeRestartPageToken(2, restartTestGeneration, raw), PageTokenRestarted: true}
		before := *action
		request, err := providerPageAction(action)
		require.NoError(t, err)
		require.Equal(t, raw, request.PageToken)
		require.EqualValues(t, 2, request.pageTokenRestarts)
		require.Equal(t, before, *action)
	}
	for _, encoded := range []string{
		"secret", "2:1:c2VjcmV0", "1:secret", "1:0:c2VjcmV0", "1:-1:c2VjcmV0", "1:4294967296:c2VjcmV0", "1:1:secret$",
		"1:1:" + strings.Repeat("!", 26) + ":", "1:1:" + restartTestGeneration + ":secret$",
	} {
		request, err := providerPageAction(&Action{PageToken: paginationRestartPrefix + encoded, PageTokenRestarted: true})
		require.Error(t, err)
		require.Nil(t, request)
		require.NotContains(t, err.Error(), "secret")
	}
	plain := &Action{PageToken: "unchanged"}
	request, err := providerPageAction(plain)
	require.NoError(t, err)
	require.Same(t, plain, request)
	_, err = providerPageAction(&Action{PageToken: "secret", PageTokenRestarted: true})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
}

func TestPageTokenRestartPolicy(t *testing.T) {
	policy := restartTestPolicy()
	expired := providerPageError{cause: errExpiredTestPageToken}
	for _, op := range []ActionOp{SyncResourceTypesOp, SyncResourcesOp, SyncEntitlementsOp, SyncStaticEntitlementsOp, SyncGrantsOp} {
		t.Run(op.String(), func(t *testing.T) {
			action := &Action{Op: op, ResourceTypeID: "type", PageToken: "next"}
			next, err := policy.next(t.Context(), action, expired)
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(next, paginationRestartPrefix))
			action.Spawned = true
			_, err = policy.next(t.Context(), action, expired)
			require.ErrorIs(t, err, errExpiredTestPageToken)
			action.Spawned = false
			action.PageToken = ""
			_, err = policy.next(t.Context(), action, expired)
			require.ErrorIs(t, err, errExpiredTestPageToken)
		})
	}
	for _, op := range []ActionOp{InitOp, SyncGrantExpansionOp, SyncExternalResourcesOp, SyncAssetsOp, SyncTargetedResourceOp, MaterializeStaticEntitlementsOp} {
		_, err := policy.next(t.Context(), &Action{Op: op, ResourceTypeID: "type", PageToken: "next"}, expired)
		require.ErrorIs(t, err, errExpiredTestPageToken)
	}
	_, err := policy.next(t.Context(), &Action{Op: SyncGrantsOp, PageToken: "store-page"}, expired)
	require.ErrorIs(t, err, errExpiredTestPageToken)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = policy.next(ctx, &Action{Op: SyncGrantsOp, ResourceTypeID: "type", PageToken: "next"}, expired)
	require.ErrorIs(t, err, errExpiredTestPageToken)
	unrelated := errors.New("provider unavailable")
	action := &Action{Op: SyncGrantsOp, ResourceTypeID: "type", PageToken: "next"}
	_, err = policy.next(t.Context(), action, providerPageError{cause: unrelated})
	require.ErrorIs(t, err, unrelated)
	for _, disabled := range []*pageTokenRestartPolicy{nil, {}, {limit: 2}, {isExpired: func(error) bool { return true }}} {
		_, err = disabled.next(t.Context(), action, expired)
		require.ErrorIs(t, err, errExpiredTestPageToken)
	}
}

func TestPageTokenRestartLeavesProviderTokensOpaque(t *testing.T) {
	for _, configured := range []bool{false, true} {
		for _, token := range []string{"baton-sdk:page-restart:1:1:bmV4dA", "baton-sdk:page-restart:provider-opaque"} {
			s := &syncer{}
			if configured {
				s.cfg.pageTokenRestartPolicy = &pageTokenRestartPolicy{limit: 2, isExpired: func(error) bool { return true }}
			}
			var received string
			err := s.invokeActionPage(t.Context(), &Action{Op: SyncGrantsOp, PageToken: token}, func(_ context.Context, action *Action) error {
				received = action.PageToken
				return nil
			}, false)
			require.NoError(t, err)
			require.Equal(t, token, received)
		}
	}
}

func TestPageTokenRestartLimitIsTerminal(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.Unavailable, codes.DeadlineExceeded, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			cause := status.Error(code, "expired")
			policy := &pageTokenRestartPolicy{limit: 2, isExpired: func(error) bool { return true }}
			_, err := policy.next(t.Context(), &Action{Op: SyncGrantsOp, ResourceTypeID: "type", PageToken: "expired", pageTokenRestarts: 2}, providerPageError{cause: cause})
			require.ErrorIs(t, err, ErrPageTokenRestartLimit)
			require.ErrorIs(t, err, cause)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.False(t, isWarning(t.Context(), err))
			retryer := retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 1, InitialDelay: time.Nanosecond})
			require.False(t, retryer.ShouldWaitAndRetry(t.Context(), err))
		})
	}
}

type restartResourceFailureStore struct {
	*checkpointOutcomeStore
}

func (s restartResourceFailureStore) GetResource(context.Context, *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	return nil, errExpiredTestPageToken
}

func TestPageTokenRestartIgnoresStoreErrors(t *testing.T) {
	s := &syncer{run: newRunState(), stats: newRunStats(), cfg: syncConfig{workerCount: 1,
		pageTokenRestartPolicy: restartTestPolicy()}}
	store := restartResourceFailureStore{checkpointOutcomeStore: &checkpointOutcomeStore{}}
	s.setStore(store)
	action := s.run.pushAction(t.Context(), Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "one", PageToken: "next"})
	err := s.invokeActionPage(t.Context(), action, s.SyncGrants, true)
	require.ErrorIs(t, err, errExpiredTestPageToken)
	require.Equal(t, "next", s.run.getAction(action.ID).PageToken)
	require.Empty(t, store.outcomes)
}

func TestPageTokenRestartCheckpointFailure(t *testing.T) {
	ctx := t.Context()
	s := &syncer{run: newRunState(), stats: newRunStats(), cfg: syncConfig{workerCount: 1,
		pageTokenRestartPolicy: restartTestPolicy()}}
	store := &checkpointOutcomeStore{failAlways: true}
	s.setStore(store)
	action := s.run.pushAction(ctx, Action{Op: SyncResourcesOp, ResourceTypeID: "user", PageToken: "expired"})
	calls := 0
	result := s.syncOneAction(ctx, zap.NewNop(), retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 1}), action, func(context.Context, *Action) error {
		calls++
		return providerPageError{cause: errExpiredTestPageToken}
	})
	require.ErrorIs(t, result.err, errExpiredTestPageToken)
	require.ErrorIs(t, result.err, errInjectedCheckpointWrite)
	require.Equal(t, 1, calls)
	require.Len(t, store.outcomes, 1)
	require.NotNil(t, s.run.getAction(action.ID))

	action.PageToken = encodeRestartPageToken(2, restartTestGeneration, "expired")
	action.PageTokenRestarted = true
	s.run.mu.Lock()
	s.run.actions[action.ID] = *action
	s.run.mu.Unlock()
	calls = 0
	result = s.syncOneAction(ctx, zap.NewNop(), retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 1}), action, func(context.Context, *Action) error {
		calls++
		return providerPageError{cause: errExpiredTestPageToken}
	})
	require.ErrorIs(t, result.err, ErrPageTokenRestartLimit)
	require.ErrorIs(t, result.err, errInjectedCheckpointWrite)
	require.Equal(t, codes.Internal, status.Code(result.err))
	require.Equal(t, 1, calls)
}

func TestPageTokenRestartCheckpointBeforeRequest(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	store, err := dotc1z.NewStore(ctx, filepath.Join(dir, "ordering.c1z"), dotc1z.WithTmpDir(dir), dotc1z.WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
	syncID, _, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	s := &syncer{syncID: syncID, run: newRunState(), stats: newRunStats(), cfg: syncConfig{
		pageTokenRestartPolicy: restartTestPolicy()}}
	s.setStore(store)
	action := s.run.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "one", PageToken: "expired"})
	require.NoError(t, s.invokeActionPage(ctx, action, func(context.Context, *Action) error {
		return providerPageError{cause: errExpiredTestPageToken}
	}, true))
	called := false
	require.NoError(t, s.invokeActionPage(ctx, s.run.current(), func(ctx context.Context, request *Action) error {
		called = true
		require.Empty(t, request.PageToken)
		stored, err := store.GetSync(ctx, &reader_v2.SyncsReaderServiceGetSyncRequest{SyncId: syncID})
		require.NoError(t, err)
		resume, _, _, err := decodeLedgerCheckpoint(stored.GetSync().GetSyncToken())
		require.NoError(t, err)
		require.Len(t, resume.actions, 1)
		persisted := ledgerActionFromIdentity(resume.actions[0].identity)
		persisted.PageTokenRestarted = resume.actions[0].pageTokenRestarted
		require.True(t, persisted.PageTokenRestarted)
		decoded, err := providerPageAction(&persisted)
		require.NoError(t, err)
		require.EqualValues(t, 1, decoded.pageTokenRestarts)
		require.Empty(t, decoded.PageToken)
		return nil
	}, true))
	require.True(t, called)
}

func TestPageTokenRestartInvalidTransition(t *testing.T) {
	s := &syncer{run: newRunState(), stats: newRunStats(), cfg: syncConfig{
		pageTokenRestartPolicy: restartTestPolicy()}}
	store := &checkpointOutcomeStore{}
	s.setStore(store)
	called := false
	err := s.invokeActionPage(t.Context(), &Action{PageTokenRestarted: true, PageToken: "private-invalid-cursor"}, func(context.Context, *Action) error {
		called = true
		return nil
	}, true)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private-invalid-cursor")
	require.False(t, called)
	s.parallelActionTransitioner = func(context.Context, *Action, string, []Action) error { return errInjectedCheckpointWrite }
	err = s.invokeActionPage(t.Context(), &Action{Op: SyncGrantsOp, ResourceTypeID: "group", PageToken: "expired"}, func(context.Context, *Action) error {
		return providerPageError{cause: errExpiredTestPageToken}
	}, true)
	require.ErrorIs(t, err, errInjectedCheckpointWrite)
	require.ErrorIs(t, err, errExpiredTestPageToken)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Empty(t, store.outcomes)
}

func TestPageTokenRestartPersistenceStatus(t *testing.T) {
	causes := []struct {
		name string
		err  error
	}{
		{"plain", errExpiredTestPageToken},
		{"not-found", fmt.Errorf("%w: %w", errExpiredTestPageToken, status.Error(codes.NotFound, "cursor gone"))},
	}
	writes := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"plain", errInjectedCheckpointWrite, codes.Internal},
		{"not-found", status.Error(codes.NotFound, "sync row missing"), codes.Internal},
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", fmt.Errorf("checkpoint: %w", context.DeadlineExceeded), codes.Internal},
	}
	for _, cause := range causes {
		for _, write := range writes {
			t.Run(cause.name+"/"+write.name, func(t *testing.T) {
				s := &syncer{run: newRunState(), stats: newRunStats(), cfg: syncConfig{pageTokenRestartPolicy: restartTestPolicy()}}
				s.setStore(&checkpointOutcomeStore{})
				s.parallelActionTransitioner = func(context.Context, *Action, string, []Action) error { return write.err }
				err := s.invokeActionPage(t.Context(), &Action{Op: SyncGrantsOp, ResourceTypeID: "group", PageToken: "expired"}, func(context.Context, *Action) error {
					return newProviderPageError(cause.err)
				}, true)
				require.ErrorIs(t, err, write.err)
				require.ErrorIs(t, err, cause.err)
				require.Equal(t, write.want, status.Code(err))
				require.False(t, isWarning(t.Context(), err))
			})
		}
	}
}

type restartCheckpointFailureStore struct {
	*checkpointOutcomeStore
	err error
}

func (s restartCheckpointFailureStore) CheckpointSync(ctx context.Context, token string) error {
	if strings.Contains(token, `"page_token_restarted":true`) {
		return s.err
	}
	return s.checkpointOutcomeStore.CheckpointSync(ctx, token)
}

func TestPageTokenRestartCheckpointDeadlineIsNotRetried(t *testing.T) {
	ctx := t.Context()
	deadline := fmt.Errorf("checkpoint: %w", context.DeadlineExceeded)
	conn := &restartRPCConnector{mockConnector: newMockConnector()}
	s := &syncer{run: newEmptySchedulerState(t), stats: newRunStats(), graph: newExpansionGraph(), connector: conn,
		checkpoint: syncCheckpoint{interval: minCheckpointInterval}, cfg: syncConfig{workerCount: 1, pageTokenRestartPolicy: restartTestPolicy()}}
	s.setStore(restartCheckpointFailureStore{checkpointOutcomeStore: &checkpointOutcomeStore{}, err: deadline})
	s.run.pushAction(ctx, Action{Op: SyncResourceTypesOp, PageToken: "provider-next"})
	_, err := s.parallelSync(ctx, ctx, nil)
	require.ErrorIs(t, err, deadline)
	require.ErrorIs(t, err, errExpiredTestPageToken)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, []string{"provider-next"}, conn.requests, "the restarted listing must not start before its checkpoint commits")
}

func TestPageTokenRestartPlainTokenAllocs(t *testing.T) {
	action := &Action{Op: SyncGrantsOp, PageToken: "provider-page"}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if _, err := providerPageAction(action); err != nil {
			t.Fatal(err)
		}
	}))
}

func TestPageTokenRestartLedgerCommitFailure(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.cfg.pageTokenRestartPolicy = restartTestPolicy()
	action := s.run.pushAction(t.Context(), Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "one", PageToken: "expired"})
	seedLedgerTestRun(t, s, action)
	before := ledgerRawSnapshot(t, f.engine)
	var requests []string
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		requests = append(requests, action.PageToken)
		if action.PageToken != "" {
			return providerPageError{cause: errExpiredTestPageToken}
		}
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	err := invokeLedgerTestPage(t, s, t.Context(), action, s.SyncGrants, true)
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, errLedgerInjectedPage)
	require.ErrorIs(t, err, errExpiredTestPageToken)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Zero(t, f.audit.writers)
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncGrants, true))
	f.audit.enter(ledgerLifecycle)
	require.Zero(t, s.run.completedActionsCount())
	work, _, err := f.ledger.PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	require.Len(t, work, 1)
	request, err := providerPageAction(s.run.current())
	require.NoError(t, err)
	require.EqualValues(t, 1, request.pageTokenRestarts)
	require.Empty(t, request.PageToken)
	marked := work[0]
	marked.PageTokenRestarted = false
	f.audit.enter(ledgerHandler)
	writer := f.ledger.BeginPage()
	require.NoError(t, writer.SetPendingWork(marked))
	err = writer.Commit(t.Context(), marked.Action.Identity, &c1zstore.LedgerRow{
		Identity: marked.Action.Identity, WorkID: marked.ID, WorkRevision: marked.Revision, NextPageToken: "next",
	})
	writer.Discard()
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, pebbleengine.ErrStaleLedgerWork)
	require.Zero(t, f.audit.writers)
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncGrants, true))
	f.audit.enter(ledgerLifecycle)
	require.EqualValues(t, 1, s.run.completedActionsCount())
	require.Equal(t, []string{"expired", "expired", ""}, requests)
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counters.Counters[ledgerCompletedActions])
	require.Zero(t, f.audit.writers)
}

type restartLookupStore struct{ *checkpointOutcomeStore }

func (s restartLookupStore) GetResource(_ context.Context, req *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	return &reader_v2.ResourcesReaderServiceGetResourceResponse{Resource: &v2.Resource{Id: req.GetResourceId()}}, nil
}

type restartRPCConnector struct {
	*mockConnector
	requests []string
	err      error
}

func (c *restartRPCConnector) expired(token string) error {
	c.requests = append(c.requests, token)
	if c.err != nil {
		return c.err
	}
	return errExpiredTestPageToken
}

func (c *restartRPCConnector) ListResourceTypes(
	_ context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return nil, c.expired(req.GetPageToken())
}

func (c *restartRPCConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	return nil, c.expired(req.GetPageToken())
}

func (c *restartRPCConnector) ListEntitlements(_ context.Context, req *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return nil, c.expired(req.GetPageToken())
}

func (c *restartRPCConnector) ListStaticEntitlements(
	_ context.Context, req *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return nil, c.expired(req.GetPageToken())
}

func (c *restartRPCConnector) ListGrants(_ context.Context, req *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	return nil, c.expired(req.GetPageToken())
}

var restartRPCOps = []ActionOp{SyncResourceTypesOp, SyncResourcesOp, SyncEntitlementsOp, SyncStaticEntitlementsOp, SyncGrantsOp}

func newRestartRPCSyncer(t *testing.T, ledgered bool, conn *restartRPCConnector, op ActionOp) (*syncer, *ledgerFixture, *Action, func(context.Context, *Action) error) {
	t.Helper()
	s := &syncer{run: newRunState(), stats: newRunStats(), cfg: syncConfig{workerCount: 1}}
	var f *ledgerFixture
	if ledgered {
		s, f = newLedgerSchedulerFixture(t, 1)
		require.NoError(t, f.store.PutResourceTypes(t.Context(), groupResourceType))
		require.NoError(t, f.store.PutResources(t.Context(), &v2.Resource{Id: &v2.ResourceId{ResourceType: groupResourceType.GetId(), Resource: "one"}}))
	} else {
		s.setStore(restartLookupStore{checkpointOutcomeStore: &checkpointOutcomeStore{}})
	}
	s.connector = conn
	action := s.run.pushAction(t.Context(), Action{Op: op, ResourceTypeID: groupResourceType.GetId(), ResourceID: "one", PageToken: "provider-next"})
	handlers := map[ActionOp]func(context.Context, *Action) error{
		SyncResourceTypesOp: s.SyncResourceTypes, SyncResourcesOp: s.SyncResources,
		SyncEntitlementsOp: s.SyncEntitlements, SyncStaticEntitlementsOp: s.SyncStaticEntitlements, SyncGrantsOp: s.SyncGrants,
	}
	return s, f, action, handlers[op]
}

func TestProviderPageErrorStatusTransparent(t *testing.T) {
	statusErr := status.Error(codes.PermissionDenied, "connector says no")
	causes := map[string]error{
		"status":         statusErr,
		"wrapped-status": fmt.Errorf("connector: %w", statusErr),
		"plain":          errors.New("connector failed"),
		"canceled":       context.Canceled,
	}
	shapes := map[string]func(error) error{
		"bare":    func(err error) error { return err },
		"wrapped": func(err error) error { return fmt.Errorf("listing: %w", err) },
		"joined":  func(err error) error { return errors.Join(err, status.Error(codes.Unavailable, "sibling")) },
	}
	for causeName, cause := range causes {
		for shapeName, shape := range shapes {
			t.Run(causeName+"/"+shapeName, func(t *testing.T) {
				want, wantOK := status.FromError(shape(cause))
				got, gotOK := status.FromError(shape(newProviderPageError(cause)))
				require.Equal(t, wantOK, gotOK)
				require.Equal(t, want.Code(), got.Code())
				require.Equal(t, want.Message(), got.Message())
				require.ErrorIs(t, shape(newProviderPageError(cause)), cause)
				var providerErr providerPageError
				require.ErrorAs(t, shape(newProviderPageError(cause)), &providerErr)
			})
		}
	}
}

func TestProviderPageErrorKeepsStatus(t *testing.T) {
	connErr := status.Error(codes.PermissionDenied, "connector says no")
	for _, ledgered := range []bool{false, true} {
		for _, op := range restartRPCOps {
			t.Run(fmt.Sprintf("ledger=%t/%s", ledgered, op), func(t *testing.T) {
				s, f, action, handler := newRestartRPCSyncer(t, ledgered, &restartRPCConnector{mockConnector: newMockConnector(), err: connErr}, op)
				if ledgered {
					f.audit.enter(ledgerHandler)
				}
				err := invokeLedgerTestPage(t, s, t.Context(), action, handler, true)
				// The status each call site returns without providerPageError.
				want := connErr
				if op == SyncGrantsOp {
					want = fmt.Errorf("sync-grants-for-resource: error listing grants: %w", connErr)
				}
				require.Equal(t, status.Code(want), status.Code(err))
				require.Equal(t, status.Convert(want).Message(), status.Convert(err).Message())
			})
		}
	}
}

func TestPageTokenRestartProviderRPCFamilies(t *testing.T) {
	for _, ledgered := range []bool{false, true} {
		for _, op := range restartRPCOps {
			t.Run(fmt.Sprintf("ledger=%t/%s", ledgered, op), func(t *testing.T) {
				conn := &restartRPCConnector{mockConnector: newMockConnector()}
				s, f, action, handler := newRestartRPCSyncer(t, ledgered, conn, op)
				s.cfg.pageTokenRestartPolicy = restartTestPolicy()
				if ledgered {
					f.audit.enter(ledgerHandler)
				}
				require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, handler, true))
				if ledgered {
					f.audit.enter(ledgerLifecycle)
					require.Zero(t, f.audit.writers)
					row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
					require.NoError(t, err)
					require.True(t, found)
					require.EqualValues(t, 1, row.ConnectorErrors)
					if row.Collection != nil {
						require.Zero(t, row.Collection.ListResponses)
						require.Zero(t, row.Collection.EmptyListResponses)
					}
					counters, err := f.ledger.LedgerCounters(t.Context())
					require.NoError(t, err)
					require.Zero(t, counters.Counters[ledgerCompletedActions])
					require.Zero(t, counters.Counters[ledgerWarningsPrefix+op.String()])
				}
				require.Equal(t, []string{"provider-next"}, conn.requests)
				request, err := providerPageAction(s.run.current())
				require.NoError(t, err)
				require.Empty(t, request.PageToken)
				require.EqualValues(t, 1, request.pageTokenRestarts)
				require.Zero(t, s.run.completedActionsCount())
			})
		}
	}
}

func FuzzPageTokenRestartCodec(f *testing.F) {
	f.Add("next", uint32(1))
	f.Add("", uint32(2))
	f.Fuzz(func(t *testing.T, token string, count uint32) {
		if count == 0 {
			count = 1
		}
		request, err := providerPageAction(&Action{PageToken: encodeRestartPageToken(count, restartTestGeneration, token), PageTokenRestarted: true})
		require.NoError(t, err)
		require.Equal(t, token, request.PageToken)
		require.Equal(t, count, request.pageTokenRestarts)
	})
}

func BenchmarkPageTokenRestartCodec(b *testing.B) {
	for _, restart := range []bool{false, true} {
		b.Run(fmt.Sprintf("restarted=%t", restart), func(b *testing.B) {
			action := &Action{PageToken: "provider-page"}
			if restart {
				action.PageToken = encodeRestartPageToken(1, restartTestGeneration, action.PageToken)
				action.PageTokenRestarted = true
			}
			b.ReportAllocs()
			for b.Loop() {
				_, err := providerPageAction(action)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPageTokenRestartCancellationCauseRedactsCursor(t *testing.T) {
	ctx := t.Context()
	s := &syncer{run: newRunState(), stats: newRunStats(), cfg: syncConfig{workerCount: 2}}
	s.run.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "fail", PageToken: "private-provider-cursor"})
	s.run.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "wait"})
	started := make(chan struct{})
	sourceErr := errors.New("provider rejected cursor")
	_, err := s.syncParallel(ctx, retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 1}), s.run.peekMatchingActions(ctx, SyncGrantsOp),
		func(ctx context.Context, action *Action) error {
			if action.ResourceID == "fail" {
				<-started
				return sourceErr
			}
			close(started)
			<-ctx.Done()
			return context.Cause(ctx)
		})
	require.ErrorIs(t, err, sourceErr)
	require.NotContains(t, err.Error(), "private-provider-cursor")
}

func (c *restartTestConnector) grant(group, user *v2.Resource) *v2.Grant {
	return &v2.Grant{
		Id:          group.GetId().GetResource() + ":" + user.GetId().GetResource(),
		Entitlement: c.entDB[group.GetId().GetResource()][0],
		Principal:   user,
	}
}

func TestPageTokenRestartKeepsCompletedWork(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		for _, workers := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/workers=%d", engine, workers), func(t *testing.T) {
				ctx := t.Context()
				dir := t.TempDir()
				path := filepath.Join(dir, "restart.c1z")
				conn := newRestartTestConnector(t)
				store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
				require.NoError(t, err)
				syncID, _, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				s, err := NewSyncer(ctx, conn, WithConnectorStore(store), WithTmpDir(dir), WithSyncID(syncID),
					WithWorkerCount(workers), WithDontExpandGrants(), WithPageTokenRestarts(2, isExpiredTestPageToken))
				require.NoError(t, err)
				require.NoError(t, s.Sync(ctx))
				require.Equal(t, []string{"", "next", "", "next"}, conn.pages["failing-group"])
				require.Equal(t, 1, conn.starts["healthy-group"])
				require.Equal(t, 1, conn.resources[groupResourceType.GetId()])
				require.Equal(t, 1, conn.resources[userResourceType.GetId()])
				require.NoError(t, s.Close(ctx))
				reopened, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, reopened.Close(context.WithoutCancel(ctx))) })
				require.NoError(t, reopened.SetCurrentSync(ctx, syncID))
				resp, err := reopened.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{PageSize: 100})
				require.NoError(t, err)
				require.ElementsMatch(t, []string{
					"failing-group:first-user", "failing-group:replacement-user", "failing-group:second-user", "healthy-group:second-user",
				}, grantIDs(resp.GetList()))
				require.NoError(t, reopened.Close(ctx))
				reopened, err = dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
				require.NoError(t, err)
				freshID, fresh, err := reopened.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.True(t, fresh)
				require.NotEqual(t, syncID, freshID)
				s, err = NewSyncer(ctx, conn, WithConnectorStore(reopened), WithTmpDir(dir), WithSyncID(freshID),
					WithWorkerCount(workers), WithDontExpandGrants(), WithPageTokenRestarts(2, isExpiredTestPageToken))
				require.NoError(t, err)
				require.NoError(t, s.Sync(ctx))
				require.NoError(t, s.Close(ctx))
				reopened, err = dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
				require.NoError(t, err)
				require.NoError(t, reopened.SetCurrentSync(ctx, freshID))
				resp, err = reopened.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{PageSize: 100})
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"failing-group:replacement-user", "failing-group:second-user", "healthy-group:second-user"}, grantIDs(resp.GetList()))
			})
		}
	}
}

func TestPageTokenRestartBudgetsAreIndependent(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		for _, workers := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/workers=%d", engine, workers), func(t *testing.T) {
				dir := t.TempDir()
				store, err := dotc1z.NewStore(t.Context(), filepath.Join(dir, "siblings.c1z"), dotc1z.WithEngine(engine), dotc1z.WithTmpDir(dir))
				require.NoError(t, err)
				conn := newRestartTestConnector(t)
				conn.expiringGroups = map[string]int{"failing-group": 1, "healthy-group": 2}
				s, err := NewSyncer(t.Context(), conn, WithConnectorStore(store), WithTmpDir(dir), WithDontExpandGrants(), WithWorkerCount(workers),
					WithPageTokenRestarts(2, isExpiredTestPageToken))
				require.NoError(t, err)
				require.NoError(t, s.Sync(t.Context()))
				require.NoError(t, s.Close(t.Context()))
				require.Equal(t, 2, conn.starts["failing-group"])
				require.Equal(t, 3, conn.starts["healthy-group"])
			})
		}
	}
}

func TestPageTokenRestartCheckpointWritersSerialize(t *testing.T) {
	var checkpoint syncCheckpoint
	var active, maximum, commits atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 32 {
		wg.Go(func() {
			<-start
			err := checkpoint.write(true, func() error {
				n := active.Add(1)
				for prior := maximum.Load(); prior < n && !maximum.CompareAndSwap(prior, n); prior = maximum.Load() {
				}
				time.Sleep(time.Millisecond)
				commits.Add(1)
				active.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("checkpoint: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	require.EqualValues(t, 32, commits.Load())
	require.EqualValues(t, 1, maximum.Load())
}
