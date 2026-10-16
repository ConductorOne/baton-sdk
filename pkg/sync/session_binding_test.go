package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	native_sync "sync"
	"testing"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	internalconnector "github.com/conductorone/baton-sdk/internal/connector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
)

const bindingWaitTimeout = 30 * time.Second

type bindingScript interface {
	grants(ctx context.Context, opts resource.SyncOpAttrs) error
}

type sessionRead struct {
	phase string
	key   string
	want  string
	got   string
	found bool
	err   error
}

type sessionTrace struct {
	syncID  string
	setErrs []error
	reads   []sessionRead
}

func (tr *sessionTrace) set(ctx context.Context, ss sessions.SessionStore, key string) {
	if err := ss.Set(ctx, key, []byte(tr.syncID+"/"+key)); err != nil {
		tr.setErrs = append(tr.setErrs, fmt.Errorf("set %s: %w", key, err))
	}
}

func (tr *sessionTrace) get(ctx context.Context, ss sessions.SessionStore, phase, key string) {
	v, found, err := ss.Get(ctx, key)
	tr.reads = append(tr.reads, sessionRead{phase: phase, key: key, want: tr.syncID + "/" + key, got: string(v), found: found, err: err})
}

func (tr *sessionTrace) requireOwnReads(t *testing.T, who string) {
	t.Helper()
	require.Empty(t, tr.setErrs, "%s session writes failed", who)
	for _, r := range tr.reads {
		require.NoError(t, r.err, "%s read of %q in phase %s", who, r.key, r.phase)
		require.True(t, r.found, "%s read of %q in phase %s missed its own write", who, r.key, r.phase)
		require.Equal(t, r.want, r.got, "%s read of %q in phase %s", who, r.key, r.phase)
	}
}

func signal(ch chan struct{}) { close(ch) }

func awaitRelease(ctx context.Context, ch chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitFor(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(bindingWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

type overlapScript struct {
	mu         native_sync.Mutex
	held       *sessionTrace
	runner     *sessionTrace
	failRunner bool

	heldAtPre       chan struct{}
	heldAtMid       chan struct{}
	runnerAtSet     chan struct{}
	releaseHeldMid  chan struct{}
	releaseRunner   chan struct{}
	releaseHeldPost chan struct{}
}

func newOverlapScript(failRunner bool) *overlapScript {
	return &overlapScript{
		failRunner:      failRunner,
		heldAtPre:       make(chan struct{}),
		heldAtMid:       make(chan struct{}),
		runnerAtSet:     make(chan struct{}),
		releaseHeldMid:  make(chan struct{}),
		releaseRunner:   make(chan struct{}),
		releaseHeldPost: make(chan struct{}),
	}
}

func (o *overlapScript) assign(syncID string) (*sessionTrace, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch {
	case o.held == nil:
		o.held = &sessionTrace{syncID: syncID}
		return o.held, true
	case o.runner == nil && o.held.syncID != syncID:
		o.runner = &sessionTrace{syncID: syncID}
		return o.runner, false
	default:
		return nil, false
	}
}

func (o *overlapScript) grants(ctx context.Context, opts resource.SyncOpAttrs) error {
	tr, held := o.assign(opts.SyncID)
	if tr == nil {
		return fmt.Errorf("unexpected extra Grants call for sync %q", opts.SyncID)
	}
	ss := opts.Session
	if held {
		tr.set(ctx, ss, "pre")
		signal(o.heldAtPre)
		if err := awaitRelease(ctx, o.releaseHeldMid); err != nil {
			return err
		}
		tr.get(ctx, ss, "mid", "pre")
		tr.set(ctx, ss, "mid")
		signal(o.heldAtMid)
		if err := awaitRelease(ctx, o.releaseHeldPost); err != nil {
			return err
		}
		tr.get(ctx, ss, "post", "pre")
		tr.get(ctx, ss, "post", "mid")
		tr.set(ctx, ss, "post")
		return nil
	}
	tr.set(ctx, ss, "run")
	signal(o.runnerAtSet)
	if err := awaitRelease(ctx, o.releaseRunner); err != nil {
		return err
	}
	tr.get(ctx, ss, "run", "run")
	if o.failRunner {
		return errors.New("runner sync failed on purpose")
	}
	return nil
}

type plainScript struct {
	mu             native_sync.Mutex
	failAfterWrite bool
	calls          int
	traces         []*sessionTrace
}

func (p *plainScript) grants(ctx context.Context, opts resource.SyncOpAttrs) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	tr := &sessionTrace{syncID: opts.SyncID}
	p.traces = append(p.traces, tr)
	p.mu.Unlock()

	if call == 1 {
		tr.set(ctx, opts.Session, "k")
		if p.failAfterWrite {
			return errors.New("first attempt failed on purpose")
		}
	}
	tr.get(ctx, opts.Session, "read", "k")
	return nil
}

type bindingRepoSyncer struct{ script bindingScript }

func (s *bindingRepoSyncer) ResourceType(context.Context) *v2.ResourceType {
	return v2.ResourceType_builder{Id: "repo", DisplayName: "Repo"}.Build()
}

func bindingRepo() *v2.Resource {
	return v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "repo", Resource: "repo-1"}.Build(),
		DisplayName: "Repo One",
	}.Build()
}

func (s *bindingRepoSyncer) List(context.Context, *v2.ResourceId, resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	return []*v2.Resource{bindingRepo()}, &resource.SyncOpResults{}, nil
}

var _ connectorbuilder.ResourceTargetedSyncerLimited = (*bindingRepoSyncer)(nil)

func (s *bindingRepoSyncer) Get(context.Context, *v2.ResourceId, *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	return bindingRepo(), nil, nil
}

func bindingMember(r *v2.Resource) *v2.Entitlement {
	return v2.Entitlement_builder{Id: "member", Resource: r, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
}

func (s *bindingRepoSyncer) Entitlements(_ context.Context, r *v2.Resource, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return []*v2.Entitlement{bindingMember(r)}, &resource.SyncOpResults{}, nil
}

func (s *bindingRepoSyncer) Grants(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	if err := s.script.grants(ctx, opts); err != nil {
		return nil, nil, err
	}
	principal := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "user-1"}.Build()}.Build()
	return []*v2.Grant{v2.Grant_builder{Id: "repo-1-member-user-1", Entitlement: bindingMember(r), Principal: principal}.Build()}, &resource.SyncOpResults{}, nil
}

type bindingUserSyncer struct{}

func (s *bindingUserSyncer) ResourceType(context.Context) *v2.ResourceType {
	return v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
}

func (s *bindingUserSyncer) List(context.Context, *v2.ResourceId, resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	return []*v2.Resource{v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "user-1"}.Build(),
		DisplayName: "User One",
	}.Build()}, &resource.SyncOpResults{}, nil
}

func (s *bindingUserSyncer) Entitlements(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return nil, &resource.SyncOpResults{}, nil
}

func (s *bindingUserSyncer) Grants(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	return nil, &resource.SyncOpResults{}, nil
}

type bindingConnector struct{ script bindingScript }

func (c *bindingConnector) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{DisplayName: "session-binding"}.Build(), nil
}

func (c *bindingConnector) Validate(context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (c *bindingConnector) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{&bindingRepoSyncer{script: c.script}, &bindingUserSyncer{}}
}

type opRecorder struct {
	sessions.SessionStore
	mu   native_sync.Mutex
	errs map[string][]error
}

func (r *opRecorder) note(ctx context.Context, op string, err error, opt []sessions.SessionStoreOption) {
	if err == nil {
		return
	}
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		_ = o(ctx, bag)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs[bag.SyncID] = append(r.errs[bag.SyncID], fmt.Errorf("%s: %w", op, err))
}

func (r *opRecorder) errsFor(syncID string) []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.errs[syncID]...)
}

func (r *opRecorder) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	v, ok, err := r.SessionStore.Get(ctx, key, opt...)
	r.note(ctx, "get", err, opt)
	return v, ok, err
}

func (r *opRecorder) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	v, rest, err := r.SessionStore.GetMany(ctx, keys, opt...)
	r.note(ctx, "get_many", err, opt)
	return v, rest, err
}

func (r *opRecorder) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	err := r.SessionStore.Set(ctx, key, value, opt...)
	r.note(ctx, "set", err, opt)
	return err
}

func (r *opRecorder) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	err := r.SessionStore.SetMany(ctx, values, opt...)
	r.note(ctx, "set_many", err, opt)
	return err
}

func (r *opRecorder) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	err := r.SessionStore.Delete(ctx, key, opt...)
	r.note(ctx, "delete", err, opt)
	return err
}

func (r *opRecorder) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	err := r.SessionStore.Clear(ctx, opt...)
	r.note(ctx, "clear", err, opt)
	return err
}

func (r *opRecorder) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	v, next, err := r.SessionStore.GetAll(ctx, pageToken, opt...)
	r.note(ctx, "get_all", err, opt)
	return v, next, err
}

type bindingHarnessOpts struct {
	connectorCache   bool
	noSessionSupport bool
}

type bindingHarness struct {
	tmpDir     string
	sessionRPC v1.BatonSessionServiceClient
	cc         types.ConnectorClient
	recorder   *opRecorder
}

func newBindingHarness(t *testing.T, script bindingScript, o bindingHarnessOpts) *bindingHarness {
	t.Helper()
	ctx := t.Context()
	h := &bindingHarness{tmpDir: t.TempDir()}

	var sessionServer *session.GRPCSessionServer
	if o.noSessionSupport {
		// The CLI hands connectors this store when the session port is 0.
		h.recorder = &opRecorder{SessionStore: &session.NoOpSessionStore{}, errs: map[string][]error{}}
	} else {
		sessionServer = session.NewGRPCSessionServer()
		sessionLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		go func() {
			_ = session.StartGRPCSessionServerWithOptions(ctx, sessionLis, sessionServer,
				grpc.ChainUnaryInterceptor(ugrpc.UnaryServerInterceptor(ctx)...))
		}()
		sessionConn, err := grpc.NewClient(sessionLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sessionConn.Close() })
		h.sessionRPC = v1.NewBatonSessionServiceClient(sessionConn)

		var childStore sessions.SessionStore
		childStore, err = session.NewGRPCSessionStore(ctx, h.sessionRPC)
		require.NoError(t, err)
		// lazyCachingSessionStore.ensureSession wraps the backend this way;
		// without it the connector reports no session usage.
		childStore = session.NewInstrumentedSessionStore(childStore, "connector_backend", "", nil)
		if o.connectorCache {
			childStore, err = session.NewMemorySessionCache(&otter.Options[string, []byte]{
				MaximumWeight:    1024,
				ExpiryCalculator: otter.ExpiryWriting[string, []byte](10 * time.Minute),
				Weigher:          func(string, []byte) uint32 { return 1 },
			}, childStore)
			require.NoError(t, err)
		}
		h.recorder = &opRecorder{SessionStore: childStore, errs: map[string][]error{}}
	}

	server, err := connectorbuilder.NewConnector(ctx, &bindingConnector{script: script}, connectorbuilder.WithSessionStore(h.recorder))
	require.NoError(t, err)
	connectorLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(ugrpc.UnaryServerInterceptor(ctx)...))
	internalconnector.Register(ctx, grpcServer, server, nil)
	go func() { _ = grpcServer.Serve(connectorLis) }()
	t.Cleanup(grpcServer.Stop)

	connectorConn, err := grpc.NewClient(connectorLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connectorConn.Close() })
	client := internalconnector.NewConnectorClient(ctx, connectorConn)
	if sessionServer != nil {
		client.SetSessionServer(sessionServer)
	}
	h.cc = client
	return h
}

func (h *bindingHarness) path(name string) string {
	return filepath.Join(h.tmpDir, name+".c1z")
}

func (h *bindingHarness) newSyncer(t *testing.T, c1zPath string, engine c1zstore.Engine, opts ...SyncOpt) Syncer {
	t.Helper()
	all := []SyncOpt{WithC1ZPath(c1zPath), WithTmpDir(h.tmpDir), WithStorageEngine(engine)}
	if binder, ok := h.cc.(sessions.SessionStoreBinder); ok {
		all = append(all, WithSessionStoreBinder(binder))
	}
	s, err := NewSyncer(t.Context(), h.cc, append(all, opts...)...)
	require.NoError(t, err)
	return s
}

type syncResult struct {
	syncErr  error
	closeErr error
}

func runSyncAndClose(ctx context.Context, s Syncer) <-chan syncResult {
	out := make(chan syncResult, 1)
	go func() {
		syncErr := s.Sync(ctx)
		out <- syncResult{syncErr: syncErr, closeErr: s.Close(ctx)}
	}()
	return out
}

func awaitResult(t *testing.T, ch <-chan syncResult, what string) syncResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(bindingWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return syncResult{}
	}
}

func openSealed(t *testing.T, c1zPath, tmpDir string) c1zstore.Store {
	t.Helper()
	store, err := dotc1z.NewStore(t.Context(), c1zPath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store
}

func sessionRows(t *testing.T, store c1zstore.Store, syncID string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	token := ""
	for {
		page, next, err := store.SessionStore().GetAll(t.Context(), token, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		for k, v := range page {
			out[k] = v
		}
		if next == "" {
			return out
		}
		token = next
	}
}

func requireSessionCounts(t *testing.T, store c1zstore.Store, syncType connectorstore.SyncType, syncID string, sets, gets int64, who string) {
	t.Helper()
	stats, err := store.SyncMeta().StatsV2(t.Context(), syncType, syncID)
	require.NoError(t, err)
	got := stats.GetSessionStoreStats()
	for _, layer := range []string{"store", "connector"} {
		require.EqualValues(t, sets, got[layer+".set"].GetCount(), "%s %s.set", who, layer)
		require.EqualValues(t, gets, got[layer+".get"].GetCount(), "%s %s.get", who, layer)
	}
}

func requireClassifiedClosedSyncError(t *testing.T, rpc v1.BatonSessionServiceClient, syncID string) {
	t.Helper()
	_, err := rpc.Get(t.Context(), v1.GetRequest_builder{SyncId: syncID, Key: "run"}.Build())
	require.Error(t, err, "request for closed sync %s must be rejected", syncID)
	code := status.Code(err)
	require.Contains(t, []codes.Code{codes.FailedPrecondition, codes.NotFound}, code,
		"request for closed sync %s returned %s: %v", syncID, code, err)
}

func syncTypeFor(isFull bool) connectorstore.SyncType {
	if isFull {
		return connectorstore.SyncTypeFull
	}
	return connectorstore.SyncTypePartial
}

// Criteria: docs/verification/session-store-sync-binding/plan.md.
func TestSessionBindingOverlap(t *testing.T) {
	type cell struct {
		engine      c1zstore.Engine
		heldIsFull  bool
		runnerFails bool
		cache       bool
	}
	var cells []cell
	for _, engine := range []c1zstore.Engine{c1zstore.EnginePebble, c1zstore.EngineSQLite} {
		for _, heldIsFull := range []bool{true, false} {
			for _, runnerFails := range []bool{false, true} {
				cells = append(cells, cell{engine: engine, heldIsFull: heldIsFull, runnerFails: runnerFails})
			}
		}
	}
	cells = append(cells, cell{engine: c1zstore.EnginePebble, heldIsFull: true, cache: true})

	for _, c := range cells {
		order := "partial-then-full"
		if c.heldIsFull {
			order = "full-then-partial"
		}
		outcome := "runner-succeeds"
		if c.runnerFails {
			outcome = "runner-fails"
		}
		name := fmt.Sprintf("%s/%s/%s", c.engine, order, outcome)
		if c.cache {
			name += "/connector-cache"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			script := newOverlapScript(c.runnerFails)
			h := newBindingHarness(t, script, bindingHarnessOpts{connectorCache: c.cache})
			targeted := WithTargetedSyncResources([]*v2.Resource{bindingRepo()})

			heldOpts, runnerOpts := []SyncOpt{}, []SyncOpt{targeted}
			if !c.heldIsFull {
				heldOpts, runnerOpts = runnerOpts, heldOpts
			}
			heldPath, runnerPath := h.path("held"), h.path("runner")
			heldSyncer := h.newSyncer(t, heldPath, c.engine, heldOpts...)
			runnerSyncer := h.newSyncer(t, runnerPath, c.engine, runnerOpts...)

			heldDone := runSyncAndClose(ctx, heldSyncer)
			waitFor(t, script.heldAtPre, "held sync to write before the runner starts")

			runnerDone := runSyncAndClose(ctx, runnerSyncer)
			waitFor(t, script.runnerAtSet, "runner sync to reach the connector")

			signal(script.releaseHeldMid)
			waitFor(t, script.heldAtMid, "held sync to read and write while the runner is mid-sync")

			signal(script.releaseRunner)
			runner := awaitResult(t, runnerDone, "runner Sync and Close")
			if c.runnerFails {
				require.Error(t, runner.syncErr, "premise: runner sync must fail")
			} else {
				require.NoError(t, runner.syncErr, "premise: runner sync must succeed")
			}
			require.NoError(t, runner.closeErr)
			runnerID := script.runner.syncID

			select {
			case r := <-heldDone:
				t.Fatalf("premise: held sync finished before release: %+v", r)
			default:
			}

			signal(script.releaseHeldPost)
			held := awaitResult(t, heldDone, "held Sync and Close")
			require.NoError(t, held.syncErr, "premise: held sync must succeed")
			require.NoError(t, held.closeErr)
			heldID := script.held.syncID
			require.NotEqual(t, heldID, runnerID)

			t.Run("V1_held_reads_own_writes", func(t *testing.T) {
				script.held.requireOwnReads(t, "held")
				require.Empty(t, h.recorder.errsFor(heldID), "held sync session operations failed")
			})
			t.Run("V2_no_foreign_rows", func(t *testing.T) {
				require.Empty(t, sessionRows(t, openSealed(t, runnerPath, h.tmpDir), heldID), "runner c1z holds held-sync session rows")
				require.Empty(t, sessionRows(t, openSealed(t, heldPath, h.tmpDir), runnerID), "held c1z holds runner-sync session rows")
			})
			t.Run("V3_held_cleanup_reaches_own_store", func(t *testing.T) {
				require.Empty(t, sessionRows(t, openSealed(t, heldPath, h.tmpDir), heldID), "held c1z still holds its own session rows after Cleanup")
			})
			t.Run("V4_closed_sync_rejected", func(t *testing.T) {
				requireClassifiedClosedSyncError(t, h.sessionRPC, runnerID)
			})
			if c.engine == c1zstore.EnginePebble && !c.cache {
				t.Run("V5_stats_attribution", func(t *testing.T) {
					requireSessionCounts(t, openSealed(t, heldPath, h.tmpDir), syncTypeFor(c.heldIsFull), heldID, 3, 3, "held")
					if !c.runnerFails {
						requireSessionCounts(t, openSealed(t, runnerPath, h.tmpDir), syncTypeFor(!c.heldIsFull), runnerID, 1, 1, "runner")
					}
				})
			}
		})
	}
}

// The skip sync learns its sync ID in skipLedgerSync, not startOrResumeSync,
// and never calls Grants, so it runs to Close without the script's gates.
func TestSessionBindingOverlapSkipSync(t *testing.T) {
	ctx := t.Context()
	script := newOverlapScript(false)
	h := newBindingHarness(t, script, bindingHarnessOpts{})
	heldPath := h.path("held")
	heldSyncer := h.newSyncer(t, heldPath, c1zstore.EnginePebble)
	skipSyncer := h.newSyncer(t, h.path("skip"), c1zstore.EnginePebble, WithSkipFullSync())

	heldDone := runSyncAndClose(ctx, heldSyncer)
	waitFor(t, script.heldAtPre, "held sync to write before the skip sync starts")

	skip := awaitResult(t, runSyncAndClose(ctx, skipSyncer), "skip sync")
	require.NoError(t, skip.syncErr)
	require.NoError(t, skip.closeErr)

	signal(script.releaseHeldMid)
	waitFor(t, script.heldAtMid, "held sync mid phase")
	signal(script.releaseHeldPost)
	held := awaitResult(t, heldDone, "held Sync and Close")
	require.NoError(t, held.syncErr, "premise: held sync must succeed")
	require.NoError(t, held.closeErr)
	heldID := script.held.syncID

	script.held.requireOwnReads(t, "held")
	require.Empty(t, h.recorder.errsFor(heldID), "held sync session operations failed")
	require.Empty(t, sessionRows(t, openSealed(t, heldPath, h.tmpDir), heldID), "held c1z still holds its own session rows after Cleanup")
}

var bindingEngines = []c1zstore.Engine{c1zstore.EnginePebble, c1zstore.EngineSQLite}

func TestSessionBindingSingleSync(t *testing.T) {
	for _, engine := range bindingEngines {
		t.Run(string(engine), func(t *testing.T) {
			script := &plainScript{}
			h := newBindingHarness(t, script, bindingHarnessOpts{})
			path := h.path("single")
			r := awaitResult(t, runSyncAndClose(t.Context(), h.newSyncer(t, path, engine)), "sync")
			require.NoError(t, r.syncErr)
			require.NoError(t, r.closeErr)

			require.Len(t, script.traces, 1)
			tr := script.traces[0]
			tr.requireOwnReads(t, "single")
			require.Len(t, tr.reads, 1)
			require.Empty(t, h.recorder.errsFor(tr.syncID))
			require.Empty(t, sessionRows(t, openSealed(t, path, h.tmpDir), tr.syncID))
		})
	}
}

func TestSessionBindingResume(t *testing.T) {
	for _, engine := range bindingEngines {
		t.Run(string(engine), func(t *testing.T) {
			script := &plainScript{failAfterWrite: true}
			h := newBindingHarness(t, script, bindingHarnessOpts{})
			path := h.path("resume")

			first := awaitResult(t, runSyncAndClose(t.Context(), h.newSyncer(t, path, engine)), "first attempt")
			require.Error(t, first.syncErr, "premise: first attempt must fail")
			require.NoError(t, first.closeErr)

			second := awaitResult(t, runSyncAndClose(t.Context(), h.newSyncer(t, path, engine)), "resumed attempt")
			require.NoError(t, second.syncErr)
			require.NoError(t, second.closeErr)

			require.Len(t, script.traces, 2)
			firstTrace, resumed := script.traces[0], script.traces[1]
			require.Equal(t, firstTrace.syncID, resumed.syncID, "premise: second attempt must resume the first sync")
			resumed.requireOwnReads(t, "resumed")
			require.Empty(t, h.recorder.errsFor(resumed.syncID))
		})
	}
}

type sequentialScript struct {
	mu            native_sync.Mutex
	traces        []*sessionTrace
	preWriteReads []string
}

func (q *sequentialScript) grants(ctx context.Context, opts resource.SyncOpAttrs) error {
	tr := &sessionTrace{syncID: opts.SyncID}
	before, found, err := opts.Session.Get(ctx, "k")
	tr.set(ctx, opts.Session, "k")
	tr.get(ctx, opts.Session, "read", "k")

	q.mu.Lock()
	defer q.mu.Unlock()
	q.traces = append(q.traces, tr)
	if found || err != nil {
		q.preWriteReads = append(q.preWriteReads, fmt.Sprintf("sync %s: found=%t value=%q err=%v", opts.SyncID, found, before, err))
	}
	return nil
}

func TestSessionBindingSequentialSyncs(t *testing.T) {
	for _, engine := range bindingEngines {
		t.Run(string(engine), func(t *testing.T) {
			script := &sequentialScript{}
			h := newBindingHarness(t, script, bindingHarnessOpts{connectorCache: true})

			var paths []string
			for i := range 3 {
				path := h.path(fmt.Sprintf("sync-%d", i))
				r := awaitResult(t, runSyncAndClose(t.Context(), h.newSyncer(t, path, engine)), path)
				require.NoError(t, r.syncErr)
				require.NoError(t, r.closeErr)
				paths = append(paths, path)
			}

			require.Len(t, script.traces, 3)
			seen := map[string]bool{}
			for _, tr := range script.traces {
				require.False(t, seen[tr.syncID], "premise: each sync gets its own sync ID")
				seen[tr.syncID] = true
			}
			require.Empty(t, script.preWriteReads, "a sync read a session value it had not written")

			for i, tr := range script.traces {
				tr.requireOwnReads(t, paths[i])
				require.Empty(t, h.recorder.errsFor(tr.syncID), "sync %d session operations failed", i)
				requireClassifiedClosedSyncError(t, h.sessionRPC, tr.syncID)

				sealed := openSealed(t, paths[i], h.tmpDir)
				for _, other := range script.traces {
					require.Empty(t, sessionRows(t, sealed, other.syncID), "%s holds session rows for sync %s", paths[i], other.syncID)
				}
			}
		})
	}
}

func TestSessionBindingSkipFullSync(t *testing.T) {
	for _, engine := range bindingEngines {
		t.Run(string(engine), func(t *testing.T) {
			h := newBindingHarness(t, &plainScript{}, bindingHarnessOpts{})
			r := awaitResult(t, runSyncAndClose(t.Context(), h.newSyncer(t, h.path("skip"), engine, WithSkipFullSync())), "skip sync")
			require.NoError(t, r.syncErr)
			require.NoError(t, r.closeErr)
			h.recorder.mu.Lock()
			defer h.recorder.mu.Unlock()
			require.Empty(t, h.recorder.errs)
		})
	}
}

func TestSessionBindingWithoutSessionSupport(t *testing.T) {
	for _, engine := range bindingEngines {
		t.Run(string(engine), func(t *testing.T) {
			script := &plainScript{}
			h := newBindingHarness(t, script, bindingHarnessOpts{noSessionSupport: true})
			r := awaitResult(t, runSyncAndClose(t.Context(), h.newSyncer(t, h.path("plain"), engine)), "sync")
			require.NoError(t, r.syncErr)
			require.NoError(t, r.closeErr)
			require.Len(t, script.traces, 1)
		})
	}
}

type gatedSessionStore struct {
	sessions.SessionStore
	entered chan struct{}
	release chan struct{}
}

func (g *gatedSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	if key == "gated" {
		signal(g.entered)
		<-g.release
	}
	return g.SessionStore.Get(ctx, key, opt...)
}

type gatingBinder struct {
	server  sessions.SessionStoreBinder
	entered chan struct{}
	release chan struct{}
}

func (g *gatingBinder) BindSessionStore(ctx context.Context, syncID string, store sessions.SessionStore) (sessions.UnbindSessionStore, error) {
	return g.server.BindSessionStore(ctx, syncID, &gatedSessionStore{SessionStore: store, entered: g.entered, release: g.release})
}

func TestSessionBindingCloseDrainsInFlight(t *testing.T) {
	for _, engine := range bindingEngines {
		t.Run(string(engine), func(t *testing.T) {
			ctx := t.Context()
			script := &plainScript{}
			h := newBindingHarness(t, script, bindingHarnessOpts{})
			server, ok := h.cc.(sessions.SessionStoreBinder)
			require.True(t, ok)
			gate := &gatingBinder{server: server, entered: make(chan struct{}), release: make(chan struct{})}
			path := h.path("drain")
			s := h.newSyncer(t, path, engine, WithSessionStoreBinder(gate))
			require.NoError(t, s.Sync(ctx))
			require.Len(t, script.traces, 1)
			syncID := script.traces[0].syncID

			inflight := make(chan error, 1)
			go func() {
				_, err := h.sessionRPC.Get(ctx, v1.GetRequest_builder{SyncId: syncID, Key: "gated"}.Build())
				inflight <- err
			}()
			waitFor(t, gate.entered, "session request to reach the store")

			closed := make(chan error, 1)
			go func() { closed <- s.Close(ctx) }()
			require.Eventually(t, func() bool {
				_, err := h.sessionRPC.Get(ctx, v1.GetRequest_builder{SyncId: syncID, Key: "k"}.Build())
				return status.Code(err) == codes.FailedPrecondition
			}, bindingWaitTimeout, 5*time.Millisecond, "Close did not release the binding")
			select {
			case err := <-closed:
				t.Fatalf("Close returned with a session request in flight: %v", err)
			case <-time.After(100 * time.Millisecond):
			}

			signal(gate.release)
			require.NoError(t, <-inflight, "in-flight session request ran against a closed store")
			require.NoError(t, <-closed)
		})
	}
}

type failingUnbindBinder struct {
	bound         bool
	drainDeadline time.Time
}

func (b *failingUnbindBinder) BindSessionStore(context.Context, string, sessions.SessionStore) (sessions.UnbindSessionStore, error) {
	b.bound = true
	return func(ctx context.Context) error {
		b.drainDeadline, _ = ctx.Deadline()
		return errors.New("drain timed out")
	}, nil
}

func TestSessionBindingCloseClosesStoreWhenDrainFails(t *testing.T) {
	for _, engine := range bindingEngines {
		t.Run(string(engine), func(t *testing.T) {
			ctx := t.Context()
			h := newBindingHarness(t, &plainScript{}, bindingHarnessOpts{noSessionSupport: true})
			binder := &failingUnbindBinder{}
			path := h.path("undrained")
			s := h.newSyncer(t, path, engine, WithSessionStoreBinder(binder))
			require.NoError(t, s.Sync(ctx))
			require.True(t, binder.bound, "premise: the sync must bind")

			require.NoError(t, s.Close(ctx))
			require.False(t, binder.drainDeadline.IsZero(), "drain ran without a deadline")
			require.False(t, binder.drainDeadline.After(time.Now().Add(sessionDrainTimeout)), "drain may outlast sessionDrainTimeout")
			openSealed(t, path, h.tmpDir)
		})
	}
}

var errBindRejected = errors.New("bind rejected")

type rejectingBinder struct{}

func (rejectingBinder) BindSessionStore(context.Context, string, sessions.SessionStore) (sessions.UnbindSessionStore, error) {
	return nil, errBindRejected
}

func TestSessionBindingBindFailureFailsSync(t *testing.T) {
	cases := []struct {
		name   string
		engine c1zstore.Engine
		opts   []SyncOpt
	}{
		{"pebble", c1zstore.EnginePebble, nil},
		{"sqlite", c1zstore.EngineSQLite, nil},
		{"pebble-skip-full-sync", c1zstore.EnginePebble, []SyncOpt{WithSkipFullSync()}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			script := &plainScript{}
			h := newBindingHarness(t, script, bindingHarnessOpts{noSessionSupport: true})
			opts := append([]SyncOpt{WithSessionStoreBinder(rejectingBinder{})}, c.opts...)
			r := awaitResult(t, runSyncAndClose(t.Context(), h.newSyncer(t, h.path("rejected"), c.engine, opts...)), "sync")
			require.ErrorIs(t, r.syncErr, errBindRejected)
			require.NoError(t, r.closeErr)
			require.Empty(t, script.traces, "the sync reached the connector without a session binding")
		})
	}
}
