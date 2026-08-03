package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// chaosPebbleStore adapts the bare engine for a syncer test. Production c1z
// stores additionally own envelope persistence; this harness deliberately
// owns only the crashable engine image.
type chaosPebbleStore struct {
	*pebble.Engine
}

func (s *chaosPebbleStore) Close(context.Context) error {
	return s.Engine.Close()
}

func (s *chaosPebbleStore) SessionStore() sessions.SessionStore {
	return chaosPebbleSessionStore{engine: s.Engine}
}

type chaosPebbleSessionStore struct {
	engine *pebble.Engine
}

func (s chaosPebbleSessionStore) Get(ctx context.Context, key string, opts ...sessions.SessionStoreOption) ([]byte, bool, error) {
	return s.engine.SessionGet(ctx, key, opts...)
}

func (s chaosPebbleSessionStore) Set(ctx context.Context, key string, value []byte, opts ...sessions.SessionStoreOption) error {
	return s.engine.SessionSet(ctx, key, value, opts...)
}

func (s chaosPebbleSessionStore) GetMany(ctx context.Context, keys []string, opts ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	return s.engine.SessionGetMany(ctx, keys, opts...)
}

func (s chaosPebbleSessionStore) GetAll(ctx context.Context, pageToken string, opts ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	return s.engine.SessionGetAll(ctx, pageToken, opts...)
}

func (s chaosPebbleSessionStore) SetMany(ctx context.Context, values map[string][]byte, opts ...sessions.SessionStoreOption) error {
	return s.engine.SessionSetMany(ctx, values, opts...)
}

func (s chaosPebbleSessionStore) Delete(ctx context.Context, key string, opts ...sessions.SessionStoreOption) error {
	return s.engine.SessionDelete(ctx, key, opts...)
}

func (s chaosPebbleSessionStore) Clear(ctx context.Context, opts ...sessions.SessionStoreOption) error {
	return s.engine.SessionClear(ctx, opts...)
}

type armOnceWriteInjector struct {
	armed    atomic.Bool
	injected atomic.Int64
}

func (i *armOnceWriteInjector) String() string {
	return "fail first write after arm"
}

func (i *armOnceWriteInjector) MaybeError(op errorfs.Op) error {
	if !i.armed.Load() || op.Kind.ReadOrWrite() != errorfs.OpIsWrite {
		return nil
	}
	if op.Kind == errorfs.OpRemove || op.Kind == errorfs.OpRemoveAll {
		return nil
	}
	if i.injected.CompareAndSwap(0, 1) {
		return errorfs.ErrInjected
	}
	return nil
}

func TestArmOnceWriteInjectorProvesPremise(t *testing.T) {
	injector := &armOnceWriteInjector{}
	read := errorfs.Op{Kind: errorfs.OpFileRead}
	write := errorfs.Op{Kind: errorfs.OpFileWrite}

	require.NoError(t, injector.MaybeError(write), "disarmed injector must be inert")
	injector.armed.Store(true)
	require.NoError(t, injector.MaybeError(read), "reads are outside the fault domain")
	require.ErrorIs(t, injector.MaybeError(write), errorfs.ErrInjected)
	require.NoError(t, injector.MaybeError(write), "bounded injector must fire exactly once")
	require.EqualValues(t, 1, injector.injected.Load())
}

type chaosFatalGate struct {
	once sync.Once
	ch   chan struct{}
	msg  atomic.Pointer[string]
}

func newChaosFatalGate() *chaosFatalGate {
	return &chaosFatalGate{ch: make(chan struct{})}
}

func (*chaosFatalGate) Infof(string, ...interface{}) {}

func (*chaosFatalGate) Errorf(string, ...interface{}) {}

func (g *chaosFatalGate) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	g.msg.Store(&message)
	g.once.Do(func() { close(g.ch) })
	select {}
}

func (g *chaosFatalGate) err() error {
	if message := g.msg.Load(); message != nil {
		return fmt.Errorf("pebble fatal: %s", *message)
	}
	return fmt.Errorf("pebble fatal")
}

func TestChaosConnectorLostResponseThenFilesystemFailureResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	expected := chaosoracle.ExpectedIdentities(manifest)

	const (
		ruleID  = "block-and-lose-first-entitlement-response"
		barrier = "arm-filesystem-failure"
	)
	schedule := chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: ruleID,
		Match: chaosconnector.Matcher{
			Domain:       chaosconnector.DomainConnector,
			Method:       chaosconnector.ExactString("ListEntitlements"),
			ResourceType: chaosconnector.ExactString(chaosconnector.FullCapabilityResourceTypeID),
			Attempt:      1,
			Phase:        chaosconnector.PhaseAfterDelegate,
		},
		Effects: []chaosconnector.Effect{
			{Kind: chaosconnector.EffectBlock, Barrier: barrier},
			{Kind: chaosconnector.EffectLoseResponse, Code: codes.Unavailable},
		},
		MinFires: 1,
		MaxFires: 1,
	})
	run, err := chaosconnector.NewRun(scenario, schedule)
	require.NoError(t, err)
	client := newCombinedFaultClient(t, ctx, run)

	base := vfs.NewCrashableMem()
	injector := &armOnceWriteInjector{}
	gate := newChaosFatalGate()
	engine, err := pebble.Open(
		ctx,
		"combined-fault-db",
		pebble.WithVFS(errorfs.Wrap(base, injector)),
		pebble.WithLogger(gate),
	)
	require.NoError(t, err)
	require.NoError(t, engine.InitCurrentSync(ctx))

	first, err := NewSyncer(
		ctx,
		client,
		WithConnectorStore(&chaosPebbleStore{Engine: engine}),
		WithDontExpandGrants(),
	)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- first.Sync(ctx) }()

	require.Eventually(t, func() bool {
		return run.Runtime().FireCounts()[ruleID] == 1
	}, 5*time.Second, 10*time.Millisecond, "connector fault did not reach its deterministic barrier")
	injector.armed.Store(true)
	run.Runtime().ReleaseBarrier(barrier)

	fatal := false
	select {
	case syncErr := <-done:
		require.Error(t, syncErr, "filesystem failure must abort the first process")
	case <-gate.ch:
		fatal = true
		t.Logf("filesystem failure surfaced as process-fatal: %v", gate.err())
	case <-ctx.Done():
		t.Fatal("combined fault run did not terminate or reach Pebble fatal")
	}
	require.NoError(t, run.Runtime().VerifyRequired())
	require.EqualValues(t, 1, injector.injected.Load(), "filesystem fault did not fire")

	// Cut before Close: only data Pebble made durable before the simulated
	// process death may appear in the recovery image.
	image := base.CrashClone(vfs.CrashCloneCfg{})
	injector.armed.Store(false)
	if !fatal {
		require.NoError(t, engine.Close())
	}

	recovered, err := pebble.Open(ctx, "combined-fault-db", pebble.WithVFS(image))
	require.NoError(t, err, "combined-fault crash image must reopen")
	require.NoError(t, recovered.InitCurrentSync(ctx))
	recoveredStore := &chaosPebbleStore{Engine: recovered}
	unfinished, err := recovered.LatestUnfinishedSyncRecord(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, unfinished, "fault after collection began must leave a resumable sync")

	resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeClient := newCombinedFaultClient(t, ctx, resumeRun)
	resumed, err := NewSyncer(
		ctx,
		resumeClient,
		WithConnectorStore(recoveredStore),
		WithDontExpandGrants(),
	)
	require.NoError(t, err)
	require.NoError(t, resumed.Sync(ctx))

	finished, err := recovered.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, finished, "recovered run must seal")
	require.Equal(t, unfinished.GetSyncId(), finished.ID, "recovery must resume the interrupted sync")
	actual, err := chaosoracle.ReadIdentities(ctx, recovered)
	require.NoError(t, err)
	require.NoError(t, chaosoracle.CompareIdentities(expected, actual))
	require.NoError(t, resumed.Close(ctx))
}

func newCombinedFaultClient(
	t *testing.T,
	ctx context.Context,
	run *chaosconnector.Run,
) types.ConnectorClient {
	t.Helper()
	builder, err := chaosconnector.NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)
	return chaosconnector.NewDirectClient(ctx, server, run)
}
