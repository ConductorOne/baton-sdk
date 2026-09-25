package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	native_sync "sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
)

var errLedgerFixtureWrite = errors.New("ledger fixture: forbidden direct write")

type ledgerWritePhase uint8

const (
	ledgerLifecycle ledgerWritePhase = iota
	ledgerWalk
	ledgerHandler
)

type ledgerWriteEvent struct {
	method string
	phase  ledgerWritePhase
	open   bool
	reason string
}

type ledgerWriteAudit struct {
	writers int
	mu      native_sync.Mutex
	phase   ledgerWritePhase
	events  []ledgerWriteEvent
}

func (a *ledgerWriteAudit) enter(phase ledgerWritePhase) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.phase = phase
}

func (a *ledgerWriteAudit) record(ctx context.Context, method string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	reason, _ := c1zstore.PageWriteBypass(ctx)
	a.events = append(a.events, ledgerWriteEvent{method: method, phase: a.phase, open: c1zstore.PageOpen(ctx), reason: reason})
	if a.phase == ledgerWalk || (a.phase == ledgerHandler && (!c1zstore.PageOpen(ctx) || reason == "")) {
		return fmt.Errorf("%w: %s", errLedgerFixtureWrite, method)
	}
	return nil
}

func (a *ledgerWriteAudit) hook(ctx context.Context, event c1zstore.WriteHookEvent) error {
	if event.Bypass == "" {
		return fmt.Errorf("%w: hook %s", errLedgerFixtureWrite, event.Method)
	}
	return a.record(ctx, event.Method)
}

type ledgerGuardedStore struct {
	EntitlementGraphStore
	c1zstore.GrantGenerationDigestReader
	dotc1z.IngestInvariantStore
	connectorstore.DBSizeProvider
	grantPrincipalKeyLister
	principalSortedGrantLister
	c1zstore.Store
	c1zstore.PageLedgerStore
	c1zstore.WriteHookStore
	caps  storeCaps
	audit *ledgerWriteAudit
}

func (s *ledgerGuardedStore) PutResourceTypes(ctx context.Context, records ...*v2.ResourceType) error {
	if err := s.audit.record(ctx, "PutResourceTypes"); err != nil {
		return err
	}
	return s.Store.PutResourceTypes(ctx, records...)
}

func (s *ledgerGuardedStore) PutResources(ctx context.Context, records ...*v2.Resource) error {
	if err := s.audit.record(ctx, "PutResources"); err != nil {
		return err
	}
	return s.Store.PutResources(ctx, records...)
}

func (s *ledgerGuardedStore) PutEntitlements(ctx context.Context, records ...*v2.Entitlement) error {
	if err := s.audit.record(ctx, "PutEntitlements"); err != nil {
		return err
	}
	return s.Store.PutEntitlements(ctx, records...)
}

func (s *ledgerGuardedStore) PutGrants(ctx context.Context, records ...*v2.Grant) error {
	if err := s.audit.record(ctx, "PutGrants"); err != nil {
		return err
	}
	return s.Store.PutGrants(ctx, records...)
}

func (s *ledgerGuardedStore) DeleteGrant(ctx context.Context, id string) error {
	if err := s.audit.record(ctx, "DeleteGrant"); err != nil {
		return err
	}
	return s.Store.DeleteGrant(ctx, id)
}

func (s *ledgerGuardedStore) PutAsset(ctx context.Context, ref *v2.AssetRef, contentType string, data []byte) error {
	if err := s.audit.record(ctx, "PutAsset"); err != nil {
		return err
	}
	return s.Store.PutAsset(ctx, ref, contentType, data)
}

func (s *ledgerGuardedStore) CheckpointSync(ctx context.Context, token string) error {
	if err := s.audit.record(ctx, "CheckpointSync"); err != nil {
		return err
	}
	return s.Store.CheckpointSync(ctx, token)
}

func (s *ledgerGuardedStore) EndSync(ctx context.Context) error {
	if err := s.audit.record(ctx, "EndSync"); err != nil {
		return err
	}
	return s.Store.EndSync(ctx)
}

type ledgerFixture struct {
	path   string
	store  *ledgerGuardedStore
	ledger c1zstore.PageLedgerStore
	engine *engine.Engine
	audit  *ledgerWriteAudit
}

func newLedgerFixture(t *testing.T) *ledgerFixture {
	t.Helper()
	return newLedgerFixtureAt(t, filepath.Join(t.TempDir(), "ledger.c1z"))
}

func newLedgerFixtureAt(t *testing.T, path string) *ledgerFixture {
	t.Helper()
	return openLedgerFixtureAt(t, path, true)
}

func openLedgerFixtureAt(t *testing.T, path string, start bool) *ledgerFixture {
	t.Helper()
	ctx := t.Context()
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(filepath.Dir(path)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	caps := resolveStoreCaps(store)
	require.NotNil(t, caps.pageLedger)
	require.NotNil(t, caps.writeHook)
	raw, ok := engine.AsEngine(store)
	require.True(t, ok)
	audit := &ledgerWriteAudit{}
	caps.writeHook.SetWriteHook(audit.hook)
	if start {
		_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
	}
	guarded := &ledgerGuardedStore{
		Store: store, PageLedgerStore: caps.pageLedger, WriteHookStore: caps.writeHook,
		caps: caps, audit: audit, EntitlementGraphStore: caps.entitlementGraph,
		GrantGenerationDigestReader: caps.grantDigest, IngestInvariantStore: caps.ingestFacts,
		DBSizeProvider: caps.dbSize, grantPrincipalKeyLister: caps.grantPrincipalKeys,
		principalSortedGrantLister: caps.principalSortedGrants,
	}
	f := &ledgerFixture{path: path, store: guarded, ledger: guarded, engine: raw, audit: audit}
	t.Cleanup(func() {
		audit.mu.Lock()
		defer audit.mu.Unlock()
		require.Zero(t, audit.writers, "page writer not committed or discarded")
	})
	return f
}

type ledgerKV struct {
	key   []byte
	value []byte
}

func ledgerRawSnapshot(t *testing.T, e *engine.Engine) []ledgerKV {
	t.Helper()
	iter, err := e.NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	var rows []ledgerKV
	for iter.First(); iter.Valid(); iter.Next() {
		rows = append(rows, ledgerKV{key: bytes.Clone(iter.Key()), value: bytes.Clone(iter.Value())})
	}
	require.NoError(t, iter.Error())
	return rows
}

func equalLedgerSnapshot(a, b []ledgerKV) bool {
	return slices.EqualFunc(a, b, func(x, y ledgerKV) bool {
		return bytes.Equal(x.key, y.key) && bytes.Equal(x.value, y.value)
	})
}

func TestLedgerWriteInstrument(t *testing.T) {
	for _, test := range []struct {
		name         string
		phase        ledgerWritePhase
		open, bypass bool
	}{
		{name: "missing page context", phase: ledgerHandler},
		{name: "unregistered page write", phase: ledgerHandler, open: true},
		{name: "walk with bypass", phase: ledgerWalk, open: true, bypass: true},
		{name: "walk without page context", phase: ledgerWalk},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newLedgerFixture(t)
			seed := v2.ResourceType_builder{Id: "seed", DisplayName: "before"}.Build()
			require.NoError(t, f.store.PutResourceTypes(t.Context(), seed))
			before := ledgerRawSnapshot(t, f.engine)
			require.NotEmpty(t, before)
			ctx := t.Context()
			if test.open {
				ctx = c1zstore.WithOpenPage(ctx)
			}
			if test.bypass {
				ctx = c1zstore.WithPageWriteBypass(ctx, "instrument mutation: attempt to override walk prohibition")
			}
			f.audit.enter(test.phase)
			err := f.store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "seed", DisplayName: "after"}.Build())
			require.ErrorIs(t, err, errLedgerFixtureWrite)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			require.Equal(t, "PutResourceTypes", f.audit.events[len(f.audit.events)-1].method)
			f.audit.enter(ledgerLifecycle)
		})
	}
}

func TestLedgerRawSnapshotDetectsValueMutation(t *testing.T) {
	f := newLedgerFixture(t)
	require.NoError(t, f.store.PutResourceTypes(t.Context(), v2.ResourceType_builder{Id: "seed", DisplayName: "before"}.Build()))
	before := ledgerRawSnapshot(t, f.engine)
	require.NoError(t, f.store.PutResourceTypes(t.Context(), v2.ResourceType_builder{Id: "seed", DisplayName: "after"}.Build()))
	after := ledgerRawSnapshot(t, f.engine)
	require.Len(t, after, len(before))
	require.False(t, equalLedgerSnapshot(before, after), "same-count value mutation must be detected")
	require.True(t, equalLedgerSnapshot(after, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerWriteHookInstrument(t *testing.T) {
	f := newLedgerFixture(t)
	before := ledgerRawSnapshot(t, f.engine)
	err := f.store.Store.PutResourceTypes(c1zstore.WithOpenPage(t.Context()), v2.ResourceType_builder{Id: "bypass-wrapper"}.Build())
	require.ErrorIs(t, err, errLedgerFixtureWrite)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerSnapshotAfterReopen(t *testing.T) {
	f := newLedgerFixture(t)
	page := f.ledger.BeginPage()
	defer page.Discard()
	id := c1zstore.LedgerActionIdentity{Op: "list-resource-types", PageToken: "first"}
	require.NoError(t, page.PutResourceTypes(t.Context(), v2.ResourceType_builder{Id: "saved", DisplayName: "durable"}.Build()))
	require.NoError(t, page.SetFact("fixture-fact"))
	require.NoError(t, page.SetCounterBucket("fixture-attempt", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"pages": 1}}))
	require.NoError(t, page.Commit(t.Context(), id, &c1zstore.LedgerRow{Identity: id}))
	before := ledgerRawSnapshot(t, f.engine)
	require.NoError(t, f.store.Close(t.Context()))
	reopened, err := dotc1z.NewStore(t.Context(), f.path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close(context.Background())) })
	raw, ok := engine.AsEngine(reopened)
	require.True(t, ok)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, raw)))
}

func (r *ledgerRuntime) prepareSeal(ctx context.Context, runCounters c1zstore.LedgerCounters, facts ...string) error {
	return r.prepareSealWithOptions(ctx, runCounters, nil, facts...)
}

func newTestLedgerRuntime(ctx context.Context, store c1zstore.PageLedgerStore, runID string) (*ledgerRuntime, error) {
	facts, err := store.LedgerFacts(ctx)
	if err != nil {
		return nil, err
	}
	return newLedgerRuntime(store, runID, facts)
}

func loadTestLedgerResume(ctx context.Context, store c1zstore.Store, ledger c1zstore.PageLedgerStore, runID string) (ledgerResume, error) {
	facts, err := ledger.LedgerFacts(ctx)
	if err != nil {
		return ledgerResume{}, err
	}
	return loadLedgerResume(ctx, store, ledger, runID, facts)
}

func (s *ledgerGuardedStore) FoldLedgerCounters(ctx context.Context, runID string) error {
	if err := s.audit.record(ctx, "FoldLedgerCounters"); err != nil {
		return err
	}
	return s.PageLedgerStore.FoldLedgerCounters(ctx, runID)
}

func observeLedgerRestore(t *testing.T, s *syncer, f *ledgerFixture) {
	t.Helper()
	var before []ledgerKV
	f.audit.enter(ledgerLifecycle)
	s.testHooks.ledgerWalk = func(entering bool) {
		if entering {
			before = ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerWalk)
		} else {
			f.audit.enter(ledgerLifecycle)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		}
	}
}
