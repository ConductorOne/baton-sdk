package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type ledgerAttachmentConnector struct {
	types.ConnectorClient
}

type ledgerMetadataStore struct {
	c1zstore.Store
	engine string
}

func (s ledgerMetadataStore) Metadata() connectorstore.StoreMetadata {
	metadata := s.Store.Metadata()
	metadata.Engine = s.engine
	return metadata
}

type ledgerMetadataCapabilityStore struct {
	ledgerMetadataStore
	c1zstore.PageLedgerStore
}

func TestLedgerPublicEngineAttachment(t *testing.T) {
	for _, engine := range []string{"pebble", "sqlite", "", "other"} {
		for _, capability := range []bool{false, true} {
			label := engine + "/without-ledger"
			if capability {
				label = engine + "/with-ledger"
			}
			t.Run(label, func(t *testing.T) {
				f := newLedgerFixture(t)
				metadataStore := ledgerMetadataStore{Store: f.store, engine: engine}
				var store c1zstore.Store = metadataStore
				if capability {
					store = ledgerMetadataCapabilityStore{ledgerMetadataStore: metadataStore, PageLedgerStore: f.ledger}
				}
				before := ledgerRawSnapshot(t, f.engine)
				f.audit.mu.Lock()
				writes := len(f.audit.events)
				f.audit.mu.Unlock()
				created, err := NewSyncer(t.Context(), ledgerAttachmentConnector{}, WithConnectorStore(store))
				require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
				f.audit.mu.Lock()
				afterWrites := len(f.audit.events)
				f.audit.mu.Unlock()
				require.Equal(t, writes, afterWrites)
				valid := engine == "pebble" && capability || engine == "sqlite" && !capability
				if !valid {
					require.Error(t, err)
					require.Nil(t, created)
					return
				}
				require.NoError(t, err)
				require.Equal(t, engine == "pebble", created.(*syncer).ledgered)
			})
		}
	}
}

func TestLedgerPublicSyncSealsWithoutToken(t *testing.T) {
	f := newLedgerFixture(t)
	c := newMockConnector()
	_, _, err := c.AddGroup(t.Context(), "team")
	require.NoError(t, err)
	created, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store))
	require.NoError(t, err)
	require.NoError(t, created.Sync(t.Context()))
	require.NoError(t, f.store.SetCurrentSync(t.Context(), created.(*syncer).syncID))
	token, err := f.store.CurrentSyncStep(t.Context())
	require.NoError(t, err)
	require.Empty(t, token)
	finished, err := f.ledger.BoundSyncFinished(t.Context())
	require.NoError(t, err)
	require.True(t, finished)
	_, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: InitOp.String()})
	require.NoError(t, err)
	require.False(t, found)
	report, err := f.ledger.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, report)
	options, err := f.ledger.GetArchivedLedgerOptions(t.Context(), "")
	require.NoError(t, err)
	require.NotNil(t, options)
	require.False(t, options.Requested.LedgerDebug)
}

func TestLedgerPublicStopResume(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "resume.c1z"), false)
	c := &ledgerTypesConnector{mockConnector: newMockConnector()}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true), WithLedgerDebug(true), WithRetainLedgerTokens(true), WithProgressHandler(func(*Progress) {
		if len(c.calls) == 1 {
			cancel()
		}
	}))
	require.NoError(t, err)
	require.ErrorIs(t, first.Sync(ctx), context.Canceled)
	require.Equal(t, []string{""}, c.calls)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	c = &ledgerTypesConnector{mockConnector: newMockConnector()}
	resumed, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true), WithLedgerDebug(true))
	require.NoError(t, err)
	walked := false
	var before []ledgerKV
	resumed.(*syncer).testHooks.ledgerWalk = func(entering bool) {
		if entering {
			walked = true
			before = ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerWalk)
		} else {
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			f.audit.enter(ledgerLifecycle)
		}
	}
	require.NoError(t, resumed.Sync(t.Context()))
	require.True(t, walked)
	require.Equal(t, []string{"connector-cursor"}, c.calls)
	require.NoError(t, f.store.SetCurrentSync(t.Context(), resumed.(*syncer).syncID))
	row, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: SyncResourceTypesOp.String()})
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, row.Scrubbed)
	require.Equal(t, "connector-cursor", row.NextPageToken)
	counts, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counts.ConnectorCalls["list-resource-types"].Count)
	options, err := f.ledger.GetArchivedLedgerOptions(t.Context(), "")
	require.NoError(t, err)
	require.False(t, options.Requested.RetainLedgerTokens)
	require.True(t, options.EffectiveRetainLedgerTokens)
	resources, err := f.store.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	require.Len(t, resources.GetList(), 3)
}

func TestLedgerPublicSkipSync(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "skip.c1z"), false)
	s, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithSkipFullSync())
	require.NoError(t, err)
	require.NoError(t, s.Sync(t.Context()))
	require.NoError(t, f.store.SetCurrentSync(t.Context(), s.(*syncer).syncID))
	finished, err := f.ledger.BoundSyncFinished(t.Context())
	require.NoError(t, err)
	require.True(t, finished)
	state, err := f.store.CurrentSyncStep(t.Context())
	require.NoError(t, err)
	require.Empty(t, state)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
	options, err := f.ledger.GetArchivedLedgerOptions(t.Context(), "")
	require.NoError(t, err)
	require.True(t, options.Requested.SkipFullSync)
}

func TestLedgerPublicDebugRetention(t *testing.T) {
	for _, retainTokens := range []bool{false, true} {
		t.Run(fmt.Sprint(retainTokens), func(t *testing.T) {
			f := newLedgerFixture(t)
			created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithLedgerDebug(true), WithRetainLedgerTokens(retainTokens))
			require.NoError(t, err)
			require.NoError(t, created.Sync(t.Context()))
			row, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: InitOp.String()})
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, !retainTokens, row.Scrubbed)
			options, err := f.ledger.GetArchivedLedgerOptions(t.Context(), "")
			require.NoError(t, err)
			require.True(t, options.Requested.LedgerDebug)
			require.Equal(t, retainTokens, options.Requested.RetainLedgerTokens)
			report, err := f.ledger.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.Contains(t, string(report), `"reference_validation_performed":true`)
		})
	}
}

func TestLedgerPublicTokenRetentionRequiresDebug(t *testing.T) {
	f := newLedgerFixture(t)
	before := ledgerRawSnapshot(t, f.engine)
	created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithRetainLedgerTokens(true))
	require.NoError(t, err)
	require.ErrorContains(t, created.Sync(t.Context()), "requires ledger debug")
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

type ledgerArchiveFailureStore struct {
	c1zstore.PageLedgerStore
}

func (s ledgerArchiveFailureStore) ArchiveLedgerReport(context.Context) ([]byte, error) {
	return nil, errors.New("archive write failed")
}

func TestLedgerPublicArchiveFailurePreservesRows(t *testing.T) {
	f := newLedgerFixture(t)
	created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store))
	require.NoError(t, err)
	created.(*syncer).caps.pageLedger = ledgerArchiveFailureStore{f.ledger}
	require.NoError(t, created.Sync(t.Context()))
	_, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: InitOp.String()})
	require.NoError(t, err)
	require.True(t, found)
	report, err := f.ledger.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.Empty(t, report)
}

func TestLedgerPublicFinishedContinuationAfterDisposal(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "continuation.c1z"), false)
	connector := &ledgerTypesConnector{mockConnector: newMockConnector()}
	first, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	require.NoError(t, first.Sync(t.Context()))
	syncID := first.(*syncer).syncID
	before, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
	require.NoError(t, err)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	connector = &ledgerTypesConnector{mockConnector: newMockConnector()}
	next, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store), WithSyncID(syncID), WithOnlyExpandGrants())
	require.NoError(t, err)
	next.(*syncer).testHooks.ledgerWalk = func(entering bool) {
		if entering {
			bound, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
			require.NoError(t, err)
			require.Equal(t, before.GetStartedAt(), bound.GetStartedAt())
			require.Equal(t, before.GetEndedAt(), bound.GetEndedAt())
		}
	}
	require.NoError(t, next.Sync(t.Context()))
	require.Empty(t, connector.calls)
	require.True(t, next.(*syncer).run.hasFact(factShouldSkipEntitlementsAndGrants))
	after, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
	require.NoError(t, err)
	require.Equal(t, before.GetStartedAt(), after.GetStartedAt())
	require.NotNil(t, after.GetEndedAt())
	report, err := f.ledger.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.Contains(t, string(report), `"preceding_collection"`)
}

func TestLedgerPublicLogsSavedStats(t *testing.T) {
	f := newLedgerFixture(t)
	core, observed := observer.New(zap.InfoLevel)
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))
	created, err := NewSyncer(ctx, newMockConnector(), WithConnectorStore(f.store))
	require.NoError(t, err)
	require.NoError(t, created.Sync(ctx))
	entries := observed.FilterMessage("sync ledger stats").All()
	require.Len(t, entries, 1)
	logged, ok := entries[0].ContextMap()["ledger_stats"].(json.RawMessage)
	require.True(t, ok)
	saved, err := f.ledger.GetArchivedLedgerReport(ctx)
	require.NoError(t, err)
	require.JSONEq(t, string(saved), string(logged))
}

func TestLedgerDebugLoggingPreservesRequestedConfig(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))
	s := &syncer{}
	require.NoError(t, s.configureLedgerReport(ctx))
	require.True(t, s.ledgerDebug)
	require.False(t, s.cfg.ledgerDebug)
}

func TestLedgerPublicPathAttachment(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EnginePebble, c1zstore.EngineSQLite} {
		t.Run(string(engine), func(t *testing.T) {
			created, err := NewSyncer(t.Context(), ledgerAttachmentConnector{}, WithC1ZPath(filepath.Join(t.TempDir(), "attach.c1z")), WithStorageEngine(engine))
			require.NoError(t, err)
			s := created.(*syncer)
			require.NoError(t, s.loadStore(t.Context()))
			require.Equal(t, engine == c1zstore.EnginePebble, s.ledgered)
			require.NoError(t, s.Close(t.Context()))
		})
	}
}

func TestLedgerPublicCancelledAfterWalkWritesNothing(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "cancel-walk.c1z"), false)
	connector := &ledgerTypesConnector{mockConnector: newMockConnector()}
	ctx, cancel := context.WithCancel(t.Context())
	first, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true), WithProgressHandler(func(*Progress) {
		if len(connector.calls) == 1 {
			cancel()
		}
	}))
	require.NoError(t, err)
	require.ErrorIs(t, first.Sync(ctx), context.Canceled)
	cancel()
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	connector = &ledgerTypesConnector{mockConnector: newMockConnector()}
	resumed, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	var before []ledgerKV
	var writes int
	resumed.(*syncer).testHooks.ledgerWalk = func(entering bool) {
		if !entering {
			before = ledgerRawSnapshot(t, f.engine)
			f.audit.mu.Lock()
			writes = len(f.audit.events)
			f.audit.mu.Unlock()
			f.audit.enter(ledgerWalk)
			cancel()
		}
	}
	err = resumed.Sync(ctx)
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, before)
	require.Empty(t, connector.calls)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	f.audit.mu.Lock()
	afterWrites := len(f.audit.events)
	f.audit.mu.Unlock()
	require.Equal(t, writes, afterWrites)
}
