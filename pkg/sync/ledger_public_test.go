package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

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
				created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(store))
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
	row, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: InitOp.String()})
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, row.Scrubbed)
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Positive(t, counters.Counters[ledgerCompletedActions])
}

func TestLedgerPublicStopResume(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "resume.c1z"), false)
	c := &ledgerTypesConnector{mockConnector: newMockConnector()}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true), WithRetainLedgerTokens(true), WithProgressHandler(func(*Progress) {
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
	resumed, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true))
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
	require.Contains(t, facts, ledgerFactSealReady)
}
