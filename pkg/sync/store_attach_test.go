package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// A store double of the shape connector repos hand to WithConnectorStore:
// a nil embedded Store with the attach-time methods answered.
type attachOnlyStore struct {
	c1zstore.Store
	engine string
}

func (s attachOnlyStore) Metadata() connectorstore.StoreMetadata {
	return connectorstore.StoreMetadata{Engine: s.engine}
}
func (attachOnlyStore) SyncMeta() c1zstore.SyncMeta { return nil }
func (attachOnlyStore) Grants() c1zstore.GrantStore { return nil }

func TestNewSyncerStoreEngineRouting(t *testing.T) {
	for _, engine := range []string{"", "future-engine", string(c1zstore.EnginePebble)} {
		t.Run("engine="+engine, func(t *testing.T) {
			_, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(attachOnlyStore{engine: engine}))
			require.ErrorContains(t, err, "requires Pebble and PageLedgerStore")
		})
	}
}

func TestNewSyncerRejectsStoreWithoutPageLedger(t *testing.T) {
	ctx := t.Context()
	_, err := NewSyncer(ctx, newMockConnector(), WithConnectorStore(attachOnlyStore{engine: ""}))
	require.ErrorContains(t, err, "requires Pebble and PageLedgerStore")

	pebbleStore, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "embed.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, pebbleStore.Close(ctx)) }()

	_, err = NewSyncer(ctx, newMockConnector(), WithConnectorStore(struct{ c1zstore.Store }{Store: pebbleStore}))
	require.ErrorContains(t, err, "requires Pebble and PageLedgerStore")
}

// withLedgerCaps exposes the real store's PageLedgerStore, WriteHookStore,
// and Metadata. Every other method still reaches the double.
func TestWithLedgerCapsAttachesLedgered(t *testing.T) {
	ctx := t.Context()
	pebbleStore, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "real.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, pebbleStore.Close(ctx)) }()

	double := attachOnlyStore{engine: ""}
	wrapped := withLedgerCaps(t, double, pebbleStore)

	caps := resolveStoreCaps(wrapped)
	realCaps := resolveStoreCaps(pebbleStore)
	require.Same(t, realCaps.pageLedger, caps.pageLedger.(*ledgeredStore).PageLedgerStore)
	require.Same(t, realCaps.writeHook, caps.writeHook.(*ledgeredStore).WriteHookStore)
	require.Nil(t, wrapped.SyncMeta(), "non-ledger calls reach the double")

	s, err := NewSyncer(ctx, newMockConnector(), WithConnectorStore(wrapped))
	require.NoError(t, err)
	require.Nil(t, s.(*syncer).storeAttachErr)
}

// A tripwire-wrapped double attaches and panics on the first ledger call,
// which is the assertion its users rely on.
func TestWithTripwireLedgerCapsPanicsOnTouch(t *testing.T) {
	wrapped := withTripwireLedgerCaps(attachOnlyStore{engine: ""})

	s, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(wrapped))
	require.NoError(t, err)
	require.Nil(t, s.(*syncer).storeAttachErr)

	require.Panics(t, func() { wrapped.(c1zstore.WriteHookStore).SetWriteHook(nil) })
	require.Panics(t, func() { _, _ = wrapped.(c1zstore.PageLedgerStore).LedgerFacts(t.Context()) })
}
