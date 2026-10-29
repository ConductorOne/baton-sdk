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

// connectorstore.StoreMetadata documents Engine "" for stores not backed by
// a c1z and tells consumers not to switch on unknown values. Both take the
// token path (CO-040); only a Pebble store without a ledger is refused.
func TestNewSyncerStoreEngineRouting(t *testing.T) {
	for _, tc := range []struct {
		engine   string
		attaches bool
	}{
		{"", true},
		{"future-engine", true},
		{string(c1zstore.EnginePebble), false},
	} {
		t.Run("engine="+tc.engine, func(t *testing.T) {
			s, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(attachOnlyStore{engine: tc.engine}))
			if !tc.attaches {
				require.ErrorContains(t, err, "requires PageLedgerStore")
				return
			}
			require.NoError(t, err)
			require.False(t, s.(*syncer).ledgered)
		})
	}
}

// A double wrapped by withLedgerCaps attaches as ledgered: the wrapper exposes
// the real store's PageLedgerStore and WriteHookStore, and its Metadata, while
// every other method still reaches the double.
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
	require.True(t, s.(*syncer).ledgered)
}

// A tripwire-wrapped double attaches as ledgered and panics on the first
// ledger call, which is the assertion its users rely on.
func TestWithTripwireLedgerCapsPanicsOnTouch(t *testing.T) {
	wrapped := withTripwireLedgerCaps(attachOnlyStore{engine: ""})

	s, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(wrapped))
	require.NoError(t, err)
	require.True(t, s.(*syncer).ledgered)

	require.Panics(t, func() { wrapped.(c1zstore.WriteHookStore).SetWriteHook(nil) })
	require.Panics(t, func() { _, _ = wrapped.(c1zstore.PageLedgerStore).LedgerFacts(t.Context()) })
}
