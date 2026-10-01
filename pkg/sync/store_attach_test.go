package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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
		{string(c1zstore.EngineSQLite), true},
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
