package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// ledgeredStore is a store double that keeps the ledger capabilities setStore
// resolves from a real Pebble store. Doubles that embed only c1zstore.Store
// hide PageLedgerStore and WriteHookStore behind the interface; wrapping them
// in this type restores both, so the syncer attaches them as ledgered.
type ledgeredStore struct {
	c1zstore.Store
	c1zstore.PageLedgerStore
	c1zstore.WriteHookStore
	meta connectorstore.StoreMetadata
}

func (s *ledgeredStore) Metadata() connectorstore.StoreMetadata { return s.meta }

// withLedgerCaps wraps double with the ledger and write-hook capabilities of
// ledgerSource, which must be an open Pebble store. Every other call reaches double.
func withLedgerCaps(t testing.TB, double, ledgerSource c1zstore.Store) c1zstore.Store {
	t.Helper()
	caps := resolveStoreCaps(ledgerSource)
	require.NotNil(t, caps.pageLedger, "real store does not implement PageLedgerStore")
	require.NotNil(t, caps.writeHook, "real store does not implement WriteHookStore")
	return &ledgeredStore{
		Store:           double,
		PageLedgerStore: caps.pageLedger,
		WriteHookStore:  caps.writeHook,
		meta:            ledgerSource.Metadata(),
	}
}

// withTripwireLedgerCaps wraps double so it type-asserts as a ledgered Pebble
// store while every ledger or write-hook call panics through the nil embedded
// interfaces. For doubles whose assertion is that the path under test touches
// nothing else in the store.
func withTripwireLedgerCaps(double c1zstore.Store) c1zstore.Store {
	return &ledgeredStore{
		Store: double,
		meta:  connectorstore.StoreMetadata{Engine: string(c1zstore.EnginePebble)},
	}
}
