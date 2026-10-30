package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// ledgeredStore restores PageLedgerStore and WriteHookStore hidden by a
// double that embeds only c1zstore.Store, and reports Metadata from the
// source store. setStore requires both.
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
	return promoteLedgeredDeletes(&ledgeredStore{
		Store:           double,
		PageLedgerStore: caps.pageLedger,
		WriteHookStore:  caps.writeHook,
		meta:            ledgerSource.Metadata(),
	}, double)
}

// withTripwireLedgerCaps wraps double so it type-asserts as a Pebble store
// with a page ledger, while every ledger or write-hook call panics through
// the nil embedded interfaces. For doubles whose assertion is that the path
// under test touches nothing else in the store.
func withTripwireLedgerCaps(double c1zstore.Store) c1zstore.Store {
	return promoteLedgeredDeletes(&ledgeredStore{
		Store: double,
		meta:  connectorstore.StoreMetadata{Engine: string(c1zstore.EnginePebble)},
	}, double)
}

func promoteLedgeredDeletes(base *ledgeredStore, double c1zstore.Store) c1zstore.Store {
	_, grantRefs := double.(grantByRefsDeleter)
	_, grantBatch := double.(grantsByRefsBatchDeleter)
	_, resources := double.(resourceRecordDeleter)
	_, entitlements := double.(entitlementRecordDeleter)
	switch {
	case resources && entitlements && grantRefs && grantBatch:
		return &ledgeredDeleteAll{ledgeredStore: base}
	case grantRefs && grantBatch && !resources && !entitlements:
		return &ledgeredGrantDeletes{ledgeredStore: base}
	case grantRefs && !grantBatch && !resources && !entitlements:
		return &ledgeredGrantRefs{ledgeredStore: base}
	case !resources && !entitlements && !grantRefs && !grantBatch:
		return base
	default:
		panic(fmt.Sprintf("ledger wrapper has no delete combination: refs=%t batch=%t resources=%t entitlements=%t",
			grantRefs, grantBatch, resources, entitlements))
	}
}

type ledgeredGrantRefs struct{ *ledgeredStore }

func (s *ledgeredGrantRefs) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.Store.(grantByRefsDeleter).DeleteGrantByRefs(ctx, grant)
}

type ledgeredGrantDeletes struct{ *ledgeredStore }

func (s *ledgeredGrantDeletes) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.Store.(grantByRefsDeleter).DeleteGrantByRefs(ctx, grant)
}

func (s *ledgeredGrantDeletes) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	return s.Store.(grantsByRefsBatchDeleter).DeleteGrantsByRefs(ctx, grants...)
}

type ledgeredDeleteAll struct{ *ledgeredStore }

func (s *ledgeredDeleteAll) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return s.Store.(resourceRecordDeleter).DeleteResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *ledgeredDeleteAll) DeleteEntitlementByRefs(ctx context.Context, entitlement *v2.Entitlement) error {
	return s.Store.(entitlementRecordDeleter).DeleteEntitlementByRefs(ctx, entitlement)
}

func (s *ledgeredDeleteAll) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.Store.(grantByRefsDeleter).DeleteGrantByRefs(ctx, grant)
}

func (s *ledgeredDeleteAll) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	return s.Store.(grantsByRefsBatchDeleter).DeleteGrantsByRefs(ctx, grants...)
}
