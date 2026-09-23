package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"reflect"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

func TestLedgerGuardMutationSurface(t *testing.T) {
	f := newLedgerFixture(t)
	reads := map[string]bool{"CurrentSyncStep": true}
	reader := reflect.TypeFor[connectorstore.Reader]()
	for i := range reader.NumMethod() {
		reads[reader.Method(i).Name] = true
	}
	delete(reads, "Close")
	surfaces := []struct {
		name     string
		contract reflect.Type
		target   any
		reads    map[string]bool
	}{
		{"writer", reflect.TypeFor[connectorstore.Writer](), f.store, reads},
		{"ledger", reflect.TypeFor[c1zstore.PageLedgerStore](), f.store, map[string]bool{
			"HasScheduledWork": true, "PendingWorkAfter": true, "PendingWork": true,
			"BeginPage": true, "GetLedgerRow": true, "GenerateLedgerReport": true, "GetArchivedLedgerReport": true, "GetArchivedLedgerOptions": true, "SetRetainLedgerTokens": true,
			"LedgerFacts": true, "LedgerCounters": true, "LedgerFrontier": true, "BoundSyncFinished": true, "BoundSyncUnstarted": true,
		}},
		{"sessions", reflect.TypeFor[sessions.SessionStore](), f.store.SessionStore(), map[string]bool{
			"Get": true, "GetMany": true, "GetAll": true,
		}},
		{"grants", reflect.TypeFor[c1zstore.GrantStore](), f.store.Grants(), map[string]bool{
			"PendingExpansion": true, "PendingExpansionPage": true, "ListWithAnnotations": true,
			"ListWithAnnotationsPage": true, "ListWithAnnotationsForResourcePage": true,
		}},
		{"meta", reflect.TypeFor[c1zstore.SyncMeta](), f.store.SyncMeta(), map[string]bool{
			"LatestFullSync": true, "LatestFinishedSyncOfAnyType": true, "Stats": true, "StatsV2": true,
		}},
		{"verification", reflect.TypeFor[c1zstore.IngestInvariantVerificationWriter](), f.store.SyncMeta(), nil},
		{"files", reflect.TypeFor[c1zstore.FileOps](), f.store.FileOps(), nil},
		{"resource-delete", reflect.TypeFor[resourceRecordDeleter](), f.store, nil},
		{"entitlement-delete", reflect.TypeFor[entitlementRecordDeleter](), f.store, nil},
		{"grant-delete", reflect.TypeFor[grantByRefsDeleter](), f.store, nil},
		{"grants-delete", reflect.TypeFor[grantsByRefsBatchDeleter](), f.store, nil},
		{"graph", reflect.TypeFor[EntitlementGraphStore](), f.store, map[string]bool{"GetEntitlementGraphBlob": true}},
		{"expanded-grants", reflect.TypeFor[newExpandedGrantStorer](), f.store.Grants(), nil},
		{"contributions", reflect.TypeFor[newExpandedGrantContributionStorer](), f.store.Grants(), nil},
		{"layer", reflect.TypeFor[expandedGrantLayerStorer](), f.store.Grants(), nil},
	}
	for _, surface := range surfaces {
		for i := range surface.contract.NumMethod() {
			method := surface.contract.Method(i)
			if surface.reads[method.Name] {
				continue
			}
			t.Run(surface.name+"/"+method.Name, func(t *testing.T) {
				before := ledgerRawSnapshot(t, f.engine)
				f.audit.enter(ledgerWalk)
				defer f.audit.enter(ledgerLifecycle)
				call := reflect.ValueOf(surface.target).MethodByName(method.Name)
				require.True(t, call.IsValid())
				args := make([]reflect.Value, call.Type().NumIn())
				for j := range args {
					args[j] = reflect.Zero(call.Type().In(j))
				}
				require.Equal(t, reflect.TypeFor[context.Context](), call.Type().In(0))
				args[0] = reflect.ValueOf(t.Context())
				var result []reflect.Value
				if call.Type().IsVariadic() {
					result = call.CallSlice(args)
				} else {
					result = call.Call(args)
				}
				require.NotEmpty(t, result)
				err, ok := result[len(result)-1].Interface().(error)
				require.True(t, ok, "expected error return from %s", method.Name)
				require.ErrorIs(t, err, errLedgerFixtureWrite)
				require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			})
		}
	}
}

func TestLedgerGuardPreservesPebbleCapabilities(t *testing.T) {
	f := newLedgerFixture(t)
	caps := reflect.ValueOf(resolveStoreCaps(f.store))
	for _, field := range reflect.VisibleFields(caps.Type()) {
		if field.Anonymous {
			continue
		}
		require.False(t, caps.FieldByIndex(field.Index).IsNil(), "guard lost %s", field.Name)
	}
}

func TestLedgerSessionWriteGuard(t *testing.T) {
	f := newLedgerFixture(t)
	session := f.store.SessionStore()
	opt := sessions.WithSyncID(f.engine.CurrentSyncID())
	require.NoError(t, session.Set(t.Context(), "seed", []byte("before"), opt))
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	err := session.Set(t.Context(), "seed", []byte("after"), opt)
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, errLedgerFixtureWrite)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}
