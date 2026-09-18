package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerDeletePageSurvivesReopen(t *testing.T) {
	for _, commit := range []bool{false, true} {
		name := "discard"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			f := newLedgerFixture(t)
			resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "type", Resource: "target"}.Build()}.Build()
			other := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "type", Resource: "other"}.Build()}.Build()
			entitlement := v2.Entitlement_builder{Id: "shared", Resource: resource}.Build()
			survivor := v2.Entitlement_builder{Id: "shared", Resource: other}.Build()
			require.NoError(t, f.store.PutResources(t.Context(), resource))
			require.NoError(t, f.store.PutEntitlements(t.Context(), entitlement))
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			before := ledgerRawSnapshot(t, f.engine)
			ctx := c1zstore.WithOpenPage(t.Context())
			f.audit.enter(ledgerHandler)
			page := f.ledger.BeginPage()
			defer page.Discard()
			require.NoError(t, page.DeleteResources(ctx, resource, resource))
			require.NoError(t, page.DeleteEntitlements(ctx, entitlement, entitlement))
			require.NoError(t, page.PutResources(ctx, resource))
			require.NoError(t, page.PutEntitlements(ctx, entitlement, survivor))
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			id := c1zstore.LedgerActionIdentity{Op: "delete"}
			if commit {
				require.NoError(t, page.Commit(ctx, id, nil))
			} else {
				page.Discard()
			}
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, commit, found)
			_, err = f.engine.GetResourceRecord(t.Context(), "type", "target")
			if commit {
				require.ErrorIs(t, err, pebble.ErrNotFound)
				ent, err := f.engine.GetEntitlementRecord(t.Context(), "shared")
				require.NoError(t, err)
				require.Equal(t, "other", ent.GetResource().GetResourceId())
			} else {
				require.NoError(t, err)
				require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			}
		})
	}
}
