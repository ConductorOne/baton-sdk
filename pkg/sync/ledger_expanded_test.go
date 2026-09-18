package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestLedgerExpandedPagePreservesStateAfterReopen(t *testing.T) {
	for _, commit := range []bool{false, true} {
		name := "discard"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			f := newLedgerFixture(t)
			annotation, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{"source"}}.Build())
			require.NoError(t, err)
			grant := v2.Grant_builder{
				Id:          "grant",
				Entitlement: v2.Entitlement_builder{Id: "ent", Resource: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()}.Build(),
				Principal:   v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build(),
				Annotations: []*anypb.Any{annotation},
			}.Build()
			require.NoError(t, f.store.PutGrants(t.Context(), grant))
			original, err := f.engine.GetGrantRecord(t.Context(), "grant")
			require.NoError(t, err)
			require.True(t, original.GetNeedsExpansion())
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			before := ledgerRawSnapshot(t, f.engine)
			grant.SetAnnotations(nil)
			ctx := c1zstore.WithOpenPage(t.Context())
			f.audit.enter(ledgerHandler)
			page := f.ledger.BeginPage()
			defer page.Discard()
			require.NoError(t, page.StoreExpandedGrants(ctx, grant))
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			id := c1zstore.LedgerActionIdentity{Op: "expanded"}
			if commit {
				require.NoError(t, page.Commit(ctx, id, nil))
			} else {
				page.Discard()
			}
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			got, err := f.engine.GetGrantRecord(t.Context(), "grant")
			require.NoError(t, err)
			require.True(t, got.GetNeedsExpansion())
			require.True(t, proto.Equal(original.GetExpansion(), got.GetExpansion()))
			require.True(t, proto.Equal(original.GetDiscoveredAt(), got.GetDiscoveredAt()))
			_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, commit, found)
			pending := 0
			for _, err := range f.store.Grants().PendingExpansion(t.Context()) {
				require.NoError(t, err)
				pending++
			}
			require.Equal(t, 1, pending)
			if !commit {
				require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			}
		})
	}
}
