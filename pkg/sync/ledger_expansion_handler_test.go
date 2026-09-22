package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

func ledgerExpansionFixture(t *testing.T) (*syncer, *ledgerFixture) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.graph = newExpansionGraph()
	s.counts = progresslog.NewProgressCounts(t.Context())
	s.syncID = f.engine.CurrentSyncID()
	require.NoError(t, f.store.PutResourceTypes(t.Context(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(), v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	require.NoError(t, f.store.PutResources(t.Context(), v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()}.Build()))
	resources := make([]*v2.Resource, 3)
	for i, name := range []string{"a", "b", "c"} {
		resources[i] = v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: name}.Build()}.Build()
	}
	require.NoError(t, f.store.PutResources(t.Context(), resources...))
	for _, resource := range resources {
		require.NoError(t, f.store.PutEntitlements(t.Context(), et.NewAssignmentEntitlement(resource, "member")))
	}
	grants := []*v2.Grant{gt.NewGrant(resources[0], "member", v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build())}
	for i := 1; i < len(resources); i++ {
		annotation := v2.GrantExpandable_builder{EntitlementIds: []string{et.NewEntitlementID(resources[i-1], "member")}}.Build()
		grants = append(grants, gt.NewGrant(resources[i], "member", resources[i-1].GetId(), gt.WithAnnotation(annotation)))
	}
	require.NoError(t, f.store.PutGrants(t.Context(), grants...))
	f.audit.enter(ledgerHandler)
	_, err := s.ledger.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "expansion-fixture"}, func(_ context.Context, page *ledgerPage) error {
		if err := page.setFact(factNeedsExpansion); err != nil {
			return err
		}
		return page.transition("")
	})
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	s.run.setFact(factNeedsExpansion)
	s.run.pushAction(t.Context(), Action{Op: SyncGrantExpansionOp})
	return s, f
}
