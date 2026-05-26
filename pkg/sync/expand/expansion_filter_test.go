package expand

import (
	"context"
	"fmt"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

// TestExpansionFilter_Suppress demonstrates the SDK-level dedup primitive.
//
// Shape mirrors the pattern in baton-azure-infrastructure that motivated this work:
//   - A role entitlement "role:StorageBlobReader:assigned" collects direct grants
//     (one per principal who has that role on some subscription).
//   - Storage-account action entitlements expand from the role entitlement via
//     the existing SDK GrantExpandable machinery.
//   - In the ScopeBinding model, the role_assignment resource already carries
//     an authoritative grant for each (principal, role, scope) triple. The
//     derived grants produced by expansion are therefore redundant — every
//     piece of access they represent is also expressible through the
//     role_assignment grants.
//
// Without a filter the expander creates N derived grants (one per source grant).
// With a filter that returns false for sources on the role entitlement, the
// expander creates zero derived grants. The source grants themselves are
// unchanged — this is pure write-load reduction on a redundant projection.
//
// The SDK doesn't encode semantic knowledge about ScopeBinding here; it only
// exposes the primitive that lets a caller (connector or syncer) pass in the
// filter. Keeps the SDK generic and lets individual connectors opt in.
func TestExpansionFilter_Suppress(t *testing.T) {
	const numPrincipals = 100

	run := func(t *testing.T, filter ExpansionFilter) int {
		t.Helper()
		ctx := context.Background()
		store := NewMockExpanderStore()

		roleResource := makeResource("role", "StorageBlobReader")
		blobResource := makeResource("storage_account", "blob1")

		sourceEnt := makeEntitlement("ent:role:StorageBlobReader:assigned", roleResource)
		descEnt := makeEntitlement("ent:storage_account:blob1:action:read", blobResource)

		store.AddEntitlement(sourceEnt)
		store.AddEntitlement(descEnt)

		for i := 0; i < numPrincipals; i++ {
			user := makeResource("user", fmt.Sprintf("u-%03d", i))
			g := makeGrant(fmt.Sprintf("g:assigned:u-%03d", i), sourceEnt, user)
			store.AddGrant(g)
		}

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(sourceEnt.GetId())
		graph.AddEntitlementID(descEnt.GetId())
		require.NoError(t, graph.AddEdge(ctx, sourceEnt.GetId(), descEnt.GetId(), false, []string{"user"}))

		var expander *Expander
		if filter != nil {
			expander = NewExpander(store, graph, WithExpansionFilter(filter))
		} else {
			expander = NewExpander(store, graph)
		}
		require.NoError(t, expander.Run(ctx))

		return len(store.GetPutGrants())
	}

	t.Run("no_filter_emits_N_derived_grants", func(t *testing.T) {
		derived := run(t, nil)
		require.Equal(t, numPrincipals, derived,
			"baseline: expander should emit one derived grant per source principal")
	})

	t.Run("filter_on_role_source_suppresses_all", func(t *testing.T) {
		// Suppress expansion whenever the source entitlement lives on a
		// role resource. This is the pattern a ScopeBinding-using connector
		// would install to prevent its role→action expansion from
		// duplicating access already captured by role_assignment grants.
		filter := func(_ context.Context, _ *v2.Grant, src *v2.Entitlement, _ *v2.Entitlement) bool {
			return src.GetResource().GetId().GetResourceType() != "role"
		}
		derived := run(t, filter)
		require.Equal(t, 0, derived,
			"with the role-source filter, expander should emit zero derived grants")
	})

	t.Run("filter_can_be_per_principal", func(t *testing.T) {
		// Partial suppression: only suppress expansion for principals whose
		// ID is even. Demonstrates the filter gets per-grant context.
		filter := func(_ context.Context, sg *v2.Grant, _, _ *v2.Entitlement) bool {
			id := sg.GetPrincipal().GetId().GetResource()
			// odd-numbered principals still expand; even-numbered get suppressed
			return len(id) > 0 && id[len(id)-1]%2 == 1
		}
		derived := run(t, filter)
		// u-000 through u-099: odd last digit = 50 principals (u-001, u-003, ..., u-099)
		require.Equal(t, 50, derived)
	})
}
