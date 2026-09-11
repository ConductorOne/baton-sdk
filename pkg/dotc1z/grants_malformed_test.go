package dotc1z

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

type grantFixtures struct {
	good           *v2.Grant
	nilPrincipal   *v2.Grant
	nilEntitlement *v2.Grant
	emptyPrincipal *v2.Grant // non-nil principal, empty ResourceId
	emptyGrantID   *v2.Grant
}

func grantTestFixtures() grantFixtures {
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	return grantFixtures{
		good:           v2.Grant_builder{Id: "grant-good", Entitlement: ent1, Principal: u1}.Build(),
		nilPrincipal:   v2.Grant_builder{Id: "grant-nil-principal", Entitlement: ent1, Principal: nil}.Build(),
		nilEntitlement: v2.Grant_builder{Id: "grant-nil-entitlement", Entitlement: nil, Principal: u1}.Build(),
		// Non-nil principal whose ResourceId is empty — passes a nil check but
		// would still be written with empty principal identity columns.
		emptyPrincipal: v2.Grant_builder{
			Id:          "grant-empty-principal-id",
			Entitlement: ent1,
			Principal:   v2.Resource_builder{Id: v2.ResourceId_builder{}.Build()}.Build(),
		}.Build(),
		// Empty grant id collides on the (external_id, sync_id) unique index.
		emptyGrantID: v2.Grant_builder{Id: "", Entitlement: ent1, Principal: u1}.Build(),
	}
}

// TestPutGrants_DropsMalformedGrants verifies that grants missing required
// identity (nil OR empty principal/entitlement/id) are dropped at write time
// rather than persisted as rows with empty identity columns. The well-formed
// grant in the same batch must still be written. Exercised on both the
// full-blob and slim writers, since both share upsertGrantsInternal.
func TestPutGrants_DropsMalformedGrants(t *testing.T) {
	for _, slim := range []bool{false, true} {
		slim := slim
		name := "fullblob"
		if slim {
			name = "slim"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var opts []C1ZOption
			if slim {
				opts = append(opts, WithV2GrantsWriter(true))
			}
			c1f, _, cleanup := newTestC1z(ctx, t, opts...)
			defer cleanup()

			f := grantTestFixtures()

			// Malformed grants are interleaved with the good one to exercise the
			// copy-from-firstBad path.
			require.NoError(t, c1f.PutGrants(ctx, f.nilPrincipal, f.good, f.nilEntitlement, f.emptyPrincipal, f.emptyGrantID))

			resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
			require.NoError(t, err)
			require.Len(t, resp.GetList(), 1, "only the well-formed grant should be persisted")
			require.Equal(t, "grant-good", resp.GetList()[0].GetId())
		})
	}
}

// TestStoreExpandedGrants_DropsMalformedGrants verifies the drop also protects
// the expansion write path (PreserveExpansion mode), not just PutGrants. A
// good grant is seeded first; a later StoreExpandedGrants batch containing a
// malformed grant must not add it.
func TestStoreExpandedGrants_DropsMalformedGrants(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	f := grantTestFixtures()
	require.NoError(t, c1f.PutGrants(ctx, f.good))

	require.NoError(t, c1f.StoreExpandedGrants(ctx, f.nilPrincipal))

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "malformed expanded grant must not be persisted")
	require.Equal(t, "grant-good", resp.GetList()[0].GetId())
}

// TestPutGrants_AllMalformedIsNoop verifies a batch consisting entirely of
// malformed grants writes nothing and does not error.
func TestPutGrants_AllMalformedIsNoop(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	f := grantTestFixtures()

	require.NoError(t, c1f.PutGrants(ctx, f.nilPrincipal, f.nilEntitlement, f.emptyPrincipal))

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, resp.GetList())
}

// TestDropMalformedGrants_AllValidReturnsSameSlice ensures the fast path
// returns the input slice itself (no reallocation) when every grant is valid.
func TestDropMalformedGrants_AllValidReturnsSameSlice(t *testing.T) {
	ctx := context.Background()
	f := grantTestFixtures()

	msgs := []*v2.Grant{f.good, f.good}
	got := dropMalformedGrants(ctx, msgs)
	require.Len(t, got, 2)
	// Identity, not just equality: the fast path must not reallocate.
	require.Same(t, &msgs[0], &got[0], "all-valid fast path must return the input slice unmodified")
}

// TestGrantHasValidIdentity covers the predicate's boundaries directly.
func TestGrantHasValidIdentity(t *testing.T) {
	f := grantTestFixtures()

	require.True(t, grantHasValidIdentity(f.good))
	require.False(t, grantHasValidIdentity(f.nilPrincipal), "nil principal")
	require.False(t, grantHasValidIdentity(f.nilEntitlement), "nil entitlement")
	require.False(t, grantHasValidIdentity(f.emptyPrincipal), "empty principal resource id")
	require.False(t, grantHasValidIdentity(f.emptyGrantID), "empty grant id")
	require.False(t, grantHasValidIdentity(nil), "nil grant")
}
