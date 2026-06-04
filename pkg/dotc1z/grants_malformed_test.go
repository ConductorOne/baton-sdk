package dotc1z

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TestPutGrants_DropsMalformedGrants verifies that grants missing a principal
// or entitlement are dropped at write time rather than persisted as rows with
// empty identity columns. The well-formed grant in the same batch must still
// be written.
func TestPutGrants_DropsMalformedGrants(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	good := v2.Grant_builder{Id: "grant-good", Entitlement: ent1, Principal: u1}.Build()
	nilPrincipal := v2.Grant_builder{Id: "grant-nil-principal", Entitlement: ent1, Principal: nil}.Build()
	nilEntitlement := v2.Grant_builder{Id: "grant-nil-entitlement", Entitlement: nil, Principal: u1}.Build()

	// Malformed grants are interleaved with the good one to exercise the
	// copy-from-firstBad path.
	require.NoError(t, c1f.PutGrants(ctx, nilPrincipal, good, nilEntitlement))

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1, "only the well-formed grant should be persisted")
	require.Equal(t, "grant-good", resp.GetList()[0].GetId())
}

// TestPutGrants_AllMalformedIsNoop verifies a batch consisting entirely of
// malformed grants writes nothing and does not error.
func TestPutGrants_AllMalformedIsNoop(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()

	require.NoError(t, c1f.PutGrants(ctx,
		v2.Grant_builder{Id: "x", Entitlement: nil, Principal: u1}.Build(),
		v2.Grant_builder{Id: "y", Entitlement: nil, Principal: nil}.Build(),
	))

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, resp.GetList())
}

// TestDropMalformedGrants_AllValidReturnsSameSlice ensures the fast path
// returns the input untouched (no reallocation) when every grant is valid.
func TestDropMalformedGrants_AllValidReturnsSameSlice(t *testing.T) {
	ctx := context.Background()
	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	msgs := []*v2.Grant{
		v2.Grant_builder{Id: "a", Entitlement: ent1, Principal: u1}.Build(),
		v2.Grant_builder{Id: "b", Entitlement: ent1, Principal: u1}.Build(),
	}
	got := dropMalformedGrants(ctx, msgs)
	require.Len(t, got, 2)
	require.Equal(t, "a", got[0].GetId())
	require.Equal(t, "b", got[1].GetId())
}
