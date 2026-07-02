package pebble

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestGrantReadArenaReconcileAbsent verifies the reconcile pass
// correctly nulls a pre-pointed nested struct that the wire bytes
// didn't populate. Stores a GrantRecord with neither Entitlement
// nor Principal set (the on-wire layout that triggers the arena
// quirk), reads it back, and asserts the nested getters return nil.
func TestGrantReadArenaReconcileAbsent(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.MarkFreshSync(syncID))

	// Write a grant with no Entitlement, no Principal (only required
	// fields). This is the on-wire shape the reconcile pass exists
	// to recover.
	r := v3.GrantRecord_builder{
		ExternalId: "bare",
	}.Build()
	val, err := marshalRecord(r)
	require.NoError(t, err)
	require.NoError(t, e.db.Set(encodeGrantIdentityKey(grantIdentity{
		entitlement:     entitlementIdentityFromParts("app", "github", "ent-A"),
		principalTypeID: "user",
		principalID:     "alice",
	}), val, pebble.NoSync), "raw set")

	got, _, err := e.PaginateGrants(ctx, "", 0)
	require.NoError(t, err, "PaginateGrantsBySync")
	require.Len(t, got, 1)
	require.Nil(t, got[0].GetEntitlement(), "GetEntitlement()")
	require.Nil(t, got[0].GetPrincipal(), "GetPrincipal()")
	require.Equal(t, "bare", got[0].GetExternalId(), "GetExternalId()")
}

// TestGrantReadArenaPopulatedRoundtrip is the happy-path
// counterpart: stored records WITH Entitlement + Principal must
// come back populated, identical to the input.
func TestGrantReadArenaPopulatedRoundtrip(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.MarkFreshSync(syncID))

	const n = 64
	for i := 0; i < n; i++ {
		require.NoError(t, e.PutGrantRecord(ctx, makeGrant(syncID, "g-"+ksuid.New().String(), "ent-A", "alice-"+strconv.Itoa(i))), "PutGrantRecord")
	}

	got, _, err := e.PaginateGrants(ctx, "", n)
	require.NoError(t, err, "PaginateGrantsBySync")
	require.Len(t, got, n)
	for _, g := range got {
		e := g.GetEntitlement()
		require.NotNil(t, e, "GetEntitlement()")
		require.Equal(t, canonicalTestEntID("ent-A"), e.GetEntitlementId(), "GetEntitlement()")
		p := g.GetPrincipal()
		require.NotNil(t, p, "GetPrincipal()")
	}
}
