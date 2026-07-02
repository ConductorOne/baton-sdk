package sync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

// TestExpanderStoreAdapterRequiresEntitlementRefs pins the expansion →
// store boundary guard: grant listings during expansion must address the
// entitlement by structured resource refs. Bare-id string resolution is
// reserved for interactive edges (CLI, explorer); a refs-less request from
// the expander is a bug and must fail loudly here, before it can reach any
// engine-side string resolution. The nil store proves the guard rejects
// the request without forwarding it.
func TestExpanderStoreAdapterRequiresEntitlementRefs(t *testing.T) {
	ctx := context.Background()
	a := expanderStoreAdapter{store: nil}

	bare := v2.Entitlement_builder{Id: "group:eng:member"}.Build()

	_, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: bare,
	}.Build())
	require.ErrorContains(t, err, "no resource refs")

	_, _, err = a.ListGrantPrincipalKeysForEntitlement(ctx, bare, "", 0)
	require.ErrorContains(t, err, "no resource refs")

	_, err = a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{}.Build())
	require.ErrorContains(t, err, "missing entitlement")

	// A full stub (fetched entitlement record) passes the guard.
	withRefs := v2.Entitlement_builder{
		Id: "group:eng:member",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "eng"}.Build(),
		}.Build(),
	}.Build()
	require.NoError(t, requireEntitlementRefs(withRefs))
}
