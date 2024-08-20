package test

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/stretchr/testify/require"
)

type builder interface {
	connectorbuilder.ResourceProvisioner
	connectorbuilder.ResourceSyncer
}

func assertGrants(ctx context.Context,
	t *testing.T,
	c builder,
	resource *v2.Resource,
) []*v2.Grant {
	grants, annotations, err := ExhaustGrantPagination(ctx, c, resource)
	require.Nil(t, err)
	AssertNoRatelimitAnnotations(t, annotations)
	return grants
}

// GrantsIntegrationTest - create a grant and then revoke it. Any connector that
// implements provisioning should be able to successfully run this test.
func GrantsIntegrationTest(
	ctx context.Context,
	t *testing.T,
	c builder,
	principal *v2.Resource,
	entitlement *v2.Entitlement,
) {
	grant := v2.Grant{
		Entitlement: entitlement,
		Principal:   principal,
	}

	grantsBefore := assertGrants(ctx, t, c, entitlement.Resource)
	grantCount := len(grantsBefore)

	annotations, err := c.Grant(ctx, principal, entitlement)
	require.Nil(t, err)
	AssertNoRatelimitAnnotations(t, annotations)

	grantsDuring := assertGrants(ctx, t, c, entitlement.Resource)
	require.Len(t, grantsDuring, grantCount+1)

	annotations, err = c.Revoke(ctx, &grant)
	require.Nil(t, err)
	AssertNoRatelimitAnnotations(t, annotations)

	grantsAfter := assertGrants(ctx, t, c, entitlement.Resource)
	require.Len(t, grantsAfter, grantCount)
}
