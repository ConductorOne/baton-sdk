package bid

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

var group0 = &v2.Resource{
	Id: &v2.ResourceId{
		Resource:     "sales",
		ResourceType: "group",
	},
}
var user0 = &v2.Resource{
	Id: &v2.ResourceId{
		Resource:     "george",
		ResourceType: "user",
	},
	ParentResourceId: group0.Id,
}

var bidsToResources = map[string]*v2.Resource{
	"bid:r:group/sales/user/george": user0,
}

var bidsToEntitlements = map[string]*v2.Entitlement{
	"bid:e:group/sales:member": {
		Resource: group0,
		Slug:     "member",
	},
}

var bidsToGrants = map[string]*v2.Grant{
	"bid:g:group/sales:member:group/sales/user/george": {
		Entitlement: &v2.Entitlement{
			Resource: group0,
			Slug:     "member",
		},
		Principal: user0,
	},
}

func TestToBid(t *testing.T) {
	for bid, resource := range bidsToResources {
		require.Equal(t, bid, ToBid(resource))
	}
	for bid, entitlement := range bidsToEntitlements {
		require.Equal(t, bid, ToBid(entitlement))
	}
	for bid, grant := range bidsToGrants {
		require.Equal(t, bid, ToBid(grant))
	}
}

func TestParseBid(t *testing.T) {
	for bid, resource := range bidsToResources {
		r, err := ParseBid(bid)
		require.NoError(t, err)
		require.Equal(t, resource, r)
	}
	for bid, entitlement := range bidsToEntitlements {
		e, err := ParseBid(bid)
		require.NoError(t, err)
		require.Equal(t, entitlement, e)
	}
	for bid, grant := range bidsToGrants {
		g, err := ParseBid(bid)
		require.NoError(t, err)
		require.Equal(t, grant, g)
	}
}
