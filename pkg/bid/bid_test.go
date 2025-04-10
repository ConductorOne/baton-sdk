package bid

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

var group0 = &v2.Resource{
	Id: &v2.ResourceId{
		Resource:     "sales",
		ResourceType: "group",
	},
}
var group1 = &v2.Resource{
	Id: &v2.ResourceId{
		Resource:     "s:al/es",
		ResourceType: "gr:=o\\up",
	},
}

var user0 = &v2.Resource{
	Id: &v2.ResourceId{
		Resource:     "george",
		ResourceType: "user",
	},
	ParentResourceId: group0.Id,
}
var user1 = &v2.Resource{
	Id: &v2.ResourceId{
		Resource:     "geo/r\\ge",
		ResourceType: "us/er",
	},
	ParentResourceId: group1.Id,
}

var bidsToResources = map[string]*v2.Resource{
	"bid:r:group/sales/user/george":                         user0,
	"bid:r:gr\\:=o\\\\up/s\\:al\\/es/us\\/er/geo\\/r\\\\ge": user1,
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
	"bid:g:team/5678:member:user/1234": {
		Entitlement: &v2.Entitlement{
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					Resource:     "5678",
					ResourceType: "team",
				},
			},
			Slug: "member",
		},
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				Resource:     "1234",
				ResourceType: "user",
			},
		},
	},
}

func TestToBid(t *testing.T) {
	for bid, resource := range bidsToResources {
		require.Equal(t, bid, MustMakeBid(resource))
	}
	for bid, entitlement := range bidsToEntitlements {
		require.Equal(t, bid, MustMakeBid(entitlement))
	}
	for bid, grant := range bidsToGrants {
		require.Equal(t, bid, MustMakeBid(grant))
	}
	_, err := MakeBid(&v2.Resource{})
	require.Error(t, err)
}

func TestParseBid(t *testing.T) {
	// r, err := ParseBid("bid:r:")
	opts := []cmp.Option{protocmp.Transform()}

	for bid, resource := range bidsToResources {
		r, err := Parse(bid)
		require.NoError(t, err, bid)
		diff := cmp.Diff(resource, r, opts...)
		require.Empty(t, diff, bid)
	}
	for bid, entitlement := range bidsToEntitlements {
		e, err := Parse(bid)
		require.NoError(t, err, bid)
		diff := cmp.Diff(entitlement, e, opts...)
		require.Empty(t, diff, bid)
	}
	for bid, grant := range bidsToGrants {
		g, err := Parse(bid)
		require.NoError(t, err, bid)
		diff := cmp.Diff(grant, g, opts...)
		require.Empty(t, diff, bid)
	}
}

func TestParseErrors(t *testing.T) {
	var badBids = map[string]*BIDParseError{
		"bid:r:":              NewBidParseError(&bidScanner{index: 6}, "invalid resource part"),
		"bid:r:1":             NewBidParseError(&bidScanner{index: 7}, "invalid resource part: ''"),
		"arn:blahblah":        NewBidParseError(&bidScanner{index: 3}, "invalid prefix: 'arn'"),
		"bid:?":               NewBidParseError(&bidScanner{index: 5}, "invalid prefix: '?'"),
		"bid:?:user/bob":      NewBidParseError(&bidScanner{index: 6}, "invalid type: '?'"),
		"bid:r:user/bob:blah": NewBidParseError(&bidScanner{index: 15}, "invalid baton id: 'bid:r:user/bob:blah'"),
	}
	for bid, expectedErr := range badBids {
		// Avoid repeating the string in two places
		expectedErr.bs.str = bid
		_, err := Parse(bid)
		require.Error(t, err)
		require.Equal(t, expectedErr.Error(), err.Error())
	}
}
