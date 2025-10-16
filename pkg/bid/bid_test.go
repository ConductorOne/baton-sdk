package bid

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

var group0 = v2.Resource_builder{
	Id: v2.ResourceId_builder{
		Resource:     "sales",
		ResourceType: "group",
	}.Build(),
}.Build()
var group1 = v2.Resource_builder{
	Id: v2.ResourceId_builder{
		Resource:     "s:al/es",
		ResourceType: "gr:=o\\up",
	}.Build(),
}.Build()

var user0 = v2.Resource_builder{
	Id: v2.ResourceId_builder{
		Resource:     "george",
		ResourceType: "user",
	}.Build(),
	ParentResourceId: group0.GetId(),
}.Build()
var user1 = v2.Resource_builder{
	Id: v2.ResourceId_builder{
		Resource:     "geo/r\\ge",
		ResourceType: "us/er",
	}.Build(),
	ParentResourceId: group1.GetId(),
}.Build()

var bidsToResources = map[string]*v2.Resource{
	"bid:r:group/sales/user/george":                         user0,
	"bid:r:gr\\:=o\\\\up/s\\:al\\/es/us\\/er/geo\\/r\\\\ge": user1,
}

var bidsToEntitlements = map[string]*v2.Entitlement{
	"bid:e:group/sales:member": v2.Entitlement_builder{
		Resource: group0,
		Slug:     "member",
	}.Build(),
}

var bidsToGrants = map[string]*v2.Grant{
	"bid:g:group/sales:member:group/sales/user/george": v2.Grant_builder{
		Entitlement: v2.Entitlement_builder{
			Resource: group0,
			Slug:     "member",
		}.Build(),
		Principal: user0,
	}.Build(),
	"bid:g:team/5678:member:user/1234": v2.Grant_builder{
		Entitlement: v2.Entitlement_builder{
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					Resource:     "5678",
					ResourceType: "team",
				}.Build(),
			}.Build(),
			Slug: "member",
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				Resource:     "1234",
				ResourceType: "user",
			}.Build(),
		}.Build(),
	}.Build(),
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
	_, err = MakeBid(v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
		}.Build(),
	}.Build())
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
