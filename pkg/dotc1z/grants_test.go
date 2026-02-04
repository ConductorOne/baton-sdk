package dotc1z

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestIsGrantExpandable_WhitespaceOnlyEntitlementIDs(t *testing.T) {
	// Create a GrantExpandable annotation with only whitespace entitlement IDs.
	// This should return false because there are no valid source entitlements.
	expandable := v2.GrantExpandable_builder{
		EntitlementIds: []string{"  ", "\t", "   \n  "},
	}.Build()

	expandableAny, err := anypb.New(expandable)
	require.NoError(t, err)

	grant := v2.Grant_builder{
		Id: "test-grant",
		Entitlement: v2.Entitlement_builder{
			Id: "test-entitlement",
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "user1",
			}.Build(),
		}.Build(),
		Annotations: []*anypb.Any{expandableAny},
	}.Build()

	require.False(t, isGrantExpandable(grant), "grant with only whitespace entitlement IDs should not be expandable")
}

func TestIsGrantExpandable_MixedWhitespaceAndValidIDs(t *testing.T) {
	// Create a GrantExpandable annotation with a mix of whitespace and valid IDs.
	// The grant should still be expandable because there's at least one valid ID.
	expandable := v2.GrantExpandable_builder{
		EntitlementIds: []string{"  ", "valid-entitlement-id", "\t"},
		Shallow:        true,
	}.Build()

	expandableAny, err := anypb.New(expandable)
	require.NoError(t, err)

	grant := v2.Grant_builder{
		Id: "test-grant",
		Entitlement: v2.Entitlement_builder{
			Id: "test-entitlement",
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "user1",
			}.Build(),
		}.Build(),
		Annotations: []*anypb.Any{expandableAny},
	}.Build()

	require.True(t, isGrantExpandable(grant), "grant with valid entitlement ID should be expandable")
}
