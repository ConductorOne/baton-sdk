package dotc1z

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestGrantExpandableColumns_WhitespaceOnlyEntitlementIDs(t *testing.T) {
	// Create a GrantExpandable annotation with only whitespace entitlement IDs.
	// After checking, this should result in is_expandable=0 because
	// there are no valid source entitlements.
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

	isExpandable, needsExpansion := grantExpandableColumns(grant)

	// After checking, this grant should NOT be marked as expandable
	// since there are no valid source entitlements.
	require.Equal(t, 0, isExpandable, "grant with only whitespace entitlement IDs should not be expandable")
	require.Equal(t, 0, needsExpansion, "grant with no valid source entitlements should not need expansion")
}

func TestGrantExpandableColumns_MixedWhitespaceAndValidIDs(t *testing.T) {
	// Create a GrantExpandable annotation with a mix of whitespace and valid IDs.
	// The grant should still be expandable with only the valid IDs.
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

	isExpandable, needsExpansion := grantExpandableColumns(grant)

	// Should still be expandable because there's at least one valid entitlement ID.
	require.Equal(t, 1, isExpandable, "grant with valid entitlement ID should be expandable")
	require.Equal(t, 1, needsExpansion, "expandable grant should need expansion on insert")
}
