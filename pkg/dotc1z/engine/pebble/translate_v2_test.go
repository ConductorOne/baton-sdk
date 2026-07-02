package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestV2GrantRoundtrip(t *testing.T) {
	original := v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id: "github-read",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "app",
					Resource:     "github",
				}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "alice",
			}.Build(),
		}.Build(),
	}.Build()

	v3rec := V2GrantToV3("sync-id-1", original)
	require.Equal(t, "grant-1", v3rec.GetExternalId(), "external_id")
	require.Equal(t, "github-read", v3rec.GetEntitlement().GetEntitlementId(), "entitlement_id")
	require.Equal(t, "user", v3rec.GetPrincipal().GetResourceTypeId(), "principal rt")

	back := V3GrantToV2(v3rec)
	// Stored external id round-trips verbatim; refs round-trip raw.
	require.Equal(t, "grant-1", back.GetId(), "roundtrip id")
	require.Equal(t, "github-read", back.GetEntitlement().GetId(), "roundtrip entitlement id")
	require.Equal(t, "app", back.GetEntitlement().GetResource().GetId().GetResourceType(), "roundtrip ent.resource.rt")
	require.Equal(t, "alice", back.GetPrincipal().GetId().GetResource(), "roundtrip principal")
}

func TestV2ResourceRoundtrip(t *testing.T) {
	original := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "alice",
		}.Build(),
		ParentResourceId: v2.ResourceId_builder{
			ResourceType: "group",
			Resource:     "engineers",
		}.Build(),
		DisplayName: "Alice",
		Description: "Senior eng",
	}.Build()

	v3rec := V2ResourceToV3("sync-1", original)
	require.Equal(t, "alice", v3rec.GetResourceId(), "resource_id")
	require.Equal(t, "group", v3rec.GetParent().GetResourceTypeId(), "parent rt")

	back := V3ResourceToV2(v3rec)
	require.Equal(t, "alice", back.GetId().GetResource(), "roundtrip resource")
	require.Equal(t, "engineers", back.GetParentResourceId().GetResource(), "roundtrip parent")
	require.Equal(t, "Alice", back.GetDisplayName(), "roundtrip display_name")
}

func TestV2ResourceTypeRoundtrip(t *testing.T) {
	original := v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER, v2.ResourceType_TRAIT_APP},
	}.Build()

	v3rec := V2ResourceTypeToV3("sync-1", original)
	require.Equal(t, "user", v3rec.GetExternalId(), "external_id")
	require.Len(t, v3rec.GetTraits(), 2, "traits count")

	back := V3ResourceTypeToV2(v3rec)
	require.Equal(t, "user", back.GetId(), "roundtrip id")
	require.Len(t, back.GetTraits(), 2, "roundtrip trait count")
	require.Equal(t, v2.ResourceType_TRAIT_USER, back.GetTraits()[0], "roundtrip trait[0]")
	require.Equal(t, v2.ResourceType_TRAIT_APP, back.GetTraits()[1], "roundtrip trait[1]")
}

func TestV2EntitlementRoundtrip(t *testing.T) {
	original := v2.Entitlement_builder{
		Id: "github-read",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "app",
				Resource:     "github",
			}.Build(),
		}.Build(),
		DisplayName: "Read",
		Description: "Read access",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
	}.Build()

	v3rec := V2EntitlementToV3("sync-1", original)
	require.Equal(t, "github-read", v3rec.GetExternalId(), "external_id")
	require.Equal(t, "github", v3rec.GetResource().GetResourceId(), "resource.resource_id")
	require.Equal(t, "PERMISSION", v3rec.GetPurpose(), "purpose")

	back := V3EntitlementToV2(v3rec)
	require.Equal(t, "github-read", back.GetId(), "roundtrip id")
	require.Equal(t, "github", back.GetResource().GetId().GetResource(), "roundtrip resource")
	require.Equal(t, v2.Entitlement_PURPOSE_VALUE_PERMISSION, back.GetPurpose(), "roundtrip purpose")
}

func TestNilTranslations(t *testing.T) {
	require.Nil(t, V2GrantToV3("sync", nil), "V2GrantToV3(nil) should be nil")
	require.Nil(t, V3GrantToV2(nil), "V3GrantToV2(nil) should be nil")
	require.Nil(t, V2ResourceToV3("sync", nil), "V2ResourceToV3(nil) should be nil")
	require.Nil(t, V3ResourceToV2(nil), "V3ResourceToV2(nil) should be nil")
	require.Nil(t, V2ResourceTypeToV3("sync", nil), "V2ResourceTypeToV3(nil) should be nil")
	require.Nil(t, V2EntitlementToV3("sync", nil), "V2EntitlementToV3(nil) should be nil")
}

func TestUnknownTraitRoundtrip(t *testing.T) {
	// Unknown trait string maps to TRAIT_UNSPECIFIED, not a panic.
	require.Equal(t, v2.ResourceType_TRAIT_UNSPECIFIED, stringToTrait("DOES_NOT_EXIST"), "unknown trait")
}

func TestGrantSourcesRoundtrip(t *testing.T) {
	original := v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id: "ent-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build(),
		Sources: v2.GrantSources_builder{
			Sources: map[string]*v2.GrantSources_GrantSource{
				"direct-source":   v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
				"indirect-source": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
			},
		}.Build(),
	}.Build()

	v3rec := V2GrantToV3("sync-1", original)
	require.Len(t, v3rec.GetSources(), 2, "source count v3")
	require.True(t, v3rec.GetSources()["direct-source"].GetIsDirect(), "direct-source.is_direct should be true")
	require.False(t, v3rec.GetSources()["indirect-source"].GetIsDirect(), "indirect-source.is_direct should be false")

	back := V3GrantToV2(v3rec)
	require.Len(t, back.GetSources().GetSources(), 2, "source count v2 roundtrip")
	require.True(t, back.GetSources().GetSources()["direct-source"].GetIsDirect(), "roundtrip direct-source.is_direct should be true")
}
