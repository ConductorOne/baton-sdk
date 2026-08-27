package pebble

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
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
		Profile: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"email": structpb.NewStringValue("alice@example.com"),
			},
		},
		Status: &v2.Status{
			Status:  v2.Status_RESOURCE_STATUS_ENABLED,
			Details: "unlocked by admin",
		},
		CreatedAt: &timestamppb.Timestamp{
			Seconds: 1716393600,
			Nanos:   0,
		},
		Icon: v2.AssetRef_builder{
			Id: "icon-alice",
		}.Build(),
	}.Build()

	v3rec := V2ResourceToV3("sync-1", original)
	require.Equal(t, "alice", v3rec.GetResourceId(), "resource_id")
	require.Equal(t, "group", v3rec.GetParent().GetResourceTypeId(), "parent rt")
	require.Equal(t, "alice@example.com", v3rec.GetProfile().GetFields()["email"].GetStringValue(), "profile email")
	require.Equal(t, v3.StatusRecord_RESOURCE_STATUS_ENABLED, v3rec.GetStatus().GetStatus(), "status")
	require.Equal(t, "unlocked by admin", v3rec.GetStatus().GetDetails(), "status details")
	require.Equal(t, int64(1716393600), v3rec.GetCreatedAt().GetSeconds(), "created_at")
	require.Equal(t, "icon-alice", v3rec.GetIconAssetExternalId(), "icon")

	back := V3ResourceToV2(v3rec)
	require.Equal(t, "alice", back.GetId().GetResource(), "roundtrip resource")
	require.Equal(t, "engineers", back.GetParentResourceId().GetResource(), "roundtrip parent")
	require.Equal(t, "Alice", back.GetDisplayName(), "roundtrip display_name")
	require.Equal(t, "alice@example.com", back.GetProfile().GetFields()["email"].GetStringValue(), "roundtrip profile email")
	require.Equal(t, v2.Status_RESOURCE_STATUS_ENABLED, back.GetStatus().GetStatus(), "roundtrip status")
	require.Equal(t, "unlocked by admin", back.GetStatus().GetDetails(), "roundtrip status details")
	require.Equal(t, int64(1716393600), back.GetCreatedAt().GetSeconds(), "roundtrip created_at")
	require.Equal(t, "icon-alice", back.GetIcon().GetId(), "roundtrip icon")
}

func TestV2ResourceWithoutIconRoundtrip(t *testing.T) {
	original := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "alice",
		}.Build(),
		DisplayName: "Alice",
	}.Build()

	v3rec := V2ResourceToV3("sync-1", original)
	require.Empty(t, v3rec.GetIconAssetExternalId(), "icon")

	back := V3ResourceToV2(v3rec)
	require.False(t, back.HasIcon(), "roundtrip icon presence")
	require.Nil(t, back.GetIcon(), "roundtrip icon")
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

// The three status enums below are maintained by hand and cast into each
// other numerically; a value added to one but not the others must fail here
// rather than mistranslate in stored data.
func TestStatusEnumMirrorsStayAligned(t *testing.T) {
	stripPrefix := func(name string) string {
		return strings.TrimPrefix(strings.TrimPrefix(name, "RESOURCE_"), "STATUS_")
	}

	require.Equal(t, len(v2.Status_ResourceStatus_name), len(v3.StatusRecord_ResourceStatus_name),
		"c1.storage.v3.StatusRecord.ResourceStatus must mirror c1.connector.v2.Status.ResourceStatus")
	require.Equal(t, len(v2.Status_ResourceStatus_name), len(v2.UserTrait_Status_Status_name),
		"c1.connector.v2.UserTrait.Status.Status must mirror c1.connector.v2.Status.ResourceStatus")

	for num, name := range v2.Status_ResourceStatus_name {
		v3Name, ok := v3.StatusRecord_ResourceStatus_name[num]
		require.Truef(t, ok, "Status_ResourceStatus value %d (%s) missing from StatusRecord_ResourceStatus", num, name)
		require.Equalf(t, name, v3Name, "Status_ResourceStatus value %d name mismatch", num)

		utName, ok := v2.UserTrait_Status_Status_name[num]
		require.Truef(t, ok, "Status_ResourceStatus value %d (%s) missing from UserTrait_Status_Status", num, name)
		require.Equalf(t, stripPrefix(name), stripPrefix(utName), "Status_ResourceStatus value %d name mismatch vs UserTrait_Status", num)
	}

	// AgentTrait_AgentStatus is cast numerically into Status_ResourceStatus.
	// It is a prefix, not a mirror (READY maps to ENABLED). Pin the mapping by
	// name so a new AgentStatus value cannot silently inherit an unrelated
	// ResourceStatus meaning — extend this table deliberately when adding one.
	expectedAgentMirror := map[int32]string{
		0: "RESOURCE_STATUS_UNSPECIFIED",
		1: "RESOURCE_STATUS_ENABLED",
		2: "RESOURCE_STATUS_DISABLED",
		3: "RESOURCE_STATUS_DELETED",
	}
	require.Len(t, v2.AgentTrait_AgentStatus_name, len(expectedAgentMirror),
		"new AgentTrait_AgentStatus value: confirm its numeric cast into Status_ResourceStatus is still meaningful, then extend expectedAgentMirror")
	for num, name := range v2.AgentTrait_AgentStatus_name {
		want, ok := expectedAgentMirror[num]
		require.Truef(t, ok, "AgentTrait_AgentStatus value %d (%s) has no reviewed Status_ResourceStatus counterpart", num, name)
		require.Equalf(t, want, v2.Status_ResourceStatus_name[num],
			"AgentTrait_AgentStatus value %d (%s) casts to an unexpected Status_ResourceStatus", num, name)
	}
}

func TestV2ResourceStatusPendingRoundtrip(t *testing.T) {
	original := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "alice",
		}.Build(),
		Status: v2.Status_builder{
			Status:  v2.Status_RESOURCE_STATUS_PENDING,
			Details: "invitation not accepted",
		}.Build(),
	}.Build()

	v3rec := V2ResourceToV3("sync-1", original)
	require.Equal(t, v3.StatusRecord_RESOURCE_STATUS_PENDING, v3rec.GetStatus().GetStatus(), "status")

	back := V3ResourceToV2(v3rec)
	require.Equal(t, v2.Status_RESOURCE_STATUS_PENDING, back.GetStatus().GetStatus(), "roundtrip status")
	require.Equal(t, "invitation not accepted", back.GetStatus().GetDetails(), "roundtrip status details")
}
