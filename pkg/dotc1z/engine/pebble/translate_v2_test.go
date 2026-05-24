//go:build batonsdkv2

package pebble

import (
	"testing"

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
	if v3rec.GetSyncId() != "sync-id-1" {
		t.Errorf("sync_id: got %q", v3rec.GetSyncId())
	}
	if v3rec.GetExternalId() != "grant-1" {
		t.Errorf("external_id: got %q", v3rec.GetExternalId())
	}
	if v3rec.GetEntitlement().GetEntitlementId() != "github-read" {
		t.Errorf("entitlement_id: got %q", v3rec.GetEntitlement().GetEntitlementId())
	}
	if v3rec.GetPrincipal().GetResourceTypeId() != "user" {
		t.Errorf("principal rt: got %q", v3rec.GetPrincipal().GetResourceTypeId())
	}

	back := V3GrantToV2(v3rec)
	if back.GetId() != "grant-1" {
		t.Errorf("roundtrip id: got %q", back.GetId())
	}
	if back.GetEntitlement().GetId() != "github-read" {
		t.Errorf("roundtrip entitlement id: got %q", back.GetEntitlement().GetId())
	}
	if back.GetEntitlement().GetResource().GetId().GetResourceType() != "app" {
		t.Errorf("roundtrip ent.resource.rt: got %q", back.GetEntitlement().GetResource().GetId().GetResourceType())
	}
	if back.GetPrincipal().GetId().GetResource() != "alice" {
		t.Errorf("roundtrip principal: got %q", back.GetPrincipal().GetId().GetResource())
	}
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
	if v3rec.GetResourceId() != "alice" {
		t.Errorf("resource_id: got %q", v3rec.GetResourceId())
	}
	if v3rec.GetParent().GetResourceTypeId() != "group" {
		t.Errorf("parent rt: got %q", v3rec.GetParent().GetResourceTypeId())
	}

	back := V3ResourceToV2(v3rec)
	if back.GetId().GetResource() != "alice" {
		t.Errorf("roundtrip resource: got %q", back.GetId().GetResource())
	}
	if back.GetParentResourceId().GetResource() != "engineers" {
		t.Errorf("roundtrip parent: got %q", back.GetParentResourceId().GetResource())
	}
	if back.GetDisplayName() != "Alice" {
		t.Errorf("roundtrip display_name: got %q", back.GetDisplayName())
	}
}

func TestV2ResourceTypeRoundtrip(t *testing.T) {
	original := v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER, v2.ResourceType_TRAIT_APP},
	}.Build()

	v3rec := V2ResourceTypeToV3("sync-1", original)
	if v3rec.GetExternalId() != "user" {
		t.Errorf("external_id: got %q", v3rec.GetExternalId())
	}
	if len(v3rec.GetTraits()) != 2 {
		t.Errorf("traits count: got %d", len(v3rec.GetTraits()))
	}

	back := V3ResourceTypeToV2(v3rec)
	if back.GetId() != "user" {
		t.Errorf("roundtrip id: got %q", back.GetId())
	}
	if len(back.GetTraits()) != 2 {
		t.Fatalf("roundtrip trait count: got %d", len(back.GetTraits()))
	}
	if back.GetTraits()[0] != v2.ResourceType_TRAIT_USER {
		t.Errorf("roundtrip trait[0]: got %v", back.GetTraits()[0])
	}
	if back.GetTraits()[1] != v2.ResourceType_TRAIT_APP {
		t.Errorf("roundtrip trait[1]: got %v", back.GetTraits()[1])
	}
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
	if v3rec.GetExternalId() != "github-read" {
		t.Errorf("external_id: got %q", v3rec.GetExternalId())
	}
	if v3rec.GetResource().GetResourceId() != "github" {
		t.Errorf("resource.resource_id: got %q", v3rec.GetResource().GetResourceId())
	}
	if v3rec.GetPurpose() != "PERMISSION" {
		t.Errorf("purpose: got %q want PERMISSION", v3rec.GetPurpose())
	}

	back := V3EntitlementToV2(v3rec)
	if back.GetId() != "github-read" {
		t.Errorf("roundtrip id: got %q", back.GetId())
	}
	if back.GetResource().GetId().GetResource() != "github" {
		t.Errorf("roundtrip resource: got %q", back.GetResource().GetId().GetResource())
	}
	if back.GetPurpose() != v2.Entitlement_PURPOSE_VALUE_PERMISSION {
		t.Errorf("roundtrip purpose: got %v", back.GetPurpose())
	}
}

func TestNilTranslations(t *testing.T) {
	if V2GrantToV3("sync", nil) != nil {
		t.Error("V2GrantToV3(nil) should be nil")
	}
	if V3GrantToV2(nil) != nil {
		t.Error("V3GrantToV2(nil) should be nil")
	}
	if V2ResourceToV3("sync", nil) != nil {
		t.Error("V2ResourceToV3(nil) should be nil")
	}
	if V3ResourceToV2(nil) != nil {
		t.Error("V3ResourceToV2(nil) should be nil")
	}
	if V2ResourceTypeToV3("sync", nil) != nil {
		t.Error("V2ResourceTypeToV3(nil) should be nil")
	}
	if V2EntitlementToV3("sync", nil) != nil {
		t.Error("V2EntitlementToV3(nil) should be nil")
	}
}

func TestUnknownTraitRoundtrip(t *testing.T) {
	// Unknown trait string maps to TRAIT_UNSPECIFIED, not a panic.
	if got := stringToTrait("DOES_NOT_EXIST"); got != v2.ResourceType_TRAIT_UNSPECIFIED {
		t.Errorf("unknown trait: got %v want UNSPECIFIED", got)
	}
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
	if len(v3rec.GetSources()) != 2 {
		t.Fatalf("source count v3: got %d", len(v3rec.GetSources()))
	}
	if !v3rec.GetSources()["direct-source"].GetIsDirect() {
		t.Error("direct-source.is_direct should be true")
	}
	if v3rec.GetSources()["indirect-source"].GetIsDirect() {
		t.Error("indirect-source.is_direct should be false")
	}

	back := V3GrantToV2(v3rec)
	if len(back.GetSources().GetSources()) != 2 {
		t.Fatalf("source count v2 roundtrip: got %d", len(back.GetSources().GetSources()))
	}
	if !back.GetSources().GetSources()["direct-source"].GetIsDirect() {
		t.Error("roundtrip direct-source.is_direct should be true")
	}
}
