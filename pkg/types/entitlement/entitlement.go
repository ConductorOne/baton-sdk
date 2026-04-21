package entitlement

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
)

type EntitlementOption func(*v2.Entitlement)

func WithAnnotation(msgs ...proto.Message) EntitlementOption {
	return func(e *v2.Entitlement) {
		annos := annotations.Annotations(e.GetAnnotations())
		for _, msg := range msgs {
			annos.Append(msg)
		}
		e.SetAnnotations(annos)
	}
}

func WithGrantableTo(grantableTo ...*v2.ResourceType) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetGrantableTo(grantableTo)
	}
}

func WithDisplayName(displayName string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetDisplayName(displayName)
	}
}

func WithSlug(slug string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetSlug(slug)
	}
}

func WithDescription(description string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetDescription(description)
	}
}

// WithExclusionGroup marks this entitlement as belonging to a mutually exclusive
// group. Two entitlements on the same resource with the same exclusionGroupID
// cannot be simultaneously granted to the same principal.
func WithExclusionGroup(exclusionGroupID string) EntitlementOption {
	return WithAnnotation(&v2.EntitlementExclusionGroup{
		ExclusionGroupId: exclusionGroupID,
	})
}

// WithExclusionGroupOrder is like WithExclusionGroup but also sets an ordering
// hint. Higher order values indicate higher privilege levels.
func WithExclusionGroupOrder(exclusionGroupID string, order uint32) EntitlementOption {
	return WithAnnotation(&v2.EntitlementExclusionGroup{
		ExclusionGroupId: exclusionGroupID,
		Order:            order,
	})
}

// WithExclusionGroupDefault marks this entitlement as the default for a mandatory
// exclusion group. When another entitlement in the group is revoked without an
// explicit replacement, this entitlement is granted as the fallback.
//
// Setting is_default on any entitlement in a group signals to ConductorOne that
// the group is mandatory (a principal must always hold exactly one).
func WithExclusionGroupDefault(exclusionGroupID string, order uint32) EntitlementOption {
	return WithAnnotation(&v2.EntitlementExclusionGroup{
		ExclusionGroupId: exclusionGroupID,
		Order:            order,
		IsDefault:        true,
	})
}

func NewEntitlementID(resource *v2.Resource, permission string) string {
	return fmt.Sprintf("%s:%s:%s", resource.GetId().GetResourceType(), resource.GetId().GetResource(), permission)
}

func NewPermissionEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}

func NewAssignmentEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}

func NewOwnershipEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_OWNERSHIP,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}

func NewEntitlement(resource *v2.Resource, name, purposeStr string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	var purpose v2.Entitlement_PurposeValue
	switch purposeStr {
	case "permission":
		purpose = v2.Entitlement_PURPOSE_VALUE_PERMISSION
	case "assignment":
		purpose = v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT
	case "ownership":
		purpose = v2.Entitlement_PURPOSE_VALUE_OWNERSHIP
	default:
		purpose = v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED
	}

	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     purpose,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}
