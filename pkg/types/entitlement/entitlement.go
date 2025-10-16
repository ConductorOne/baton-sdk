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

func WithDescription(description string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetDescription(description)
	}
}

func NewEntitlementID(resource *v2.Resource, permission string) string {
	return fmt.Sprintf("%s:%s:%s", resource.GetId().GetResourceType(), resource.GetId().GetResource(), permission)
}

func NewPermissionEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlementBuilder := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Resource:    resource,
	}
	entitlement := entitlementBuilder.Build()
	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}

func NewAssignmentEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlementBuilder := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Resource:    resource,
	}
	entitlement := entitlementBuilder.Build()
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
	default:
		purpose = v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED
	}

	entitlementBuilder := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     purpose,
		Resource:    resource,
	}
	entitlement := entitlementBuilder.Build()
	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}
