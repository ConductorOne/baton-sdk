package bid

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func ToBid(b interface{}) string {
	switch bType := b.(type) {
	case *v2.Resource:
		return ResourceToBid(bType)
	case *v2.Entitlement:
		return EntitlementToBid(bType)
	case *v2.Grant:
		return GrantToBid(bType)
	}
	return "☠️"
}

func ParseBid(bidStr string) (interface{}, error) {
	return nil, nil
}

// TODO: escape colon and slash
func EscapeParts(str string) string {
	return ""
}

func resourcePartToStr(r *v2.Resource) string {
	if r.ParentResourceId == nil {
		return fmt.Sprintf("%s/%s", r.Id.ResourceType, r.Id.Resource)
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.ParentResourceId.ResourceType, r.ParentResourceId.Resource, r.Id.ResourceType, r.Id.Resource)
}

func entitlementPartToStr(e *v2.Entitlement) string {
	resourcePart := resourcePartToStr(e.Resource)
	return fmt.Sprintf("%s:%s", resourcePart, e.Slug)
}

func ResourceToBid(r *v2.Resource) string {
	return fmt.Sprintf("bid:r:%s", resourcePartToStr(r))
}

func EntitlementToBid(e *v2.Entitlement) string {
	return fmt.Sprintf("bid:e:%s", entitlementPartToStr(e))
}

func GrantToBid(g *v2.Grant) string {
	principalPart := resourcePartToStr(g.Principal)
	entitlementPart := entitlementPartToStr(g.Entitlement)

	return fmt.Sprintf("bid:g:%s:%s", entitlementPart, principalPart)
}
