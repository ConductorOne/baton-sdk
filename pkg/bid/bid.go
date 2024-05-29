package bid

import (
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	BidPrefix            = "bid:"
	ResourceBidPrefix    = BidPrefix + "r:"
	EntitlementBidPrefix = BidPrefix + "e:"
	GrantBidPrefix       = BidPrefix + "g:"
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
	return "☠️" // TODO: panic? this should never happen
}

func ParseBid(bidStr string) (interface{}, error) {
	if len(bidStr) < 6 {
		return nil, fmt.Errorf("baton id string too short: %s", bidStr)
	}
	bidPrefix := bidStr[:4]
	if bidPrefix != BidPrefix {
		return nil, fmt.Errorf("invalid baton id prefix: %s", bidPrefix)
	}

	bidType := bidStr[:6]
	switch bidType {
	case ResourceBidPrefix:
		return ParseResourceBid(bidStr)
	case EntitlementBidPrefix:
		return ParseEntitlementBid(bidStr)
	case GrantBidPrefix:
		return ParseGrantBid(bidStr)
	}
	return nil, fmt.Errorf("invalid baton id type: %s", bidType)
}

func parseResourcePart(resourcePart string) (*v2.Resource, error) {
	resourceId := &v2.ResourceId{}
	parentResourceId := &v2.ResourceId{}
	parts := strings.Split(resourcePart, "/")
	if len(parts) == 4 {
		parentResourceId.ResourceType = parts[0]
		parentResourceId.Resource = parts[1]
		resourceId.ResourceType = parts[2]
		resourceId.Resource = parts[3]
		return &v2.Resource{
			Id:               resourceId,
			ParentResourceId: parentResourceId,
		}, nil
	}
	if len(parts) == 2 {
		resourceId.ResourceType = parts[0]
		resourceId.Resource = parts[1]
		return &v2.Resource{Id: resourceId}, nil
	}

	return nil, fmt.Errorf("invalid baton id resource part: %s", resourcePart)
}

func ParseResourceBid(bidStr string) (*v2.Resource, error) {
	return parseResourcePart(bidStr[6:])
}

func parseEntitlementPart(entitlementPart string) (*v2.Entitlement, error) {
	parts := strings.Split(entitlementPart, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid baton id entitlement part: %s", entitlementPart)
	}
	resourcePart := parts[0]
	entitlementSlug := parts[1]

	resource, err := parseResourcePart(resourcePart)
	if err != nil {
		return nil, err
	}

	return &v2.Entitlement{
		Slug:     entitlementSlug,
		Resource: resource,
	}, nil
}

func ParseEntitlementBid(bidStr string) (*v2.Entitlement, error) {
	return parseEntitlementPart(bidStr[6:])
}

func ParseGrantBid(bidStr string) (*v2.Grant, error) {
	parts := strings.Split(bidStr[6:], ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid baton id grant: %s", bidStr)
	}
	entitlement, err := parseEntitlementPart(parts[0] + ":" + parts[1])
	if err != nil {
		return nil, err
	}
	principal, err := parseResourcePart(parts[2])
	if err != nil {
		return nil, err
	}
	return &v2.Grant{
		Entitlement: entitlement,
		Principal:   principal,
	}, nil
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
	return ResourceBidPrefix + resourcePartToStr(r)
}

func EntitlementToBid(e *v2.Entitlement) string {
	return EntitlementBidPrefix + entitlementPartToStr(e)
}

func GrantToBid(g *v2.Grant) string {
	principalPart := resourcePartToStr(g.Principal)
	entitlementPart := entitlementPartToStr(g.Entitlement)

	return GrantBidPrefix + fmt.Sprintf("%s:%s", entitlementPart, principalPart)
}
