package bid

import (
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	BidPrefix            = "bid"
	ResourceBidPrefix    = "r"
	EntitlementBidPrefix = "e"
	GrantBidPrefix       = "g"
)

type BID interface {
	proto.Message
	// TODO: be smarter about this interface.
	GetAnnotations() []*anypb.Any
}

func MakeBid(b BID) string {
	switch bType := b.(type) {
	case *v2.Resource:
		return MakeResourceBid(bType)
	case *v2.Entitlement:
		return MakeEntitlementBid(bType)
	case *v2.Grant:
		return MakeGrantBid(bType)
	}
	return ""
}

func MustMakeBid(b BID) string {
	str := MakeBid(b)
	if str == "" {
		panic("failed to make bid")
	}
	return str
}

// escape colon, slash, and backslash.
func escapeParts(str string) string {
	escaped := strings.ReplaceAll(str, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "/", "\\/")
	escaped = strings.ReplaceAll(escaped, ":", "\\:")
	return escaped
}

func resourcePartToStr(r *v2.Resource) string {
	resourceType := escapeParts(r.Id.ResourceType)
	resource := escapeParts(r.Id.Resource)
	if r.ParentResourceId == nil {
		return fmt.Sprintf("%s/%s", resourceType, resource)
	}
	parentResourceType := escapeParts(r.ParentResourceId.ResourceType)
	parentResource := escapeParts(r.ParentResourceId.Resource)

	return fmt.Sprintf("%s/%s/%s/%s", parentResourceType, parentResource, resourceType, resource)
}

func entitlementPartToStr(e *v2.Entitlement) string {
	resourcePart := resourcePartToStr(e.Resource)
	return fmt.Sprintf("%s:%s", resourcePart, escapeParts(e.Slug))
}

func MakeResourceBid(r *v2.Resource) string {
	return BidPrefix + ":" + ResourceBidPrefix + ":" + resourcePartToStr(r)
}

func MakeEntitlementBid(e *v2.Entitlement) string {
	return BidPrefix + ":" + EntitlementBidPrefix + ":" + entitlementPartToStr(e)
}

func MakeGrantBid(g *v2.Grant) string {
	principalPart := resourcePartToStr(g.Principal)
	entitlementPart := entitlementPartToStr(g.Entitlement)

	return BidPrefix + ":" + GrantBidPrefix + ":" + fmt.Sprintf("%s:%s", entitlementPart, principalPart)
}
