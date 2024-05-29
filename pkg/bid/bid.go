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

func MakeBid(b BID) (string, error) {
	switch bType := b.(type) {
	case *v2.Resource:
		return makeResourceBid(bType)
	case *v2.Entitlement:
		return makeEntitlementBid(bType)
	case *v2.Grant:
		return makeGrantBid(bType)
	}
	return "", fmt.Errorf("unknown bid type: %T", b)
}

func MustMakeBid(b BID) string {
	str, err := MakeBid(b)
	if err != nil {
		panic(err)
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

func resourcePartToStr(r *v2.Resource) (string, error) {
	rid := r.GetId()
	resourceType := escapeParts(rid.GetResourceType())
	resource := escapeParts(rid.GetResource())
	if resourceType == "" || resource == "" {
		return "", fmt.Errorf("resource type or id is empty")
	}
	if r.ParentResourceId == nil {
		return fmt.Sprintf("%s/%s", resourceType, resource), nil
	}

	prid := r.GetParentResourceId()
	parentResourceType := escapeParts(prid.GetResourceType())
	parentResource := escapeParts(prid.GetResource())
	if parentResourceType == "" || parentResource == "" {
		return "", fmt.Errorf("parent resource type or id is empty")
	}

	return fmt.Sprintf("%s/%s/%s/%s", parentResourceType, parentResource, resourceType, resource), nil
}

func entitlementPartToStr(e *v2.Entitlement) (string, error) {
	resourcePart, err := resourcePartToStr(e.Resource)
	if err != nil {
		return "", err
	}
	if e.Slug == "" {
		return "", fmt.Errorf("entitlement slug is empty")
	}
	return fmt.Sprintf("%s:%s", resourcePart, escapeParts(e.Slug)), nil
}

func makeResourceBid(r *v2.Resource) (string, error) {
	resourcePart, err := resourcePartToStr(r)
	if err != nil {
		return "", err
	}
	return BidPrefix + ":" + ResourceBidPrefix + ":" + resourcePart, nil
}

func makeEntitlementBid(e *v2.Entitlement) (string, error) {
	entitlementPart, err := entitlementPartToStr(e)
	if err != nil {
		return "", err
	}

	return BidPrefix + ":" + EntitlementBidPrefix + ":" + entitlementPart, nil
}

func makeGrantBid(g *v2.Grant) (string, error) {
	principalPart, err := resourcePartToStr(g.Principal)
	if err != nil {
		return "", err
	}
	entitlementPart, err := entitlementPartToStr(g.Entitlement)
	if err != nil {
		return "", err
	}

	return BidPrefix + ":" + GrantBidPrefix + ":" + fmt.Sprintf("%s:%s", entitlementPart, principalPart), nil
}
