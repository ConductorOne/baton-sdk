/*
Package bid implements functions to create and parse Baton IDs.

Baton IDs are standardized identifiers for Baton resources, entitlements, and grants.
They are not required when writing a connector, but they should help make serializing and deserializing IDs easier.

Format:

Resource: `bid:r:<parent_resource_type>/<parent_resource_id>/<resource_type>/<resource_id>`
Entitlement: `bid:e:<parent_resource_type>/<parent_resource_id>/<resource_type>/<resource_id>:<entitlement_slug>`
Grant: `bid:g:<ent_parent_resource_type>/<ent_parent_resource_id>/<ent_resource_type>/<ent_resource_id>:<ent_slug>:<principal_parent_type>/<principal_parent_id>/<principal_type>/<principal_id>`

Trailing colons and slashes are omitted. Empty values in the middle must still have colons. Colons and slashes in values are escaped with backslash. Backslash is escaped with backslash.

Examples:

Resource, type user, id 1234. parent resource type group, id 5678
`bid:r:group/5678/user/1234`
Resource, type user, id 1234. no parent resource
`bid:r:user/1234`

Entitlement, type team, id 5678, slug member. parent resource: type org, id 9012
`bid:e:org/9012/team/5678:member`
Entitlement, type team, id 5678, slug: member. no parent resource
`bid:e:team/56768:member`

Grant, entitlement resource type team, id 5678, slug member. parent resource type org, id 9012. principal type user, id 1234, parent resource type team, id 5678
`bid:g:org/9012/team/5678:member:team/5678/user/1234`

Grant, entitlement resource type team, id 5678, slug member. no parent resource. principal type user, id 1234, no parent resource
`bid:g:team/5678:member:user/1234`
*/

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

type BIDError struct {
	Msg string
	bs  *bidScanner
}

func (e *BIDError) Error() string {
	if e.bs == nil || e.bs.index < 0 {
		return fmt.Sprintf("error parsing baton id '%s': %s", e.bs.str, e.Msg)
	}
	// TODO: make a really cool multiline error message
	return fmt.Sprintf("error parsing baton id '%s' at location %v: %s", e.bs.str, e.bs.index, e.Msg)
}

func NewBidError(bs *bidScanner, msg string, a ...any) *BIDError {
	msg = fmt.Sprintf(msg, a...)
	return &BIDError{Msg: msg, bs: bs}
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
	return "", NewBidError(nil, "unknown bid type: %T", b)
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
		return "", NewBidError(nil, "resource type or id is empty")
	}
	if r.ParentResourceId == nil {
		return strings.Join([]string{resourceType, resource}, "/"), nil
	}

	prid := r.GetParentResourceId()
	parentResourceType := escapeParts(prid.GetResourceType())
	parentResource := escapeParts(prid.GetResource())
	if parentResourceType == "" || parentResource == "" {
		return "", NewBidError(nil, "parent resource type or id is empty")
	}

	return strings.Join([]string{parentResourceType, parentResource, resourceType, resource}, "/"), nil
}

func entitlementPartToStr(e *v2.Entitlement) (string, error) {
	resourcePart, err := resourcePartToStr(e.Resource)
	if err != nil {
		return "", err
	}
	if e.Slug == "" {
		return "", NewBidError(nil, "entitlement slug is empty")
	}

	return strings.Join([]string{resourcePart, escapeParts(e.Slug)}, ":"), nil
}

func makeResourceBid(r *v2.Resource) (string, error) {
	resourcePart, err := resourcePartToStr(r)
	if err != nil {
		return "", err
	}

	return strings.Join([]string{BidPrefix, ResourceBidPrefix, resourcePart}, ":"), nil
}

func makeEntitlementBid(e *v2.Entitlement) (string, error) {
	entitlementPart, err := entitlementPartToStr(e)
	if err != nil {
		return "", err
	}

	return strings.Join([]string{BidPrefix, EntitlementBidPrefix, entitlementPart}, ":"), nil
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

	return strings.Join([]string{BidPrefix, GrantBidPrefix, entitlementPart, principalPart}, ":"), nil
}
