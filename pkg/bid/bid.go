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

func ParseBid(bidStr string) (BID, error) {
	rs := &ResourceScan{str: bidStr}

	tType, val, err := rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != literal || val != BidPrefix {
		return nil, fmt.Errorf("invalid baton id prefix: %s", val)
	}

	tType, val, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != colon || val != ":" {
		return nil, fmt.Errorf("invalid baton id prefix: %s", val)
	}

	tType, val, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != literal {
		return nil, fmt.Errorf("invalid baton id prefix: %s", val)
	}
	tType, _, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != colon {
		return nil, fmt.Errorf("invalid baton id prefix: %s", val)
	}

	var rv BID
	switch val {
	case ResourceBidPrefix:
		rv, err = parseResourcePart(rs)
	case EntitlementBidPrefix:
		rv, err = parseEntitlementPart(rs)
	case GrantBidPrefix:
		rv, err = parseGrantPart(rs)
	default:
		return nil, fmt.Errorf("invalid baton id type: %s", val)
	}
	if err != nil {
		return nil, err
	}

	tType, _, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != eof {
		return nil, fmt.Errorf("invalid baton id: %s", bidStr)
	}
	return rv, nil
}

type TokenType int

const (
	colon   TokenType = iota
	slash   TokenType = iota
	literal TokenType = iota
	eof     TokenType = iota
)

type ResourceScan struct {
	index int
	str   string
}

func (rs *ResourceScan) nextToken() (TokenType, string, int, error) {
	// Do not modify rs.index in here. It will break Peek()
	if rs.index >= len(rs.str) {
		return eof, "", len(rs.str), nil
	}

	if rs.str[rs.index] == ':' {
		return colon, ":", rs.index + 1, nil
	}
	if rs.str[rs.index] == '/' {
		return slash, "/", rs.index + 1, nil
	}

	var token strings.Builder
	for i := rs.index; i < len(rs.str); i++ {
		switch rs.str[i] {
		case ':', '/':
			return literal, token.String(), i, nil
		case '\\':
			if i+1 >= len(rs.str) {
				return -1, "", -1, fmt.Errorf("invalid escape sequence")
			}
			nextChar := rs.str[i+1]
			switch nextChar {
			case ':', '/', '\\':
				i++
				err := token.WriteByte(nextChar)
				if err != nil {
					return -1, "", -1, err
				}
			default:
				return -1, "", -1, fmt.Errorf("invalid escape sequence")
			}
		default:
			err := token.WriteByte(rs.str[i])
			if err != nil {
				return -1, "", -1, err
			}
		}
	}

	return literal, token.String(), len(rs.str), nil
}

func (rs *ResourceScan) PeekToken() (TokenType, string, error) {
	tType, val, _, err := rs.nextToken()
	if err != nil {
		return -1, "", err
	}
	return tType, val, nil
}

func (rs *ResourceScan) NextToken() (TokenType, string, error) {
	tType, val, idx, err := rs.nextToken()
	if err != nil {
		return -1, "", err
	}
	rs.index = idx
	return tType, val, nil
}

func (rs *ResourceScan) SkipToken() error {
	_, _, idx, err := rs.nextToken()
	if err != nil {
		return err
	}
	rs.index = idx
	return nil
}

func parseResourcePart(rs *ResourceScan) (*v2.Resource, error) {
	// "bid:r:group/sales/user/george"
	//        ^
	//        Parsing in this function starts here
	resourceId := &v2.ResourceId{}
	parentResourceId := &v2.ResourceId{}

	tokens := []string{}
	// Each loop grabs resource type, slash, resource id
	for {
		// Should be the resource type, or the parent resource type, or eof/colon (we're done parsing)
		tType, token, err := rs.PeekToken()
		if err != nil {
			return nil, err
		}
		if tType == eof || tType == colon {
			// Colon is in the case that this resource is part of an entitlement
			break
		}
		if tType != literal {
			return nil, fmt.Errorf("invalid baton id resource part: %s %v %v", token, rs.str, rs.index)
		}
		// We just peeked the token and we want to use it in this case
		err = rs.SkipToken()
		if err != nil {
			return nil, err
		}

		// Add resource type or parent resource type to tokens
		tokens = append(tokens, token)

		// Slash separates resource type from resource id
		tType, token, err = rs.NextToken()
		if err != nil {
			return nil, err
		}
		if tType != slash {
			return nil, fmt.Errorf("invalid baton id resource part: %s", token)
		}

		// Resource id or parent resource id
		tType, token, err = rs.NextToken()
		if err != nil {
			return nil, err
		}
		if tType != literal {
			return nil, fmt.Errorf("invalid baton id resource part: %s", token)
		}

		tokens = append(tokens, token)

		// Slash separates resource type from resource id
		tType, _, err = rs.PeekToken()
		if err != nil {
			return nil, err
		}
		if tType != slash {
			continue
		}
		err = rs.SkipToken()
		if err != nil {
			return nil, err
		}
		tType, _, err = rs.PeekToken()
		if err != nil {
			return nil, err
		}
		if tType != literal {
			return nil, fmt.Errorf("invalid baton id resource part: %s", rs.str)
		}
	}

	if len(tokens) == 4 {
		parentResourceId.ResourceType = tokens[0]
		parentResourceId.Resource = tokens[1]
		resourceId.ResourceType = tokens[2]
		resourceId.Resource = tokens[3]
		return &v2.Resource{
			Id:               resourceId,
			ParentResourceId: parentResourceId,
		}, nil
	}
	if len(tokens) == 2 {
		resourceId.ResourceType = tokens[0]
		resourceId.Resource = tokens[1]
		return &v2.Resource{Id: resourceId}, nil
	}

	return nil, fmt.Errorf("invalid baton id resource part: %s", rs.str)
}

func ParseResourceBid(bidStr string) (*v2.Resource, error) {
	ret, err := ParseBid(bidStr)
	if err != nil {
		return nil, err
	}
	if rv, ok := ret.(*v2.Resource); ok {
		return rv, nil
	}
	return nil, fmt.Errorf("invalid baton id resource: %s", bidStr)
}

func parseEntitlementPart(rs *ResourceScan) (*v2.Entitlement, error) {
	resource, err := parseResourcePart(rs)
	if err != nil {
		return nil, err
	}
	// bid:e:group/sales:member
	//                  ^
	//									ResourceScan should be here
	tType, val, err := rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != colon {
		return nil, fmt.Errorf("invalid baton id entitlement part: %s", val)
	}
	// Get entitlement slug
	tType, val, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != literal {
		return nil, fmt.Errorf("invalid baton id entitlement part: %s", val)
	}

	return &v2.Entitlement{
		Slug:     val,
		Resource: resource,
	}, nil
}

func ParseEntitlementBid(bidStr string) (*v2.Entitlement, error) {
	ret, err := ParseBid(bidStr)
	if err != nil {
		return nil, err
	}
	if rv, ok := ret.(*v2.Entitlement); ok {
		return rv, nil
	}
	return nil, fmt.Errorf("invalid baton id entitlement: %s", bidStr)
}

func parseGrantPart(rs *ResourceScan) (*v2.Grant, error) {
	entitlement, err := parseEntitlementPart(rs)
	if err != nil {
		return nil, err
	}

	tType, val, err := rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != colon {
		return nil, fmt.Errorf("invalid baton id grant part: %s", val)
	}

	principal, err := parseResourcePart(rs)
	if err != nil {
		return nil, err
	}

	return &v2.Grant{
		Entitlement: entitlement,
		Principal:   principal,
	}, nil
}

func ParseGrantBid(bidStr string) (*v2.Grant, error) {
	ret, err := ParseBid(bidStr)
	if err != nil {
		return nil, err
	}
	if rv, ok := ret.(*v2.Grant); ok {
		return rv, nil
	}
	return nil, fmt.Errorf("invalid baton id grant: %s", bidStr)
}

// escape colon and slash.
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
