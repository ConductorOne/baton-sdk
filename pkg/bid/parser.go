package bid

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func Parse(bidStr string) (BID, error) {
	rs := &bidScanner{str: bidStr}

	tType, val, err := rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != literal || val != BidPrefix {
		return nil, NewBidParseError(rs, "invalid prefix: '%s'", val)
	}

	tType, val, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != colon || val != ":" {
		return nil, NewBidParseError(rs, "invalid prefix: '%s'", val)
	}

	tType, val, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != literal {
		return nil, NewBidParseError(rs, "invalid prefix: '%s'", val)
	}
	tType, _, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != colon {
		return nil, NewBidParseError(rs, "invalid prefix: '%s'", val)
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
		return nil, NewBidParseError(rs, "invalid type: '%s'", val)
	}
	if err != nil {
		return nil, err
	}

	tType, _, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != eof {
		return nil, NewBidParseError(rs, "invalid baton id: '%s'", bidStr)
	}
	return rv, nil
}

func parseResourcePart(rs *bidScanner) (*v2.Resource, error) {
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
			return nil, NewBidParseError(rs, "invalid resource part")
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
			return nil, NewBidParseError(rs, "invalid resource part: '%s'", token)
		}

		// Resource id or parent resource id
		tType, token, err = rs.NextToken()
		if err != nil {
			return nil, err
		}
		if tType != literal {
			return nil, NewBidParseError(rs, "invalid resource part: '%s'", token)
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
			return nil, NewBidParseError(rs, "invalid resource part")
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

	return nil, NewBidParseError(rs, "invalid resource part")
}

func ParseResourceBid(bidStr string) (*v2.Resource, error) {
	ret, err := Parse(bidStr)
	if err != nil {
		return nil, err
	}
	if rv, ok := ret.(*v2.Resource); ok {
		return rv, nil
	}
	return nil, NewBidParseError(nil, "invalid baton id resource: %s", bidStr)
}

func parseEntitlementPart(rs *bidScanner) (*v2.Entitlement, error) {
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
		return nil, NewBidParseError(rs, "invalid baton id entitlement part: %s", val)
	}
	// Get entitlement slug
	tType, val, err = rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != literal {
		return nil, NewBidParseError(rs, "invalid baton id entitlement part: %s", val)
	}

	return &v2.Entitlement{
		Slug:     val,
		Resource: resource,
	}, nil
}

func ParseEntitlementBid(bidStr string) (*v2.Entitlement, error) {
	ret, err := Parse(bidStr)
	if err != nil {
		return nil, err
	}
	if rv, ok := ret.(*v2.Entitlement); ok {
		return rv, nil
	}
	return nil, NewBidParseError(nil, "invalid baton id entitlement: %s", bidStr)
}

func parseGrantPart(rs *bidScanner) (*v2.Grant, error) {
	entitlement, err := parseEntitlementPart(rs)
	if err != nil {
		return nil, err
	}

	tType, val, err := rs.NextToken()
	if err != nil {
		return nil, err
	}
	if tType != colon {
		return nil, NewBidParseError(nil, "invalid baton id grant part: %s", val)
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
	ret, err := Parse(bidStr)
	if err != nil {
		return nil, err
	}
	if rv, ok := ret.(*v2.Grant); ok {
		return rv, nil
	}
	return nil, NewBidParseError(nil, "invalid baton id grant: %s", bidStr)
}
