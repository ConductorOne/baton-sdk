package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantsRevoked returns a GrantsRevoked annotation suitable for appending to
// the annotations slice returned from a Revoke provisioning call.
//
// grants are the grants removed for the revoke principal. Each grant's
// entitlement id selects the binding. ConductorOne does not parse grant.id.
// Other entitlements on the same resource are left alone.
//
// Typical use:
//
//	annos := annotations.Annotations{}
//	annos.Append(grant.NewGrantsRevoked(removed))
//	return annos, nil
func NewGrantsRevoked(grants ...*v2.Grant) *v2.GrantsRevoked {
	return &v2.GrantsRevoked{
		Grants: grants,
	}
}

// AppendGrantsRevoked appends a GrantsRevoked annotation to the given
// annotations slice and returns the updated slice. Convenience wrapper around
// NewGrantsRevoked for the common case where the caller is building a response
// annotations slice inline.
func AppendGrantsRevoked(annos annotations.Annotations, grants ...*v2.Grant) annotations.Annotations {
	annos.Append(NewGrantsRevoked(grants...))
	return annos
}
