package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantsRevoked returns a GrantsRevoked annotation suitable for appending to
// the annotations slice returned from a Revoke provisioning call.
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
