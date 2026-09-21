package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantsRevoked returns a GrantsRevoked annotation suitable for appending to
// the annotations slice returned from a Revoke provisioning call.
//
// resourceIDs are the resources whose grants for the revoke principal were
// removed as a side effect. ConductorOne revokes those bindings without
// dispatching a separate Revoke RPC. The resources themselves are not deleted.
//
// Typical use:
//
//	annos := annotations.Annotations{}
//	annos.Append(grant.NewGrantsRevoked(roleID))
//	return annos, nil
func NewGrantsRevoked(resourceIDs ...*v2.ResourceId) *v2.GrantsRevoked {
	return &v2.GrantsRevoked{
		ResourceIds: resourceIDs,
	}
}

// AppendGrantsRevoked appends a GrantsRevoked annotation to the given
// annotations slice and returns the updated slice. Convenience wrapper around
// NewGrantsRevoked for the common case where the caller is building a response
// annotations slice inline.
func AppendGrantsRevoked(annos annotations.Annotations, resourceIDs ...*v2.ResourceId) annotations.Annotations {
	annos.Append(NewGrantsRevoked(resourceIDs...))
	return annos
}
