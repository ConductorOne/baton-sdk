package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantsRevoked returns a GrantsRevoked annotation suitable for appending to
// the annotations slice returned from a Revoke provisioning call.
//
// entitlementIDs are entitlements whose grants for the revoke principal were
// removed as a side effect. Each id has the form
// "{resource_type}:{resource_id}:{slug}". ConductorOne revokes those bindings
// without dispatching a separate Revoke RPC. Other entitlements on the same
// resource are left alone.
//
// Typical use:
//
//	annos := annotations.Annotations{}
//	annos.Append(grant.NewGrantsRevoked("role:role-2:member"))
//	return annos, nil
func NewGrantsRevoked(entitlementIDs ...string) *v2.GrantsRevoked {
	return &v2.GrantsRevoked{
		EntitlementIds: entitlementIDs,
	}
}

// AppendGrantsRevoked appends a GrantsRevoked annotation to the given
// annotations slice and returns the updated slice. Convenience wrapper around
// NewGrantsRevoked for the common case where the caller is building a response
// annotations slice inline.
func AppendGrantsRevoked(annos annotations.Annotations, entitlementIDs ...string) annotations.Annotations {
	annos.Append(NewGrantsRevoked(entitlementIDs...))
	return annos
}
