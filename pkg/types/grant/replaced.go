package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantReplaced returns a GrantReplaced annotation suitable for appending to
// the annotations slice returned from a Grant provisioning call.
//
// replacedGrantID is the ID of the grant that the new grant atomically
// replaced. ConductorOne marks it revoked without dispatching a separate
// Revoke RPC.
//
// Typical use:
//
//	annos := annotations.Annotations{}
//	annos.Append(grant.NewGrantReplaced(currentGrant.GetId()))
//	return newGrants, annos, nil
func NewGrantReplaced(replacedGrantID string) *v2.GrantReplaced {
	return &v2.GrantReplaced{
		ReplacedGrantId: replacedGrantID,
	}
}

// AppendGrantReplaced appends a GrantReplaced annotation to the given
// annotations slice and returns the updated slice. Convenience wrapper around
// NewGrantReplaced for the common case where the caller is building a response
// annotations slice inline.
func AppendGrantReplaced(annos annotations.Annotations, replacedGrantID string) annotations.Annotations {
	annos.Append(NewGrantReplaced(replacedGrantID))
	return annos
}
