package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewAdditionalGrantsRevoked returns an AdditionalGrantsRevoked annotation
// suitable for appending to the annotations slice returned from a Revoke
// provisioning call.
//
// revokedGrantIDs are the IDs of additional grants that the connector revoked
// while servicing the Revoke request. ConductorOne marks them revoked without
// dispatching a separate Revoke RPC for each.
//
// Typical use:
//
//	annos := annotations.Annotations{}
//	annos.Append(grant.NewAdditionalGrantsRevoked(hasAccessGrant.GetId()))
//	return annos, nil
func NewAdditionalGrantsRevoked(revokedGrantIDs ...string) *v2.AdditionalGrantsRevoked {
	return &v2.AdditionalGrantsRevoked{
		RevokedGrantIds: revokedGrantIDs,
	}
}

// AppendAdditionalGrantsRevoked appends an AdditionalGrantsRevoked annotation to
// the given annotations slice and returns the updated slice. Convenience wrapper
// around NewAdditionalGrantsRevoked for the common case where the caller is
// building a response annotations slice inline.
func AppendAdditionalGrantsRevoked(annos annotations.Annotations, revokedGrantIDs ...string) annotations.Annotations {
	annos.Append(NewAdditionalGrantsRevoked(revokedGrantIDs...))
	return annos
}
