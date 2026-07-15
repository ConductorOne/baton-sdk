package resource

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewResourceDeleted returns a ResourceDeleted annotation suitable for appending
// to the annotations slice returned from a Grant or Revoke provisioning call.
//
// resourceId is the resource that was deleted in the downstream system as a
// side effect of the operation. For a Revoke this is normally the grant's
// principal (e.g. revoking a user's last role caused the app to delete the
// user). ConductorOne marks the corresponding account deleted without waiting
// for the next full sync.
//
// Typical use:
//
//	annos := annotations.Annotations{}
//	annos.Append(resource.NewResourceDeleted(grant.GetPrincipal().GetId()))
//	return annos, nil
func NewResourceDeleted(resourceId *v2.ResourceId) *v2.ResourceDeleted {
	return &v2.ResourceDeleted{
		ResourceId: resourceId,
	}
}

// AppendResourceDeleted appends a ResourceDeleted annotation to the given
// annotations slice and returns the updated slice. Convenience wrapper around
// NewResourceDeleted for the common case where the caller is building a
// response annotations slice inline.
func AppendResourceDeleted(annos annotations.Annotations, resourceId *v2.ResourceId) annotations.Annotations {
	annos.Append(NewResourceDeleted(resourceId))
	return annos
}
