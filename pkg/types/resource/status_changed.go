package resource

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewResourceStatusChanged returns a ResourceStatusChanged annotation suitable for appending
// to the annotations slice returned from a Grant or Revoke provisioning call.
func NewResourceStatusChanged(resourceId *v2.ResourceId, status v2.Status_ResourceStatus, details string) *v2.ResourceStatusChanged {
	return &v2.ResourceStatusChanged{
		// the id of the resource whose status changed in the downstream system as a side effect of the operation
		ResourceId: resourceId,
		Status: &v2.Status{
			Status:  status,
			Details: details,
		},
	}
}

func AppendResourceStatusChanged(annos annotations.Annotations, resourceId *v2.ResourceId, status v2.Status_ResourceStatus, details string) annotations.Annotations {
	annos.Append(NewResourceStatusChanged(resourceId, status, details))
	return annos
}
