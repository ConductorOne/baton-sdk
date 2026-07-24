package resource

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewResourceDeleted returns a ResourceDeleted annotation suitable for appending
// to the annotations slice returned from a Grant or Revoke provisioning call.
func NewResourceDeleted(resourceId *v2.ResourceId) *v2.ResourceDeleted {
	return &v2.ResourceDeleted{
		// the id of the resource that was deleted in the downstream system as a side effect of the operation
		ResourceId: resourceId,
	}
}

func AppendResourceDeleted(annos annotations.Annotations, resourceId *v2.ResourceId) annotations.Annotations {
	annos.Append(NewResourceDeleted(resourceId))
	return annos
}
