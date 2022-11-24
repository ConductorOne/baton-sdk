package sdk

import (
	"fmt"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func convertIDToString(id interface{}) (string, error) {
	var resourceID string
	switch objID := id.(type) {
	case string:
		resourceID = objID
	case int64:
		resourceID = strconv.FormatInt(objID, 10)
	case int:
		resourceID = strconv.Itoa(objID)
	default:
		return "", fmt.Errorf("unexpected type for id")
	}

	return resourceID, nil
}

// NewResourceID returns a new resource ID given a resource type parent ID, and arbitrary object ID.
func NewResourceID(resourceType *v2.ResourceType, parentResourceID *v2.ResourceId, objectID interface{}) (*v2.ResourceId, error) {
	id, err := convertIDToString(objectID)
	if err != nil {
		return nil, err
	}

	if parentResourceID != nil {
		id = fmt.Sprintf("%s:%s", parentResourceID.Resource, id)
	}

	return &v2.ResourceId{
		ResourceType: resourceType.Id,
		Resource:     id,
	}, nil
}

// NewEntitlementID returns a valid entitlement ID given a resource and permission.
func NewEntitlementID(resource *v2.Resource, permission string) string {
	return fmt.Sprintf("%s:%s:%s", resource.Id.ResourceType, resource.Id.Resource, permission)
}

// NewGrantID returns a valid grant ID given an entitlement and principal resource.
func NewGrantID(entitlement *v2.Entitlement, principal *v2.Resource) string {
	return fmt.Sprintf("%s:%s:%s", entitlement.Id, principal.Id.ResourceType, principal.Id.Resource)
}
