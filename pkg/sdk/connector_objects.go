package sdk

import (
	"fmt"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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
func NewResourceID(resourceType *v2.ResourceType, objectID interface{}) (*v2.ResourceId, error) {
	id, err := convertIDToString(objectID)
	if err != nil {
		return nil, err
	}

	return &v2.ResourceId{
		ResourceType: resourceType.Id,
		Resource:     id,
	}, nil
}

// NewResource returns a new resource instance with no traits.
func NewResource(name string, resourceType *v2.ResourceType, parentResourceID *v2.ResourceId, objectID interface{}) (*v2.Resource, error) {
	rID, err := NewResourceID(resourceType, objectID)
	if err != nil {
		return nil, err
	}

	return &v2.Resource{
		Id:               rID,
		ParentResourceId: parentResourceID,
		DisplayName:      name,
	}, nil
}

// NewUserResource returns a new resource instance with a configured user trait.
// The trait is configured with the provided email address and profile and status set to enabled.
func NewUserResource(
	name string,
	resourceType *v2.ResourceType,
	parentResourceID *v2.ResourceId,
	objectID interface{},
	primaryEmail string,
	profile map[string]interface{},
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID)
	if err != nil {
		return nil, err
	}

	userTrait, err := NewUserTrait(primaryEmail, v2.UserTrait_Status_STATUS_ENABLED, profile)
	if err != nil {
		return nil, err
	}

	var annos annotations.Annotations
	annos.Update(userTrait)

	ret.Annotations = annos

	return ret, nil
}

// NewGroupResource returns a new resource instance with a configured group trait.
// The trait is configured with the provided profile.
func NewGroupResource(
	name string,
	resourceType *v2.ResourceType,
	parentResourceID *v2.ResourceId,
	objectID interface{},
	profile map[string]interface{},
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID)
	if err != nil {
		return nil, err
	}

	groupTrait, err := NewGroupTrait(profile)
	if err != nil {
		return nil, err
	}

	var annos annotations.Annotations
	annos.Update(groupTrait)

	ret.Annotations = annos

	return ret, nil
}

// NewRoleResource returns a new resource instance with a configured role trait.
// The trait is configured with the provided profile.
func NewRoleResource(
	name string,
	resourceType *v2.ResourceType,
	parentResourceID *v2.ResourceId,
	objectID interface{},
	profile map[string]interface{},
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID)
	if err != nil {
		return nil, err
	}

	roleTrait, err := NewRoleTrait(profile)
	if err != nil {
		return nil, err
	}

	var annos annotations.Annotations
	annos.Update(roleTrait)

	ret.Annotations = annos

	return ret, nil
}

// NewAppResource returns a new resource instance with a configured app trait.
// The trait is configured with the provided helpURL and profile.
func NewAppResource(
	name string,
	resourceType *v2.ResourceType,
	parentResourceID *v2.ResourceId,
	objectID interface{},
	helpURL string,
	profile map[string]interface{},
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID)
	if err != nil {
		return nil, err
	}

	appTrait, err := NewAppTrait(helpURL, profile)
	if err != nil {
		return nil, err
	}

	var annos annotations.Annotations
	annos.Update(appTrait)

	ret.Annotations = annos

	return ret, nil
}

func NewEntitlementID(resource *v2.Resource, permission string) string {
	return fmt.Sprintf("%s:%s:%s", resource.Id.ResourceType, resource.Id.Resource, permission)
}

func NewAssignmentEntitlement(resource *v2.Resource, name string, grantableTo ...*v2.ResourceType) *v2.Entitlement {
	return &v2.Entitlement{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		GrantableTo: grantableTo,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Resource:    resource,
	}
}

func NewPermissionEntitlement(resource *v2.Resource, name string, grantableTo ...*v2.ResourceType) *v2.Entitlement {
	return &v2.Entitlement{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		GrantableTo: grantableTo,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Resource:    resource,
	}
}

// NewGrant returns a new grant for the given entitlement on the resource for the provided principal resource ID.
func NewGrant(resource *v2.Resource, entitlementName string, principal *v2.ResourceId) *v2.Grant {
	entitlement := &v2.Entitlement{
		Id:       NewEntitlementID(resource, entitlementName),
		Resource: resource,
	}

	return &v2.Grant{
		Entitlement: entitlement,
		Principal:   &v2.Resource{Id: principal},
		Id:          fmt.Sprintf("%s:%s:%s", entitlement.Id, principal.ResourceType, principal.Resource),
	}
}
