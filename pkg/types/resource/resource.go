package resource

import (
	"fmt"
	"strconv"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	sdk "github.com/conductorone/baton-sdk/pkg/sdk"
	"google.golang.org/protobuf/proto"
)

type ResourceOption func(*v2.Resource)

func WithAnnotation(msgs ...proto.Message) ResourceOption {
	return func(r *v2.Resource) {
		annos := annotations.Annotations(r.Annotations)
		for _, msg := range msgs {
			annos.Append(msg)
		}
		r.Annotations = annos
	}
}

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

// NewResourceType returns a new *v2.ResourceType where the id is the name lowercased with spaces replaced by hyphens.
func NewResourceType(name string, requiredTraits []v2.ResourceType_Trait, msgs ...proto.Message) *v2.ResourceType {
	id := strings.ReplaceAll(strings.ToLower(name), " ", "-")

	var annos annotations.Annotations
	for _, msg := range msgs {
		annos.Append(msg)
	}

	return &v2.ResourceType{
		Id:          id,
		DisplayName: name,
		Traits:      requiredTraits,
		Annotations: annos,
	}
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
func NewResource(name string, resourceType *v2.ResourceType, parentResourceID *v2.ResourceId, objectID interface{}, resourceOptions ...ResourceOption) (*v2.Resource, error) {
	rID, err := NewResourceID(resourceType, objectID)
	if err != nil {
		return nil, err
	}

	resource := &v2.Resource{
		Id:               rID,
		ParentResourceId: parentResourceID,
		DisplayName:      name,
	}
	for _, resourceOption := range resourceOptions {
		resourceOption(resource)
	}
	return resource, nil
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
	resourceOptions ...ResourceOption,
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID, resourceOptions...)
	if err != nil {
		return nil, err
	}

	userTrait, err := sdk.NewUserTrait(primaryEmail, v2.UserTrait_Status_STATUS_ENABLED, profile)
	if err != nil {
		return nil, err
	}

	annos := annotations.Annotations(ret.Annotations)
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
	resourceOptions ...ResourceOption,
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID, resourceOptions...)
	if err != nil {
		return nil, err
	}

	groupTrait, err := sdk.NewGroupTrait(profile)
	if err != nil {
		return nil, err
	}

	annos := annotations.Annotations(ret.Annotations)
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
	resourceOptions ...ResourceOption,
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID, resourceOptions...)
	if err != nil {
		return nil, err
	}

	roleTrait, err := sdk.NewRoleTrait(profile)
	if err != nil {
		return nil, err
	}

	annos := annotations.Annotations(ret.Annotations)
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
	resourceOptions ...ResourceOption,
) (*v2.Resource, error) {
	ret, err := NewResource(name, resourceType, parentResourceID, objectID, resourceOptions...)
	if err != nil {
		return nil, err
	}

	appTrait, err := sdk.NewAppTrait(helpURL, profile)
	if err != nil {
		return nil, err
	}

	annos := annotations.Annotations(ret.Annotations)
	annos.Update(appTrait)

	ret.Annotations = annos

	return ret, nil
}
