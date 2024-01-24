package resource

import (
	"fmt"
	"strconv"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
)

type ResourceOption func(*v2.Resource) error

func WithAnnotation(msgs ...proto.Message) ResourceOption {
	return func(r *v2.Resource) error {
		annos := annotations.Annotations(r.Annotations)
		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			annos.Append(msg)
		}
		r.Annotations = annos

		return nil
	}
}

func WithExternalID(externalID *v2.ExternalId) ResourceOption {
	return func(r *v2.Resource) error {
		r.ExternalId = externalID
		return nil
	}
}

func WithParentResourceID(parentResourceID *v2.ResourceId) ResourceOption {
	return func(r *v2.Resource) error {
		r.ParentResourceId = parentResourceID

		return nil
	}
}

func WithDescription(description string) ResourceOption {
	return func(r *v2.Resource) error {
		r.Description = description

		return nil
	}
}

func WithUserTrait(opts ...UserTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		var err error
		ut := &v2.UserTrait{}

		annos := annotations.Annotations(r.Annotations)

		picked, err := annos.Pick(ut)
		if err != nil {
			return err
		}
		if picked {
			// We found an existing user trait, so we want to update it in place
			for _, o := range opts {
				err = o(ut)
				if err != nil {
					return err
				}
			}
		} else {
			// No existing user trait found, so create a new one with the provided options
			ut, err = NewUserTrait(opts...)
			if err != nil {
				return err
			}
		}

		annos.Update(ut)
		r.Annotations = annos
		return nil
	}
}

func WithGroupTrait(opts ...GroupTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		ut := &v2.GroupTrait{}

		annos := annotations.Annotations(r.Annotations)
		_, err := annos.Pick(ut)
		if err != nil {
			return err
		}

		for _, o := range opts {
			err = o(ut)
			if err != nil {
				return err
			}
		}

		annos.Update(ut)
		r.Annotations = annos
		return nil
	}
}

func WithRoleTrait(opts ...RoleTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		rt := &v2.RoleTrait{}

		annos := annotations.Annotations(r.Annotations)
		_, err := annos.Pick(rt)
		if err != nil {
			return err
		}

		for _, o := range opts {
			err := o(rt)
			if err != nil {
				return err
			}
		}

		annos.Update(rt)
		r.Annotations = annos

		return nil
	}
}

func WithAppTrait(opts ...AppTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		at := &v2.AppTrait{}

		annos := annotations.Annotations(r.Annotations)
		_, err := annos.Pick(at)
		if err != nil {
			return err
		}

		for _, o := range opts {
			err := o(at)
			if err != nil {
				return err
			}
		}

		annos.Update(at)
		r.Annotations = annos

		return nil
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
func NewResource(name string, resourceType *v2.ResourceType, objectID interface{}, resourceOptions ...ResourceOption) (*v2.Resource, error) {
	rID, err := NewResourceID(resourceType, objectID)
	if err != nil {
		return nil, err
	}

	resource := &v2.Resource{
		Id:          rID,
		DisplayName: name,
	}

	for _, resourceOption := range resourceOptions {
		err = resourceOption(resource)
		if err != nil {
			return nil, err
		}
	}
	return resource, nil
}

// NewUserResource returns a new resource instance with a configured user trait.
// The trait is configured with the provided email address and profile and status set to enabled.
func NewUserResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	userTraitOpts []UserTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	opts = append(opts, WithUserTrait(userTraitOpts...))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// NewGroupResource returns a new resource instance with a configured group trait.
// The trait is configured with the provided profile.
func NewGroupResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	groupTraitOpts []GroupTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	opts = append(opts, WithGroupTrait(groupTraitOpts...))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// NewRoleResource returns a new resource instance with a configured role trait.
// The trait is configured with the provided profile.
func NewRoleResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	roleTraitOpts []RoleTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	opts = append(opts, WithRoleTrait(roleTraitOpts...))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// NewAppResource returns a new resource instance with a configured app trait.
// The trait is configured with the provided helpURL and profile.
func NewAppResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	appTraitOpts []AppTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	opts = append(opts, WithAppTrait(appTraitOpts...))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}
