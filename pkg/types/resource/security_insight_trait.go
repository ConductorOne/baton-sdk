package resource

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// SecurityInsightTraitOption is a functional option for configuring a SecurityInsightTrait.
type SecurityInsightTraitOption func(*v2.SecurityInsightTrait) error

// WithInsightType sets the insight type. This is typically set via NewSecurityInsightTrait,
// but can be used to override or update the type on an existing trait.
func WithInsightType(insightType string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		if insightType == "" {
			return fmt.Errorf("insight type cannot be empty")
		}
		t.SetInsightType(insightType)
		return nil
	}
}

// WithInsightValue sets the value of the security insight.
func WithInsightValue(value string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		t.SetValue(value)
		return nil
	}
}

// WithInsightObservedAt sets the observation timestamp for the insight.
func WithInsightObservedAt(observedAt time.Time) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		t.SetObservedAt(timestamppb.New(observedAt))
		return nil
	}
}

// WithInsightUserTarget sets the user target (by email) for the insight.
// Use this when the insight should be resolved to a C1 User by Uplift.
func WithInsightUserTarget(email string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		t.SetUser(v2.SecurityInsightTrait_UserTarget_builder{
			Email: email,
		}.Build())
		return nil
	}
}

// WithInsightResourceTarget sets a direct resource reference for the insight.
// Use this when the connector knows the actual resource (synced by this connector).
func WithInsightResourceTarget(resourceId *v2.ResourceId) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		t.SetResourceId(resourceId)
		return nil
	}
}

// WithInsightExternalResourceTarget sets the external resource target for the insight.
// Use this when the connector only has an external ID (e.g., ARN) and needs Uplift to resolve it.
func WithInsightExternalResourceTarget(externalId string, appHint string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		t.SetExternalResource(v2.SecurityInsightTrait_ExternalResourceTarget_builder{
			ExternalId: externalId,
			AppHint:    appHint,
		}.Build())
		return nil
	}
}

// NewSecurityInsightTrait creates a new SecurityInsightTrait with the given insight type and options.
func NewSecurityInsightTrait(insightType string, opts ...SecurityInsightTraitOption) (*v2.SecurityInsightTrait, error) {
	if insightType == "" {
		return nil, fmt.Errorf("insight type cannot be empty")
	}

	trait := v2.SecurityInsightTrait_builder{
		InsightType: insightType,
		ObservedAt:  timestamppb.Now(),
	}.Build()

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	return trait, nil
}

// GetSecurityInsightTrait attempts to return the SecurityInsightTrait from a resource's annotations.
func GetSecurityInsightTrait(resource *v2.Resource) (*v2.SecurityInsightTrait, error) {
	ret := &v2.SecurityInsightTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("security insight trait was not found on resource")
	}

	return ret, nil
}

// WithSecurityInsightTrait adds or updates a SecurityInsightTrait annotation on a resource.
// The insightType parameter is required to ensure the trait is always valid.
// If the resource already has a SecurityInsightTrait, it will be updated with the provided options.
// If not, a new trait will be created with the given insightType.
func WithSecurityInsightTrait(insightType string, opts ...SecurityInsightTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.SecurityInsightTrait{}
		annos := annotations.Annotations(r.GetAnnotations())
		existing, err := annos.Pick(t)
		if err != nil {
			return err
		}

		if !existing {
			// Creating a new trait - insightType is required
			if insightType == "" {
				return fmt.Errorf("insight type is required when creating a new security insight trait")
			}
			t.SetInsightType(insightType)
		} else if insightType != "" {
			// Updating existing trait with a new type
			t.SetInsightType(insightType)
		}
		// If existing and insightType is empty, keep the existing type

		for _, o := range opts {
			if err := o(t); err != nil {
				return err
			}
		}

		annos.Update(t)
		r.SetAnnotations(annos)

		return nil
	}
}

// NewUserSecurityInsightResource creates a security insight resource targeting a user by email.
// Use this when the insight should be resolved to a C1 User by Uplift.
func NewUserSecurityInsightResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	insightType string,
	value string,
	userEmail string,
	traitOpts []SecurityInsightTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	allTraitOpts := append([]SecurityInsightTraitOption{
		WithInsightValue(value),
		WithInsightUserTarget(userEmail),
	}, traitOpts...)

	trait, err := NewSecurityInsightTrait(insightType, allTraitOpts...)
	if err != nil {
		return nil, err
	}

	opts = append(opts, WithAnnotation(trait))

	return NewResource(name, resourceType, objectID, opts...)
}

// NewResourceSecurityInsightResource creates a security insight resource with a direct resource reference.
// Use this when the connector knows the actual resource (synced by this connector).
func NewResourceSecurityInsightResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	insightType string,
	value string,
	targetResourceId *v2.ResourceId,
	traitOpts []SecurityInsightTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	allTraitOpts := append([]SecurityInsightTraitOption{
		WithInsightValue(value),
		WithInsightResourceTarget(targetResourceId),
	}, traitOpts...)

	trait, err := NewSecurityInsightTrait(insightType, allTraitOpts...)
	if err != nil {
		return nil, err
	}

	opts = append(opts, WithAnnotation(trait))

	return NewResource(name, resourceType, objectID, opts...)
}

// NewExternalResourceSecurityInsightResource creates a security insight resource targeting an external resource.
// Use this when the connector only has an external ID (e.g., ARN) and needs Uplift to resolve it.
func NewExternalResourceSecurityInsightResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	insightType string,
	value string,
	targetExternalId string,
	targetAppHint string,
	traitOpts []SecurityInsightTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	allTraitOpts := append([]SecurityInsightTraitOption{
		WithInsightValue(value),
		WithInsightExternalResourceTarget(targetExternalId, targetAppHint),
	}, traitOpts...)

	trait, err := NewSecurityInsightTrait(insightType, allTraitOpts...)
	if err != nil {
		return nil, err
	}

	opts = append(opts, WithAnnotation(trait))

	return NewResource(name, resourceType, objectID, opts...)
}

// IsSecurityInsightResource checks if a resource type has the TRAIT_SECURITY_INSIGHT trait.
func IsSecurityInsightResource(resourceType *v2.ResourceType) bool {
	for _, trait := range resourceType.GetTraits() {
		if trait == v2.ResourceType_TRAIT_SECURITY_INSIGHT {
			return true
		}
	}
	return false
}

// --- Target type checkers ---

// IsUserTarget returns true if the insight targets a user.
func IsUserTarget(trait *v2.SecurityInsightTrait) bool {
	return trait.GetUser() != nil
}

// IsResourceTarget returns true if the insight has a direct resource reference.
func IsResourceTarget(trait *v2.SecurityInsightTrait) bool {
	return trait.GetResourceId() != nil
}

// IsExternalResourceTarget returns true if the insight targets an external resource.
func IsExternalResourceTarget(trait *v2.SecurityInsightTrait) bool {
	return trait.GetExternalResource() != nil
}

// --- Target data extractors ---

// GetUserTargetEmail returns the user email from a SecurityInsightTrait, or empty string if not a user target.
func GetUserTargetEmail(trait *v2.SecurityInsightTrait) string {
	if user := trait.GetUser(); user != nil {
		return user.GetEmail()
	}
	return ""
}

// GetResourceTarget returns the ResourceId from a SecurityInsightTrait, or nil if not a resource target.
func GetResourceTarget(trait *v2.SecurityInsightTrait) *v2.ResourceId {
	return trait.GetResourceId()
}

// GetExternalResourceTargetId returns the external ID from a SecurityInsightTrait, or empty string if not an external resource target.
func GetExternalResourceTargetId(trait *v2.SecurityInsightTrait) string {
	if ext := trait.GetExternalResource(); ext != nil {
		return ext.GetExternalId()
	}
	return ""
}

// GetExternalResourceTargetAppHint returns the app hint from a SecurityInsightTrait, or empty string if not an external resource target.
func GetExternalResourceTargetAppHint(trait *v2.SecurityInsightTrait) string {
	if ext := trait.GetExternalResource(); ext != nil {
		return ext.GetAppHint()
	}
	return ""
}
