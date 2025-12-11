package resource

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// SecurityInsightTraitOption is a functional option for configuring a SecurityInsightTrait.
type SecurityInsightTraitOption func(*v2.SecurityInsightTrait) error

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

// WithInsightContext sets additional context for the insight (deep links, remediation, etc.).
func WithInsightContext(ctx map[string]interface{}) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		s, err := structpb.NewStruct(ctx)
		if err != nil {
			return fmt.Errorf("failed to create context struct: %w", err)
		}
		t.SetContext(s)
		return nil
	}
}

// WithInsightUserTarget sets the user target (by email) for the insight.
func WithInsightUserTarget(email string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		t.SetUser(v2.SecurityInsightTrait_UserTarget_builder{
			Email: email,
		}.Build())
		return nil
	}
}

// WithInsightResourceTarget sets the resource target (by external ID) for the insight.
func WithInsightResourceTarget(externalId string, appHint string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		t.SetResource(v2.SecurityInsightTrait_ResourceTarget_builder{
			ExternalId: externalId,
			AppHint:    appHint,
		}.Build())
		return nil
	}
}

// NewSecurityInsightTrait creates a new SecurityInsightTrait with the given insight type and options.
func NewSecurityInsightTrait(insightType string, opts ...SecurityInsightTraitOption) (*v2.SecurityInsightTrait, error) {
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

// WithSecurityInsightTrait adds a SecurityInsightTrait annotation to a resource.
func WithSecurityInsightTrait(opts ...SecurityInsightTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.SecurityInsightTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
		_, err := annos.Pick(t)
		if err != nil {
			return err
		}

		for _, o := range opts {
			err := o(t)
			if err != nil {
				return err
			}
		}

		annos.Update(t)
		r.SetAnnotations(annos)

		return nil
	}
}

// NewSecurityInsightResourceType creates a new ResourceType with the TRAIT_SECURITY_INSIGHT trait.
func NewSecurityInsightResourceType(name string) *v2.ResourceType {
	return NewResourceType(name, []v2.ResourceType_Trait{v2.ResourceType_TRAIT_SECURITY_INSIGHT})
}

// NewUserSecurityInsightResource creates a security insight resource targeting a user by email.
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
	// Create the trait with the insight type, value, and user target
	allTraitOpts := append([]SecurityInsightTraitOption{
		WithInsightValue(value),
		WithInsightUserTarget(userEmail),
	}, traitOpts...)

	trait, err := NewSecurityInsightTrait(insightType, allTraitOpts...)
	if err != nil {
		return nil, err
	}

	// Add the trait annotation
	opts = append(opts, WithAnnotation(trait))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// NewResourceSecurityInsightResource creates a security insight resource targeting a resource by external ID.
func NewResourceSecurityInsightResource(
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
	// Create the trait with the insight type, value, and resource target
	allTraitOpts := append([]SecurityInsightTraitOption{
		WithInsightValue(value),
		WithInsightResourceTarget(targetExternalId, targetAppHint),
	}, traitOpts...)

	trait, err := NewSecurityInsightTrait(insightType, allTraitOpts...)
	if err != nil {
		return nil, err
	}

	// Add the trait annotation
	opts = append(opts, WithAnnotation(trait))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
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

// GetUserTargetEmail returns the user email from a SecurityInsightTrait, or empty string if not a user target.
func GetUserTargetEmail(trait *v2.SecurityInsightTrait) string {
	if user := trait.GetUser(); user != nil {
		return user.GetEmail()
	}
	return ""
}

// GetResourceTargetExternalId returns the external ID from a SecurityInsightTrait, or empty string if not a resource target.
func GetResourceTargetExternalId(trait *v2.SecurityInsightTrait) string {
	if resource := trait.GetResource(); resource != nil {
		return resource.GetExternalId()
	}
	return ""
}

// GetResourceTargetAppHint returns the app hint from a SecurityInsightTrait, or empty string if not a resource target.
func GetResourceTargetAppHint(trait *v2.SecurityInsightTrait) string {
	if resource := trait.GetResource(); resource != nil {
		return resource.GetAppHint()
	}
	return ""
}

// IsUserTarget returns true if the insight targets a user.
func IsUserTarget(trait *v2.SecurityInsightTrait) bool {
	return trait.GetUser() != nil
}

// IsResourceTarget returns true if the insight targets a resource.
func IsResourceTarget(trait *v2.SecurityInsightTrait) bool {
	return trait.GetResource() != nil
}
