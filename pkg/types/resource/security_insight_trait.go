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

// WithRiskScore sets the insight type to risk score with the given value.
func WithRiskScore(value string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		if value == "" {
			return fmt.Errorf("risk score value cannot be empty")
		}
		t.SetRiskScore(&v2.RiskScore{
			Value: value,
		})
		return nil
	}
}

// WithIssue sets the insight type to issue with the given value.
func WithIssue(value string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		if value == "" {
			return fmt.Errorf("issue value cannot be empty")
		}
		issue := &v2.Issue{
			Value: value,
		}
		t.SetIssue(issue)
		return nil
	}
}

// WithIssueSeverity sets or updates the severity on an issue insight.
// This should be used after WithIssue or on an existing issue insight.
func WithIssueSeverity(severity string) SecurityInsightTraitOption {
	return func(t *v2.SecurityInsightTrait) error {
		issue := t.GetIssue()
		if issue == nil {
			return fmt.Errorf("cannot set severity: insight is not an issue type (use WithIssue first)")
		}
		issue.SetSeverity(severity)
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

// NewSecurityInsightTrait creates a new SecurityInsightTrait with the given options.
// You must provide either WithRiskScore or WithIssue to set the insight type.
//
// Example usage:
//
//	trait, err := NewSecurityInsightTrait(
//	    WithIssue("CVE-2024-1234", "Critical"),
//	    WithInsightUserTarget("user@example.com"))
//
//	trait, err := NewSecurityInsightTrait(
//	    WithRiskScore("85"),
//	    WithInsightResourceTarget(resourceId))
func NewSecurityInsightTrait(opts ...SecurityInsightTraitOption) (*v2.SecurityInsightTrait, error) {
	trait := &v2.SecurityInsightTrait{
		ObservedAt: timestamppb.Now(),
	}

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	// Validate that an insight type was set
	if trait.GetRiskScore() == nil && trait.GetIssue() == nil {
		return nil, fmt.Errorf("insight type must be set (use WithRiskScore or WithIssue)")
	}

	if trait.GetTarget() == nil {
		return nil, fmt.Errorf("target must be set (use WithInsightUserTarget, WithInsightResourceTarget, WithInsightExternalResourceTarget, or WithInsightAppUserTarget)")
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
// The insight type (risk score or issue) must be set via the provided options.
// If the resource already has a SecurityInsightTrait, it will be updated with the provided options.
// If not, a new trait will be created.
//
// Example usage:
//
//	resource, err := NewResource(
//	    "Security Finding",
//	    resourceType,
//	    objectID,
//	    WithSecurityInsightTrait(
//	        WithIssue("CVE-2024-1234", "Critical"),
//	        WithInsightUserTarget("user@example.com")))
func WithSecurityInsightTrait(opts ...SecurityInsightTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.SecurityInsightTrait{}
		annos := annotations.Annotations(r.GetAnnotations())
		existing, err := annos.Pick(t)
		if err != nil {
			return err
		}

		if !existing {
			// Creating a new trait - set default observation time
			t.SetObservedAt(timestamppb.Now())
		}

		for _, o := range opts {
			if err := o(t); err != nil {
				return err
			}
		}

		// Validate that an insight type was set
		if t.GetRiskScore() == nil && t.GetIssue() == nil {
			return fmt.Errorf("insight type must be set (use WithRiskScore or WithIssue)")
		}

		annos.Update(t)
		r.SetAnnotations(annos)

		return nil
	}
}

// NewSecurityInsightResource creates a security insight resource with the given trait options.
// This is a flexible constructor that uses the options pattern to configure all aspects of the insight.
//
// Example usage:
//
//	// Risk score for a user
//	resource, err := NewSecurityInsightResource(
//	    "User Risk Score",
//	    securityInsightResourceType,
//	    "user-123",
//	    WithRiskScore("85"),
//	    WithInsightUserTarget("user@example.com"))
//
//	// Issue with severity for a resource
//	resource, err := NewSecurityInsightResource(
//	    "Critical Vulnerability",
//	    securityInsightResourceType,
//	    "vuln-456",
//	    WithIssue("CVE-2024-1234", "Critical"),
//	    WithInsightResourceTarget(resourceId))
//
//	// Issue for external resource with custom observation time
//	resource, err := NewSecurityInsightResource(
//	    "AWS Security Finding",
//	    securityInsightResourceType,
//	    "finding-789",
//	    WithIssue("S3 bucket publicly accessible"),
//	    WithIssueSeverity("High"),
//	    WithInsightExternalResourceTarget("arn:aws:s3:::my-bucket", "aws"),
//	    WithInsightObservedAt(time.Now()))
func NewSecurityInsightResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	traitOpts ...SecurityInsightTraitOption,
) (*v2.Resource, error) {
	trait, err := NewSecurityInsightTrait(traitOpts...)
	if err != nil {
		return nil, err
	}

	return NewResource(name, resourceType, objectID, WithAnnotation(trait))
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

// --- Insight type checkers ---

// IsRiskScore returns true if the insight is a risk score.
func IsRiskScore(trait *v2.SecurityInsightTrait) bool {
	return trait.GetRiskScore() != nil
}

// IsIssue returns true if the insight is an issue.
func IsIssue(trait *v2.SecurityInsightTrait) bool {
	return trait.GetIssue() != nil
}

// GetInsightValue returns the value of the insight (either risk score or issue).
func GetInsightValue(trait *v2.SecurityInsightTrait) string {
	if rs := trait.GetRiskScore(); rs != nil {
		return rs.GetValue()
	}
	if issue := trait.GetIssue(); issue != nil {
		return issue.GetValue()
	}
	return ""
}

// GetIssueSeverity returns the severity of an issue insight, or empty string if not set or not an issue.
func GetIssueSeverity(trait *v2.SecurityInsightTrait) string {
	if issue := trait.GetIssue(); issue != nil {
		return issue.GetSeverity()
	}
	return ""
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
