package resource

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrNoAlias = fmt.Errorf("no aliases found for resource")
var ErrEmptyAlias = fmt.Errorf("alias cannot be empty")

type ResourceOption func(*v2.Resource) error

func WithAnnotation(msgs ...proto.Message) ResourceOption {
	return func(r *v2.Resource) error {
		annos := annotations.Annotations(r.GetAnnotations())
		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			annos.Append(msg)
		}
		r.SetAnnotations(annos)

		return nil
	}
}

// WithExternalID: Deprecated. This field is no longer used.
func WithExternalID(externalID *v2.ExternalId) ResourceOption {
	return func(r *v2.Resource) error {
		r.SetExternalId(externalID) //nolint:staticcheck // Deprecated.
		return nil
	}
}

func WithParentResourceID(parentResourceID *v2.ResourceId) ResourceOption {
	return func(r *v2.Resource) error {
		r.SetParentResourceId(parentResourceID)

		return nil
	}
}

func WithDescription(description string) ResourceOption {
	return func(r *v2.Resource) error {
		r.SetDescription(description)

		return nil
	}
}

// WithResourceProfile sets the profile on the resource itself.
func WithResourceProfile(profile map[string]interface{}) ResourceOption {
	return func(r *v2.Resource) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}

		r.SetProfile(p)
		return nil
	}
}

// WithResourceIcon sets the icon on the resource itself.
func WithResourceIcon(assetRef *v2.AssetRef) ResourceOption {
	return func(r *v2.Resource) error {
		r.SetIcon(assetRef)
		return nil
	}
}

// WithResourceStatus sets the status on the resource itself. details may be
// empty.
func WithResourceStatus(resourceStatus v2.Status_ResourceStatus, details string) ResourceOption {
	return func(r *v2.Resource) error {
		r.SetStatus(v2.Status_builder{
			Status:  resourceStatus,
			Details: details,
		}.Build())
		return nil
	}
}

// WithResourceCreatedAt sets the creation time on the resource itself.
func WithResourceCreatedAt(createdAt time.Time) ResourceOption {
	return func(r *v2.Resource) error {
		r.SetCreatedAt(timestamppb.New(createdAt))
		return nil
	}
}

// The profile, icon, status, and created_at fields on traits are deprecated
// and have moved to attributes on Resource. The sync helpers below mirror the
// deprecated trait fields onto the resource so that connectors using the
// deprecated trait options still populate the resource-level attributes.
// Resource-level values that were already set explicitly are not overwritten.

//nolint:staticcheck // intentionally reads deprecated trait fields for backwards compatibility
func syncUserTraitToResource(r *v2.Resource, ut *v2.UserTrait) {
	if ut.HasProfile() && !r.HasProfile() {
		r.SetProfile(ut.GetProfile())
	}
	if ut.HasIcon() && !r.HasIcon() {
		r.SetIcon(ut.GetIcon())
	}
	if ut.HasCreatedAt() && !r.HasCreatedAt() {
		r.SetCreatedAt(ut.GetCreatedAt())
	}
	if ut.HasStatus() && !r.HasStatus() {
		// UserTrait_Status_Status and Status_ResourceStatus enum values are identical.
		r.SetStatus(v2.Status_builder{
			Status:  v2.Status_ResourceStatus(ut.GetStatus().GetStatus()),
			Details: ut.GetStatus().GetDetails(),
		}.Build())
	}
}

//nolint:staticcheck // intentionally reads deprecated trait fields for backwards compatibility
func syncGroupTraitToResource(r *v2.Resource, gt *v2.GroupTrait) {
	if gt.HasProfile() && !r.HasProfile() {
		r.SetProfile(gt.GetProfile())
	}
	if gt.HasIcon() && !r.HasIcon() {
		r.SetIcon(gt.GetIcon())
	}
}

//nolint:staticcheck // intentionally reads deprecated trait fields for backwards compatibility
func syncRoleTraitToResource(r *v2.Resource, rt *v2.RoleTrait) {
	if rt.HasProfile() && !r.HasProfile() {
		r.SetProfile(rt.GetProfile())
	}
}

//nolint:staticcheck // intentionally reads deprecated trait fields for backwards compatibility
func syncAppTraitToResource(r *v2.Resource, at *v2.AppTrait) {
	if at.HasProfile() && !r.HasProfile() {
		r.SetProfile(at.GetProfile())
	}
	if at.HasIcon() && !r.HasIcon() {
		r.SetIcon(at.GetIcon())
	}
}

//nolint:staticcheck // intentionally reads deprecated trait fields for backwards compatibility
func syncSecretTraitToResource(r *v2.Resource, st *v2.SecretTrait) {
	if st.HasProfile() && !r.HasProfile() {
		r.SetProfile(st.GetProfile())
	}
	if st.HasCreatedAt() && !r.HasCreatedAt() {
		r.SetCreatedAt(st.GetCreatedAt())
	}
}

//nolint:staticcheck // intentionally reads deprecated trait fields for backwards compatibility
func syncAgentTraitToResource(r *v2.Resource, at *v2.AgentTrait) {
	if at.HasProfile() && !r.HasProfile() {
		r.SetProfile(at.GetProfile())
	}
	if at.GetStatus() != v2.AgentTrait_AGENT_STATUS_UNSPECIFIED && !r.HasStatus() {
		// AgentTrait_AgentStatus and Status_ResourceStatus enum values are
		// identical (READY maps to ENABLED).
		r.SetStatus(v2.Status_builder{
			Status: v2.Status_ResourceStatus(at.GetStatus()),
		}.Build())
	}
}

func WithUserTrait(opts ...UserTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		var err error
		ut := &v2.UserTrait{}

		annos := annotations.Annotations(r.GetAnnotations())

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
		r.SetAnnotations(annos)
		syncUserTraitToResource(r, ut)
		return nil
	}
}

func WithGroupTrait(opts ...GroupTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		ut := &v2.GroupTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
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
		r.SetAnnotations(annos)
		syncGroupTraitToResource(r, ut)
		return nil
	}
}

func WithRoleTrait(opts ...RoleTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		rt := &v2.RoleTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
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
		r.SetAnnotations(annos)
		syncRoleTraitToResource(r, rt)

		return nil
	}
}

func WithScopeBindingTrait(opts ...ScopeBindingTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		rt := &v2.ScopeBindingTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
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

		roleId := rt.GetRoleId()
		scopeResourceId := rt.GetScopeResourceId()
		if roleId == nil {
			return status.Errorf(codes.InvalidArgument, "role ID is required for scope binding trait")
		}
		if scopeResourceId == nil {
			return status.Errorf(codes.InvalidArgument, "scope resource ID is required for scope binding trait")
		}

		annos.Update(rt)
		r.SetAnnotations(annos)

		return nil
	}
}

func WithAppTrait(opts ...AppTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		at := &v2.AppTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
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
		r.SetAnnotations(annos)
		syncAppTraitToResource(r, at)

		return nil
	}
}

func WithSecretTrait(opts ...SecretTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		rt := &v2.SecretTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
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
		r.SetAnnotations(annos)
		syncSecretTraitToResource(r, rt)

		return nil
	}
}

func WithManagedDeviceTrait(opts ...ManagedDeviceTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		rt := &v2.ManagedDeviceTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
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
		r.SetAnnotations(annos)

		return nil
	}
}

// WithNHIType adds or updates a NonHumanIdentityTrait annotation on a
// resource, marking it as a non-human identity. It is kind-agnostic and may
// be combined with any resource trait (e.g. TRAIT_APP or TRAIT_ROLE).
func WithNHIType(nhiType v2.NonHumanIdentityTrait_NhiType, detail string) ResourceOption {
	return func(r *v2.Resource) error {
		nhi := &v2.NonHumanIdentityTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
		_, err := annos.Pick(nhi)
		if err != nil {
			return err
		}

		nhi.SetNhiType(nhiType)
		nhi.SetNhiDetail(detail)

		annos.Update(nhi)
		r.SetAnnotations(annos)

		return nil
	}
}

// GetNonHumanIdentityTrait returns the NonHumanIdentityTrait from a resource's
// annotations.
func GetNonHumanIdentityTrait(resource *v2.Resource) (*v2.NonHumanIdentityTrait, error) {
	ret := &v2.NonHumanIdentityTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("non-human identity trait was not found on resource")
	}
	return ret, nil
}

// WithAliases sets the aliases id for a resource.
func WithAliases(aliases ...string) ResourceOption {
	return func(r *v2.Resource) error {
		if len(aliases) == 0 {
			return ErrNoAlias
		}

		aliasV := &v2.Aliases{}

		annos := annotations.Annotations(r.GetAnnotations())
		_, err := annos.Pick(aliasV)
		if err != nil {
			return err
		}

		uniqueAlias := make(map[string]struct{}, len(aliasV.GetIds())+len(aliases))
		for _, alias := range aliasV.GetIds() {
			uniqueAlias[alias] = struct{}{}
		}

		for _, alias := range aliases {
			if alias == "" {
				return ErrEmptyAlias
			}

			uniqueAlias[alias] = struct{}{}
		}

		ids := make([]string, 0, len(uniqueAlias))
		for alias := range uniqueAlias {
			ids = append(ids, alias)
		}

		aliasV.Ids = ids

		err = aliasV.Validate()
		if err != nil {
			return err
		}

		annos.Update(aliasV)
		r.SetAnnotations(annos)

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

	return v2.ResourceType_builder{
		Id:          id,
		DisplayName: name,
		Traits:      requiredTraits,
		Annotations: annos,
	}.Build()
}

// NewResourceID returns a new resource ID given a resource type parent ID, and arbitrary object ID.
func NewResourceID(resourceType *v2.ResourceType, objectID interface{}) (*v2.ResourceId, error) {
	id, err := convertIDToString(objectID)
	if err != nil {
		return nil, err
	}

	return v2.ResourceId_builder{
		ResourceType: resourceType.GetId(),
		Resource:     id,
	}.Build(), nil
}

// NewResource returns a new resource instance with no traits.
func NewResource(name string, resourceType *v2.ResourceType, objectID interface{}, resourceOptions ...ResourceOption) (*v2.Resource, error) {
	rID, err := NewResourceID(resourceType, objectID)
	if err != nil {
		return nil, err
	}

	resource := v2.Resource_builder{
		Id:          rID,
		DisplayName: name,
	}.Build()

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

// NewScopeBindingResource returns a new resource instance with a configured scope binding trait.
func NewScopeBindingResource(
	name string,
	resourceType *v2.ResourceType,
	objectID any,
	scopeBindingOpts []ScopeBindingTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	opts = append(opts, WithScopeBindingTrait(scopeBindingOpts...))

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

func NewSecretResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	traitOpts []SecretTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	opts = append(opts, WithSecretTrait(traitOpts...))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func NewManagedDeviceResource(
	name string,
	resourceType *v2.ResourceType,
	objectID interface{},
	traitOpts []ManagedDeviceTraitOption,
	opts ...ResourceOption,
) (*v2.Resource, error) {
	opts = append(opts, WithManagedDeviceTrait(traitOpts...))

	ret, err := NewResource(name, resourceType, objectID, opts...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

type SyncOpAttrs struct {
	Session   sessions.SessionStore
	SyncID    string
	PageToken pagination.Token
}

type SyncOpResults struct {
	NextPageToken string
	Annotations   annotations.Annotations
}
