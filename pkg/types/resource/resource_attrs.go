package resource

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The profile, icon, status, and created_at attributes moved from trait
// annotations to fields on Resource. Data written by older connectors only
// carries them on the traits, so the getters in this file read the
// resource-level attribute first and fall back to the deprecated trait
// fields. New code that only needs to handle new data can read the resource
// fields directly.

func pickTrait(r *v2.Resource, trait proto.Message) bool {
	annos := annotations.Annotations(r.GetAnnotations())
	ok, err := annos.Pick(trait)
	return err == nil && ok
}

// GetProfile returns the resource's profile, falling back to the deprecated
// trait-level profile for data written before profile moved to Resource.
//
//nolint:staticcheck // reads deprecated trait fields as a fallback for data written by older connectors
func GetProfile(r *v2.Resource) *structpb.Struct {
	if r.HasProfile() {
		return r.GetProfile()
	}
	if ut := (&v2.UserTrait{}); pickTrait(r, ut) && ut.HasProfile() {
		return ut.GetProfile()
	}
	if gt := (&v2.GroupTrait{}); pickTrait(r, gt) && gt.HasProfile() {
		return gt.GetProfile()
	}
	if rt := (&v2.RoleTrait{}); pickTrait(r, rt) && rt.HasProfile() {
		return rt.GetProfile()
	}
	if at := (&v2.AppTrait{}); pickTrait(r, at) && at.HasProfile() {
		return at.GetProfile()
	}
	if st := (&v2.SecretTrait{}); pickTrait(r, st) && st.HasProfile() {
		return st.GetProfile()
	}
	if agt := (&v2.AgentTrait{}); pickTrait(r, agt) && agt.HasProfile() {
		return agt.GetProfile()
	}
	return nil
}

// GetIcon returns the resource's icon, falling back to the deprecated
// trait-level icon for data written before icon moved to Resource.
//
//nolint:staticcheck // reads deprecated trait fields as a fallback for data written by older connectors
func GetIcon(r *v2.Resource) *v2.AssetRef {
	if r.HasIcon() {
		return r.GetIcon()
	}
	if ut := (&v2.UserTrait{}); pickTrait(r, ut) && ut.HasIcon() {
		return ut.GetIcon()
	}
	if gt := (&v2.GroupTrait{}); pickTrait(r, gt) && gt.HasIcon() {
		return gt.GetIcon()
	}
	if at := (&v2.AppTrait{}); pickTrait(r, at) && at.HasIcon() {
		return at.GetIcon()
	}
	return nil
}

// GetStatus returns the resource's status, falling back to the deprecated
// trait-level status for data written before status moved to Resource.
// Returns nil when no status is set anywhere.
//
//nolint:staticcheck // reads deprecated trait fields as a fallback for data written by older connectors
func GetStatus(r *v2.Resource) *v2.Status {
	if r.HasStatus() {
		return r.GetStatus()
	}
	if ut := (&v2.UserTrait{}); pickTrait(r, ut) && ut.HasStatus() {
		// UserTrait_Status_Status and Status_ResourceStatus enum values are identical.
		return v2.Status_builder{
			Status:  v2.Status_ResourceStatus(ut.GetStatus().GetStatus()),
			Details: ut.GetStatus().GetDetails(),
		}.Build()
	}
	if agt := (&v2.AgentTrait{}); pickTrait(r, agt) && agt.GetStatus() != v2.AgentTrait_AGENT_STATUS_UNSPECIFIED {
		// AgentTrait_AgentStatus and Status_ResourceStatus enum values are
		// identical (READY maps to ENABLED).
		return v2.Status_builder{
			Status: v2.Status_ResourceStatus(agt.GetStatus()),
		}.Build()
	}
	return nil
}

// GetCreatedAt returns the resource's creation time, falling back to the
// deprecated trait-level created_at for data written before created_at moved
// to Resource. Returns nil when no creation time is set anywhere.
//
//nolint:staticcheck // reads deprecated trait fields as a fallback for data written by older connectors
func GetCreatedAt(r *v2.Resource) *timestamppb.Timestamp {
	if r.HasCreatedAt() {
		return r.GetCreatedAt()
	}
	if ut := (&v2.UserTrait{}); pickTrait(r, ut) && ut.HasCreatedAt() {
		return ut.GetCreatedAt()
	}
	if st := (&v2.SecretTrait{}); pickTrait(r, st) && st.HasCreatedAt() {
		return st.GetCreatedAt()
	}
	return nil
}
