package resource

import (
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// resourceWithTrait builds a resource carrying only a trait annotation, the
// shape of data written by connectors from before profile/icon/status/
// created_at moved to Resource.
func resourceWithTrait(t *testing.T, trait proto.Message) *v2.Resource {
	t.Helper()
	var annos annotations.Annotations
	annos.Update(trait)
	return v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "test", Resource: "1"}.Build(),
		Annotations: annos,
	}.Build()
}

func TestGetProfileFallsBackToTrait(t *testing.T) {
	profile, err := structpb.NewStruct(map[string]interface{}{"first_name": "Trait"})
	require.NoError(t, err)

	r := resourceWithTrait(t, v2.UserTrait_builder{Profile: profile}.Build())
	require.False(t, r.HasProfile())
	got, ok := GetProfileStringValue(GetProfile(r), "first_name")
	require.True(t, ok)
	require.Equal(t, "Trait", got)

	r = resourceWithTrait(t, v2.GroupTrait_builder{Profile: profile}.Build())
	got, ok = GetProfileStringValue(GetProfile(r), "first_name")
	require.True(t, ok)
	require.Equal(t, "Trait", got)

	// Resource-level profile wins over the trait profile.
	resourceProfile, err := structpb.NewStruct(map[string]interface{}{"first_name": "Resource"})
	require.NoError(t, err)
	r = resourceWithTrait(t, v2.UserTrait_builder{Profile: profile}.Build())
	r.SetProfile(resourceProfile)
	got, ok = GetProfileStringValue(GetProfile(r), "first_name")
	require.True(t, ok)
	require.Equal(t, "Resource", got)

	require.Nil(t, GetProfile(v2.Resource_builder{}.Build()))
}

func TestGetIconFallsBackToTrait(t *testing.T) {
	icon := v2.AssetRef_builder{Id: "traitIcon"}.Build()

	r := resourceWithTrait(t, v2.GroupTrait_builder{Icon: icon}.Build())
	require.False(t, r.HasIcon())
	require.Equal(t, "traitIcon", GetIcon(r).GetId())

	r.SetIcon(v2.AssetRef_builder{Id: "resourceIcon"}.Build())
	require.Equal(t, "resourceIcon", GetIcon(r).GetId())

	require.Nil(t, GetIcon(v2.Resource_builder{}.Build()))
}

func TestGetStatusFallsBackToTrait(t *testing.T) {
	r := resourceWithTrait(t, v2.UserTrait_builder{
		Status: v2.UserTrait_Status_builder{
			Status:  v2.UserTrait_Status_STATUS_DISABLED,
			Details: "locked by admin",
		}.Build(),
	}.Build())
	st := GetStatus(r)
	require.Equal(t, v2.Status_RESOURCE_STATUS_DISABLED, st.GetStatus())
	require.Equal(t, "locked by admin", st.GetDetails())

	r = resourceWithTrait(t, v2.AgentTrait_builder{
		Status: v2.AgentTrait_AGENT_STATUS_READY,
	}.Build())
	require.Equal(t, v2.Status_RESOURCE_STATUS_ENABLED, GetStatus(r).GetStatus())

	r.SetStatus(v2.Status_builder{Status: v2.Status_RESOURCE_STATUS_DELETED}.Build())
	require.Equal(t, v2.Status_RESOURCE_STATUS_DELETED, GetStatus(r).GetStatus())

	require.Nil(t, GetStatus(v2.Resource_builder{}.Build()))
}

func TestGetCreatedAtFallsBackToTrait(t *testing.T) {
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	r := resourceWithTrait(t, v2.SecretTrait_builder{CreatedAt: timestamppb.New(createdAt)}.Build())
	require.False(t, r.HasCreatedAt())
	require.Equal(t, createdAt, GetCreatedAt(r).AsTime())

	resourceCreatedAt := createdAt.Add(time.Hour)
	r.SetCreatedAt(timestamppb.New(resourceCreatedAt))
	require.Equal(t, resourceCreatedAt, GetCreatedAt(r).AsTime())

	require.Nil(t, GetCreatedAt(v2.Resource_builder{}.Build()))
}
