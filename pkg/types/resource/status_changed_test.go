package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestNewResourceStatusChanged(t *testing.T) {
	resourceID := v2.ResourceId_builder{
		ResourceType: "user",
		Resource:     "user-1",
	}.Build()

	rsc := NewResourceStatusChanged(resourceID, v2.Status_RESOURCE_STATUS_DISABLED, "deactivated after last role revoked")
	require.NotNil(t, rsc)
	require.Equal(t, resourceID, rsc.GetResourceId())
	require.Equal(t, v2.Status_RESOURCE_STATUS_DISABLED, rsc.GetStatus().GetStatus())
	require.Equal(t, "deactivated after last role revoked", rsc.GetStatus().GetDetails())
	require.NoError(t, rsc.Validate())
}

func TestResourceStatusChangedUpdatePick(t *testing.T) {
	resourceID := v2.ResourceId_builder{
		ResourceType: "user",
		Resource:     "user-1",
	}.Build()

	annos := annotations.Annotations{}
	annos.Update(NewResourceStatusChanged(resourceID, v2.Status_RESOURCE_STATUS_DISABLED, ""))
	// Update replaces an existing annotation of the same type.
	annos.Update(NewResourceStatusChanged(resourceID, v2.Status_RESOURCE_STATUS_ENABLED, "reactivated on grant"))
	require.Len(t, annos, 1)

	got := &v2.ResourceStatusChanged{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, proto.Equal(resourceID, got.GetResourceId()))
	require.Equal(t, v2.Status_RESOURCE_STATUS_ENABLED, got.GetStatus().GetStatus())
	require.Equal(t, "reactivated on grant", got.GetStatus().GetDetails())
}

func TestAppendResourceStatusChanged(t *testing.T) {
	resourceID := v2.ResourceId_builder{
		ResourceType: "user",
		Resource:     "user-1",
	}.Build()

	annos := annotations.Annotations{}
	annos = AppendResourceStatusChanged(annos, resourceID, v2.Status_RESOURCE_STATUS_ENABLED, "")

	got := &v2.ResourceStatusChanged{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, proto.Equal(resourceID, got.GetResourceId()))
	require.Equal(t, v2.Status_RESOURCE_STATUS_ENABLED, got.GetStatus().GetStatus())
	require.Empty(t, got.GetStatus().GetDetails())
}

func TestResourceStatusChangedValidation(t *testing.T) {
	resourceID := v2.ResourceId_builder{
		ResourceType: "user",
		Resource:     "user-1",
	}.Build()

	require.Error(t, NewResourceStatusChanged(nil, v2.Status_RESOURCE_STATUS_DISABLED, "").Validate())

	invalidResourceID := v2.ResourceId_builder{
		ResourceType: "user",
	}.Build()
	require.Error(t, NewResourceStatusChanged(invalidResourceID, v2.Status_RESOURCE_STATUS_DISABLED, "").Validate())

	require.Error(t, (&v2.ResourceStatusChanged{ResourceId: resourceID}).Validate())
	require.Error(t, NewResourceStatusChanged(resourceID, v2.Status_ResourceStatus(99), "").Validate())
}
