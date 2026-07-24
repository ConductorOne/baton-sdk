package resource

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestNewResourceDeleted(t *testing.T) {
	resourceID := v2.ResourceId_builder{
		ResourceType: "user",
		Resource:     "user-1",
	}.Build()

	rd := NewResourceDeleted(resourceID)
	require.NotNil(t, rd)
	require.Equal(t, resourceID, rd.GetResourceId())
	require.NoError(t, rd.Validate())
}

func TestAppendResourceDeleted(t *testing.T) {
	resourceID := v2.ResourceId_builder{
		ResourceType: "user",
		Resource:     "user-1",
	}.Build()

	annos := annotations.Annotations{}
	annos = AppendResourceDeleted(annos, resourceID)

	got := &v2.ResourceDeleted{}
	found, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, proto.Equal(resourceID, got.GetResourceId()))
}

func TestResourceDeletedValidation(t *testing.T) {
	require.Error(t, NewResourceDeleted(nil).Validate())

	invalidResourceID := v2.ResourceId_builder{
		ResourceType: "user",
	}.Build()
	require.Error(t, NewResourceDeleted(invalidResourceID).Validate())
}
