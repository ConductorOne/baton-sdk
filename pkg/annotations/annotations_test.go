package annotations

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAnnotations_Append(t *testing.T) {
	var annos Annotations

	annos.Append(&v2.GroupTrait{}, &v2.RoleTrait{})
	require.Len(t, annos, 2)
	for i, a := range annos {
		switch i {
		case 0:
			require.True(t, a.MessageIs(&v2.GroupTrait{}))
		case 1:
			require.True(t, a.MessageIs(&v2.RoleTrait{}))
		}
	}
}

func TestAnnotations_Contains(t *testing.T) {
	var annos Annotations

	annos.Append(&v2.GroupTrait{}, &v2.RoleTrait{})
	require.True(t, annos.Contains(&v2.GroupTrait{}))
	require.True(t, annos.Contains(&v2.RoleTrait{}))
	require.False(t, annos.Contains(&v2.AppTrait{}))
}

func TestAnnotations_Pick(t *testing.T) {
	var annos Annotations

	annos.Append(&v2.GroupTrait{Profile: &structpb.Struct{}})

	trait := &v2.GroupTrait{}
	require.Nil(t, trait.Profile)
	ok, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, trait.Profile)

	roleTrait := &v2.RoleTrait{}
	ok, err = annos.Pick(roleTrait)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestAnnotations_Update(t *testing.T) {
	var annos Annotations

	require.Len(t, annos, 0)
	annos.Update(&v2.GroupTrait{})
	require.Len(t, annos, 1)
	annos.Append(&v2.RoleTrait{})
	require.Len(t, annos, 2)
	annos.Update(&v2.GroupTrait{Profile: &structpb.Struct{}})
	require.Len(t, annos, 2)
	annos.Update(&v2.UserTrait{})
	require.Len(t, annos, 3)

	gt := &v2.GroupTrait{}
	ok, err := annos.Pick(gt)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, gt.Profile)
}

func TestAnnotations_WithRateLimiting(t *testing.T) {
	var annos Annotations

	require.Len(t, annos, 0)
	annos.WithRateLimiting(&v2.RateLimitDescription{})
	require.Len(t, annos, 1)

	rld := &v2.RateLimitDescription{}
	ok, err := annos.Pick(rld)
	require.NoError(t, err)
	require.True(t, ok)
}
