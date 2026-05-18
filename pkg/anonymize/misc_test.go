package anonymize

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestAnonymizeResourceType_Basic(t *testing.T) {
	a := newWithDefaults()

	originalID := "user"
	originalDisplayName := "User"

	rt := &v2.ResourceType{
		Id:          originalID,
		DisplayName: originalDisplayName,
		Description: "A user in the system",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_USER,
		},
	}

	err := a.AnonymizeResourceType(rt)
	require.NoError(t, err)

	// ID should be anonymized with 8 char hash
	require.NotEqual(t, originalID, rt.GetId())
	require.Len(t, rt.GetId(), 8)

	// Display name should be anonymized with 16 char hash
	require.NotEqual(t, originalDisplayName, rt.GetDisplayName())
	require.Len(t, rt.GetDisplayName(), 16)

	// Description should be anonymized
	require.Equal(t, "[ANONYMIZED]", rt.GetDescription())

	// Traits should be preserved
	require.Len(t, rt.GetTraits(), 1)
	require.Equal(t, v2.ResourceType_TRAIT_USER, rt.GetTraits()[0])
}

func TestAnonymizeResourceType_NilResourceType(t *testing.T) {
	a := newWithDefaults()

	err := a.AnonymizeResourceType(nil)
	require.NoError(t, err)
}

func TestAnonymizeResourceType_Deterministic(t *testing.T) {
	a := newWithDefaults()

	rt1 := &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
	}

	rt2 := &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
	}

	err := a.AnonymizeResourceType(rt1)
	require.NoError(t, err)

	err = a.AnonymizeResourceType(rt2)
	require.NoError(t, err)

	// Same input should produce same output
	require.Equal(t, rt1.GetId(), rt2.GetId())
	require.Equal(t, rt1.GetDisplayName(), rt2.GetDisplayName())
}
