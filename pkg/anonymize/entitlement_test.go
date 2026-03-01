package anonymize

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestAnonymizeEntitlement_Basic(t *testing.T) {
	a := newWithDefaults()

	originalID := "entitlement-123"
	originalDisplayName := "Admin Access"
	originalSlug := "admin-access"

	e := &v2.Entitlement{
		Id:          originalID,
		DisplayName: originalDisplayName,
		Description: "Full administrative access",
		Slug:        originalSlug,
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "app",
				Resource:     "my-app",
			},
			DisplayName: "My Application",
		},
	}

	err := a.AnonymizeEntitlement(e)
	require.NoError(t, err)

	// ID should be anonymized with max(32, len) chars
	require.NotEqual(t, originalID, e.GetId())
	require.Len(t, e.GetId(), max(32, len(originalID)))

	// Display name should be anonymized with 16 chars
	require.NotEqual(t, originalDisplayName, e.GetDisplayName())
	require.Len(t, e.GetDisplayName(), 16)

	// Description should be anonymized
	require.Equal(t, "[ANONYMIZED]", e.GetDescription())

	// Slug should be anonymized with 12 chars
	require.NotEqual(t, originalSlug, e.GetSlug())
	require.Len(t, e.GetSlug(), 12)

	// Embedded resource should be anonymized
	require.NotEqual(t, "My Application", e.GetResource().GetDisplayName())
	require.NotEqual(t, "my-app", e.GetResource().GetId().GetResource())
}

func TestAnonymizeEntitlement_NilEntitlement(t *testing.T) {
	a := newWithDefaults()

	err := a.AnonymizeEntitlement(nil)
	require.NoError(t, err)
}

func TestAnonymizeEntitlement_NilResource(t *testing.T) {
	a := newWithDefaults()

	originalID := "entitlement-123"

	e := &v2.Entitlement{
		Id:          originalID,
		DisplayName: "Test Entitlement",
		Resource:    nil, // No embedded resource
	}

	err := a.AnonymizeEntitlement(e)
	require.NoError(t, err)

	// Should still anonymize other fields with max(32, len) chars
	require.NotEqual(t, originalID, e.GetId())
	require.Len(t, e.GetId(), max(32, len(originalID)))
}

func TestAnonymizeEntitlement_Deterministic(t *testing.T) {
	a := newWithDefaults()

	e1 := &v2.Entitlement{
		Id:          "entitlement-123",
		DisplayName: "Admin Access",
	}

	e2 := &v2.Entitlement{
		Id:          "entitlement-123",
		DisplayName: "Admin Access",
	}

	err := a.AnonymizeEntitlement(e1)
	require.NoError(t, err)

	err = a.AnonymizeEntitlement(e2)
	require.NoError(t, err)

	// Same input should produce same output
	require.Equal(t, e1.GetId(), e2.GetId())
	require.Equal(t, e1.GetDisplayName(), e2.GetDisplayName())
}
