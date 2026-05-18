package anonymize

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

const testGrantID = "grant-123"

func TestAnonymizeGrant_Basic(t *testing.T) {
	a := newWithDefaults()

	originalGrantID := testGrantID
	originalEntitlementID := "entitlement-456"

	g := &v2.Grant{
		Id: originalGrantID,
		Entitlement: &v2.Entitlement{
			Id:          originalEntitlementID,
			DisplayName: "Admin Access",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "app",
					Resource:     "my-app",
				},
				DisplayName: "My Application",
			},
		},
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "john.doe@example.com",
			},
			DisplayName: "John Doe",
		},
	}

	err := a.AnonymizeGrant(g)
	require.NoError(t, err)

	// Grant ID should be anonymized with max(32, len) chars
	require.NotEqual(t, originalGrantID, g.GetId())
	require.Len(t, g.GetId(), max(32, len(originalGrantID)))

	// Entitlement should be anonymized with max(32, len) chars
	require.NotEqual(t, originalEntitlementID, g.GetEntitlement().GetId())
	require.Len(t, g.GetEntitlement().GetId(), max(32, len(originalEntitlementID)))
	require.NotEqual(t, "Admin Access", g.GetEntitlement().GetDisplayName())

	// Entitlement's resource should be anonymized
	require.NotEqual(t, "my-app", g.GetEntitlement().GetResource().GetId().GetResource())

	// Principal should be anonymized
	require.NotEqual(t, "john.doe@example.com", g.GetPrincipal().GetId().GetResource())
	require.NotEqual(t, "John Doe", g.GetPrincipal().GetDisplayName())
}

func TestAnonymizeGrant_WithSources(t *testing.T) {
	a := newWithDefaults()

	g := &v2.Grant{
		Id: testGrantID,
		Sources: &v2.GrantSources{
			Sources: map[string]*v2.GrantSources_GrantSource{
				"source-1": {},
				"source-2": {},
			},
		},
	}

	err := a.AnonymizeGrant(g)
	require.NoError(t, err)

	// Sources should be anonymized (keys)
	require.Len(t, g.GetSources().GetSources(), 2)
	for key := range g.GetSources().GetSources() {
		require.NotEqual(t, "source-1", key)
		require.NotEqual(t, "source-2", key)
	}
}

func TestAnonymizeGrant_NilGrant(t *testing.T) {
	a := newWithDefaults()

	err := a.AnonymizeGrant(nil)
	require.NoError(t, err)
}

func TestAnonymizeGrant_NilEntitlement(t *testing.T) {
	a := newWithDefaults()

	originalGrantID := testGrantID
	originalResource := "user-123"

	g := &v2.Grant{
		Id:          originalGrantID,
		Entitlement: nil,
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     originalResource,
			},
			DisplayName: "Test User",
		},
	}

	err := a.AnonymizeGrant(g)
	require.NoError(t, err)

	// Should still anonymize other fields with max(32, len) chars
	require.NotEqual(t, originalGrantID, g.GetId())
	require.Len(t, g.GetId(), max(32, len(originalGrantID)))
	require.NotEqual(t, originalResource, g.GetPrincipal().GetId().GetResource())
	require.Len(t, g.GetPrincipal().GetId().GetResource(), max(32, len(originalResource)))
}

func TestAnonymizeGrant_NilPrincipal(t *testing.T) {
	a := newWithDefaults()

	originalGrantID := testGrantID
	originalEntitlementID := "entitlement-456"

	g := &v2.Grant{
		Id: originalGrantID,
		Entitlement: &v2.Entitlement{
			Id:          originalEntitlementID,
			DisplayName: "Test Entitlement",
		},
		Principal: nil,
	}

	err := a.AnonymizeGrant(g)
	require.NoError(t, err)

	// Should still anonymize other fields with max(32, len) chars
	require.NotEqual(t, originalGrantID, g.GetId())
	require.Len(t, g.GetId(), max(32, len(originalGrantID)))
	require.NotEqual(t, originalEntitlementID, g.GetEntitlement().GetId())
	require.Len(t, g.GetEntitlement().GetId(), max(32, len(originalEntitlementID)))
}

func TestAnonymizeGrant_Deterministic(t *testing.T) {
	a := newWithDefaults()

	g1 := &v2.Grant{
		Id: testGrantID,
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-123",
			},
			DisplayName: "Test User",
		},
	}

	g2 := &v2.Grant{
		Id: testGrantID,
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-123",
			},
			DisplayName: "Test User",
		},
	}

	err := a.AnonymizeGrant(g1)
	require.NoError(t, err)

	err = a.AnonymizeGrant(g2)
	require.NoError(t, err)

	// Same input should produce same output
	require.Equal(t, g1.GetId(), g2.GetId())
	require.Equal(t, g1.GetPrincipal().GetId().GetResource(), g2.GetPrincipal().GetId().GetResource())
	require.Equal(t, g1.GetPrincipal().GetDisplayName(), g2.GetPrincipal().GetDisplayName())
}
