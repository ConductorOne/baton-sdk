package anonymize

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAnonymizeResource_Basic(t *testing.T) {
	a := newWithDefaults()

	originalType := "user"
	originalResource := "john.doe@example.com"
	originalDisplayName := "John Doe"

	r := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: originalType,
			Resource:     originalResource,
		},
		DisplayName: originalDisplayName,
		Description: "Test user description",
	}

	err := a.AnonymizeResource(r)
	require.NoError(t, err)

	// Display name should be anonymized with 16 char hash
	require.NotEqual(t, originalDisplayName, r.GetDisplayName())
	require.Len(t, r.GetDisplayName(), 16)

	// Description should be anonymized
	require.Equal(t, "[ANONYMIZED]", r.GetDescription())

	// Resource ID should be fully anonymized
	require.NotEqual(t, originalType, r.GetId().GetResourceType())
	require.Len(t, r.GetId().GetResourceType(), 8)
	require.NotEqual(t, originalResource, r.GetId().GetResource())
	require.Len(t, r.GetId().GetResource(), max(32, len(originalResource)))
}

func TestAnonymizeResource_WithUserTrait(t *testing.T) {
	a := newWithDefaults()

	originalEmail1 := "john.doe@example.com"
	originalEmail2 := "jdoe@work.com"
	originalLogin := "johndoe"
	originalAlias1 := "jd"
	originalAlias2 := "john.d"
	originalEmpID1 := "EMP001"
	originalEmpID2 := "EMP002"
	originalGivenName := "John"
	originalFamilyName := "Doe"
	originalMiddleName := "Robert"

	// Create a resource with UserTrait
	userTrait := &v2.UserTrait{
		Emails: []*v2.UserTrait_Email{
			{Address: originalEmail1, IsPrimary: true},
			{Address: originalEmail2, IsPrimary: false},
		},
		Login:        originalLogin,
		LoginAliases: []string{originalAlias1, originalAlias2},
		EmployeeIds:  []string{originalEmpID1, originalEmpID2},
		Profile: func() *structpb.Struct {
			s, _ := structpb.NewStruct(map[string]interface{}{
				"department": "Engineering",
				"manager":    "Jane Smith",
			})
			return s
		}(),
		StructuredName: &v2.UserTrait_StructuredName{
			GivenName:   originalGivenName,
			FamilyName:  originalFamilyName,
			MiddleNames: []string{originalMiddleName},
		},
	}

	annos := annotations.New(userTrait)
	r := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     originalEmail1,
		},
		DisplayName: "John Doe",
		Annotations: annos,
	}

	err := a.AnonymizeResource(r)
	require.NoError(t, err)

	// Check that UserTrait was anonymized
	ut := &v2.UserTrait{}
	updatedAnnos := annotations.Annotations(r.GetAnnotations())
	found, err := updatedAnnos.Pick(ut)
	require.NoError(t, err)
	require.True(t, found)

	// Emails should be anonymized with hash@hash format (33 chars: 16 + @ + 16)
	require.Len(t, ut.GetEmails(), 2)
	require.NotEqual(t, originalEmail1, ut.GetEmails()[0].GetAddress())
	require.Len(t, ut.GetEmails()[0].GetAddress(), 33)
	require.NotEqual(t, originalEmail2, ut.GetEmails()[1].GetAddress())
	require.Len(t, ut.GetEmails()[1].GetAddress(), 33)

	// Login should be anonymized with max(32, len) chars
	require.NotEqual(t, originalLogin, ut.GetLogin())
	require.Len(t, ut.GetLogin(), max(32, len(originalLogin)))

	// Login aliases should be anonymized with max(32, len) chars
	require.NotEqual(t, originalAlias1, ut.GetLoginAliases()[0])
	require.Len(t, ut.GetLoginAliases()[0], max(32, len(originalAlias1)))
	require.NotEqual(t, originalAlias2, ut.GetLoginAliases()[1])
	require.Len(t, ut.GetLoginAliases()[1], max(32, len(originalAlias2)))

	// Employee IDs should be anonymized with max(32, len) chars
	require.NotEqual(t, originalEmpID1, ut.GetEmployeeIds()[0])
	require.Len(t, ut.GetEmployeeIds()[0], max(32, len(originalEmpID1)))
	require.NotEqual(t, originalEmpID2, ut.GetEmployeeIds()[1])
	require.Len(t, ut.GetEmployeeIds()[1], max(32, len(originalEmpID2)))

	// Profile should be cleared
	require.False(t, ut.HasProfile())

	// Structured name should be anonymized with 16 char hash
	require.True(t, ut.HasStructuredName())
	require.NotEqual(t, originalGivenName, ut.GetStructuredName().GetGivenName())
	require.Len(t, ut.GetStructuredName().GetGivenName(), 16)
	require.NotEqual(t, originalFamilyName, ut.GetStructuredName().GetFamilyName())
	require.Len(t, ut.GetStructuredName().GetFamilyName(), 16)
	require.Len(t, ut.GetStructuredName().GetMiddleNames(), 1)
	require.NotEqual(t, originalMiddleName, ut.GetStructuredName().GetMiddleNames()[0])
	require.Len(t, ut.GetStructuredName().GetMiddleNames()[0], 16)
}

func TestAnonymizeResource_WithGroupTrait(t *testing.T) {
	a := newWithDefaults()

	originalDisplayName := "Engineering Team"

	groupTrait := &v2.GroupTrait{
		Profile: func() *structpb.Struct {
			s, _ := structpb.NewStruct(map[string]interface{}{
				"department": "Engineering",
			})
			return s
		}(),
	}

	annos := annotations.New(groupTrait)
	r := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "group",
			Resource:     "engineering-team",
		},
		DisplayName: originalDisplayName,
		Annotations: annos,
	}

	err := a.AnonymizeResource(r)
	require.NoError(t, err)

	// Check that GroupTrait profile was cleared
	gt := &v2.GroupTrait{}
	updatedAnnos := annotations.Annotations(r.GetAnnotations())
	found, err := updatedAnnos.Pick(gt)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, gt.HasProfile())

	// Display name should be anonymized with 16 char hash
	require.NotEqual(t, originalDisplayName, r.GetDisplayName())
	require.Len(t, r.GetDisplayName(), 16)
}

func TestAnonymizeResource_WithAppTrait(t *testing.T) {
	a := newWithDefaults()

	appTrait := &v2.AppTrait{
		HelpUrl: "https://help.example.com/docs",
		Profile: func() *structpb.Struct {
			s, _ := structpb.NewStruct(map[string]interface{}{
				"version": "1.0",
			})
			return s
		}(),
	}

	annos := annotations.New(appTrait)
	r := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "app",
			Resource:     "my-app",
		},
		DisplayName: "My Application",
		Annotations: annos,
	}

	err := a.AnonymizeResource(r)
	require.NoError(t, err)

	// Check that AppTrait was anonymized
	at := &v2.AppTrait{}
	updatedAnnos := annotations.Annotations(r.GetAnnotations())
	found, err := updatedAnnos.Pick(at)
	require.NoError(t, err)
	require.True(t, found)

	// HelpUrl should be anonymized
	require.Contains(t, at.GetHelpUrl(), "https://example.com/")

	// Profile should be cleared
	require.False(t, at.HasProfile())
}

func TestAnonymizeResource_WithExternalID(t *testing.T) {
	a := newWithDefaults()

	originalExtID := "external-123"

	r := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-123",
		},
		DisplayName: "Test User",
		ExternalId: &v2.ExternalId{
			Id:          originalExtID,
			Link:        "https://example.com/user/123",
			Description: "External user link",
		},
	}

	err := a.AnonymizeResource(r)
	require.NoError(t, err)

	// External ID should be anonymized with max(32, len) chars
	require.NotEqual(t, originalExtID, r.GetExternalId().GetId())
	require.Len(t, r.GetExternalId().GetId(), max(32, len(originalExtID)))
	require.Contains(t, r.GetExternalId().GetLink(), "https://example.com/")
	require.Equal(t, "[ANONYMIZED]", r.GetExternalId().GetDescription())
}

func TestAnonymizeResource_WithParentResourceID(t *testing.T) {
	a := newWithDefaults()

	originalParentType := "organization"
	originalParentResource := "org-456"

	r := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-123",
		},
		ParentResourceId: &v2.ResourceId{
			ResourceType: originalParentType,
			Resource:     originalParentResource,
		},
		DisplayName: "Test User",
	}

	err := a.AnonymizeResource(r)
	require.NoError(t, err)

	// Parent resource ID should be fully anonymized
	require.NotEqual(t, originalParentType, r.GetParentResourceId().GetResourceType())
	require.Len(t, r.GetParentResourceId().GetResourceType(), 8)
	require.NotEqual(t, originalParentResource, r.GetParentResourceId().GetResource())
	require.Len(t, r.GetParentResourceId().GetResource(), max(32, len(originalParentResource)))
}

func TestAnonymizeResource_NilResource(t *testing.T) {
	a := newWithDefaults()

	err := a.AnonymizeResource(nil)
	require.NoError(t, err)
}

func TestAnonymizeResource_Deterministic(t *testing.T) {
	a := newWithDefaults()

	r1 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "john.doe@example.com",
		},
		DisplayName: "John Doe",
	}

	r2 := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "john.doe@example.com",
		},
		DisplayName: "John Doe",
	}

	err := a.AnonymizeResource(r1)
	require.NoError(t, err)

	err = a.AnonymizeResource(r2)
	require.NoError(t, err)

	// Same input should produce same output
	require.Equal(t, r1.GetDisplayName(), r2.GetDisplayName())
	require.Equal(t, r1.GetId().GetResource(), r2.GetId().GetResource())
}
