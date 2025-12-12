package dotc1z

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestUpdateProtobufField(t *testing.T) {
	// Create a test grant with sources
	entitlement := &v2.Entitlement{
		Id: "ent-1",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "rt-1",
				Resource:     "r-1",
			},
		},
	}
	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "rt-2",
			Resource:     "r-2",
		},
	}
	grant := &v2.Grant{
		Id:          "grant-1",
		Entitlement: entitlement,
		Principal:   principal,
		Sources: &v2.GrantSources{
			Sources: map[string]*v2.GrantSources_GrantSource{
				"source-1": {},
				"source-2": {},
			},
		},
	}

	originalBlob, err := proto.Marshal(grant)
	require.NoError(t, err)
	require.NotEmpty(t, originalBlob)

	// Create new sources
	newSources := &v2.GrantSources{
		Sources: map[string]*v2.GrantSources_GrantSource{
			"source-3": {},
			"source-4": {},
		},
	}
	newSourcesBlob, err := proto.Marshal(newSources)
	require.NoError(t, err)

	// Update field 5 (sources)
	updatedBlob, err := updateProtobufField(originalBlob, 5, newSourcesBlob)
	require.NoError(t, err)
	require.NotEmpty(t, updatedBlob)

	// Verify we can unmarshal the updated grant
	updatedGrant := &v2.Grant{}
	err = proto.Unmarshal(updatedBlob, updatedGrant)
	require.NoError(t, err)

	// Verify sources were updated
	require.NotNil(t, updatedGrant.GetSources())
	require.Len(t, updatedGrant.GetSources().GetSources(), 2)
	require.Contains(t, updatedGrant.GetSources().GetSources(), "source-3")
	require.Contains(t, updatedGrant.GetSources().GetSources(), "source-4")
	require.NotContains(t, updatedGrant.GetSources().GetSources(), "source-1")
	require.NotContains(t, updatedGrant.GetSources().GetSources(), "source-2")

	// Verify other fields are preserved
	require.Equal(t, grant.GetId(), updatedGrant.GetId())
	require.Equal(t, grant.GetEntitlement().GetId(), updatedGrant.GetEntitlement().GetId())
	require.Equal(t, grant.GetPrincipal().GetId().GetResourceType(), updatedGrant.GetPrincipal().GetId().GetResourceType())
}

func TestUpdateProtobufField_AddMissingField(t *testing.T) {
	// Create a grant without sources
	entitlement := &v2.Entitlement{
		Id: "ent-1",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "rt-1",
				Resource:     "r-1",
			},
		},
	}
	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "rt-2",
			Resource:     "r-2",
		},
	}
	grant := &v2.Grant{
		Id:          "grant-1",
		Entitlement: entitlement,
		Principal:   principal,
		// No Sources field
	}

	originalBlob, err := proto.Marshal(grant)
	require.NoError(t, err)

	// Add sources
	newSources := &v2.GrantSources{
		Sources: map[string]*v2.GrantSources_GrantSource{
			"source-1": {},
		},
	}
	newSourcesBlob, err := proto.Marshal(newSources)
	require.NoError(t, err)

	// Update field 5 (sources) - should append
	updatedBlob, err := updateProtobufField(originalBlob, 5, newSourcesBlob)
	require.NoError(t, err)

	// Verify we can unmarshal
	updatedGrant := &v2.Grant{}
	err = proto.Unmarshal(updatedBlob, updatedGrant)
	require.NoError(t, err)

	// Verify sources were added
	require.NotNil(t, updatedGrant.GetSources())
	require.Len(t, updatedGrant.GetSources().GetSources(), 1)
	require.Contains(t, updatedGrant.GetSources().GetSources(), "source-1")
}

func TestUpdateProtobufField_RemoveField(t *testing.T) {
	// Create a grant with sources
	grant := &v2.Grant{
		Id: "grant-1",
		Entitlement: &v2.Entitlement{
			Id: "ent-1",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "rt-1",
					Resource:     "r-1",
				},
			},
		},
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "rt-2",
				Resource:     "r-2",
			},
		},
		Sources: &v2.GrantSources{
			Sources: map[string]*v2.GrantSources_GrantSource{
				"source-1": {},
			},
		},
	}

	originalBlob, err := proto.Marshal(grant)
	require.NoError(t, err)

	// Remove sources by passing empty value
	updatedBlob, err := updateProtobufField(originalBlob, 5, []byte{})
	require.NoError(t, err)

	// Verify we can unmarshal
	updatedGrant := &v2.Grant{}
	err = proto.Unmarshal(updatedBlob, updatedGrant)
	require.NoError(t, err)

	// Verify sources were removed (nil or empty)
	if updatedGrant.GetSources() != nil {
		require.Empty(t, updatedGrant.GetSources().GetSources())
	}
}

func TestJsonSourcesToProtobuf(t *testing.T) {
	tests := []struct {
		name           string
		jsonSources    string
		expectEmpty    bool
		expectedCount  int
		expectedKeys   []string
	}{
		{
			name:          "valid sources",
			jsonSources:   `{"source-1":{},"source-2":{}}`,
			expectEmpty:   false,
			expectedCount: 2,
			expectedKeys:  []string{"source-1", "source-2"},
		},
		{
			name:        "empty object",
			jsonSources: `{}`,
			expectEmpty: true,
		},
		{
			name:        "null",
			jsonSources: `null`,
			expectEmpty: true,
		},
		{
			name:        "empty string",
			jsonSources: ``,
			expectEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protoBlob, err := jsonSourcesToProtobuf([]byte(tt.jsonSources))
			require.NoError(t, err)

			if tt.expectEmpty {
				require.Empty(t, protoBlob)
			} else {
				require.NotEmpty(t, protoBlob)

				// Verify we can unmarshal it
				grantSources := &v2.GrantSources{}
				err = proto.Unmarshal(protoBlob, grantSources)
				require.NoError(t, err)

				require.Len(t, grantSources.GetSources(), tt.expectedCount)
				for _, key := range tt.expectedKeys {
					require.Contains(t, grantSources.GetSources(), key)
				}
			}
		})
	}
}

func TestUpdateProtobufField_PreservesOtherFields(t *testing.T) {
	// Create a grant with all fields populated
	grant := &v2.Grant{
		Id: "grant-1",
		Entitlement: &v2.Entitlement{
			Id: "ent-1",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "rt-1",
					Resource:     "r-1",
				},
			},
		},
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "rt-2",
				Resource:     "r-2",
			},
		},
		Sources: &v2.GrantSources{
			Sources: map[string]*v2.GrantSources_GrantSource{
				"old-source": {},
			},
		},
	}

	originalBlob, err := proto.Marshal(grant)
	require.NoError(t, err)

	// Update only sources
	newSources := &v2.GrantSources{
		Sources: map[string]*v2.GrantSources_GrantSource{
			"new-source": {},
		},
	}
	newSourcesBlob, err := proto.Marshal(newSources)
	require.NoError(t, err)

	updatedBlob, err := updateProtobufField(originalBlob, 5, newSourcesBlob)
	require.NoError(t, err)

	// Verify all other fields are preserved
	updatedGrant := &v2.Grant{}
	err = proto.Unmarshal(updatedBlob, updatedGrant)
	require.NoError(t, err)

	require.Equal(t, grant.GetId(), updatedGrant.GetId())
	require.Equal(t, grant.GetEntitlement().GetId(), updatedGrant.GetEntitlement().GetId())
	require.Equal(t, grant.GetPrincipal().GetId().GetResourceType(), updatedGrant.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, grant.GetPrincipal().GetId().GetResource(), updatedGrant.GetPrincipal().GetId().GetResource())

	// Only sources should have changed
	require.NotEqual(t, grant.GetSources().GetSources(), updatedGrant.GetSources().GetSources())
	require.Contains(t, updatedGrant.GetSources().GetSources(), "new-source")
	require.NotContains(t, updatedGrant.GetSources().GetSources(), "old-source")
}

func TestUpdateProtobufField_EmptyBlob(t *testing.T) {
	newSources := &v2.GrantSources{
		Sources: map[string]*v2.GrantSources_GrantSource{
			"source-1": {},
		},
	}
	newSourcesBlob, err := proto.Marshal(newSources)
	require.NoError(t, err)

	// Update empty blob
	updatedBlob, err := updateProtobufField([]byte{}, 5, newSourcesBlob)
	require.NoError(t, err)

	// Should have added the field
	require.NotEmpty(t, updatedBlob)

	// Verify we can unmarshal
	grant := &v2.Grant{}
	err = proto.Unmarshal(updatedBlob, grant)
	require.NoError(t, err)
	require.NotNil(t, grant.GetSources())
}
