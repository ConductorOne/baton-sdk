package dotc1z

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestUpdateGrantSourcesProto(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-grant-sources.c1z")

	// Create a new C1Z file
	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)
	defer f.Close()

	// Start a sync
	syncID, _, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Create test resources and entitlement
	resourceType := &v2.ResourceType{
		Id:          "rt-1",
		DisplayName: "Test Resource Type",
	}
	err = f.PutResourceTypes(ctx, resourceType)
	require.NoError(t, err)

	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "rt-1",
			Resource:     "r-1",
		},
		DisplayName: "Test Resource",
	}
	err = f.PutResources(ctx, resource)
	require.NoError(t, err)

	entitlement := &v2.Entitlement{
		Id: "ent-1",
		Resource: resource,
		DisplayName: "Test Entitlement",
	}
	err = f.PutEntitlements(ctx, entitlement)
	require.NoError(t, err)

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "rt-1",
			Resource:     "p-1",
		},
		DisplayName: "Test Principal",
	}
	err = f.PutResources(ctx, principal)
	require.NoError(t, err)

	// Create a grant with initial sources
	grant := &v2.Grant{
		Id:          "grant-1",
		Entitlement: entitlement,
		Principal:   principal,
		Sources: &v2.GrantSources{
			Sources: map[string]*v2.GrantSources_GrantSource{
				"source-1": {},
			},
		},
	}
	err = f.PutGrants(ctx, grant)
	require.NoError(t, err)

	// Get the grant ID from the database
	var grantID int64
	err = f.rawDb.QueryRowContext(ctx, `
		SELECT id FROM v1_grants WHERE external_id = ? AND sync_id = ?
	`, grant.GetId(), syncID).Scan(&grantID)
	require.NoError(t, err)

	// Update sources via SQL
	newSources := map[string]*v2.GrantSources_GrantSource{
		"source-2": {},
		"source-3": {},
	}
	sourcesJSON, err := json.Marshal(newSources)
	require.NoError(t, err)

	err = f.UpdateGrantSourcesProto(ctx, grantID, sourcesJSON)
	require.NoError(t, err)

	// Verify the update worked by reading the grant back
	updatedGrant, err := f.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: grant.GetId(),
		Annotations: []*anypb.Any{},
	})
	require.NoError(t, err)
	require.NotNil(t, updatedGrant.GetGrant())

	// Verify sources were updated
	require.NotNil(t, updatedGrant.GetGrant().GetSources())
	require.Len(t, updatedGrant.GetGrant().GetSources().GetSources(), 2)
	require.Contains(t, updatedGrant.GetGrant().GetSources().GetSources(), "source-2")
	require.Contains(t, updatedGrant.GetGrant().GetSources().GetSources(), "source-3")
	require.NotContains(t, updatedGrant.GetGrant().GetSources().GetSources(), "source-1")

	// Verify other fields are preserved
	require.Equal(t, grant.GetId(), updatedGrant.GetGrant().GetId())
	require.Equal(t, grant.GetEntitlement().GetId(), updatedGrant.GetGrant().GetEntitlement().GetId())
}

func TestUpdateGrantSourcesProtoBatch(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-grant-sources-batch.c1z")

	// Create a new C1Z file
	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)
	defer f.Close()

	// Start a sync
	syncID, _, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Create test resources and entitlement (reuse from previous test setup)
	resourceType := &v2.ResourceType{
		Id:          "rt-1",
		DisplayName: "Test Resource Type",
	}
	err = f.PutResourceTypes(ctx, resourceType)
	require.NoError(t, err)

	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "rt-1",
			Resource:     "r-1",
		},
		DisplayName: "Test Resource",
	}
	err = f.PutResources(ctx, resource)
	require.NoError(t, err)

	entitlement := &v2.Entitlement{
		Id: "ent-1",
		Resource: resource,
		DisplayName: "Test Entitlement",
	}
	err = f.PutEntitlements(ctx, entitlement)
	require.NoError(t, err)

	// Create multiple grants
	grants := []*v2.Grant{
		{
			Id:          "grant-1",
			Entitlement: entitlement,
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "rt-1",
					Resource:     "p-1",
				},
			},
		},
		{
			Id:          "grant-2",
			Entitlement: entitlement,
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "rt-1",
					Resource:     "p-2",
				},
			},
		},
	}
	err = f.PutGrants(ctx, grants...)
	require.NoError(t, err)

	// Get grant IDs
	var grantID1, grantID2 int64
	err = f.rawDb.QueryRowContext(ctx, `
		SELECT id FROM v1_grants WHERE external_id = ? AND sync_id = ?
	`, "grant-1", syncID).Scan(&grantID1)
	require.NoError(t, err)

	err = f.rawDb.QueryRowContext(ctx, `
		SELECT id FROM v1_grants WHERE external_id = ? AND sync_id = ?
	`, "grant-2", syncID).Scan(&grantID2)
	require.NoError(t, err)

	// Prepare batch updates
	sources1 := map[string]*v2.GrantSources_GrantSource{"source-1": {}}
	sources2 := map[string]*v2.GrantSources_GrantSource{"source-2": {}}

	sourcesJSON1, err := json.Marshal(sources1)
	require.NoError(t, err)
	sourcesJSON2, err := json.Marshal(sources2)
	require.NoError(t, err)

	updates := map[int64][]byte{
		grantID1: sourcesJSON1,
		grantID2: sourcesJSON2,
	}

	err = f.UpdateGrantSourcesProtoBatch(ctx, updates)
	require.NoError(t, err)

	// Verify updates
	grant1, err := f.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: "grant-1",
		Annotations: []*anypb.Any{},
	})
	require.NoError(t, err)
	require.Contains(t, grant1.GetGrant().GetSources().GetSources(), "source-1")

	grant2, err := f.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: "grant-2",
		Annotations: []*anypb.Any{},
	})
	require.NoError(t, err)
	require.Contains(t, grant2.GetGrant().GetSources().GetSources(), "source-2")
}
