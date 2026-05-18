package anonymize

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// TestAnonymizeC1ZFile_EndToEnd verifies that AnonymizeC1ZFile produces
// an anonymized c1z file on disk with all PII properly anonymized.
func TestAnonymizeC1ZFile_EndToEnd(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	// Original test data
	const (
		originalResourceType = "user"
		originalResourceID   = "john.doe@example.com"
		originalDisplayName  = "John Doe"
		originalDescription  = "A test user for engineering team"

		originalEntitlementID   = "admin-access-entitlement"
		originalEntitlementName = "Admin Access"
		originalEntitlementDesc = "Full administrative permissions"
		originalEntitlementSlug = "admin-access"

		originalGrantID = "grant-john-admin"
	)

	inputPath := filepath.Join(tempDir, "input.c1z")

	// Step 1: Create a c1z file with known PII data
	createTestC1ZFile(t, ctx, inputPath,
		originalResourceType, originalResourceID, originalDisplayName, originalDescription,
		originalEntitlementID, originalEntitlementName, originalEntitlementDesc, originalEntitlementSlug,
		originalGrantID,
	)

	// Step 2: Anonymize the file
	outputPath := filepath.Join(tempDir, "output.c1z.anonymized")
	anonymizer := New(Config{Salt: "test-salt-for-e2e"})
	stats, err := anonymizer.AnonymizeC1ZFile(ctx, inputPath, outputPath)
	require.NoError(t, err, "AnonymizeC1ZFile should succeed")
	// // Verify stats
	require.Equal(t, 1, stats.ResourceTypesProcessed, "Should process 1 resource type")
	require.Equal(t, 1, stats.ResourcesProcessed, "Should process 1 resource")
	require.Equal(t, 1, stats.EntitlementsProcessed, "Should process 1 entitlement")
	require.Equal(t, 1, stats.GrantsProcessed, "Should process 1 grant")
	require.True(t, stats.AssetsDeleted, "Assets should be deleted")
	require.True(t, stats.SessionsCleared, "Sessions should be cleared")
	require.True(t, stats.SyncRunsCleared, "Sync runs should be cleared")

	// Step 4: Open the anonymized file and verify data is anonymized
	verifyAnonymizedC1ZFile(t, ctx, outputPath,
		originalResourceType, originalResourceID, originalDisplayName, originalDescription,
		originalEntitlementID, originalEntitlementName, originalEntitlementDesc, originalEntitlementSlug,
		originalGrantID,
	)
}

// TestAnonymizeC1ZFile_DefaultOutputPath verifies that when outputPath is empty,
// the file is created with ".anonymized" suffix.
func TestAnonymizeC1ZFile_DefaultOutputPath(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	inputPath := filepath.Join(tempDir, "input.c1z")
	expectedOutputPath := inputPath + ".anonymized"

	// Create a minimal c1z file
	createMinimalC1ZFile(t, ctx, inputPath)

	// Anonymize with empty output path
	anonymizer := New(Config{Salt: "test-salt"})
	_, err := anonymizer.AnonymizeC1ZFile(ctx, inputPath, "")
	require.NoError(t, err)

	// Verify the default output path was used
	_, err = os.Stat(expectedOutputPath)
	require.NoError(t, err, "Output file should be created at input.c1z.anonymized")
}

// TestAnonymizeC1ZFile_Deterministic verifies that anonymizing the same file
// twice with the same salt produces identical results.
func TestAnonymizeC1ZFile_Deterministic(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	inputPath := filepath.Join(tempDir, "input.c1z")
	outputPath1 := filepath.Join(tempDir, "output1.c1z")
	outputPath2 := filepath.Join(tempDir, "output2.c1z")

	// Create a c1z file
	createMinimalC1ZFile(t, ctx, inputPath)

	// Anonymize twice with the same salt
	salt := "deterministic-salt"
	anonymizer := New(Config{Salt: salt})

	_, err := anonymizer.AnonymizeC1ZFile(ctx, inputPath, outputPath1)
	require.NoError(t, err)

	_, err = anonymizer.AnonymizeC1ZFile(ctx, inputPath, outputPath2)
	require.NoError(t, err)

	// Open both files and compare the anonymized display names
	c1f1, err := dotc1z.NewC1ZFile(ctx, outputPath1, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer c1f1.Close()

	c1f2, err := dotc1z.NewC1ZFile(ctx, outputPath2, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer c1f2.Close()

	resp1, err := c1f1.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)

	resp2, err := c1f2.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)

	require.Equal(t, len(resp1.GetList()), len(resp2.GetList()))
	for i := range resp1.GetList() {
		require.Equal(t, resp1.GetList()[i].GetId(), resp2.GetList()[i].GetId(),
			"Same salt should produce same anonymized IDs")
		require.Equal(t, resp1.GetList()[i].GetDisplayName(), resp2.GetList()[i].GetDisplayName(),
			"Same salt should produce same anonymized display names")
	}
}

// TestAnonymizeC1ZFile_DifferentSalts verifies that different salts produce
// different anonymized output.
func TestAnonymizeC1ZFile_DifferentSalts(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	inputPath := filepath.Join(tempDir, "input.c1z")
	outputPath1 := filepath.Join(tempDir, "output1.c1z")
	outputPath2 := filepath.Join(tempDir, "output2.c1z")

	// Create a c1z file
	createMinimalC1ZFile(t, ctx, inputPath)

	// Anonymize with different salts
	anonymizer1 := New(Config{Salt: "salt-one"})
	anonymizer2 := New(Config{Salt: "salt-two"})

	_, err := anonymizer1.AnonymizeC1ZFile(ctx, inputPath, outputPath1)
	require.NoError(t, err)

	_, err = anonymizer2.AnonymizeC1ZFile(ctx, inputPath, outputPath2)
	require.NoError(t, err)

	// Open both files and verify the anonymized data is different
	c1f1, err := dotc1z.NewC1ZFile(ctx, outputPath1, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer c1f1.Close()

	c1f2, err := dotc1z.NewC1ZFile(ctx, outputPath2, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer c1f2.Close()

	resp1, err := c1f1.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)

	resp2, err := c1f2.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)

	require.Equal(t, len(resp1.GetList()), len(resp2.GetList()))
	require.NotEqual(t, resp1.GetList()[0].GetId(), resp2.GetList()[0].GetId(),
		"Different salts should produce different anonymized IDs")
}

// TestAnonymizeC1ZFile_InvalidInputPath verifies error handling for non-existent input file.
func TestAnonymizeC1ZFile_InvalidInputPath(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	inputPath := filepath.Join(tempDir, "does-not-exist.c1z")
	outputPath := filepath.Join(tempDir, "output.c1z")

	anonymizer := New(Config{Salt: "test-salt"})
	_, err := anonymizer.AnonymizeC1ZFile(ctx, inputPath, outputPath)
	require.Error(t, err, "Should fail for non-existent input file")
}

// createTestC1ZFile creates a c1z file with comprehensive test data.
func createTestC1ZFile(t *testing.T, ctx context.Context, path string,
	resourceType, resourceID, displayName, description string,
	entitlementID, entitlementName, entitlementDesc, entitlementSlug string,
	grantID string,
) {
	t.Helper()

	c1f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

	// Start a sync
	_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Add resource type
	err = c1f.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          resourceType,
		DisplayName: resourceType + " type",
	}.Build())
	require.NoError(t, err)

	// Add resource
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceType,
			Resource:     resourceID,
		}.Build(),
		DisplayName: displayName,
		Description: description,
	}.Build()
	err = c1f.PutResources(ctx, resource)
	require.NoError(t, err)

	// Add entitlement
	entitlement := v2.Entitlement_builder{
		Id:          entitlementID,
		DisplayName: entitlementName,
		Description: entitlementDesc,
		Slug:        entitlementSlug,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceType,
				Resource:     resourceID,
			}.Build(),
		}.Build(),
	}.Build()
	err = c1f.PutEntitlements(ctx, entitlement)
	require.NoError(t, err)

	// Add grant
	grant := v2.Grant_builder{
		Id: grantID,
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: resourceType,
				Resource:     resourceID,
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: entitlementID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceType,
					Resource:     resourceID,
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	err = c1f.PutGrants(ctx, grant)
	require.NoError(t, err)

	// End sync and close
	err = c1f.EndSync(ctx)
	require.NoError(t, err)

	err = c1f.Close()
	require.NoError(t, err)
}

// createMinimalC1ZFile creates a minimal c1z file for simple tests.
func createMinimalC1ZFile(t *testing.T, ctx context.Context, path string) {
	t.Helper()

	c1f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

	_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = c1f.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          "test-type",
		DisplayName: "Test Type",
	}.Build())
	require.NoError(t, err)

	err = c1f.EndSync(ctx)
	require.NoError(t, err)

	err = c1f.Close()
	require.NoError(t, err)
}

// verifyAnonymizedC1ZFile opens the anonymized file and verifies all data is properly anonymized.
func verifyAnonymizedC1ZFile(t *testing.T, ctx context.Context, path string,
	originalResourceType, originalResourceID, originalDisplayName, originalDescription string,
	originalEntitlementID, originalEntitlementName, originalEntitlementDesc, originalEntitlementSlug string,
	originalGrantID string,
) {
	t.Helper()

	c1f, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer c1f.Close()

	// Verify resource types are anonymized
	rtResp, err := c1f.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 1)

	rt := rtResp.GetList()[0]
	require.NotEqual(t, originalResourceType, rt.GetId(), "Resource type ID should be anonymized")
	require.Len(t, rt.GetId(), 8, "Resource type ID should be 8 chars (HashN with 8)")

	// Verify resources are anonymized
	rResp, err := c1f.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, rResp.GetList(), 1)

	r := rResp.GetList()[0]
	require.NotEqual(t, originalDisplayName, r.GetDisplayName(), "Resource display name should be anonymized")
	require.Len(t, r.GetDisplayName(), 16, "Resource display name should be 16 chars (HashN with 16)")
	require.NotEqual(t, originalDescription, r.GetDescription(), "Resource description should be anonymized")
	require.NotEqual(t, originalResourceID, r.GetId().GetResource(), "Resource ID should be anonymized")
	require.Len(t, r.GetId().GetResource(), max(32, len(originalResourceID)), "Resource ID length should be max(32, original)")

	// Verify entitlements are anonymized
	eResp, err := c1f.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, eResp.GetList(), 1)

	e := eResp.GetList()[0]
	require.NotEqual(t, originalEntitlementID, e.GetId(), "Entitlement ID should be anonymized")
	require.Len(t, e.GetId(), max(32, len(originalEntitlementID)), "Entitlement ID length should be max(32, original)")
	require.NotEqual(t, originalEntitlementName, e.GetDisplayName(), "Entitlement display name should be anonymized")
	require.Len(t, e.GetDisplayName(), 16, "Entitlement display name should be 16 chars")
	require.NotEqual(t, originalEntitlementDesc, e.GetDescription(), "Entitlement description should be anonymized")
	require.NotEqual(t, originalEntitlementSlug, e.GetSlug(), "Entitlement slug should be anonymized")
	require.Len(t, e.GetSlug(), 12, "Entitlement slug should be 12 chars")

	// Verify grants are anonymized
	gResp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, gResp.GetList(), 1)

	g := gResp.GetList()[0]
	require.NotEqual(t, originalGrantID, g.GetId(), "Grant ID should be anonymized")
	require.Len(t, g.GetId(), max(32, len(originalGrantID)), "Grant ID length should be max(32, original)")

	// Verify the principal resource within the grant is anonymized
	require.NotNil(t, g.GetPrincipal())
	require.NotEqual(t, originalResourceID, g.GetPrincipal().GetId().GetResource(),
		"Grant principal resource ID should be anonymized")

	// Verify the entitlement within the grant is anonymized
	require.NotNil(t, g.GetEntitlement())
	require.NotEqual(t, originalEntitlementID, g.GetEntitlement().GetId(),
		"Grant entitlement ID should be anonymized")
}
