package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func TestCompactorWithTmpDir(t *testing.T) {
	ctx := context.Background()

	inputSyncsDir, err := os.MkdirTemp("", "compactor-test")
	require.NoError(t, err)
	defer os.RemoveAll(inputSyncsDir)

	outputDir, err := os.MkdirTemp("", "compactor-output")
	require.NoError(t, err)
	defer os.RemoveAll(outputDir)

	// Create temporary directory for intermediate files
	tmpDir, err := os.MkdirTemp("", "compactor-tmp")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	runCompactorTest(t, ctx, inputSyncsDir, func(compactableSyncs []*CompactableSync) (*Compactor, func() error, error) {
		return NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(tmpDir))
	})
}

func runCompactorTest(t *testing.T, ctx context.Context, inputSyncsDir string, createCompactor func([]*CompactableSync) (*Compactor, func() error, error)) {
	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
	}

	// Create the first sync file
	firstSyncPath := filepath.Join(inputSyncsDir, "first-sync.c1z")
	firstSync, err := dotc1z.NewC1ZFile(ctx, firstSyncPath, opts...)
	require.NoError(t, err)

	// Start a new sync
	firstSyncID, isNewSync, err := firstSync.StartSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, firstSyncID)
	require.True(t, isNewSync)

	// Create resource types
	userResourceTypeID := "user"
	groupResourceTypeID := "group"
	roleResourceTypeID := "role"

	err = firstSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          userResourceTypeID,
		DisplayName: "User",
	})
	require.NoError(t, err)

	err = firstSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	})
	require.NoError(t, err)

	err = firstSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	})
	require.NoError(t, err)

	// Create resources for the first sync
	// Users
	onlyInFirstUser := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: userResourceTypeID,
			Resource:     "user-only-first",
		},
		DisplayName: "User Only First",
	}
	err = firstSync.PutResources(ctx, onlyInFirstUser)
	require.NoError(t, err)

	inBothUser := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: userResourceTypeID,
			Resource:     "user-in-both",
		},
		DisplayName: "User In Both",
	}
	err = firstSync.PutResources(ctx, inBothUser)
	require.NoError(t, err)

	// Groups
	onlyInFirstGroup := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: groupResourceTypeID,
			Resource:     "group-only-first",
		},
		DisplayName: "Group Only First",
	}
	err = firstSync.PutResources(ctx, onlyInFirstGroup)
	require.NoError(t, err)

	inBothGroup := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: groupResourceTypeID,
			Resource:     "group-in-both",
		},
		DisplayName: "Group In Both",
	}
	err = firstSync.PutResources(ctx, inBothGroup)
	require.NoError(t, err)

	// Roles
	adminRole := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: roleResourceTypeID,
			Resource:     "admin",
		},
		DisplayName: "Admin Role",
	}
	err = firstSync.PutResources(ctx, adminRole)
	require.NoError(t, err)

	// Create entitlements for the first sync
	memberEntitlement := &v2.Entitlement{
		Id:          "member",
		DisplayName: "Member",
		Resource:    inBothGroup,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}
	err = firstSync.PutEntitlements(ctx, memberEntitlement)
	require.NoError(t, err)

	adminEntitlement := &v2.Entitlement{
		Id:          "admin",
		DisplayName: "Admin",
		Resource:    adminRole,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}
	err = firstSync.PutEntitlements(ctx, adminEntitlement)
	require.NoError(t, err)

	// Create grants for the first sync
	// Grant user membership to group
	userGroupGrant := &v2.Grant{
		Id:          "user-group-grant",
		Principal:   onlyInFirstUser,
		Entitlement: memberEntitlement,
	}
	err = firstSync.PutGrants(ctx, userGroupGrant)
	require.NoError(t, err)

	// Grant user admin role
	userAdminGrant := &v2.Grant{
		Id:          "user-admin-grant",
		Principal:   inBothUser,
		Entitlement: adminEntitlement,
	}
	err = firstSync.PutGrants(ctx, userAdminGrant)
	require.NoError(t, err)

	// End the first sync
	err = firstSync.EndSync(ctx)
	require.NoError(t, err)
	err = firstSync.Close()
	require.NoError(t, err)

	// Create the second sync file
	secondSyncPath := filepath.Join(inputSyncsDir, "second-sync.c1z")
	secondSync, err := dotc1z.NewC1ZFile(ctx, secondSyncPath, opts...)
	require.NoError(t, err)

	// Start a new sync
	secondSyncID, isNewSync, err := secondSync.StartSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, secondSyncID)
	require.True(t, isNewSync)

	// Create the same resource types
	err = secondSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          userResourceTypeID,
		DisplayName: "User",
	})
	require.NoError(t, err)

	err = secondSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	})
	require.NoError(t, err)

	err = secondSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	})
	require.NoError(t, err)

	// Create resources for the second sync
	// Users - only in second sync
	onlyInSecondUser := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: userResourceTypeID,
			Resource:     "user-only-second",
		},
		DisplayName: "User Only Second",
	}
	err = secondSync.PutResources(ctx, onlyInSecondUser)
	require.NoError(t, err)

	// Users - in both syncs (same as first)
	inBothUserInSecond := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: userResourceTypeID,
			Resource:     "user-in-both",
		},
		DisplayName: "User In Both",
	}
	err = secondSync.PutResources(ctx, inBothUserInSecond)
	require.NoError(t, err)

	// Groups - only in second sync
	onlyInSecondGroup := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: groupResourceTypeID,
			Resource:     "group-only-second",
		},
		DisplayName: "Group Only Second",
	}
	err = secondSync.PutResources(ctx, onlyInSecondGroup)
	require.NoError(t, err)

	// Groups - in both syncs (same as first)
	inBothGroupInSecond := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: groupResourceTypeID,
			Resource:     "group-in-both",
		},
		DisplayName: "Group In Both",
	}
	err = secondSync.PutResources(ctx, inBothGroupInSecond)
	require.NoError(t, err)

	// Roles - viewer role only in second sync
	viewerRole := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: roleResourceTypeID,
			Resource:     "viewer",
		},
		DisplayName: "Viewer Role",
	}
	err = secondSync.PutResources(ctx, viewerRole)
	require.NoError(t, err)

	// Create entitlements for the second sync
	memberEntitlementSecond := &v2.Entitlement{
		Id:          "member",
		DisplayName: "Member",
		Resource:    inBothGroupInSecond,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}
	err = secondSync.PutEntitlements(ctx, memberEntitlementSecond)
	require.NoError(t, err)

	viewerEntitlement := &v2.Entitlement{
		Id:          "viewer",
		DisplayName: "Viewer",
		Resource:    viewerRole,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}
	err = secondSync.PutEntitlements(ctx, viewerEntitlement)
	require.NoError(t, err)

	// Create grants for the second sync
	// Grant new user membership to group
	newUserGroupGrant := &v2.Grant{
		Id:          "new-user-group-grant",
		Principal:   onlyInSecondUser,
		Entitlement: memberEntitlementSecond,
	}
	err = secondSync.PutGrants(ctx, newUserGroupGrant)
	require.NoError(t, err)

	// Grant user viewer role
	userViewerGrant := &v2.Grant{
		Id:          "user-viewer-grant",
		Principal:   inBothUserInSecond,
		Entitlement: viewerEntitlement,
	}
	err = secondSync.PutGrants(ctx, userViewerGrant)
	require.NoError(t, err)

	// End the second sync
	err = secondSync.EndSync(ctx)
	require.NoError(t, err)
	err = secondSync.Close()
	require.NoError(t, err)

	// Create compactable syncs
	firstCompactableSync := &CompactableSync{
		FilePath: firstSyncPath,
		SyncID:   firstSyncID,
	}
	secondCompactableSync := &CompactableSync{
		FilePath: secondSyncPath,
		SyncID:   secondSyncID,
	}

	// Create compactor
	compactableSyncs := []*CompactableSync{firstCompactableSync, secondCompactableSync}
	compactor, cleanup, err := createCompactor(compactableSyncs)
	require.NoError(t, err)
	defer func() {
		err := cleanup()
		require.NoError(t, err)
	}()

	// Compact the syncs
	compactedSync, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, compactedSync)

	// Open the compacted file
	compactedFile, err := dotc1z.NewC1ZFile(ctx, compactedSync.FilePath, opts...)
	require.NoError(t, err)
	defer compactedFile.Close()

	// Verify the compacted file contains the expected data

	// ========= Resource Types Verification =========
	// All resource types should be present
	userRtResp, err := compactedFile.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: userResourceTypeID,
	})
	require.NoError(t, err)
	require.Equal(t, userResourceTypeID, userRtResp.ResourceType.Id)
	require.Equal(t, "User", userRtResp.ResourceType.DisplayName)

	groupRtResp, err := compactedFile.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: groupResourceTypeID,
	})
	require.NoError(t, err)
	require.Equal(t, groupResourceTypeID, groupRtResp.ResourceType.Id)
	require.Equal(t, "Group", groupRtResp.ResourceType.DisplayName)

	roleRtResp, err := compactedFile.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: roleResourceTypeID,
	})
	require.NoError(t, err)
	require.Equal(t, roleResourceTypeID, roleRtResp.ResourceType.Id)
	require.Equal(t, "Role", roleRtResp.ResourceType.DisplayName)

	// ========= Resources Verification =========
	// Resources from first sync only
	onlyFirstUserResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInFirstUser.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInFirstUser.Id.Resource, onlyFirstUserResp.Resource.Id.Resource)
	require.Equal(t, "User Only First", onlyFirstUserResp.Resource.DisplayName)

	onlyFirstGroupResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInFirstGroup.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInFirstGroup.Id.Resource, onlyFirstGroupResp.Resource.Id.Resource)
	require.Equal(t, "Group Only First", onlyFirstGroupResp.Resource.DisplayName)

	// Resources from second sync only
	onlySecondUserResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInSecondUser.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInSecondUser.Id.Resource, onlySecondUserResp.Resource.Id.Resource)
	require.Equal(t, "User Only Second", onlySecondUserResp.Resource.DisplayName)

	onlySecondGroupResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInSecondGroup.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInSecondGroup.Id.Resource, onlySecondGroupResp.Resource.Id.Resource)
	require.Equal(t, "Group Only Second", onlySecondGroupResp.Resource.DisplayName)

	viewerRoleResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: viewerRole.Id,
	})
	require.NoError(t, err)
	require.Equal(t, viewerRole.Id.Resource, viewerRoleResp.Resource.Id.Resource)
	require.Equal(t, "Viewer Role", viewerRoleResp.Resource.DisplayName)

	// Resources in both syncs
	inBothUserResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: inBothUser.Id,
	})
	require.NoError(t, err)
	require.Equal(t, inBothUser.Id.Resource, inBothUserResp.Resource.Id.Resource)
	require.Equal(t, "User In Both", inBothUserResp.Resource.DisplayName)

	inBothGroupResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: inBothGroup.Id,
	})
	require.NoError(t, err)
	require.Equal(t, inBothGroup.Id.Resource, inBothGroupResp.Resource.Id.Resource)
	require.Equal(t, "Group In Both", inBothGroupResp.Resource.DisplayName)

	adminRoleResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: adminRole.Id,
	})
	require.NoError(t, err)
	require.Equal(t, adminRole.Id.Resource, adminRoleResp.Resource.Id.Resource)
	require.Equal(t, "Admin Role", adminRoleResp.Resource.DisplayName)

	// ========= Entitlements Verification =========
	// Entitlements from first sync
	memberEntitlementResp, err := compactedFile.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: memberEntitlement.Id,
	})
	require.NoError(t, err)
	require.Equal(t, memberEntitlement.Id, memberEntitlementResp.Entitlement.Id)
	require.Equal(t, "Member", memberEntitlementResp.Entitlement.DisplayName)

	adminEntitlementResp, err := compactedFile.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: adminEntitlement.Id,
	})
	require.NoError(t, err)
	require.Equal(t, adminEntitlement.Id, adminEntitlementResp.Entitlement.Id)
	require.Equal(t, "Admin", adminEntitlementResp.Entitlement.DisplayName)

	// Entitlements from second sync
	viewerEntitlementResp, err := compactedFile.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: viewerEntitlement.Id,
	})
	require.NoError(t, err)
	require.Equal(t, viewerEntitlement.Id, viewerEntitlementResp.Entitlement.Id)
	require.Equal(t, "Viewer", viewerEntitlementResp.Entitlement.DisplayName)

	// ========= Grants Verification =========
	// Grants from first sync
	userGroupGrantResp, err := compactedFile.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: userGroupGrant.Id,
	})
	require.NoError(t, err)
	require.Equal(t, userGroupGrant.Id, userGroupGrantResp.Grant.Id)

	userAdminGrantResp, err := compactedFile.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: userAdminGrant.Id,
	})
	require.NoError(t, err)
	require.Equal(t, userAdminGrant.Id, userAdminGrantResp.Grant.Id)

	// Grants from second sync
	newUserGroupGrantResp, err := compactedFile.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: newUserGroupGrant.Id,
	})
	require.NoError(t, err)
	require.Equal(t, newUserGroupGrant.Id, newUserGroupGrantResp.Grant.Id)

	userViewerGrantResp, err := compactedFile.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: userViewerGrant.Id,
	})
	require.NoError(t, err)
	require.Equal(t, userViewerGrant.Id, userViewerGrantResp.Grant.Id)
}
