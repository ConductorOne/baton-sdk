package synccompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAttachedCompactorWithTmpDir(t *testing.T) {
	ctx := context.Background()

	inputSyncsDir, err := os.MkdirTemp("", "compactor-test")
	require.NoError(t, err)
	defer os.RemoveAll(inputSyncsDir)

	outputDir, err := os.MkdirTemp("", "compactor-output")
	require.NoError(t, err)
	defer os.RemoveAll(outputDir)

	// Create temporary directory for intermediate files.
	tmpDir, err := os.MkdirTemp("", "compactor-tmp")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	runCompactorTest(t, ctx, inputSyncsDir, func(compactableSyncs []*CompactableSync) (*Compactor, func() error, error) {
		return NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(tmpDir), WithCompactorType(CompactorTypeAttached))
	})
}

func TestNaiveCompactorWithTmpDir(t *testing.T) {
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
		return NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(tmpDir), WithCompactorType(CompactorTypeNaive))
	})
}

func runCompactorTest(t *testing.T, ctx context.Context, inputSyncsDir string, createCompactor func([]*CompactableSync) (*Compactor, func() error, error)) {
	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
	}

	// Create the first sync file
	firstSyncPath := filepath.Join(inputSyncsDir, "first-sync.c1z")
	firstSync, err := dotc1z.NewC1ZFile(ctx, firstSyncPath, opts...)
	require.NoError(t, err)

	// Start a new sync
	firstSyncID, isNewSync, err := firstSync.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
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
	secondSyncID, err := secondSync.StartNewSync(ctx, connectorstore.SyncTypePartial, firstSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, secondSyncID)

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

	// Create the third sync file
	thirdSyncPath := filepath.Join(inputSyncsDir, "third-sync.c1z")
	thirdSync, err := dotc1z.NewC1ZFile(ctx, thirdSyncPath, opts...)
	require.NoError(t, err)

	// Start a new sync
	thirdSyncID, err := thirdSync.StartNewSync(ctx, connectorstore.SyncTypePartial, secondSyncID)
	require.NoError(t, err)
	require.NotEmpty(t, thirdSyncID)

	// Create the same resource types
	err = thirdSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          userResourceTypeID,
		DisplayName: "User",
	})
	require.NoError(t, err)

	err = thirdSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	})
	require.NoError(t, err)

	err = thirdSync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	})
	require.NoError(t, err)

	// Create resources for the third sync
	// Users - only in third sync
	onlyInThirdUser := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: userResourceTypeID,
			Resource:     "user-only-third",
		},
		DisplayName: "User Only Third",
	}
	err = thirdSync.PutResources(ctx, onlyInThirdUser)
	require.NoError(t, err)

	// Users - in all syncs (same as first and second)
	inAllUserInThird := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: userResourceTypeID,
			Resource:     "user-in-both",
		},
		DisplayName: "User In Both",
	}
	err = thirdSync.PutResources(ctx, inAllUserInThird)
	require.NoError(t, err)

	// Groups - only in third sync
	onlyInThirdGroup := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: groupResourceTypeID,
			Resource:     "group-only-third",
		},
		DisplayName: "Group Only Third",
	}
	err = thirdSync.PutResources(ctx, onlyInThirdGroup)
	require.NoError(t, err)

	// Groups - in all syncs (same as first and second)
	inAllGroupInThird := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: groupResourceTypeID,
			Resource:     "group-in-both",
		},
		DisplayName: "Group In Both",
	}
	err = thirdSync.PutResources(ctx, inAllGroupInThird)
	require.NoError(t, err)

	// Roles - moderator role only in third sync
	moderatorRole := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: roleResourceTypeID,
			Resource:     "moderator",
		},
		DisplayName: "Moderator Role",
	}
	err = thirdSync.PutResources(ctx, moderatorRole)
	require.NoError(t, err)

	// Create entitlements for the third sync
	memberEntitlementThird := &v2.Entitlement{
		Id:          "member",
		DisplayName: "Member",
		Resource:    inAllGroupInThird,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}
	err = thirdSync.PutEntitlements(ctx, memberEntitlementThird)
	require.NoError(t, err)

	moderatorEntitlement := &v2.Entitlement{
		Id:          "moderator",
		DisplayName: "Moderator",
		Resource:    moderatorRole,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}
	err = thirdSync.PutEntitlements(ctx, moderatorEntitlement)
	require.NoError(t, err)

	// Create grants for the third sync
	// Grant new user membership to group
	thirdUserGroupGrant := &v2.Grant{
		Id:          "third-user-group-grant",
		Principal:   onlyInThirdUser,
		Entitlement: memberEntitlementThird,
	}
	err = thirdSync.PutGrants(ctx, thirdUserGroupGrant)
	require.NoError(t, err)

	// Grant user moderator role
	userModeratorGrant := &v2.Grant{
		Id:          "user-moderator-grant",
		Principal:   inAllUserInThird,
		Entitlement: moderatorEntitlement,
	}
	err = thirdSync.PutGrants(ctx, userModeratorGrant)
	require.NoError(t, err)

	// End the third sync
	err = thirdSync.EndSync(ctx)
	require.NoError(t, err)
	err = thirdSync.Close()
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
	thirdCompactableSync := &CompactableSync{
		FilePath: thirdSyncPath,
		SyncID:   thirdSyncID,
	}

	// Create compactor
	compactableSyncs := []*CompactableSync{firstCompactableSync, secondCompactableSync, thirdCompactableSync}
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

	// The compacted file should have one full sync.
	syncs, _, err := compactedFile.ListSyncRuns(ctx, "", 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(syncs))
	require.Equal(t, connectorstore.SyncTypeFull, syncs[0].Type)

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

	// Resources from third sync only
	onlyThirdUserResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInThirdUser.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInThirdUser.Id.Resource, onlyThirdUserResp.Resource.Id.Resource)
	require.Equal(t, "User Only Third", onlyThirdUserResp.Resource.DisplayName)

	onlyThirdGroupResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: onlyInThirdGroup.Id,
	})
	require.NoError(t, err)
	require.Equal(t, onlyInThirdGroup.Id.Resource, onlyThirdGroupResp.Resource.Id.Resource)
	require.Equal(t, "Group Only Third", onlyThirdGroupResp.Resource.DisplayName)

	moderatorRoleResp, err := compactedFile.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: moderatorRole.Id,
	})
	require.NoError(t, err)
	require.Equal(t, moderatorRole.Id.Resource, moderatorRoleResp.Resource.Id.Resource)
	require.Equal(t, "Moderator Role", moderatorRoleResp.Resource.DisplayName)

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

	// Entitlements from third sync
	memberEntitlementThirdResp, err := compactedFile.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: memberEntitlementThird.Id,
	})
	require.NoError(t, err)
	require.Equal(t, memberEntitlementThird.Id, memberEntitlementThirdResp.Entitlement.Id)
	require.Equal(t, "Member", memberEntitlementThirdResp.Entitlement.DisplayName)

	moderatorEntitlementResp, err := compactedFile.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: moderatorEntitlement.Id,
	})
	require.NoError(t, err)
	require.Equal(t, moderatorEntitlement.Id, moderatorEntitlementResp.Entitlement.Id)
	require.Equal(t, "Moderator", moderatorEntitlementResp.Entitlement.DisplayName)

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

	// Grants from third sync
	thirdUserGroupGrantResp, err := compactedFile.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: thirdUserGroupGrant.Id,
	})
	require.NoError(t, err)
	require.Equal(t, thirdUserGroupGrant.Id, thirdUserGroupGrantResp.Grant.Id)

	userModeratorGrantResp, err := compactedFile.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: userModeratorGrant.Id,
	})
	require.NoError(t, err)
	require.Equal(t, userModeratorGrant.Id, userModeratorGrantResp.Grant.Id)
}

func makeEmptySync(t *testing.T, ctx context.Context, inputSyncsDir string, opts []dotc1z.C1ZOption, syncType connectorstore.SyncType) *CompactableSync {
	// Create the first sync file
	randomID := uuid.New().String()
	syncPath := filepath.Join(inputSyncsDir, fmt.Sprintf("sync-%s.c1z", randomID))
	sync, err := dotc1z.NewC1ZFile(ctx, syncPath, opts...)
	require.NoError(t, err)

	// Start a new sync
	syncID, isNewSync, err := sync.StartOrResumeSync(ctx, syncType, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)
	require.True(t, isNewSync)

	// Create resource types
	userResourceTypeID := "user"
	groupResourceTypeID := "group"
	roleResourceTypeID := "role"

	err = sync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          userResourceTypeID,
		DisplayName: "User",
	})
	require.NoError(t, err)

	err = sync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	})
	require.NoError(t, err)

	err = sync.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	})
	require.NoError(t, err)

	// End the first sync
	err = sync.EndSync(ctx)
	require.NoError(t, err)
	err = sync.Close()
	require.NoError(t, err)

	return &CompactableSync{
		FilePath: syncPath,
		SyncID:   syncID,
	}
}

// Test cases for sync type union logic.
func TestSyncTypeUnion_AttachedCompactor(t *testing.T) {
	runSyncTypeUnionTests(t, CompactorTypeAttached, getAllSyncTypeTestCases())
}

func TestSyncTypeUnion_NaiveCompactor(t *testing.T) {
	runSyncTypeUnionTests(t, CompactorTypeNaive, getBasicSyncTypeTestCases())
}

// getAllSyncTypeTestCases returns comprehensive test cases for sync type union logic.
func getAllSyncTypeTestCases() []syncTypeTestCase {
	return []syncTypeTestCase{
		// Two-sync combinations
		{
			name:     "Full + Full = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypeFull},
			expected: connectorstore.SyncTypeFull,
		},
		{
			name:     "Full + Partial = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeFull,
		},
		{
			name:     "Partial + Full = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeFull},
			expected: connectorstore.SyncTypeFull,
		},
		{
			name:     "Full + ResourcesOnly = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypeResourcesOnly},
			expected: connectorstore.SyncTypeFull,
		},
		{
			name:     "ResourcesOnly + Full = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypeFull},
			expected: connectorstore.SyncTypeFull,
		},
		{
			name:     "ResourcesOnly + ResourcesOnly = ResourcesOnly",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypeResourcesOnly},
			expected: connectorstore.SyncTypeResourcesOnly,
		},
		{
			name:     "ResourcesOnly + Partial = ResourcesOnly",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeResourcesOnly,
		},
		{
			name:     "Partial + ResourcesOnly = ResourcesOnly",
			input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeResourcesOnly},
			expected: connectorstore.SyncTypeResourcesOnly,
		},
		{
			name:     "Partial + Partial = Partial",
			input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypePartial,
		},

		// Three-sync combinations
		{
			name:     "Full + Partial + ResourcesOnly = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypePartial, connectorstore.SyncTypeResourcesOnly},
			expected: connectorstore.SyncTypeFull,
		},
		{
			name:     "Partial + ResourcesOnly + Partial = ResourcesOnly",
			input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeResourcesOnly,
		},
		{
			name:     "Partial + Partial + Partial = Partial",
			input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypePartial, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypePartial,
		},

		// Four-sync combinations
		{
			name:     "ResourcesOnly + Partial + Partial + Partial = ResourcesOnly",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial, connectorstore.SyncTypePartial, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeResourcesOnly,
		},
		{
			name:     "Partial + Full + ResourcesOnly + Partial = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeFull, connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeFull,
		},
	}
}

// getBasicSyncTypeTestCases returns a subset of test cases for basic validation.
func getBasicSyncTypeTestCases() []syncTypeTestCase {
	return []syncTypeTestCase{
		{
			name:     "Full + Partial = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeFull,
		},
		{
			name:     "ResourcesOnly + Partial = ResourcesOnly",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeResourcesOnly,
		},
		{
			name:     "Partial + Partial = Partial",
			input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypePartial,
		},
		{
			name:     "Full + ResourcesOnly + Partial = Full",
			input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
			expected: connectorstore.SyncTypeFull,
		},
	}
}

type syncTypeTestCase struct {
	name     string
	input    []connectorstore.SyncType
	expected connectorstore.SyncType
}

// runSyncTypeUnionTests runs sync type union tests for a specific compactor type.
func runSyncTypeUnionTests(t *testing.T, compactorType CompactorType, testCases []syncTypeTestCase) {
	ctx := context.Background()

	inputSyncsDir, err := os.MkdirTemp("", "compactor-sync-type-test")
	require.NoError(t, err)
	defer os.RemoveAll(inputSyncsDir)

	outputDir, err := os.MkdirTemp("", "compactor-output")
	require.NoError(t, err)
	defer os.RemoveAll(outputDir)

	// Create temporary directory for intermediate files.
	tmpDir, err := os.MkdirTemp("", "compactor-tmp")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	createCompactor := func(compactableSyncs []*CompactableSync) (*Compactor, func() error, error) {
		return NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(tmpDir), WithCompactorType(compactorType))
	}

	// Run all test cases.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runSyncTypeTest(t, ctx, inputSyncsDir, createCompactor, tc.input, tc.expected)
		})
	}
}

func runSyncTypeTest(
	t *testing.T,
	ctx context.Context,
	inputSyncsDir string,
	createCompactor func([]*CompactableSync) (*Compactor, func() error, error),
	types []connectorstore.SyncType,
	expectedSyncType connectorstore.SyncType,
) {
	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
	}

	// Create empty syncs for each sync type.
	var compactableSyncs []*CompactableSync
	for _, syncType := range types {
		compactableSync := makeEmptySync(t, ctx, inputSyncsDir, opts, syncType)
		compactableSyncs = append(compactableSyncs, compactableSync)
	}

	// Create compactor using the callback.
	compactor, cleanup, err := createCompactor(compactableSyncs)
	require.NoError(t, err)
	defer func() {
		err := cleanup()
		require.NoError(t, err)
	}()

	// Run the compaction.
	compactedSync, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, compactedSync)

	// Open the compacted file.
	compactedFile, err := dotc1z.NewC1ZFile(ctx, compactedSync.FilePath, opts...)
	require.NoError(t, err)
	defer compactedFile.Close()

	syncRuns, _, err := compactedFile.ListSyncRuns(ctx, "", 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(syncRuns))
	require.Equal(t, expectedSyncType, syncRuns[0].Type)
}

// The compacting two partial syncs should result in a partial sync.
func TestAttachedCompactorFailsWithNoFullSyncInBase(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	baseFile := filepath.Join(tmpDir, "base.c1z")
	appliedFile := filepath.Join(tmpDir, "applied.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithPragma("journal_mode", "WAL"),
		dotc1z.WithTmpDir(tmpDir),
	}

	baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, opts...)
	require.NoError(t, err)

	baseSyncID, err := baseDB.StartNewSync(ctx, connectorstore.SyncTypePartial, "some-parent")
	require.NoError(t, err)
	require.NotEmpty(t, baseSyncID)

	err = baseDB.EndSync(ctx)
	require.NoError(t, err)
	err = baseDB.Close()
	require.NoError(t, err)

	baseCompactableSync := &CompactableSync{
		FilePath: baseFile,
		SyncID:   baseSyncID,
	}

	appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, opts...)
	require.NoError(t, err)

	appliedSyncID, err := appliedDB.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NotEmpty(t, appliedSyncID)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)
	err = appliedDB.Close()
	require.NoError(t, err)

	appliedCompactableSync := &CompactableSync{
		FilePath: appliedFile,
		SyncID:   appliedSyncID,
	}

	compactableSyncs := []*CompactableSync{baseCompactableSync, appliedCompactableSync}
	compactor, cleanup, err := NewCompactor(
		ctx,
		tmpDir,
		compactableSyncs,
		WithTmpDir(tmpDir),
		WithCompactorType(CompactorTypeAttached))
	require.NoError(t, err)
	defer func() {
		err := cleanup()
		require.NoError(t, err)
	}()

	compactedSync, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, compactedSync)

	compactedFile, err := dotc1z.NewC1ZFile(ctx, compactedSync.FilePath, opts...)
	require.NoError(t, err)
	defer compactedFile.Close()

	// The compacted file should have one partial sync.
	syncRuns, _, err := compactedFile.ListSyncRuns(ctx, "", 10)
	require.NoError(t, err)
	require.Equal(t, connectorstore.SyncTypePartial, syncRuns[0].Type)
	require.Equal(t, compactedSync.SyncID, syncRuns[0].ID)
	require.NotNil(t, syncRuns[0].EndedAt)
}
