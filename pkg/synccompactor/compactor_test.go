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

	err = firstSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          userResourceTypeID,
		DisplayName: "User",
	}.Build())
	require.NoError(t, err)

	err = firstSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	}.Build())
	require.NoError(t, err)

	err = firstSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	}.Build())
	require.NoError(t, err)

	// Create resources for the first sync
	// Users
	onlyInFirstUser := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: userResourceTypeID,
			Resource:     "user-only-first",
		}.Build(),
		DisplayName: "User Only First",
	}.Build()
	err = firstSync.PutResources(ctx, onlyInFirstUser)
	require.NoError(t, err)

	inBothUser := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: userResourceTypeID,
			Resource:     "user-in-both",
		}.Build(),
		DisplayName: "User In Both",
	}.Build()
	err = firstSync.PutResources(ctx, inBothUser)
	require.NoError(t, err)

	// Groups
	onlyInFirstGroup := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: groupResourceTypeID,
			Resource:     "group-only-first",
		}.Build(),
		DisplayName: "Group Only First",
	}.Build()
	err = firstSync.PutResources(ctx, onlyInFirstGroup)
	require.NoError(t, err)

	inBothGroup := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: groupResourceTypeID,
			Resource:     "group-in-both",
		}.Build(),
		DisplayName: "Group In Both",
	}.Build()
	err = firstSync.PutResources(ctx, inBothGroup)
	require.NoError(t, err)

	// Roles
	adminRole := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: roleResourceTypeID,
			Resource:     "admin",
		}.Build(),
		DisplayName: "Admin Role",
	}.Build()
	err = firstSync.PutResources(ctx, adminRole)
	require.NoError(t, err)

	// Create entitlements for the first sync
	memberEntitlement := v2.Entitlement_builder{
		Id:          "member",
		DisplayName: "Member",
		Resource:    inBothGroup,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	err = firstSync.PutEntitlements(ctx, memberEntitlement)
	require.NoError(t, err)

	adminEntitlement := v2.Entitlement_builder{
		Id:          "admin",
		DisplayName: "Admin",
		Resource:    adminRole,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	err = firstSync.PutEntitlements(ctx, adminEntitlement)
	require.NoError(t, err)

	// Create grants for the first sync
	// Grant user membership to group
	userGroupGrant := v2.Grant_builder{
		Id:          "user-group-grant",
		Principal:   onlyInFirstUser,
		Entitlement: memberEntitlement,
	}.Build()
	err = firstSync.PutGrants(ctx, userGroupGrant)
	require.NoError(t, err)

	// Grant user admin role
	userAdminGrant := v2.Grant_builder{
		Id:          "user-admin-grant",
		Principal:   inBothUser,
		Entitlement: adminEntitlement,
	}.Build()
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
	err = secondSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          userResourceTypeID,
		DisplayName: "User",
	}.Build())
	require.NoError(t, err)

	err = secondSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	}.Build())
	require.NoError(t, err)

	err = secondSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	}.Build())
	require.NoError(t, err)

	// Create resources for the second sync
	// Users - only in second sync
	onlyInSecondUser := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: userResourceTypeID,
			Resource:     "user-only-second",
		}.Build(),
		DisplayName: "User Only Second",
	}.Build()
	err = secondSync.PutResources(ctx, onlyInSecondUser)
	require.NoError(t, err)

	// Users - in both syncs (same as first)
	inBothUserInSecond := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: userResourceTypeID,
			Resource:     "user-in-both",
		}.Build(),
		DisplayName: "User In Both",
	}.Build()
	err = secondSync.PutResources(ctx, inBothUserInSecond)
	require.NoError(t, err)

	// Groups - only in second sync
	onlyInSecondGroup := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: groupResourceTypeID,
			Resource:     "group-only-second",
		}.Build(),
		DisplayName: "Group Only Second",
	}.Build()
	err = secondSync.PutResources(ctx, onlyInSecondGroup)
	require.NoError(t, err)

	// Groups - in both syncs (same as first)
	inBothGroupInSecond := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: groupResourceTypeID,
			Resource:     "group-in-both",
		}.Build(),
		DisplayName: "Group In Both",
	}.Build()
	err = secondSync.PutResources(ctx, inBothGroupInSecond)
	require.NoError(t, err)

	// Roles - viewer role only in second sync
	viewerRole := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: roleResourceTypeID,
			Resource:     "viewer",
		}.Build(),
		DisplayName: "Viewer Role",
	}.Build()
	err = secondSync.PutResources(ctx, viewerRole)
	require.NoError(t, err)

	// Create entitlements for the second sync
	memberEntitlementSecond := v2.Entitlement_builder{
		Id:          "member",
		DisplayName: "Member",
		Resource:    inBothGroupInSecond,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	err = secondSync.PutEntitlements(ctx, memberEntitlementSecond)
	require.NoError(t, err)

	viewerEntitlement := v2.Entitlement_builder{
		Id:          "viewer",
		DisplayName: "Viewer",
		Resource:    viewerRole,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	err = secondSync.PutEntitlements(ctx, viewerEntitlement)
	require.NoError(t, err)

	// Create grants for the second sync
	// Grant new user membership to group
	newUserGroupGrant := v2.Grant_builder{
		Id:          "new-user-group-grant",
		Principal:   onlyInSecondUser,
		Entitlement: memberEntitlementSecond,
	}.Build()
	err = secondSync.PutGrants(ctx, newUserGroupGrant)
	require.NoError(t, err)

	// Grant user viewer role
	userViewerGrant := v2.Grant_builder{
		Id:          "user-viewer-grant",
		Principal:   inBothUserInSecond,
		Entitlement: viewerEntitlement,
	}.Build()
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
	err = thirdSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          userResourceTypeID,
		DisplayName: "User",
	}.Build())
	require.NoError(t, err)

	err = thirdSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	}.Build())
	require.NoError(t, err)

	err = thirdSync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	}.Build())
	require.NoError(t, err)

	// Create resources for the third sync
	// Users - only in third sync
	onlyInThirdUser := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: userResourceTypeID,
			Resource:     "user-only-third",
		}.Build(),
		DisplayName: "User Only Third",
	}.Build()
	err = thirdSync.PutResources(ctx, onlyInThirdUser)
	require.NoError(t, err)

	// Users - in all syncs (same as first and second)
	inAllUserInThird := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: userResourceTypeID,
			Resource:     "user-in-both",
		}.Build(),
		DisplayName: "User In Both",
	}.Build()
	err = thirdSync.PutResources(ctx, inAllUserInThird)
	require.NoError(t, err)

	// Groups - only in third sync
	onlyInThirdGroup := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: groupResourceTypeID,
			Resource:     "group-only-third",
		}.Build(),
		DisplayName: "Group Only Third",
	}.Build()
	err = thirdSync.PutResources(ctx, onlyInThirdGroup)
	require.NoError(t, err)

	// Groups - in all syncs (same as first and second)
	inAllGroupInThird := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: groupResourceTypeID,
			Resource:     "group-in-both",
		}.Build(),
		DisplayName: "Group In Both",
	}.Build()
	err = thirdSync.PutResources(ctx, inAllGroupInThird)
	require.NoError(t, err)

	// Roles - moderator role only in third sync
	moderatorRole := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: roleResourceTypeID,
			Resource:     "moderator",
		}.Build(),
		DisplayName: "Moderator Role",
	}.Build()
	err = thirdSync.PutResources(ctx, moderatorRole)
	require.NoError(t, err)

	// Create entitlements for the third sync
	memberEntitlementThird := v2.Entitlement_builder{
		Id:          "member",
		DisplayName: "Member",
		Resource:    inAllGroupInThird,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	err = thirdSync.PutEntitlements(ctx, memberEntitlementThird)
	require.NoError(t, err)

	moderatorEntitlement := v2.Entitlement_builder{
		Id:          "moderator",
		DisplayName: "Moderator",
		Resource:    moderatorRole,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	err = thirdSync.PutEntitlements(ctx, moderatorEntitlement)
	require.NoError(t, err)

	// Create grants for the third sync
	// Grant new user membership to group
	thirdUserGroupGrant := v2.Grant_builder{
		Id:          "third-user-group-grant",
		Principal:   onlyInThirdUser,
		Entitlement: memberEntitlementThird,
	}.Build()
	err = thirdSync.PutGrants(ctx, thirdUserGroupGrant)
	require.NoError(t, err)

	// Grant user moderator role
	userModeratorGrant := v2.Grant_builder{
		Id:          "user-moderator-grant",
		Principal:   inAllUserInThird,
		Entitlement: moderatorEntitlement,
	}.Build()
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
	userRtResp, err := compactedFile.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: userResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, userResourceTypeID, userRtResp.GetResourceType().GetId())
	require.Equal(t, "User", userRtResp.GetResourceType().GetDisplayName())

	groupRtResp, err := compactedFile.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: groupResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, groupResourceTypeID, groupRtResp.GetResourceType().GetId())
	require.Equal(t, "Group", groupRtResp.GetResourceType().GetDisplayName())

	roleRtResp, err := compactedFile.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: roleResourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, roleResourceTypeID, roleRtResp.GetResourceType().GetId())
	require.Equal(t, "Role", roleRtResp.GetResourceType().GetDisplayName())

	// ========= Resources Verification =========
	// Resources from first sync only
	onlyFirstUserResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: onlyInFirstUser.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, onlyInFirstUser.GetId().GetResource(), onlyFirstUserResp.GetResource().GetId().GetResource())
	require.Equal(t, "User Only First", onlyFirstUserResp.GetResource().GetDisplayName())

	onlyFirstGroupResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: onlyInFirstGroup.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, onlyInFirstGroup.GetId().GetResource(), onlyFirstGroupResp.GetResource().GetId().GetResource())
	require.Equal(t, "Group Only First", onlyFirstGroupResp.GetResource().GetDisplayName())

	// Resources from second sync only
	onlySecondUserResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: onlyInSecondUser.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, onlyInSecondUser.GetId().GetResource(), onlySecondUserResp.GetResource().GetId().GetResource())
	require.Equal(t, "User Only Second", onlySecondUserResp.GetResource().GetDisplayName())

	onlySecondGroupResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: onlyInSecondGroup.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, onlyInSecondGroup.GetId().GetResource(), onlySecondGroupResp.GetResource().GetId().GetResource())
	require.Equal(t, "Group Only Second", onlySecondGroupResp.GetResource().GetDisplayName())

	viewerRoleResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: viewerRole.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, viewerRole.GetId().GetResource(), viewerRoleResp.GetResource().GetId().GetResource())
	require.Equal(t, "Viewer Role", viewerRoleResp.GetResource().GetDisplayName())

	// Resources in both syncs
	inBothUserResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: inBothUser.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, inBothUser.GetId().GetResource(), inBothUserResp.GetResource().GetId().GetResource())
	require.Equal(t, "User In Both", inBothUserResp.GetResource().GetDisplayName())

	inBothGroupResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: inBothGroup.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, inBothGroup.GetId().GetResource(), inBothGroupResp.GetResource().GetId().GetResource())
	require.Equal(t, "Group In Both", inBothGroupResp.GetResource().GetDisplayName())

	adminRoleResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: adminRole.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, adminRole.GetId().GetResource(), adminRoleResp.GetResource().GetId().GetResource())
	require.Equal(t, "Admin Role", adminRoleResp.GetResource().GetDisplayName())

	// Resources from third sync only
	onlyThirdUserResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: onlyInThirdUser.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, onlyInThirdUser.GetId().GetResource(), onlyThirdUserResp.GetResource().GetId().GetResource())
	require.Equal(t, "User Only Third", onlyThirdUserResp.GetResource().GetDisplayName())

	onlyThirdGroupResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: onlyInThirdGroup.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, onlyInThirdGroup.GetId().GetResource(), onlyThirdGroupResp.GetResource().GetId().GetResource())
	require.Equal(t, "Group Only Third", onlyThirdGroupResp.GetResource().GetDisplayName())

	moderatorRoleResp, err := compactedFile.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: moderatorRole.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, moderatorRole.GetId().GetResource(), moderatorRoleResp.GetResource().GetId().GetResource())
	require.Equal(t, "Moderator Role", moderatorRoleResp.GetResource().GetDisplayName())

	// ========= Entitlements Verification =========
	// Entitlements from first sync
	memberEntitlementResp, err := compactedFile.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: memberEntitlement.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, memberEntitlement.GetId(), memberEntitlementResp.GetEntitlement().GetId())
	require.Equal(t, "Member", memberEntitlementResp.GetEntitlement().GetDisplayName())

	adminEntitlementResp, err := compactedFile.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: adminEntitlement.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, adminEntitlement.GetId(), adminEntitlementResp.GetEntitlement().GetId())
	require.Equal(t, "Admin", adminEntitlementResp.GetEntitlement().GetDisplayName())

	// Entitlements from second sync
	viewerEntitlementResp, err := compactedFile.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: viewerEntitlement.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, viewerEntitlement.GetId(), viewerEntitlementResp.GetEntitlement().GetId())
	require.Equal(t, "Viewer", viewerEntitlementResp.GetEntitlement().GetDisplayName())

	// Entitlements from third sync
	memberEntitlementThirdResp, err := compactedFile.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: memberEntitlementThird.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, memberEntitlementThird.GetId(), memberEntitlementThirdResp.GetEntitlement().GetId())
	require.Equal(t, "Member", memberEntitlementThirdResp.GetEntitlement().GetDisplayName())

	moderatorEntitlementResp, err := compactedFile.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: moderatorEntitlement.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, moderatorEntitlement.GetId(), moderatorEntitlementResp.GetEntitlement().GetId())
	require.Equal(t, "Moderator", moderatorEntitlementResp.GetEntitlement().GetDisplayName())

	// ========= Grants Verification =========
	// Grants from first sync
	userGroupGrantResp, err := compactedFile.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: userGroupGrant.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, userGroupGrant.GetId(), userGroupGrantResp.GetGrant().GetId())

	userAdminGrantResp, err := compactedFile.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: userAdminGrant.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, userAdminGrant.GetId(), userAdminGrantResp.GetGrant().GetId())

	// Grants from second sync
	newUserGroupGrantResp, err := compactedFile.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: newUserGroupGrant.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, newUserGroupGrant.GetId(), newUserGroupGrantResp.GetGrant().GetId())

	userViewerGrantResp, err := compactedFile.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: userViewerGrant.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, userViewerGrant.GetId(), userViewerGrantResp.GetGrant().GetId())

	// Grants from third sync
	thirdUserGroupGrantResp, err := compactedFile.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: thirdUserGroupGrant.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, thirdUserGroupGrant.GetId(), thirdUserGroupGrantResp.GetGrant().GetId())

	userModeratorGrantResp, err := compactedFile.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: userModeratorGrant.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, userModeratorGrant.GetId(), userModeratorGrantResp.GetGrant().GetId())
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

	err = sync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          userResourceTypeID,
		DisplayName: "User",
	}.Build())
	require.NoError(t, err)

	err = sync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	}.Build())
	require.NoError(t, err)

	err = sync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	}.Build())
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
	ctx := t.Context()

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
		return NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(tmpDir), WithCompactorType(CompactorTypeAttached))
	}

	// Run all test cases.
	for _, tc := range allSyncTypeTestCases {
		t.Run(tc.name, func(t *testing.T) {
			runSyncTypeTest(t, ctx, inputSyncsDir, createCompactor, tc.input, tc.expected)
		})
	}
}

// getAllSyncTypeTestCases returns comprehensive test cases for sync type union logic.
var allSyncTypeTestCases = []syncTypeTestCase{
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

type syncTypeTestCase struct {
	name     string
	input    []connectorstore.SyncType
	expected connectorstore.SyncType
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
	require.Len(t, syncRuns, 1)
	require.Equal(t, connectorstore.SyncTypePartial, syncRuns[0].Type)
	require.Equal(t, compactedSync.SyncID, syncRuns[0].ID)
	require.NotNil(t, syncRuns[0].EndedAt)
}
