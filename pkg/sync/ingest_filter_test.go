package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

func ingestFilterResource(resourceTypeID, resourceID string) *v2.Resource {
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: resourceTypeID,
			Resource:     resourceID,
		}.Build(),
		DisplayName: resourceID,
	}.Build()
}

func ingestFilterEntitlement(resource *v2.Resource, id string) *v2.Entitlement {
	return v2.Entitlement_builder{Resource: resource, Id: id, Slug: id}.Build()
}

func ingestFilterGrant(id string, entitlement *v2.Entitlement, principal *v2.Resource, annos ...*anypb.Any) *v2.Grant {
	return v2.Grant_builder{
		Id:          id,
		Entitlement: entitlement,
		Principal:   principal,
		Annotations: annos,
	}.Build()
}

func newIngestFilterStore(ctx context.Context, t *testing.T, engine c1zstore.Engine) c1zstore.Store {
	t.Helper()
	tmpDir := t.TempDir()
	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "filter.c1z"),
		dotc1z.WithEngine(engine),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close(context.Background()))
	})
	return store
}

func TestFreshIngestFilterMirrorsMachineryExemptions(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)

	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			store := newIngestFilterStore(ctx, t, engine)
			require.NoError(t, store.PutResourceTypes(ctx,
				v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
				v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
			))

			group := ingestFilterResource("group", "g1")
			user := ingestFilterResource("user", "u1")
			disabled := ingestFilterResource("iam_policy", "p1")
			goodEntitlement := ingestFilterEntitlement(group, "group:g1:member")
			disabledEntitlement := ingestFilterEntitlement(disabled, "iam_policy:p1:assigned")
			s := &syncer{store: store, syncType: connectorstore.SyncTypeFull}

			entitlements, err := s.filterFreshEntitlements(ctx, []*v2.Entitlement{goodEntitlement, disabledEntitlement})
			require.NoError(t, err)
			require.Equal(t, []*v2.Entitlement{goodEntitlement}, entitlements)

			plainDisabledEntitlement := ingestFilterGrant("plain-disabled-entitlement", disabledEntitlement, user)
			// InsertResourceGrants does not exempt the entitlement-type check:
			// uplift can't use a grant whose entitlement resource type is
			// absent from the sync, so keeping it would be dead data.
			irgDisabledEntitlement := ingestFilterGrant(
				"irg-disabled-entitlement", disabledEntitlement, user,
				annotations.New(&v2.InsertResourceGrants{})...,
			)
			plainDisabledPrincipal := ingestFilterGrant("plain-disabled-principal", goodEntitlement, disabled)
			matchDisabledPrincipal := ingestFilterGrant(
				"match-disabled-principal", goodEntitlement, disabled,
				annotations.New(v2.ExternalResourceMatchID_builder{Id: "external-p1"}.Build())...,
			)
			matchDoesNotExemptEntitlement := ingestFilterGrant(
				"match-disabled-entitlement", disabledEntitlement, disabled,
				annotations.New(v2.ExternalResourceMatchID_builder{Id: "external-p1"}.Build())...,
			)
			irgDoesNotExemptPrincipal := ingestFilterGrant(
				"irg-disabled-principal", disabledEntitlement, disabled,
				annotations.New(&v2.InsertResourceGrants{})...,
			)
			good := ingestFilterGrant("good", goodEntitlement, user)

			grants, err := s.filterFreshGrants(ctx, []*v2.Grant{
				plainDisabledEntitlement,
				irgDisabledEntitlement,
				plainDisabledPrincipal,
				matchDisabledPrincipal,
				matchDoesNotExemptEntitlement,
				irgDoesNotExemptPrincipal,
				good,
			})
			require.NoError(t, err)
			require.Equal(t, []*v2.Grant{matchDisabledPrincipal, good}, grants)
			require.Equal(t, int64(1), s.ingestFilterStats.entitlementsDropped.Load())
			require.Equal(t, int64(5), s.ingestFilterStats.grantsDropped.Load())
		})
	}
}

func TestFreshIngestFilterNarrowsExpansionWithoutWidening(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	store := newIngestFilterStore(ctx, t, c1zstore.EnginePebble)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
	))

	group := ingestFilterResource("group", "g1")
	user := ingestFilterResource("user", "u1")
	entitlement := ingestFilterEntitlement(group, "group:g1:member")
	expandable := func(id string, resourceTypeIDs ...string) *v2.Grant {
		return ingestFilterGrant(id, entitlement, user, annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{"group:g2:member"},
			ResourceTypeIds: resourceTypeIDs,
		}.Build())...)
	}
	mixed := expandable("mixed", "user", "iam_policy")
	disabledOnly := expandable("disabled-only", "iam_policy")
	unfiltered := expandable("unfiltered")
	matchExpansion := expandable("match", "iam_policy")
	matchExpansion.SetAnnotations(append(
		matchExpansion.GetAnnotations(),
		annotations.New(v2.ExternalResourceMatchID_builder{Id: "external-user"}.Build())...,
	))

	s := &syncer{store: store, syncType: connectorstore.SyncTypeFull}
	filtered, err := s.filterFreshGrants(ctx, []*v2.Grant{mixed, disabledOnly, unfiltered, matchExpansion})
	require.NoError(t, err)
	require.Len(t, filtered, 4, "filtering expansion metadata does not drop the base grant")
	require.NotSame(t, mixed, filtered[0], "rewriting must not mutate connector-owned response objects")
	require.NotSame(t, disabledOnly, filtered[1], "rewriting must not mutate connector-owned response objects")
	mixed, disabledOnly, unfiltered, matchExpansion = filtered[0], filtered[1], filtered[2], filtered[3]

	mixedExpansion := &v2.GrantExpandable{}
	mixedAnnos := annotations.Annotations(mixed.GetAnnotations())
	ok, err := mixedAnnos.Pick(mixedExpansion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"user"}, mixedExpansion.GetResourceTypeIds())

	disabledOnlyAnnos := annotations.Annotations(disabledOnly.GetAnnotations())
	ok, err = disabledOnlyAnnos.Pick(&v2.GrantExpandable{})
	require.NoError(t, err)
	require.False(t, ok, "empty intersection must remove expansion; an empty filter means unfiltered")

	unfilteredExpansion := &v2.GrantExpandable{}
	unfilteredAnnos := annotations.Annotations(unfiltered.GetAnnotations())
	ok, err = unfilteredAnnos.Pick(unfilteredExpansion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, unfilteredExpansion.GetResourceTypeIds())

	matchExpandable := &v2.GrantExpandable{}
	matchAnnos := annotations.Annotations(matchExpansion.GetAnnotations())
	ok, err = matchAnnos.Pick(matchExpandable)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"iam_policy"}, matchExpandable.GetResourceTypeIds(),
		"external-match placeholders remain available for later remapping")
}

func TestFreshIngestFilterRunsInFullSyncPipeline(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "pipeline.c1z")

	policyRT := v2.ResourceType_builder{Id: "iam_policy", DisplayName: "IAM Policy"}.Build()
	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType, policyRT)
	group, goodEntitlement, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	user, err := mc.AddUser(ctx, "u1")
	require.NoError(t, err)
	disabled := ingestFilterResource("iam_policy", "p1")
	disabledEntitlement := ingestFilterEntitlement(disabled, "iam_policy:p1:assigned")
	mc.entDB[group.GetId().GetResource()] = append(mc.entDB[group.GetId().GetResource()], disabledEntitlement)
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{
		ingestFilterGrant("good", goodEntitlement, user),
		ingestFilterGrant("disabled-entitlement", disabledEntitlement, user),
		ingestFilterGrant("disabled-principal", goodEntitlement, disabled),
		ingestFilterGrant(
			"match-carrier", goodEntitlement, disabled,
			annotations.New(v2.ExternalResourceMatchID_builder{Id: "external-p1"}.Build())...,
		),
	}

	syncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(path),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithSyncResourceTypes([]string{"group", "user"}),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	latest, ok := store.(connectorstore.LatestFinishedSyncIDFetcher)
	require.True(t, ok)
	syncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentSync(ctx, syncID))
	entitlementsResp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	for _, entitlement := range entitlementsResp.GetList() {
		require.NotEqual(t, "iam_policy", entitlement.GetResource().GetId().GetResourceType())
	}
	grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"good", "match-carrier"}, grantIDs(grantsResp.GetList()))
}

func grantIDs(grants []*v2.Grant) []string {
	out := make([]string, 0, len(grants))
	for _, grant := range grants {
		out = append(out, grant.GetId())
	}
	return out
}

func TestFreshIngestFilterBypassesPartialSyncs(t *testing.T) {
	s := &syncer{syncType: connectorstore.SyncTypePartial}
	disabled := ingestFilterEntitlement(ingestFilterResource("iam_policy", "p1"), "iam_policy:p1:assigned")
	entitlements, err := s.filterFreshEntitlements(t.Context(), []*v2.Entitlement{disabled})
	require.NoError(t, err)
	require.Equal(t, []*v2.Entitlement{disabled}, entitlements)
}

// TestFreshIngestFilterExternalMatchResolvesUnknownPrincipalType exercises the
// interplay between the fresh-ingest filter and the external-resource phase.
// The connector emits two grants whose placeholder principals reference a
// resource type it never lists ("hris_user"):
//
//   - one carries ExternalResourceMatch, so the filter must keep it; the
//     external phase later rewrites it to the matched external user (whose
//     "user" type is copied into the store by that phase), and
//   - one carries no annotations, so nothing downstream could ever resolve it
//     and the filter must drop it.
func TestFreshIngestFilterExternalMatchResolvesUnknownPrincipalType(t *testing.T) {
	runWithSyncModes(t, func(t *testing.T, extraOpts []SyncOpt) {
		ctx := t.Context()
		tempDir := t.TempDir()

		externalMc := newMockConnector()
		externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)
		externalUser, err := externalMc.AddUserProfile(ctx, "ext-user-1", map[string]any{
			"external_id_match": "ext-1",
		})
		require.NoError(t, err)

		internalMc := newMockConnector()
		// The internal connector lists only "group": the placeholder type
		// "hris_user" is never listed, and "user" only arrives when the
		// external phase copies it over — after grant collection ran.
		internalMc.rtDB = append(internalMc.rtDB, groupResourceType)
		internalGroup, _, err := internalMc.AddGroup(ctx, "g1")
		require.NoError(t, err)

		placeholder := func(id string) *v2.ResourceId {
			return v2.ResourceId_builder{ResourceType: "hris_user", Resource: id}.Build()
		}
		internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
			gt.NewGrant(internalGroup, "member", placeholder("match-me"),
				gt.WithAnnotation(v2.ExternalResourceMatch_builder{
					Key:          "external_id_match",
					Value:        "ext-1",
					ResourceType: v2.ResourceType_TRAIT_USER,
				}.Build()),
			),
			gt.NewGrant(internalGroup, "member", placeholder("never-resolved")),
		}

		externalC1zPath := filepath.Join(tempDir, "external.c1z")
		externalOpts := append([]SyncOpt{WithC1ZPath(externalC1zPath), WithTmpDir(tempDir)}, extraOpts...)
		externalSyncer, err := NewSyncer(ctx, externalMc, externalOpts...)
		require.NoError(t, err)
		require.NoError(t, externalSyncer.Sync(ctx))
		require.NoError(t, externalSyncer.Close(ctx))

		internalC1zPath := filepath.Join(tempDir, "internal.c1z")
		internalOpts := append([]SyncOpt{
			WithC1ZPath(internalC1zPath),
			WithTmpDir(tempDir),
			WithExternalResourceC1ZPath(externalC1zPath),
		}, extraOpts...)
		internalSyncer, err := NewSyncer(ctx, internalMc, internalOpts...)
		require.NoError(t, err)
		require.NoError(t, internalSyncer.Sync(ctx))
		require.NoError(t, internalSyncer.Close(ctx))

		store, err := dotc1z.NewC1ZFile(ctx, internalC1zPath)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close(ctx)) }()

		// The external phase copied the external "user" type into the sync;
		// the placeholder type never lands in the store.
		rtResp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
		require.NoError(t, err)
		rtIDs := make([]string, 0, len(rtResp.GetList()))
		for _, rt := range rtResp.GetList() {
			rtIDs = append(rtIDs, rt.GetId())
		}
		require.ElementsMatch(t, []string{"group", "user"}, rtIDs)

		// Exactly one grant survives: the annotated one, rewritten to the
		// external principal. The unannotated dangling grant is gone.
		grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Len(t, grantsResp.GetList(), 1)
		surviving := grantsResp.GetList()[0]
		require.Equal(t, externalUser.GetId().GetResourceType(), surviving.GetPrincipal().GetId().GetResourceType())
		require.Equal(t, externalUser.GetId().GetResource(), surviving.GetPrincipal().GetId().GetResource())
		require.Equal(t, internalGroup.GetId().GetResource(), surviving.GetEntitlement().GetResource().GetId().GetResource())
	})
}

// TestFreshIngestFilterInsertResourceGrantsResourceInserts verifies the
// resource side of the InsertResourceGrants flow: a grant-discovered resource
// of a scheduled type is written even when every grant that discovered it is
// dropped for an unscheduled principal type, while a grant-discovered
// resource of an unscheduled type is dropped along with its grant.
func TestFreshIngestFilterInsertResourceGrantsResourceInserts(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "irg-filter.c1z")

	mc := newInsertResourceGrantsMockConnector(true)
	// "shared_drive" is listed (scheduled); "secret_vault" never is.
	mc.rtDB = append(mc.rtDB, v2.ResourceType_builder{Id: "shared_drive", DisplayName: "Shared Drive"}.Build())

	group, _, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	user, err := mc.AddUser(ctx, "u1")
	require.NoError(t, err)

	deadGrantDrive := ingestFilterResource("shared_drive", "d1")
	liveGrantDrive := ingestFilterResource("shared_drive", "d2")
	vault := ingestFilterResource("secret_vault", "v1")
	hrisPrincipal := v2.ResourceId_builder{ResourceType: "hris_user", Resource: "p1"}.Build()

	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{
		// Dropped (unscheduled principal type), but d1 is still a legitimate
		// discovery of a scheduled-type resource.
		gt.NewGrant(deadGrantDrive, "access", hrisPrincipal),
		// Survives; d2 discovered normally.
		gt.NewGrant(liveGrantDrive, "access", user.GetId()),
		// Dropped (unscheduled entitlement resource type); v1 must not be
		// written — without its type row the whole chain is dead downstream.
		gt.NewGrant(vault, "access", user.GetId()),
	}

	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	store, err := dotc1z.NewC1ZFile(ctx, c1zPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	drives, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: "shared_drive"}.Build())
	require.NoError(t, err)
	driveIDs := make([]string, 0, len(drives.GetList()))
	for _, r := range drives.GetList() {
		driveIDs = append(driveIDs, r.GetId().GetResource())
	}
	require.ElementsMatch(t, []string{"d1", "d2"}, driveIDs)

	vaults, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: "secret_vault"}.Build())
	require.NoError(t, err)
	require.Empty(t, vaults.GetList(), "unscheduled-type resource must not be written")

	grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1)
	surviving := grantsResp.GetList()[0]
	require.Equal(t, "d2", surviving.GetEntitlement().GetResource().GetId().GetResource())
	require.Equal(t, "u1", surviving.GetPrincipal().GetId().GetResource())
}

// TestFreshIngestFilterRunsBeforeExclusionGroupValidation: an entitlement the
// filter drops must not mutate exclusion-group state or fail the sync. Before
// the ordering fix this sync failed with an "exclusion group used on multiple
// resource types" error triggered by the filtered iam_policy entitlement.
func TestFreshIngestFilterRunsBeforeExclusionGroupValidation(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "exclusion-filter.c1z")

	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	group, _, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)

	scheduledEnt := ingestFilterEntitlement(group, "group:g1:admin")
	scheduledEnt.SetAnnotations(annotations.New(v2.EntitlementExclusionGroup_builder{ExclusionGroupId: "eg-1"}.Build()))
	droppedEnt := ingestFilterEntitlement(ingestFilterResource("iam_policy", "p1"), "iam_policy:p1:assigned")
	droppedEnt.SetAnnotations(annotations.New(v2.EntitlementExclusionGroup_builder{ExclusionGroupId: "eg-1"}.Build()))
	mc.entDB[group.GetId().GetResource()] = append(mc.entDB[group.GetId().GetResource()], scheduledEnt, droppedEnt)

	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	store, err := dotc1z.NewC1ZFile(ctx, c1zPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	entsResp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	entIDs := make([]string, 0, len(entsResp.GetList()))
	for _, ent := range entsResp.GetList() {
		require.NotEqual(t, "iam_policy", ent.GetResource().GetId().GetResourceType())
		entIDs = append(entIDs, ent.GetId())
	}
	require.Contains(t, entIDs, scheduledEnt.GetId())
}
