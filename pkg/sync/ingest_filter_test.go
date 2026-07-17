package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
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
			// Every external-match variant owns its placeholder principal, so
			// all three exempt the principal-type check.
			matchIDDisabledPrincipal := ingestFilterGrant(
				"match-id-disabled-principal", goodEntitlement, disabled,
				annotations.New(v2.ExternalResourceMatchID_builder{Id: "external-p1"}.Build())...,
			)
			matchAllDisabledPrincipal := ingestFilterGrant(
				"match-all-disabled-principal", goodEntitlement, disabled,
				annotations.New(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build())...,
			)
			matchKeyDisabledPrincipal := ingestFilterGrant(
				"match-key-disabled-principal", goodEntitlement, disabled,
				annotations.New(v2.ExternalResourceMatch_builder{
					Key:          "email",
					Value:        "external@example.com",
					ResourceType: v2.ResourceType_TRAIT_USER,
				}.Build())...,
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
				matchIDDisabledPrincipal,
				matchAllDisabledPrincipal,
				matchKeyDisabledPrincipal,
				matchDoesNotExemptEntitlement,
				irgDoesNotExemptPrincipal,
				good,
			})
			require.NoError(t, err)
			require.Equal(t, []*v2.Grant{matchIDDisabledPrincipal, matchAllDisabledPrincipal, matchKeyDisabledPrincipal, good}, grants)
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
	matchAllExpansion := expandable("match-all", "iam_policy")
	matchAllExpansion.SetAnnotations(append(
		matchAllExpansion.GetAnnotations(),
		annotations.New(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build())...,
	))
	matchKeyExpansion := expandable("match-key", "iam_policy")
	matchKeyExpansion.SetAnnotations(append(
		matchKeyExpansion.GetAnnotations(),
		annotations.New(v2.ExternalResourceMatch_builder{
			Key:          "email",
			Value:        "external@example.com",
			ResourceType: v2.ResourceType_TRAIT_USER,
		}.Build())...,
	))

	s := &syncer{store: store, syncType: connectorstore.SyncTypeFull}
	filtered, err := s.filterFreshGrants(ctx, []*v2.Grant{mixed, disabledOnly, unfiltered, matchExpansion, matchAllExpansion, matchKeyExpansion})
	require.NoError(t, err)
	require.Len(t, filtered, 6, "filtering expansion metadata does not drop the base grant")
	require.NotSame(t, mixed, filtered[0], "rewriting must not mutate connector-owned response objects")
	require.NotSame(t, disabledOnly, filtered[1], "rewriting must not mutate connector-owned response objects")
	mixed, disabledOnly, unfiltered = filtered[0], filtered[1], filtered[2]

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

	// All three external-match variants carry placeholders the later matching
	// phase rewrites, so their expansion metadata must stay untouched.
	for i, carrier := range []*v2.Grant{filtered[3], filtered[4], filtered[5]} {
		carrierExpandable := &v2.GrantExpandable{}
		carrierAnnos := annotations.Annotations(carrier.GetAnnotations())
		ok, err = carrierAnnos.Pick(carrierExpandable)
		require.NoError(t, err)
		require.True(t, ok, "carrier %d", i)
		require.Equal(t, []string{"iam_policy"}, carrierExpandable.GetResourceTypeIds(),
			"external-match placeholders remain available for later remapping")
	}
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
// The connector emits three grants whose principals reference resource types
// it never lists:
//
//   - one carries ExternalResourceMatch on a placeholder type ("hris_user"),
//     so the filter must keep it; the external phase later rewrites it to the
//     matched external user (whose "user" type is copied into the store by
//     that phase),
//   - one carries no annotations but references the external c1z's "user"
//     type directly; the external phase copies that type and its resources
//     into the sync, so uplift can consume the grant and the filter must
//     keep it, and
//   - one carries no annotations on the placeholder type, so nothing
//     downstream could ever resolve it and the filter must drop it.
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
		directExternalUser, err := externalMc.AddUserProfile(ctx, "ext-user-2", map[string]any{})
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
			gt.NewGrant(internalGroup, "member", directExternalUser.GetId()),
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

		// Exactly two grants survive: the annotated one, rewritten to the
		// matched external principal, and the unannotated one that referenced
		// the external "user" type directly. The unannotated placeholder
		// grant is gone.
		grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Len(t, grantsResp.GetList(), 2)
		principalIDs := make([]string, 0, 2)
		for _, surviving := range grantsResp.GetList() {
			require.Equal(t, "user", surviving.GetPrincipal().GetId().GetResourceType())
			require.Equal(t, internalGroup.GetId().GetResource(), surviving.GetEntitlement().GetResource().GetId().GetResource())
			principalIDs = append(principalIDs, surviving.GetPrincipal().GetId().GetResource())
		}
		require.ElementsMatch(t, []string{externalUser.GetId().GetResource(), directExternalUser.GetId().GetResource()}, principalIDs)
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

// TestFreshIngestFilterDropsNilEntries: literal nil entries carry nothing to
// store or validate and are dropped, while malformed-but-present records
// (missing refs) stay on the normal write path. Neither counts as a
// disabled-type drop.
func TestFreshIngestFilterDropsNilEntries(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	store := newIngestFilterStore(ctx, t, c1zstore.EnginePebble)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))

	s := &syncer{store: store, syncType: connectorstore.SyncTypeFull}

	missingRefEnt := v2.Entitlement_builder{Id: "no-resource"}.Build()
	goodEnt := ingestFilterEntitlement(ingestFilterResource("group", "g1"), "group:g1:member")
	entitlements, err := s.filterFreshEntitlements(ctx, []*v2.Entitlement{nil, missingRefEnt, goodEnt})
	require.NoError(t, err)
	require.Equal(t, []*v2.Entitlement{missingRefEnt, goodEnt}, entitlements)

	missingRefGrant := v2.Grant_builder{Id: "no-refs"}.Build()
	goodGrant := ingestFilterGrant("good", goodEnt, ingestFilterResource("group", "g2"))
	grants, err := s.filterFreshGrants(ctx, []*v2.Grant{nil, missingRefGrant, goodGrant})
	require.NoError(t, err)
	require.Equal(t, []*v2.Grant{missingRefGrant, goodGrant}, grants)

	require.Zero(t, s.ingestFilterStats.entitlementsDropped.Load())
	require.Zero(t, s.ingestFilterStats.grantsDropped.Load())
}

// resourceTypeProbeStore wraps a real store to observe or fail the
// GetResourceType probes the fresh-ingest filter issues.
type resourceTypeProbeStore struct {
	c1zstore.Store
	calls map[string]int
	err   error
}

func (s *resourceTypeProbeStore) GetResourceType(
	ctx context.Context,
	req *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest,
) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	if s.calls != nil {
		s.calls[req.GetResourceTypeId()]++
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.Store.GetResourceType(ctx, req)
}

// TestFreshIngestFilterProbeFailureFailsSyncNotDrops: a non-NotFound read
// failure — from the local store or the external reader — must surface as an
// error, never as a drop verdict.
func TestFreshIngestFilterProbeFailureFailsSyncNotDrops(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	store := newIngestFilterStore(ctx, t, c1zstore.EnginePebble)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))

	entitlement := ingestFilterEntitlement(ingestFilterResource("iam_policy", "p1"), "iam_policy:p1:assigned")
	grant := ingestFilterGrant("g", entitlement, ingestFilterResource("iam_policy", "p2"))

	t.Run("local store failure", func(t *testing.T) {
		failing := &resourceTypeProbeStore{Store: store, err: status.Error(codes.Internal, "probe boom")}
		s := &syncer{store: failing, syncType: connectorstore.SyncTypeFull}

		_, err := s.filterFreshEntitlements(ctx, []*v2.Entitlement{entitlement})
		require.ErrorContains(t, err, "probe boom")
		_, err = s.filterFreshGrants(ctx, []*v2.Grant{grant})
		require.ErrorContains(t, err, "probe boom")
		require.Zero(t, s.ingestFilterStats.entitlementsDropped.Load())
		require.Zero(t, s.ingestFilterStats.grantsDropped.Load())
	})

	t.Run("external reader failure", func(t *testing.T) {
		external := newIngestFilterStore(ctx, t, c1zstore.EnginePebble)
		failingExternal := &resourceTypeProbeStore{Store: external, err: status.Error(codes.Internal, "external boom")}
		s := &syncer{store: store, externalResourceReader: failingExternal, syncType: connectorstore.SyncTypeFull}

		_, err := s.filterFreshEntitlements(ctx, []*v2.Entitlement{entitlement})
		require.ErrorContains(t, err, "external boom")
		require.Zero(t, s.ingestFilterStats.entitlementsDropped.Load())
	})
}

// TestFreshIngestFilterCachesProbeVerdicts: both positive and negative
// verdicts are cached, so repeated references never re-probe the store.
func TestFreshIngestFilterCachesProbeVerdicts(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	store := newIngestFilterStore(ctx, t, c1zstore.EnginePebble)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))

	counting := &resourceTypeProbeStore{Store: store, calls: map[string]int{}}
	s := &syncer{store: counting, syncType: connectorstore.SyncTypeFull}

	group := ingestFilterResource("group", "g1")
	disabled := ingestFilterResource("iam_policy", "p1")
	entitlements := []*v2.Entitlement{
		ingestFilterEntitlement(group, "group:g1:member"),
		ingestFilterEntitlement(disabled, "iam_policy:p1:assigned"),
	}
	for range 3 {
		filtered, err := s.filterFreshEntitlements(ctx, entitlements)
		require.NoError(t, err)
		require.Len(t, filtered, 1)
	}
	require.Equal(t, 1, counting.calls["group"], "positive verdicts are cached")
	require.Equal(t, 1, counting.calls["iam_policy"], "negative verdicts are cached")
}

// TestFreshIngestFilterKeepsExternalSuppliedTypes: a reference into a type
// the external-resource phase will copy into this sync (present in the
// external c1z with a user or group trait) must be kept, while external types
// without those traits are never copied and remain droppable.
func TestFreshIngestFilterKeepsExternalSuppliedTypes(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	store := newIngestFilterStore(ctx, t, c1zstore.EnginePebble)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))

	external := newIngestFilterStore(ctx, t, c1zstore.EnginePebble)
	require.NoError(t, external.PutResourceTypes(ctx,
		v2.ResourceType_builder{
			Id:          "user",
			DisplayName: "User",
			Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		}.Build(),
		v2.ResourceType_builder{Id: "widget", DisplayName: "Widget"}.Build(),
	))

	s := &syncer{store: store, externalResourceReader: external, syncType: connectorstore.SyncTypeFull}

	group := ingestFilterResource("group", "g1")
	externalUser := ingestFilterResource("user", "u1")
	externalWidget := ingestFilterResource("widget", "w1")
	groupEntitlement := ingestFilterEntitlement(group, "group:g1:member")

	entitlements, err := s.filterFreshEntitlements(ctx, []*v2.Entitlement{
		groupEntitlement,
		ingestFilterEntitlement(externalUser, "user:u1:linked"),
		ingestFilterEntitlement(externalWidget, "widget:w1:linked"),
	})
	require.NoError(t, err)
	require.Len(t, entitlements, 2)
	require.Equal(t, "user:u1:linked", entitlements[1].GetId(),
		"user-traited external types will be copied into the sync; keep references to them")

	grants, err := s.filterFreshGrants(ctx, []*v2.Grant{
		ingestFilterGrant("external-user-principal", groupEntitlement, externalUser),
		ingestFilterGrant("external-widget-principal", groupEntitlement, externalWidget),
		ingestFilterGrant("unknown-principal", groupEntitlement, ingestFilterResource("hris_user", "p1")),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"external-user-principal"}, grantIDs(grants),
		"only user/group-traited external types are copied by the external phase")
}

// pagedResourceTypesMockConnector serves one resource type per page so tests
// can pin behavior for types first seen on later pages.
type pagedResourceTypesMockConnector struct {
	*mockConnector
}

func (mc *pagedResourceTypesMockConnector) ListResourceTypes(
	_ context.Context,
	in *v2.ResourceTypesServiceListResourceTypesRequest,
	_ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	idx := 0
	if in.GetPageToken() != "" {
		parsed, err := strconv.Atoi(in.GetPageToken())
		if err != nil {
			return nil, err
		}
		idx = parsed
	}
	if idx >= len(mc.rtDB) {
		return v2.ResourceTypesServiceListResourceTypesResponse_builder{}.Build(), nil
	}
	next := ""
	if idx+1 < len(mc.rtDB) {
		next = strconv.Itoa(idx + 1)
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{
		List:          []*v2.ResourceType{mc.rtDB[idx]},
		NextPageToken: next,
	}.Build(), nil
}

// TestFreshIngestFilterHandlesMultiPageResourceTypes: the resource-types step
// completes (all pages stored) before entitlement and grant collection, so a
// scheduled type first listed on a later page must never be dropped.
func TestFreshIngestFilterHandlesMultiPageResourceTypes(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()
	c1zPath := filepath.Join(tempDir, "paged-resource-types.c1z")

	mc := &pagedResourceTypesMockConnector{mockConnector: newMockConnector()}
	// "group" arrives on page 1, "user" on page 2.
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	group, groupEntitlement, err := mc.AddGroup(ctx, "g1")
	require.NoError(t, err)
	user, err := mc.AddUser(ctx, "u1")
	require.NoError(t, err)

	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{
		ingestFilterGrant("good", groupEntitlement, user),
		ingestFilterGrant("disabled", groupEntitlement, ingestFilterResource("iam_policy", "p1")),
	}

	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	store, err := dotc1z.NewC1ZFile(ctx, c1zPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	grantsResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"good"}, grantIDs(grantsResp.GetList()),
		"a type from a later resource-types page is scheduled, not disabled")
}
