package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func externalPageFixture(t *testing.T, ledger bool) (*syncer, *ledgerFixture, *ledgerFixture) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.ledgered = ledger
	s.counts = progresslog.NewProgressCounts(t.Context())
	source := newLedgerFixture(t)
	principal := externalMatchPrincipal(t, "fresh", map[string]any{"email": "fresh@example.com"})
	sourceType := proto.Clone(userResourceType).(*v2.ResourceType)
	sourceType.SetAnnotations(nil)
	require.NoError(t, source.store.PutResourceTypes(t.Context(), sourceType))
	require.NoError(t, source.store.PutResources(t.Context(), principal))
	require.NoError(t, source.store.PutEntitlements(t.Context(), et.NewAssignmentEntitlement(principal, "member")))
	target := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "application"}.Build()}.Build()
	require.NoError(t, f.store.PutResourceTypes(t.Context(), groupResourceType, userResourceType))
	require.NoError(t, f.store.PutResources(t.Context(), target))
	require.NoError(t, f.store.PutEntitlements(t.Context(), et.NewAssignmentEntitlement(target, "member")))
	carrier := gt.NewGrant(target, "member", v2.ResourceId_builder{ResourceType: "user", Resource: "placeholder"}.Build(),
		gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()))
	if ledger {
		f.audit.enter(ledgerHandler)
		_, err := s.ledger.runPage(t.Context(), 0, ledgerIdentity(&Action{Op: SyncGrantsOp, ResourceTypeID: "fixture"}), func(ctx context.Context, page *ledgerPage) error {
			if err := page.writer.PutGrants(ctx, carrier); err != nil {
				return err
			}
			if err := page.setFact(factHasExternalResourceGrants); err != nil {
				return err
			}
			return page.transition("")
		})
		require.NoError(t, err)
		f.audit.enter(ledgerLifecycle)
	} else {
		require.NoError(t, f.store.PutGrants(t.Context(), carrier))
	}
	s.run.setFact(factHasExternalResourceGrants)
	s.externalResourceReader = source.store
	s.run.pushAction(t.Context(), Action{Op: SyncExternalResourcesOp})
	return s, f, source
}
func equalProtoLists[T proto.Message](t *testing.T, want, got []T) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Truef(t, proto.Equal(want[i], got[i]), "record %d differs: expected %v, got %v", i, want[i], got[i])
	}
}
func TestLedgerExternalUsesMainHandler(t *testing.T) {
	testLedgerExternalParity(t, nil)
}

func testLedgerExternalParity(t *testing.T, configure func(*syncer, *ledgerFixture, *ledgerFixture)) {
	t.Helper()
	s, f, source := externalPageFixture(t, true)
	baseline, b, baselineSource := externalPageFixture(t, false)
	if configure != nil {
		configure(s, f, source)
		configure(baseline, b, baselineSource)
	}
	f.audit.enter(ledgerLifecycle)
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, baseline.SyncExternalResources(t.Context(), baseline.run.current()))
	gotTypes, err := f.store.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	wantTypes, err := b.store.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	equalProtoLists(t, wantTypes.GetList(), gotTypes.GetList())
	gotResources, err := f.store.ListResources(t.Context(), &v2.ResourcesServiceListResourcesRequest{})
	require.NoError(t, err)
	wantResources, err := b.store.ListResources(t.Context(), &v2.ResourcesServiceListResourcesRequest{})
	require.NoError(t, err)
	equalProtoLists(t, wantResources.GetList(), gotResources.GetList())
	gotEnts, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	wantEnts, err := b.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	equalProtoLists(t, wantEnts.GetList(), gotEnts.GetList())
	gotGrants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	wantGrants, err := b.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	equalProtoLists(t, wantGrants.GetList(), gotGrants.GetList())
}
func TestLedgerExternalImportedGrantIsMatched(t *testing.T) {
	s, f, source := externalPageFixture(t, true)
	principal := externalMatchPrincipal(t, "fresh", nil)
	imported := gt.NewGrant(principal, "member", v2.ResourceId_builder{ResourceType: "user", Resource: "placeholder"}.Build(),
		gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()))
	require.NoError(t, source.store.PutGrants(t.Context(), imported))
	f.audit.enter(ledgerLifecycle)
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 2)
	for _, grant := range grants.GetList() {
		require.Equal(t, "fresh", grant.GetPrincipal().GetId().GetResource())
	}
}

func TestLedgerExternalStaleGrantReimport(t *testing.T) {
	s, f, source := externalPageFixture(t, true)
	stale := externalMatchPrincipal(t, "stale", nil)
	fresh := externalMatchPrincipal(t, "fresh", nil)
	imported := gt.NewGrant(fresh, "member", stale.GetId())
	require.NoError(t, f.store.PutResources(t.Context(), stale))
	require.NoError(t, f.store.PutEntitlements(t.Context(), et.NewAssignmentEntitlement(stale, "member")))
	require.NoError(t, f.store.PutGrants(t.Context(), imported))
	require.NoError(t, source.store.PutGrants(t.Context(), imported))
	f.audit.enter(ledgerLifecycle)
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	_, err = f.store.GetResource(t.Context(), reader_v2.ResourcesReaderServiceGetResourceRequest_builder{ResourceId: stale.GetId()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	found := false
	for _, grant := range grants.GetList() {
		if ledgerGrantIdentity(grant) == ledgerGrantIdentity(imported) {
			found = true
		}
	}
	require.True(t, found, "main deletes stale grants before reimport; the reimported identity must survive")
}

func TestLedgerExternalFilteredPrincipalState(t *testing.T) {
	s, f, source := externalPageFixture(t, true)
	fresh := externalMatchPrincipal(t, "fresh", nil)
	zebra := externalMatchPrincipal(t, "zebra", nil)
	require.NoError(t, source.store.PutResources(t.Context(), zebra))
	filter := et.NewAssignmentEntitlement(fresh, "member")
	require.NoError(t, source.store.PutGrants(t.Context(), gt.NewGrant(fresh, "member", zebra.GetId()), gt.NewGrant(fresh, "member", fresh.GetId())))
	s.cfg.externalResourceEntitlementIdFilter = filter.GetId()
	f.audit.enter(ledgerLifecycle)
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 4)
}

func TestLedgerExternalDeleteFullIdentity(t *testing.T) {
	s, f, _ := externalPageFixture(t, true)
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	carrier := proto.Clone(grants.GetList()[0]).(*v2.Grant)
	carrier.SetId("shared")
	ordinary := ledgerGrant("shared", "group", "unrelated", "user")
	require.NoError(t, f.store.PutGrants(t.Context(), carrier, ordinary))
	f.audit.enter(ledgerLifecycle)
	_, err = s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	grants, err = f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	found := false
	for _, grant := range grants.GetList() {
		require.NotEqual(t, ledgerGrantIdentity(carrier), ledgerGrantIdentity(grant))
		if ledgerGrantIdentity(grant) == ledgerGrantIdentity(ordinary) {
			found = true
		}
	}
	require.True(t, found)
}

func TestLedgerExternalExpansionRemap(t *testing.T) {
	s, f, _ := externalPageFixture(t, true)
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	carrier := grants.GetList()[0]
	match, err := anypb.New(v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build())
	require.NoError(t, err)
	placeholderID, err := bid.MakeBid(v2.Entitlement_builder{Resource: carrier.GetPrincipal(), Slug: "member"}.Build())
	require.NoError(t, err)
	expansion, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{placeholderID}}.Build())
	require.NoError(t, err)
	carrier.SetAnnotations([]*anypb.Any{match, expansion})
	require.NoError(t, f.store.PutGrants(t.Context(), carrier))
	f.audit.enter(ledgerLifecycle)
	_, err = s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	pending, _, err := f.store.Grants().PendingExpansionPage(t.Context(), "")
	require.NoError(t, err)
	require.Len(t, pending, 1)
	fresh := externalMatchPrincipal(t, "fresh", nil)
	require.Equal(t, []string{et.NewEntitlementID(fresh, "member")}, pending[0].Annotation.GetEntitlementIds())
}

func TestLedgerExternalSelectionMatchesTokenHandler(t *testing.T) {
	for _, mode := range []string{"filtered", "skip-type", "skip-resource", "skip-grants", "traits", "profile", "missing-profile"} {
		t.Run(mode, func(t *testing.T) {
			testLedgerExternalParity(t, func(s *syncer, f, source *ledgerFixture) {
				fresh := externalMatchPrincipal(t, "fresh", map[string]any{"email": "fresh@example.com"})
				imported := gt.NewGrant(fresh, "member", fresh.GetId())
				require.NoError(t, source.store.PutGrants(t.Context(), imported))
				switch mode {
				case "filtered":
					s.cfg.externalResourceEntitlementIdFilter = et.NewEntitlementID(fresh, "member")
				case "skip-type":
					require.NoError(t, source.store.PutResourceTypes(t.Context(), userResourceType))
				case "skip-resource", "skip-grants":
					var message proto.Message = &v2.SkipEntitlementsAndGrants{}
					if mode == "skip-grants" {
						message = &v2.SkipGrants{}
					}
					annotation, err := anypb.New(message)
					require.NoError(t, err)
					fresh.SetAnnotations(append(fresh.GetAnnotations(), annotation))
					require.NoError(t, source.store.PutResources(t.Context(), fresh))
					require.NoError(t, source.store.PutEntitlements(t.Context(), et.NewAssignmentEntitlement(fresh, "member")))
				case "traits":
					s.cfg.externalResourceTraits = []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}
				case "profile", "missing-profile":
					value := "fresh@example.com"
					if mode == "missing-profile" {
						value = "absent@example.com"
					}
					grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
					require.NoError(t, err)
					carrier := grants.GetList()[0]
					annotation, err := anypb.New(v2.ExternalResourceMatch_builder{Key: "email", Value: value, ResourceType: v2.ResourceType_TRAIT_USER}.Build())
					require.NoError(t, err)
					carrier.SetAnnotations([]*anypb.Any{annotation})
					require.NoError(t, f.store.PutGrants(t.Context(), carrier))
				}
			})
		})
	}
}
