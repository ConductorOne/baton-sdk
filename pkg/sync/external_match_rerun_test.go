package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// externalMatchRerunFixture is the external source and the connector grants
// for one rerun test. slugsByPrincipal lists the entitlement slugs the
// external source holds for each principal id.
type externalMatchRerunFixture struct {
	resourceTypes     []*v2.ResourceType
	principals        []*v2.Resource
	slugsByPrincipal  map[string][]string
	extraEntitlements []*v2.Entitlement
	grants            []*v2.Grant
}

func newExternalMatchRerunSyncer(t *testing.T, f externalMatchRerunFixture) (*syncer, c1zstore.Store) {
	t.Helper()
	ctx := t.Context()
	tmp := t.TempDir()

	ext, err := dotc1z.NewStore(ctx, filepath.Join(tmp, "external.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ext.Close(ctx) })
	_, err = ext.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, ext.PutResourceTypes(ctx, f.resourceTypes...))
	require.NoError(t, ext.PutResources(ctx, f.principals...))
	for _, principal := range f.principals {
		for _, slug := range f.slugsByPrincipal[principal.GetId().GetResource()] {
			require.NoError(t, ext.PutEntitlements(ctx, et.NewAssignmentEntitlement(principal, slug)))
		}
	}
	if len(f.extraEntitlements) > 0 {
		require.NoError(t, ext.PutEntitlements(ctx, f.extraEntitlements...))
	}
	require.NoError(t, ext.EndSync(ctx))

	dst, err := dotc1z.NewStore(ctx, filepath.Join(tmp, "internal.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close(ctx) })
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, dst.PutGrants(ctx, f.grants...))

	run := newRunState()
	run.setFact(factHasExternalResourceGrants)
	s := &syncer{run: run, stats: newRunStats(), graph: newExpansionGraph(), externalResourceReader: ext}
	s.setStore(dst)
	return s, dst
}

func externalMatchRerunGroup(t *testing.T, id string) *v2.Resource {
	t.Helper()
	group, err := rs.NewGroupResource(id, groupResourceType, id,
		[]rs.GroupTraitOption{rs.WithGroupProfile(map[string]any{"dept": "eng"})})
	require.NoError(t, err)
	return group
}

// externalMatchRerunCarrier is a connector grant of groupID's member entitlement to a
// placeholder principal, with a GrantExpandable naming the placeholder's
// entitlements by BID.
func externalMatchRerunCarrier(t *testing.T, groupID string, placeholder *v2.ResourceId, match *v2.ExternalResourceMatch, matchID *v2.ExternalResourceMatchID, slugs ...string) *v2.Grant {
	t.Helper()
	group, err := rs.NewGroupResource(groupID, groupResourceType, groupID, nil)
	require.NoError(t, err)
	var entries []string
	for _, slug := range slugs {
		entBID, err := bid.MakeBid(v2.Entitlement_builder{Resource: v2.Resource_builder{Id: placeholder}.Build(), Slug: slug}.Build())
		require.NoError(t, err)
		entries = append(entries, entBID)
	}
	opts := []gt.GrantOption{gt.WithAnnotation(v2.GrantExpandable_builder{EntitlementIds: entries}.Build())}
	if match != nil {
		opts = append(opts, gt.WithAnnotation(match))
	}
	if matchID != nil {
		opts = append(opts, gt.WithAnnotation(matchID))
	}
	return gt.NewGrant(group, "member", placeholder, opts...)
}

var externalMatchDeptEng = v2.ExternalResourceMatch_builder{Key: "dept", Value: "eng", ResourceType: v2.ResourceType_TRAIT_GROUP}.Build()

// TestExternalMatchProfileFanoutMixedRemapRerun: one carrier matches two groups by
// profile. p1 holds the entitlement the carrier's expansion names; p2 does
// not. No interruption. A rerun of the step must leave the store as the first
// run left it.
func TestExternalMatchProfileFanoutMixedRemapRerun(t *testing.T) {
	ctx := t.Context()
	f := externalMatchRerunFixture{
		resourceTypes:    []*v2.ResourceType{groupResourceType},
		principals:       []*v2.Resource{externalMatchRerunGroup(t, "p1"), externalMatchRerunGroup(t, "p2")},
		slugsByPrincipal: map[string][]string{"p1": {"member"}, "p2": {"viewer"}},
		grants: []*v2.Grant{
			externalMatchRerunCarrier(t, "g1", v2.ResourceId_builder{ResourceType: "group", Resource: "ph1"}.Build(), externalMatchDeptEng, nil, "member"),
		},
	}
	s, dst := newExternalMatchRerunSyncer(t, f)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	wantPending, wantGrants := expansionReplaySnapshot(t, dst)
	require.Equal(t, map[string][]string{"group:g1:member:group:p1": {"group:p1:member"}}, wantPending,
		"premise: the first run remaps the expansion for p1 and writes none for p2")
	require.Len(t, wantGrants, 2, "premise: one grant per matched group, carrier deleted")

	for attempt := 1; attempt <= 2; attempt++ {
		require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
		pending, grants := expansionReplaySnapshot(t, dst)
		require.Equal(t, wantPending, pending, "rerun %d", attempt)
		require.Equal(t, wantGrants, grants, "rerun %d", attempt)
	}
}

var errExternalMatchPutCut = errors.New("external match: injected put cut")

// externalMatchPutCutStore fails the failAt-th non-empty PutGrants call without
// writing any of it, as a process killed between two chunk commits. Earlier
// calls commit whole. recorded holds the size of every non-empty call.
type externalMatchPutCutStore struct {
	c1zstore.Store
	failAt   int
	recorded []int
}

func (s *externalMatchPutCutStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	if len(grants) > 0 {
		s.recorded = append(s.recorded, len(grants))
		if s.failAt > 0 && len(s.recorded) == s.failAt {
			return errExternalMatchPutCut
		}
	}
	return s.Store.PutGrants(ctx, grants...)
}

func (s *externalMatchPutCutStore) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	return s.Store.(grantsByRefsBatchDeleter).DeleteGrantsByRefs(ctx, grants...)
}

func (s *externalMatchPutCutStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.Store.(grantByRefsDeleter).DeleteGrantByRefs(ctx, grant)
}

func (s *externalMatchPutCutStore) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return s.Store.(resourceRecordDeleter).DeleteResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *externalMatchPutCutStore) DeleteEntitlementByRefs(ctx context.Context, ent *v2.Entitlement) error {
	return s.Store.(entitlementRecordDeleter).DeleteEntitlementByRefs(ctx, ent)
}

// TestExternalMatchProfileFanoutCutBetweenPutChunks: one carrier matches two groups
// by profile; both groups hold every entitlement the expansion names. The
// first attempt commits the chunk holding p1's grant and is cut before the
// chunk holding p2's grant. The rerun must reach the uninterrupted result.
// The placeholder id decides where the carrier sorts against the rows the
// first attempt wrote.
func TestExternalMatchProfileFanoutCutBetweenPutChunks(t *testing.T) {
	for _, placeholderID := range []string{"zz-placeholder", "a0-placeholder"} {
		t.Run("placeholder "+placeholderID, func(t *testing.T) {
			ctx := t.Context()
			f := externalMatchRerunFixture{
				resourceTypes:    []*v2.ResourceType{groupResourceType},
				principals:       []*v2.Resource{externalMatchRerunGroup(t, "p1"), externalMatchRerunGroup(t, "p2")},
				slugsByPrincipal: map[string][]string{"p1": {"member", "admin"}, "p2": {"member", "admin"}},
				grants: []*v2.Grant{
					externalMatchRerunCarrier(t, "g1", v2.ResourceId_builder{ResourceType: "group", Resource: placeholderID}.Build(), externalMatchDeptEng, nil, "member", "admin"),
				},
			}

			golden, goldenDst := newExternalMatchRerunSyncer(t, f)
			require.NoError(t, golden.SyncExternalResourcesUsersAndGroups(ctx))
			wantPending, wantGrants := expansionReplaySnapshot(t, goldenDst)
			require.Equal(t, map[string][]string{
				"group:g1:member:group:p1": {"group:p1:member", "group:p1:admin"},
				"group:g1:member:group:p2": {"group:p2:member", "group:p2:admin"},
			}, wantPending, "premise: the uninterrupted run remaps both groups")

			s, dst := newExternalMatchRerunSyncer(t, f)
			putChunk := 1
			s.testHooks.externalMatchGrantPutChunk = &putChunk
			cut := &externalMatchPutCutStore{Store: dst, failAt: 2}
			s.setStore(cut)
			err := s.SyncExternalResourcesUsersAndGroups(ctx)
			require.ErrorIs(t, err, errExternalMatchPutCut)
			require.Equal(t, []int{1, 1}, cut.recorded, "premise: the first chunk committed, the second was cut")
			pendingAfterCut, grantsAfterCut := expansionReplaySnapshot(t, dst)
			require.Len(t, grantsAfterCut, 2, "premise: the carrier and p1's grant are stored, p2's is not")
			require.Equal(t, []string{"group:p1:member", "group:p1:admin"}, pendingAfterCut["group:g1:member:group:p1"],
				"premise: p1's grant committed with its remapped expansion")
			require.NotContains(t, pendingAfterCut, "group:g1:member:group:p2")

			s.testHooks.externalMatchGrantPutChunk = nil
			s.setStore(dst)
			for attempt := 1; attempt <= 2; attempt++ {
				require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
				pending, grants := expansionReplaySnapshot(t, dst)
				require.Equal(t, wantPending, pending, "rerun %d", attempt)
				require.Equal(t, wantGrants, grants, "rerun %d", attempt)
			}
		})
	}
}

// TestExternalMatchMatchIDRemapNamesEntitlementOnOtherResource: the carrier's
// expansion names slug "owner". The matched user has no "owner" entitlement,
// but another resource holds an entitlement whose id is "user:fresh:owner".
func TestExternalMatchMatchIDRemapNamesEntitlementOnOtherResource(t *testing.T) {
	ctx := t.Context()
	fresh := expansionReplayUser(t, "fresh")
	other := expansionReplayUser(t, "other")
	onOther := et.NewAssignmentEntitlement(other, "collides")
	onOther.SetId("user:fresh:owner")
	f := externalMatchRerunFixture{
		resourceTypes:     []*v2.ResourceType{expandableUserResourceType},
		principals:        []*v2.Resource{fresh, other},
		slugsByPrincipal:  map[string][]string{"fresh": {"member"}, "other": {"member"}},
		extraEntitlements: []*v2.Entitlement{onOther},
		grants: []*v2.Grant{
			externalMatchRerunCarrier(t, "g", v2.ResourceId_builder{ResourceType: "user", Resource: "ph"}.Build(), nil,
				v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build(), "member", "owner"),
		},
	}
	s, dst := newExternalMatchRerunSyncer(t, f)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	wantPending, wantGrants := expansionReplaySnapshot(t, dst)
	require.Equal(t, []string{"user:fresh:member"}, wantPending["group:g:member:user:fresh"])
	require.Len(t, wantPending, 1)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	pending, grants := expansionReplaySnapshot(t, dst)
	require.Equal(t, wantPending, pending, "rerun")
	require.Equal(t, wantGrants, grants, "rerun")
}

// TestExternalMatchMatchAllRerunPutCount checks a match-all carrier over n
// users. The first run writes one grant per user. The rerun has no carriers
// left, so it writes none.
func TestExternalMatchMatchAllRerunPutCount(t *testing.T) {
	ctx := t.Context()
	const n = 40
	users := make([]*v2.Resource, 0, n)
	for i := range n {
		users = append(users, expansionReplayUser(t, fmt.Sprintf("u%03d", i)))
	}
	group, err := rs.NewGroupResource("g", groupResourceType, "g", nil)
	require.NoError(t, err)
	carrier := gt.NewGrant(group, "member", v2.ResourceId_builder{ResourceType: "user", Resource: "ph"}.Build(),
		gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()))
	f := externalMatchRerunFixture{
		resourceTypes: []*v2.ResourceType{expandableUserResourceType},
		principals:    users,
		grants:        []*v2.Grant{carrier},
	}
	s, dst := newExternalMatchRerunSyncer(t, f)
	counting := &externalMatchPutCutStore{Store: dst}
	s.setStore(counting)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	first := 0
	for _, c := range counting.recorded {
		first += c
	}
	_, wantGrants := expansionReplaySnapshot(t, dst)
	require.Len(t, wantGrants, n)

	counting.recorded = nil
	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	rerun := 0
	for _, c := range counting.recorded {
		rerun += c
	}
	_, grants := expansionReplaySnapshot(t, dst)
	require.Len(t, grants, n)
	t.Logf("n=%d users: first run PutGrants rows=%d, rerun PutGrants rows=%d", n, first, rerun)
	require.Equal(t, n, first)
	require.Zero(t, rerun)
}

// TestExternalMatchCustomIDCarrierOnMatchedPrincipal: a connector grant with a
// connector-chosen id, whose principal is already the external principal its
// ExternalResourceMatchID names.
func TestExternalMatchCustomIDCarrierOnMatchedPrincipal(t *testing.T) {
	ctx := t.Context()
	fresh := expansionReplayUser(t, "fresh")
	carrier := externalMatchRerunCarrier(t, "g", fresh.GetId(), nil, v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build(), "member")
	carrier.SetId("connector-grant-1")
	f := externalMatchRerunFixture{
		resourceTypes:    []*v2.ResourceType{expandableUserResourceType},
		principals:       []*v2.Resource{fresh},
		slugsByPrincipal: map[string][]string{"fresh": {"member"}},
		grants:           []*v2.Grant{carrier},
	}
	s, dst := newExternalMatchRerunSyncer(t, f)
	_, before := expansionReplaySnapshot(t, dst)
	require.Len(t, before, 1, "premise: the connector grant is stored")

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	pending, grants := expansionReplaySnapshot(t, dst)
	require.Len(t, grants, 1, "the grant of g:member to user fresh must survive the step")
	require.Equal(t, map[string][]string{"group:g:member:user:fresh": {"user:fresh:member"}}, pending)
}

// TestExternalMatchMatchAllSiblingOverwritesRemappedRow: a match-all carrier and an
// expandable match-by-id carrier grant the same entitlement. The match-by-id
// carrier is processed last on the first run, so the matched user's grant
// holds the remapped expansion. No interruption. A rerun must leave it.
func TestExternalMatchMatchAllSiblingOverwritesRemappedRow(t *testing.T) {
	ctx := t.Context()
	group, err := rs.NewGroupResource("g", groupResourceType, "g", nil)
	require.NoError(t, err)
	matchAll := gt.NewGrant(group, "member", v2.ResourceId_builder{ResourceType: "user", Resource: "a0"}.Build(),
		gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()))
	f := externalMatchRerunFixture{
		resourceTypes:    []*v2.ResourceType{expandableUserResourceType},
		principals:       []*v2.Resource{expansionReplayUser(t, "fresh"), expansionReplayUser(t, "u2")},
		slugsByPrincipal: map[string][]string{"fresh": {"member"}, "u2": {"member"}},
		grants: []*v2.Grant{
			matchAll,
			externalMatchRerunCarrier(t, "g", v2.ResourceId_builder{ResourceType: "user", Resource: "ph"}.Build(), nil,
				v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build(), "member"),
		},
	}
	s, dst := newExternalMatchRerunSyncer(t, f)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	wantPending, wantGrants := expansionReplaySnapshot(t, dst)
	require.Equal(t, map[string][]string{"group:g:member:user:fresh": {"user:fresh:member"}}, wantPending,
		"premise: the first run leaves the remapped expansion on the matched user's grant")

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	pending, grants := expansionReplaySnapshot(t, dst)
	require.Equal(t, wantPending, pending, "rerun")
	require.Equal(t, wantGrants, grants, "rerun")
}

// TestExternalMatchTwoPlainCarriersCutBetweenPutChunks: two carriers without a
// GrantExpandable name the same external user by id and differ in their
// GrantMetadata. The first attempt commits the chunk holding the first
// carrier's grant and is cut before the second chunk. The rerun must reach
// the uninterrupted result.
func TestExternalMatchTwoPlainCarriersCutBetweenPutChunks(t *testing.T) {
	for _, placeholders := range [][2]string{{"zz1", "zz2"}, {"a1", "a2"}} {
		t.Run(fmt.Sprintf("placeholders %s %s", placeholders[0], placeholders[1]), func(t *testing.T) {
			ctx := t.Context()
			group, err := rs.NewGroupResource("g", groupResourceType, "g", nil)
			require.NoError(t, err)
			carrier := func(placeholderID, label string) *v2.Grant {
				return gt.NewGrant(group, "member", v2.ResourceId_builder{ResourceType: "user", Resource: placeholderID}.Build(),
					gt.WithAnnotation(v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build()),
					gt.WithGrantMetadata(map[string]any{"label": label}),
				)
			}
			f := externalMatchRerunFixture{
				resourceTypes:    []*v2.ResourceType{expandableUserResourceType},
				principals:       []*v2.Resource{expansionReplayUser(t, "fresh")},
				slugsByPrincipal: map[string][]string{"fresh": {"member"}},
				grants:           []*v2.Grant{carrier(placeholders[0], "one"), carrier(placeholders[1], "two")},
			}

			golden, goldenDst := newExternalMatchRerunSyncer(t, f)
			require.NoError(t, golden.SyncExternalResourcesUsersAndGroups(ctx))
			_, wantGrants := expansionReplaySnapshot(t, goldenDst)
			require.Len(t, wantGrants, 1, "premise: one grant for the matched user")

			s, dst := newExternalMatchRerunSyncer(t, f)
			putChunk := 1
			s.testHooks.externalMatchGrantPutChunk = &putChunk
			cut := &externalMatchPutCutStore{Store: dst, failAt: 2}
			s.setStore(cut)
			require.ErrorIs(t, s.SyncExternalResourcesUsersAndGroups(ctx), errExternalMatchPutCut)
			require.Equal(t, []int{1, 1}, cut.recorded, "premise: the first chunk committed, the second was cut")
			_, afterCut := expansionReplaySnapshot(t, dst)
			require.Len(t, afterCut, 3, "premise: both carriers and the first carrier's grant are stored")

			s.testHooks.externalMatchGrantPutChunk = nil
			s.setStore(dst)
			for attempt := 1; attempt <= 2; attempt++ {
				require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
				_, grants := expansionReplaySnapshot(t, dst)
				require.Equal(t, wantGrants, grants, "rerun %d", attempt)
			}
		})
	}
}

// TestExternalMatchLookalikeProfileFanoutRerun: a connector grant already addressed
// to external group p1, with plain entitlement ids on p1, whose profile match
// also selects p2. No interruption.
func TestExternalMatchLookalikeProfileFanoutRerun(t *testing.T) {
	ctx := t.Context()
	p1 := externalMatchRerunGroup(t, "p1")
	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	lookalike := gt.NewGrant(group, "member", p1.GetId(),
		gt.WithAnnotation(externalMatchDeptEng),
		gt.WithAnnotation(v2.GrantExpandable_builder{EntitlementIds: []string{"group:p1:member"}}.Build()),
	)
	f := externalMatchRerunFixture{
		resourceTypes:    []*v2.ResourceType{groupResourceType},
		principals:       []*v2.Resource{p1, externalMatchRerunGroup(t, "p2")},
		slugsByPrincipal: map[string][]string{"p1": {"member"}, "p2": {"member"}},
		grants:           []*v2.Grant{lookalike},
	}
	s, dst := newExternalMatchRerunSyncer(t, f)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	wantPending, wantGrants := expansionReplaySnapshot(t, dst)
	t.Logf("first run pending: %v", wantPending)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	pending, grants := expansionReplaySnapshot(t, dst)
	t.Logf("rerun pending: %v", pending)
	require.Equal(t, wantPending, pending, "rerun")
	require.Equal(t, wantGrants, grants, "rerun")
}

func TestExternalMatchRemapSkipsEntitlementOnOtherResourceSoGraphBuilds(t *testing.T) {
	ctx := t.Context()
	fresh := expansionReplayUser(t, "fresh")
	other := expansionReplayUser(t, "other")
	onOther := et.NewAssignmentEntitlement(other, "collides")
	onOther.SetId("user:fresh:owner")
	f := externalMatchRerunFixture{
		resourceTypes:     []*v2.ResourceType{expandableUserResourceType},
		principals:        []*v2.Resource{fresh, other},
		slugsByPrincipal:  map[string][]string{"fresh": {"member"}, "other": {"member"}},
		extraEntitlements: []*v2.Entitlement{onOther},
		grants: []*v2.Grant{
			externalMatchRerunCarrier(t, "g", v2.ResourceId_builder{ResourceType: "user", Resource: "ph"}.Build(), nil,
				v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build(), "member", "owner"),
		},
	}
	s, _ := newExternalMatchRerunSyncer(t, f)
	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	_, _, err := s.rebuildLedgerPreservedGraph(ctx)
	require.NoError(t, err)
}

func TestExternalMatchExpansionRewritesGrantExpandable(t *testing.T) {
	const slug = "member"

	t.Run("match_all", func(t *testing.T) {
		g, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
		require.NoError(t, err)
		carrier := expansionReplayCarrier(t, "g", "group", "placeholder",
			v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_GROUP}.Build(),
			slug,
		)
		s, dst := newExpansionReplaySyncer(t, expansionReplayCase{
			resourceType: groupResourceType,
			principals:   []*v2.Resource{g},
		}, []*v2.Grant{carrier})
		require.NoError(t, s.SyncExternalResourcesUsersAndGroups(t.Context()))
		got := grantExpandableIDs(t, dst, g)
		require.Equal(t, []string{et.NewEntitlementID(g, slug)}, got)
	})

	t.Run("match_user_by_key", func(t *testing.T) {
		u, err := rs.NewUserResource("fresh", expandableUserResourceType, "fresh",
			[]rs.UserTraitOption{rs.WithUserProfile(map[string]any{"dept": "eng"})},
		)
		require.NoError(t, err)
		carrier := expansionReplayCarrier(t, "g", "user", "placeholder",
			v2.ExternalResourceMatch_builder{
				Key:          "dept",
				Value:        "eng",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}.Build(),
			slug,
		)
		s, dst := newExpansionReplaySyncer(t, expansionReplayCase{
			resourceType: expandableUserResourceType,
			principals:   []*v2.Resource{u},
		}, []*v2.Grant{carrier})
		require.NoError(t, s.SyncExternalResourcesUsersAndGroups(t.Context()))
		got := grantExpandableIDs(t, dst, u)
		require.Equal(t, []string{et.NewEntitlementID(u, slug)}, got)
	})

	t.Run("match_group_by_key", func(t *testing.T) {
		g, err := rs.NewGroupResource("g1", groupResourceType, "g1",
			[]rs.GroupTraitOption{rs.WithGroupProfile(map[string]any{"dept": "eng"})},
		)
		require.NoError(t, err)
		carrier := expansionReplayCarrier(t, "g", "group", "placeholder",
			v2.ExternalResourceMatch_builder{
				Key:          "dept",
				Value:        "eng",
				ResourceType: v2.ResourceType_TRAIT_GROUP,
			}.Build(),
			slug,
		)
		s, dst := newExpansionReplaySyncer(t, expansionReplayCase{
			resourceType: groupResourceType,
			principals:   []*v2.Resource{g},
		}, []*v2.Grant{carrier})
		require.NoError(t, s.SyncExternalResourcesUsersAndGroups(t.Context()))
		got := grantExpandableIDs(t, dst, g)
		require.Equal(t, []string{et.NewEntitlementID(g, slug)}, got)
	})
}

func grantExpandableIDs(t *testing.T, store c1zstore.Store, principal *v2.Resource) []string {
	t.Helper()
	wantType := principal.GetId().GetResourceType()
	wantID := principal.GetId().GetResource()
	var ids []string
	n := 0
	for ga, err := range store.Grants().ListWithAnnotations(t.Context()) {
		require.NoError(t, err)
		if ga.PrincipalResourceTypeID != wantType || ga.PrincipalResourceID != wantID {
			continue
		}
		n++
		if ga.Annotation != nil {
			ids = append(ids, ga.Annotation.GetEntitlementIds()...)
		}
	}
	if n != 1 {
		t.Logf("principal %s/%s has %d stored grants", wantType, wantID, n)
	}
	return ids
}
