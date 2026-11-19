package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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

func externalMatchRerunUser(t *testing.T, id string, profile map[string]any) *v2.Resource {
	t.Helper()
	var opts []rs.UserTraitOption
	if profile != nil {
		opts = append(opts, rs.WithUserProfile(profile))
	}
	user, err := rs.NewUserResource(id, expandableUserResourceType, id, opts)
	require.NoError(t, err)
	return user
}

func externalMatchGrant(t *testing.T, entitlementResource, principalType, principalID string, expansion []string, annos ...proto.Message) *v2.Grant {
	t.Helper()
	group, err := rs.NewGroupResource(entitlementResource, groupResourceType, entitlementResource, nil)
	require.NoError(t, err)
	var opts []gt.GrantOption
	if len(expansion) > 0 {
		opts = append(opts, gt.WithAnnotation(v2.GrantExpandable_builder{EntitlementIds: expansion}.Build()))
	}
	for _, anno := range annos {
		opts = append(opts, gt.WithAnnotation(anno))
	}
	return gt.NewGrant(group, "member", v2.ResourceId_builder{ResourceType: principalType, Resource: principalID}.Build(), opts...)
}

func externalMatchEntitlementBIDs(t *testing.T, resourceType, resourceID string, slugs ...string) []string {
	t.Helper()
	var out []string
	for _, slug := range slugs {
		entBID, err := bid.MakeBid(v2.Entitlement_builder{
			Resource: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: resourceType, Resource: resourceID}.Build()}.Build(),
			Slug:     slug,
		}.Build())
		require.NoError(t, err)
		out = append(out, entBID)
	}
	return out
}

// externalMatchAnnotatedLines returns pending expansion ids by grant id, and
// one sorted line per stored grant: id, annotation names, and wire form.
func externalMatchAnnotatedLines(t *testing.T, store c1zstore.Store) (map[string][]string, []string) {
	t.Helper()
	ctx := t.Context()
	pending := make(map[string][]string)
	pageToken := ""
	for {
		page, next, err := store.Grants().PendingExpansionPage(ctx, pageToken)
		require.NoError(t, err)
		for _, def := range page {
			pending[def.GrantExternalID] = def.Annotation.GetEntitlementIds()
		}
		if next == "" {
			break
		}
		pageToken = next
	}
	marshal := proto.MarshalOptions{Deterministic: true}
	var lines []string
	for ga, err := range store.Grants().ListWithAnnotations(ctx) {
		require.NoError(t, err)
		wire, err := marshal.Marshal(ga.Grant)
		require.NoError(t, err)
		var types []string
		for _, anno := range ga.Grant.GetAnnotations() {
			types = append(types, string(anno.MessageName().Name()))
		}
		lines = append(lines, fmt.Sprintf("%s %v %s", ga.Grant.GetId(), types, hex.EncodeToString(wire)))
	}
	slices.Sort(lines)
	return pending, lines
}

func externalMatchLineIDs(lines []string) []string {
	ids := make([]string, 0, len(lines))
	for _, line := range lines {
		var id string
		_, _ = fmt.Sscanf(line, "%s", &id)
		ids = append(ids, id)
	}
	return ids
}

// TestExternalMatchSelfCarrierFanoutCutBetweenPutChunks: a carrier whose
// principal is one of the external principals it matches, and which matches
// more than one. The carrier's own output is written last. The cut is the
// second one-grant put, before that own output.
func TestExternalMatchSelfCarrierFanoutCutBetweenPutChunks(t *testing.T) {
	cutAndResume := func(t *testing.T, f externalMatchRerunFixture, wantIDs []string) {
		t.Helper()
		ctx := t.Context()
		golden, goldenDst := newExternalMatchRerunSyncer(t, f)
		require.NoError(t, golden.SyncExternalResourcesUsersAndGroups(ctx))
		_, goldenLines := externalMatchAnnotatedLines(t, goldenDst)
		require.Equal(t, wantIDs, externalMatchLineIDs(goldenLines), "premise: uninterrupted result")

		s, dst := newExternalMatchRerunSyncer(t, f)
		putChunk := 1
		s.testHooks.externalMatchGrantPutChunk = &putChunk
		cut := &externalMatchPutCutStore{Store: dst, failAt: 2}
		s.setStore(cut)
		require.ErrorIs(t, s.SyncExternalResourcesUsersAndGroups(ctx), errExternalMatchPutCut)
		require.Equal(t, []int{1, 1}, cut.recorded, "premise: the first chunk committed, the second was cut")
		_, afterCut := externalMatchAnnotatedLines(t, dst)
		require.Len(t, afterCut, 2, "the first other principal's grant committed and the carrier row is still stored")

		s.testHooks.externalMatchGrantPutChunk = nil
		s.setStore(dst)
		for attempt := 1; attempt <= 2; attempt++ {
			require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
			_, lines := externalMatchAnnotatedLines(t, dst)
			require.Equal(t, wantIDs, externalMatchLineIDs(lines), "rerun %d", attempt)
		}
	}
	t.Run("profile match", func(t *testing.T) {
		cutAndResume(t, externalMatchRerunFixture{
			resourceTypes: []*v2.ResourceType{groupResourceType},
			principals:    []*v2.Resource{externalMatchRerunGroup(t, "p1"), externalMatchRerunGroup(t, "p2")},
			grants: []*v2.Grant{
				externalMatchGrant(t, "g1", "group", "p1", nil,
					v2.ExternalResourceMatch_builder{Key: "dept", Value: "eng", ResourceType: v2.ResourceType_TRAIT_GROUP}.Build()),
			},
		}, []string{"group:g1:member:group:p1", "group:g1:member:group:p2"})
	})
	t.Run("match all", func(t *testing.T) {
		cutAndResume(t, externalMatchRerunFixture{
			resourceTypes: []*v2.ResourceType{expandableUserResourceType},
			principals: []*v2.Resource{
				externalMatchRerunUser(t, "u1", map[string]any{"upn": "shared@example.com"}),
				externalMatchRerunUser(t, "u2", map[string]any{"upn": "shared@example.com"}),
				externalMatchRerunUser(t, "u3", map[string]any{"upn": "shared@example.com"}),
			},
			grants: []*v2.Grant{
				externalMatchGrant(t, "g1", "user", "u1", nil,
					v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()),
			},
		}, []string{"group:g1:member:user:u1", "group:g1:member:user:u2", "group:g1:member:user:u3"})
	})
}

func externalMatchResumeOverOldRows(
	t *testing.T, f externalMatchRerunFixture, carriers, oldRows []*v2.Grant,
) (map[string][]string, []string, map[string][]string, []string) {
	t.Helper()
	ctx := t.Context()
	f.grants = carriers
	golden, goldenDst := newExternalMatchRerunSyncer(t, f)
	require.NoError(t, golden.SyncExternalResourcesUsersAndGroups(ctx))
	wantPending, want := externalMatchAnnotatedLines(t, goldenDst)

	f.grants = oldRows
	s, dst := newExternalMatchRerunSyncer(t, f)
	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	gotPending, got := externalMatchAnnotatedLines(t, dst)
	return wantPending, want, gotPending, got
}

// TestExternalMatchResumeOverPreviousSDKRows seeds, per match branch, the
// rows the previous SDK's step leaves once its carriers are deleted.
func TestExternalMatchResumeOverPreviousSDKRows(t *testing.T) {
	deptEng := v2.ExternalResourceMatch_builder{Key: "dept", Value: "eng", ResourceType: v2.ResourceType_TRAIT_GROUP}.Build()
	upn := v2.ExternalResourceMatch_builder{Key: "upn", Value: "shared@example.com", ResourceType: v2.ResourceType_TRAIT_USER}.Build()
	matchAll := v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()
	matchFresh := v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build()
	sharedProfile := map[string]any{"upn": "shared@example.com"}

	for _, tc := range []struct {
		name     string
		fixture  externalMatchRerunFixture
		carriers []*v2.Grant
		oldRows  []*v2.Grant
		// droppedExpansion is set when the previous SDK stored baton ids
		// naming the stand-in, so the rerun cannot rebuild expansion entries.
		droppedExpansion []string
	}{
		{
			name: "match by id, expandable",
			fixture: externalMatchRerunFixture{
				resourceTypes:    []*v2.ResourceType{expandableUserResourceType},
				principals:       []*v2.Resource{externalMatchRerunUser(t, "fresh", nil)},
				slugsByPrincipal: map[string][]string{"fresh": {"member", "admin"}},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "user", "ph", externalMatchEntitlementBIDs(t, "user", "ph", "member", "admin"), matchFresh)},
			oldRows:  []*v2.Grant{externalMatchGrant(t, "g", "user", "fresh", []string{"user:fresh:member", "user:fresh:admin"}, matchFresh)},
		},
		{
			name: "group profile, every group holds every slug",
			fixture: externalMatchRerunFixture{
				resourceTypes:    []*v2.ResourceType{groupResourceType},
				principals:       []*v2.Resource{externalMatchRerunGroup(t, "p1"), externalMatchRerunGroup(t, "p2")},
				slugsByPrincipal: map[string][]string{"p1": {"member", "admin"}, "p2": {"member", "admin"}},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "group", "ph", externalMatchEntitlementBIDs(t, "group", "ph", "member", "admin"), deptEng)},
			oldRows: []*v2.Grant{
				externalMatchGrant(t, "g", "group", "p1", []string{"group:p1:member", "group:p1:admin"}, deptEng),
				externalMatchGrant(t, "g", "group", "p2", []string{"group:p2:member", "group:p2:admin"}, deptEng),
			},
		},
		{
			name: "group profile, p2 holds fewer slugs than p1",
			fixture: externalMatchRerunFixture{
				resourceTypes:    []*v2.ResourceType{groupResourceType},
				principals:       []*v2.Resource{externalMatchRerunGroup(t, "p1"), externalMatchRerunGroup(t, "p2")},
				slugsByPrincipal: map[string][]string{"p1": {"member", "admin"}, "p2": {"member"}},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "group", "ph", externalMatchEntitlementBIDs(t, "group", "ph", "member", "admin"), deptEng)},
			oldRows: []*v2.Grant{
				externalMatchGrant(t, "g", "group", "p1", []string{"group:p1:member", "group:p1:admin"}, deptEng),
				externalMatchGrant(t, "g", "group", "p2", []string{"group:p2:member"}, deptEng),
			},
		},
		{
			name: "group profile, p2 holds none of the slugs",
			fixture: externalMatchRerunFixture{
				resourceTypes:    []*v2.ResourceType{groupResourceType},
				principals:       []*v2.Resource{externalMatchRerunGroup(t, "p1"), externalMatchRerunGroup(t, "p2")},
				slugsByPrincipal: map[string][]string{"p1": {"member", "admin"}},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "group", "ph", externalMatchEntitlementBIDs(t, "group", "ph", "member", "admin"), deptEng)},
			oldRows: []*v2.Grant{
				externalMatchGrant(t, "g", "group", "p1", []string{"group:p1:member", "group:p1:admin"}, deptEng),
				externalMatchGrant(t, "g", "group", "p2", nil, deptEng),
			},
		},
		{
			name: "user profile, not expandable",
			fixture: externalMatchRerunFixture{
				resourceTypes: []*v2.ResourceType{expandableUserResourceType},
				principals: []*v2.Resource{
					externalMatchRerunUser(t, "u1", sharedProfile),
					externalMatchRerunUser(t, "u2", sharedProfile),
				},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "user", "ph", nil, upn)},
			oldRows: []*v2.Grant{
				externalMatchGrant(t, "g", "user", "u1", nil, upn),
				externalMatchGrant(t, "g", "user", "u2", nil, upn),
			},
		},
		{
			name: "user profile, expandable rows keep each principal and drop expansion",
			fixture: externalMatchRerunFixture{
				resourceTypes: []*v2.ResourceType{expandableUserResourceType},
				principals: []*v2.Resource{
					externalMatchRerunUser(t, "u1", sharedProfile),
					externalMatchRerunUser(t, "u2", sharedProfile),
				},
				slugsByPrincipal: map[string][]string{"u1": {"member"}, "u2": {"member"}},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "user", "ph", externalMatchEntitlementBIDs(t, "user", "ph", "member"), upn)},
			oldRows: []*v2.Grant{
				externalMatchGrant(t, "g", "user", "u1", externalMatchEntitlementBIDs(t, "user", "ph", "member"), upn),
				externalMatchGrant(t, "g", "user", "u2", externalMatchEntitlementBIDs(t, "user", "ph", "member"), upn),
			},
			droppedExpansion: []string{"group:g:member:user:u1", "group:g:member:user:u2"},
		},
		{
			name: "match all, not expandable",
			fixture: externalMatchRerunFixture{
				resourceTypes: []*v2.ResourceType{expandableUserResourceType},
				principals: []*v2.Resource{
					externalMatchRerunUser(t, "u1", nil),
					externalMatchRerunUser(t, "u2", nil),
					externalMatchRerunUser(t, "u3", nil),
				},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "user", "ph", nil, matchAll)},
			oldRows: []*v2.Grant{
				externalMatchGrant(t, "g", "user", "u1", nil, matchAll),
				externalMatchGrant(t, "g", "user", "u2", nil, matchAll),
				externalMatchGrant(t, "g", "user", "u3", nil, matchAll),
			},
		},
		{
			name: "match all, expandable rows keep each principal and drop expansion",
			fixture: externalMatchRerunFixture{
				resourceTypes: []*v2.ResourceType{expandableUserResourceType},
				principals: []*v2.Resource{
					externalMatchRerunUser(t, "u1", nil),
					externalMatchRerunUser(t, "u2", nil),
				},
				slugsByPrincipal: map[string][]string{"u1": {"member"}, "u2": {"member"}},
			},
			carriers: []*v2.Grant{externalMatchGrant(t, "g", "user", "ph", externalMatchEntitlementBIDs(t, "user", "ph", "member"), matchAll)},
			oldRows: []*v2.Grant{
				externalMatchGrant(t, "g", "user", "u1", externalMatchEntitlementBIDs(t, "user", "ph", "member"), matchAll),
				externalMatchGrant(t, "g", "user", "u2", externalMatchEntitlementBIDs(t, "user", "ph", "member"), matchAll),
			},
			droppedExpansion: []string{"group:g:member:user:u1", "group:g:member:user:u2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantPending, want, gotPending, got := externalMatchResumeOverOldRows(t, tc.fixture, tc.carriers, tc.oldRows)
			if tc.droppedExpansion != nil {
				require.Empty(t, gotPending)
				require.Equal(t, tc.droppedExpansion, externalMatchLineIDs(got))
				for _, line := range got {
					require.NotContains(t, line, "ExternalResourceMatch")
					require.NotContains(t, line, "GrantExpandable")
				}
				return
			}
			require.Equal(t, wantPending, gotPending)
			require.Equal(t, want, got)
		})
	}
}

type externalMatchCutRow struct {
	principal string
	match     proto.Message
	label     string
	expansion []string
}

type externalMatchCutScenario struct {
	name   string
	groups map[string]string
	// profile holds fields merged onto the dept value from groups.
	profile map[string]map[string]any
	slugs   map[string][]string
	rows    []externalMatchCutRow
	// want is the uninterrupted store when the scenario pins which content wins.
	want []string
	// wholeCallsOnly drops the one-grant-per-call mode.
	wholeCallsOnly bool
}

type externalMatchCutMode struct {
	name            string
	putChunk        int
	perGrantDeletes bool
}

var externalMatchCutWholeCalls = externalMatchCutMode{name: "whole calls"}
var externalMatchCutOneGrant = externalMatchCutMode{name: "one grant per call", putChunk: 1, perGrantDeletes: true}

var errExternalMatchCut = errors.New("external match: injected cut")

type externalMatchCutStore struct {
	c1zstore.Store
	failAt          int
	perGrantDeletes bool
	ops             []string
}

func externalMatchCutPrincipals(grants []*v2.Grant) string {
	ids := make([]string, 0, len(grants))
	for _, g := range grants {
		ids = append(ids, g.GetPrincipal().GetId().GetResource())
	}
	return strings.Join(ids, ",")
}

func (s *externalMatchCutStore) step(desc string) error {
	s.ops = append(s.ops, desc)
	if s.failAt > 0 && len(s.ops) == s.failAt {
		return errExternalMatchCut
	}
	return nil
}

func (s *externalMatchCutStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	if len(grants) > 0 {
		if err := s.step("put[" + externalMatchCutPrincipals(grants) + "]"); err != nil {
			return err
		}
	}
	return s.Store.PutGrants(ctx, grants...)
}

func (s *externalMatchCutStore) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	deleter := s.Store.(grantsByRefsBatchDeleter)
	if !s.perGrantDeletes {
		if err := s.step("delete[" + externalMatchCutPrincipals(grants) + "]"); err != nil {
			return err
		}
		return deleter.DeleteGrantsByRefs(ctx, grants...)
	}
	for _, g := range grants {
		if err := s.step("delete[" + externalMatchCutPrincipals([]*v2.Grant{g}) + "]"); err != nil {
			return err
		}
		if err := deleter.DeleteGrantsByRefs(ctx, g); err != nil {
			return err
		}
	}
	return nil
}

func (s *externalMatchCutStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.Store.(grantByRefsDeleter).DeleteGrantByRefs(ctx, grant)
}

func (s *externalMatchCutStore) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return s.Store.(resourceRecordDeleter).DeleteResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *externalMatchCutStore) DeleteEntitlementByRefs(ctx context.Context, ent *v2.Entitlement) error {
	return s.Store.(entitlementRecordDeleter).DeleteEntitlementByRefs(ctx, ent)
}

func externalMatchCutSnapshot(t *testing.T, store c1zstore.Store) []string {
	t.Helper()
	var lines []string
	for ga, err := range store.Grants().ListWithAnnotations(t.Context()) {
		require.NoError(t, err)
		annos := annotations.Annotations(ga.Grant.GetAnnotations())
		label := ""
		md := &v2.GrantMetadata{}
		if ok, err := annos.Pick(md); err == nil && ok {
			label = md.GetMetadata().GetFields()["label"].GetStringValue()
		}
		carrier := annos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{})
		line := fmt.Sprintf("%s label=%q", ga.Grant.GetPrincipal().GetId().GetResource(), label)
		if carrier {
			line += " CARRIER"
		}
		if ga.Annotation != nil {
			line += fmt.Sprintf(" expansion=%v", ga.Annotation.GetEntitlementIds())
		}
		lines = append(lines, line)
	}
	slices.Sort(lines)
	return lines
}

func newExternalMatchCutExternal(t *testing.T, sc externalMatchCutScenario) c1zstore.Store {
	t.Helper()
	ctx := t.Context()
	tmp := t.TempDir()
	ext, err := dotc1z.NewStore(ctx, filepath.Join(tmp, "external.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ext.Close(ctx) })
	_, err = ext.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, ext.PutResourceTypes(ctx, groupResourceType))
	ids := make([]string, 0, len(sc.groups))
	for id := range sc.groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		profile := map[string]any{}
		if dept, ok := sc.groups[id]; ok {
			profile["dept"] = dept
		}
		for k, v := range sc.profile[id] {
			profile[k] = v
		}
		group, err := rs.NewGroupResource(id, groupResourceType, id,
			[]rs.GroupTraitOption{rs.WithGroupProfile(profile)})
		require.NoError(t, err)
		require.NoError(t, ext.PutResources(ctx, group))
		for _, slug := range sc.slugs[id] {
			require.NoError(t, ext.PutEntitlements(ctx, et.NewAssignmentEntitlement(group, slug)))
		}
	}
	require.NoError(t, ext.EndSync(ctx))
	return ext
}

func newExternalMatchCutRun(t *testing.T, sc externalMatchCutScenario, ext c1zstore.Store) (*syncer, c1zstore.Store) {
	t.Helper()
	ctx := t.Context()
	tmp := t.TempDir()
	dst, err := dotc1z.NewStore(ctx, filepath.Join(tmp, "internal.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close(ctx) })
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	entitlementResource, err := rs.NewGroupResource("g", groupResourceType, "g", nil)
	require.NoError(t, err)
	grants := make([]*v2.Grant, 0, len(sc.rows))
	for _, row := range sc.rows {
		opts := []gt.GrantOption{gt.WithAnnotation(row.match)}
		if len(row.expansion) > 0 {
			opts = append(opts, gt.WithAnnotation(v2.GrantExpandable_builder{EntitlementIds: row.expansion}.Build()))
		}
		if row.label != "" {
			opts = append(opts, gt.WithGrantMetadata(map[string]any{"label": row.label}))
		}
		grants = append(grants, gt.NewGrant(entitlementResource, "member",
			v2.ResourceId_builder{ResourceType: "group", Resource: row.principal}.Build(), opts...))
	}
	require.NoError(t, dst.PutGrants(ctx, grants...))

	run := newRunState()
	run.setFact(factHasExternalResourceGrants)
	s := &syncer{run: run, stats: newRunStats(), graph: newExpansionGraph(), externalResourceReader: ext}
	s.setStore(dst)
	return s, dst
}

func externalMatchCutSweep(t *testing.T, sc externalMatchCutScenario, mode externalMatchCutMode) {
	t.Helper()
	ctx := t.Context()
	ext := newExternalMatchCutExternal(t, sc)

	arm := func(s *syncer, dst c1zstore.Store, failAt int) *externalMatchCutStore {
		cut := &externalMatchCutStore{Store: dst, failAt: failAt, perGrantDeletes: mode.perGrantDeletes}
		if mode.putChunk > 0 {
			putChunk := mode.putChunk
			s.testHooks.externalMatchGrantPutChunk = &putChunk
		}
		s.setStore(cut)
		return cut
	}

	golden, goldenDst := newExternalMatchCutRun(t, sc, ext)
	require.NoError(t, golden.SyncExternalResourcesUsersAndGroups(ctx))
	want := externalMatchCutSnapshot(t, goldenDst)
	if sc.want != nil {
		require.Equal(t, sc.want, want)
	}

	counted, countedDst := newExternalMatchCutRun(t, sc, ext)
	counter := arm(counted, countedDst, 0)
	require.NoError(t, counted.SyncExternalResourcesUsersAndGroups(ctx))
	require.Equal(t, want, externalMatchCutSnapshot(t, countedDst), "premise: chunking does not change the uninterrupted store")

	for failAt := 1; failAt <= len(counter.ops); failAt++ {
		t.Run(fmt.Sprintf("cut at %d %s", failAt, counter.ops[failAt-1]), func(t *testing.T) {
			s, dst := newExternalMatchCutRun(t, sc, ext)
			cut := arm(s, dst, failAt)
			require.ErrorIs(t, s.SyncExternalResourcesUsersAndGroups(ctx), errExternalMatchCut)
			require.Equal(t, counter.ops[:failAt], cut.ops, "premise: the cut run issued the same mutations up to the cut")
			afterCut := externalMatchCutSnapshot(t, dst)

			s.testHooks.externalMatchGrantPutChunk = nil
			s.setStore(dst)
			for attempt := 1; attempt <= 2; attempt++ {
				require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
				require.Equal(t, want, externalMatchCutSnapshot(t, dst), "rerun %d; store after the cut: %v", attempt, afterCut)
			}
		})
	}
}

func externalMatchCutID(id string) proto.Message {
	return v2.ExternalResourceMatchID_builder{Id: id}.Build()
}

func externalMatchCutDept(value string) proto.Message {
	return v2.ExternalResourceMatch_builder{Key: "dept", Value: value, ResourceType: v2.ResourceType_TRAIT_GROUP}.Build()
}

func externalMatchCutScenarios(t *testing.T) []externalMatchCutScenario {
	t.Helper()
	eng2 := map[string]string{"p1": "eng", "p2": "eng"}
	eng3 := map[string]string{"p1": "eng", "p2": "eng", "p3": "eng"}
	return []externalMatchCutScenario{
		{
			name:   "one carrier on a matched group, two matches",
			groups: eng2,
			rows:   []externalMatchCutRow{{principal: "p1", match: externalMatchCutDept("eng"), label: "L"}},
		},
		{
			name:   "two carriers for one grant, stand-in scanned first",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "a0", match: externalMatchCutID("p1"), label: "stand-in"},
				{principal: "p1", match: externalMatchCutID("p1"), label: "own"},
			},
			want: []string{`p1 label="own"`},
		},
		{
			name:   "two carriers for one grant, stand-in scanned last",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "p1", match: externalMatchCutID("p1"), label: "own"},
				{principal: "zz", match: externalMatchCutID("p1"), label: "stand-in"},
			},
			want: []string{`p1 label="own"`},
		},
		{
			name:   "stand-in lands on a carrier that names another group",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "a0", match: externalMatchCutID("p1"), label: "A"},
				{principal: "p1", match: externalMatchCutID("p2"), label: "B"},
			},
			want: []string{`p1 label="A"`, `p2 label="B"`},
		},
		{
			name:   "stand-in lands on a carrier that matches nothing",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "a0", match: externalMatchCutID("p1"), label: "A"},
				{principal: "p1", match: externalMatchCutID("absent"), label: "B"},
			},
		},
		{
			name:   "two stand-ins for one grant",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "a1", match: externalMatchCutID("p1"), label: "first"},
				{principal: "a2", match: externalMatchCutID("p1"), label: "second"},
			},
		},
		{
			name:   "chain: p1 row names p2, p2 row names p3, p3 row names itself",
			groups: eng3,
			rows: []externalMatchCutRow{
				{principal: "p1", match: externalMatchCutID("p2"), label: "A"},
				{principal: "p2", match: externalMatchCutID("p3"), label: "B"},
				{principal: "p3", match: externalMatchCutID("p3"), label: "C"},
			},
		},
		{
			name:           "chain: p1 row names p2, p2 row names p3, p3 row matches nothing",
			wholeCallsOnly: true,
			groups:         eng3,
			rows: []externalMatchCutRow{
				{principal: "p1", match: externalMatchCutID("p2"), label: "A"},
				{principal: "p2", match: externalMatchCutID("p3"), label: "B"},
				{principal: "p3", match: externalMatchCutID("absent"), label: "C"},
			},
		},
		{
			name:           "two carriers on matched groups, each matching both",
			wholeCallsOnly: true,
			groups:         eng2,
			rows: []externalMatchCutRow{
				{principal: "p1", match: externalMatchCutDept("eng"), label: "r1"},
				{principal: "p2", match: externalMatchCutDept("eng"), label: "r2"},
			},
		},
		{
			name:           "previous-SDK profile rows, p1 holds more slugs than p2",
			wholeCallsOnly: true,
			groups:         eng2,
			slugs:          map[string][]string{"p1": {"member", "admin"}, "p2": {"member"}},
			rows: []externalMatchCutRow{
				{principal: "p1", match: externalMatchCutDept("eng"), expansion: []string{"group:p1:member", "group:p1:admin"}},
				{principal: "p2", match: externalMatchCutDept("eng"), expansion: []string{"group:p2:member"}},
			},
		},
		{
			name:           "previous-SDK profile rows beside their carrier, carrier scanned first",
			wholeCallsOnly: true,
			groups:         eng2,
			slugs:          map[string][]string{"p1": {"member", "admin"}, "p2": {"member"}},
			rows: []externalMatchCutRow{
				{principal: "a0", match: externalMatchCutDept("eng"), expansion: externalMatchEntitlementBIDs(t, "group", "a0", "member", "admin")},
				{principal: "p1", match: externalMatchCutDept("eng"), expansion: []string{"group:p1:member", "group:p1:admin"}},
				{principal: "p2", match: externalMatchCutDept("eng"), expansion: []string{"group:p2:member"}},
			},
		},
		{
			name:           "previous-SDK profile rows beside their carrier, carrier scanned last",
			wholeCallsOnly: true,
			groups:         eng2,
			slugs:          map[string][]string{"p1": {"member", "admin"}, "p2": {"member"}},
			rows: []externalMatchCutRow{
				{principal: "p1", match: externalMatchCutDept("eng"), expansion: []string{"group:p1:member", "group:p1:admin"}},
				{principal: "p2", match: externalMatchCutDept("eng"), expansion: []string{"group:p2:member"}},
				{principal: "zz", match: externalMatchCutDept("eng"), expansion: externalMatchEntitlementBIDs(t, "group", "zz", "member", "admin")},
			},
		},
		{
			name:   "carrier on p1 matching p1 and p2, then a stand-in for p2",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "p1", match: externalMatchCutDept("eng"), label: "L"},
				{principal: "zz", match: externalMatchCutID("p2"), label: "stand-in"},
			},
		},
		{
			name:   "a stand-in for p2, then a carrier on p1 matching p1 and p2",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "a0", match: externalMatchCutID("p2"), label: "stand-in"},
				{principal: "p1", match: externalMatchCutDept("eng"), label: "L"},
			},
		},
		{
			name:   "stand-in matching p1 and p2 with p2 row matching nothing, then a stand-in for p1",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "a0", match: externalMatchCutDept("eng"), label: "wide"},
				{principal: "p2", match: externalMatchCutID("absent"), label: "x"},
				{principal: "zz", match: externalMatchCutID("p1"), label: "narrow"},
			},
		},
		{
			name:   "carriers that match nothing",
			groups: eng2,
			rows: []externalMatchCutRow{
				{principal: "a0", match: externalMatchCutID("absent"), label: "A"},
				{principal: "p1", match: externalMatchCutDept("ops"), label: "B"},
			},
		},
	}
}

func TestExternalMatchCutSweep(t *testing.T) {
	for _, sc := range externalMatchCutScenarios(t) {
		modes := []externalMatchCutMode{externalMatchCutWholeCalls, externalMatchCutOneGrant}
		if sc.wholeCallsOnly {
			modes = []externalMatchCutMode{externalMatchCutWholeCalls}
		}
		for _, mode := range modes {
			t.Run(sc.name+"/"+mode.name, func(t *testing.T) {
				externalMatchCutSweep(t, sc, mode)
			})
		}
	}
}
