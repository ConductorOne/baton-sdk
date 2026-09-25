package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

var expandableUserResourceType = v2.ResourceType_builder{
	Id:     "user",
	Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
}.Build()

var expansionReplaySlugs = []string{"member", "admin"}

type expansionReplayCase struct {
	name              string
	resourceType      *v2.ResourceType
	principals        []*v2.Resource
	extraEntitlements []*v2.Entitlement
	carriers          []*v2.Grant
	wantPending       map[string][]string
}

func expansionReplayCases(t *testing.T) []expansionReplayCase {
	t.Helper()
	groups := make([]*v2.Resource, 0, 2)
	for _, id := range []string{"p1", "p2"} {
		group, err := rs.NewGroupResource(id, groupResourceType, id,
			[]rs.GroupTraitOption{rs.WithGroupProfile(map[string]any{"dept": "eng"})})
		require.NoError(t, err)
		groups = append(groups, group)
	}
	profileMatch := v2.ExternalResourceMatch_builder{Key: "dept", Value: "eng", ResourceType: v2.ResourceType_TRAIT_GROUP}.Build()

	return []expansionReplayCase{
		{
			name:         "match by id",
			resourceType: expandableUserResourceType,
			principals:   []*v2.Resource{expansionReplayUser(t, "fresh1"), expansionReplayUser(t, "fresh2")},
			carriers: []*v2.Grant{
				expansionReplayCarrier(t, "g1", "user", "ph1", v2.ExternalResourceMatchID_builder{Id: "fresh1"}.Build()),
				expansionReplayCarrier(t, "g2", "user", "ph2", v2.ExternalResourceMatchID_builder{Id: "fresh2"}.Build()),
			},
			wantPending: map[string][]string{
				"group:g1:member:user:fresh1": {"user:fresh1:member", "user:fresh1:admin"},
				"group:g2:member:user:fresh2": {"user:fresh2:member", "user:fresh2:admin"},
			},
		},
		{
			name:         "two carriers for the same grant",
			resourceType: expandableUserResourceType,
			principals:   []*v2.Resource{expansionReplayUser(t, "fresh")},
			carriers: []*v2.Grant{
				expansionReplayCarrier(t, "g", "user", "ph1", v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build(), "member"),
				expansionReplayCarrier(t, "g", "user", "ph2", v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build(), "admin"),
			},
			wantPending: map[string][]string{
				"group:g:member:user:fresh": {"user:fresh:admin"},
			},
		},
		{
			name:         "match by group profile",
			resourceType: groupResourceType,
			principals:   groups,
			carriers: []*v2.Grant{
				expansionReplayCarrier(t, "g1", "group", "ph1", profileMatch),
				expansionReplayCarrier(t, "g2", "group", "ph2", profileMatch),
			},
			wantPending: map[string][]string{
				"group:g1:member:group:p1": {"group:p1:member", "group:p1:admin"},
				"group:g1:member:group:p2": {"group:p2:member", "group:p2:admin"},
				"group:g2:member:group:p1": {"group:p1:member", "group:p1:admin"},
				"group:g2:member:group:p2": {"group:p2:member", "group:p2:admin"},
			},
		},
	}
}

// TestExternalMatchExpansionResumeMatchesUninterruptedRun interrupts the
// external-resource import at every grant it writes and every carrier it
// deletes, resumes it twice on the same store, and requires the uninterrupted
// run's grants exactly.
func TestExternalMatchExpansionResumeMatchesUninterruptedRun(t *testing.T) {
	for _, tc := range expansionReplayCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			s, dst := newExpansionReplaySyncer(t, tc, tc.carriers)
			require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
			wantPending, wantGrants := expansionReplaySnapshot(t, dst)
			require.Equal(t, tc.wantPending, wantPending, "premise: the uninterrupted run remaps the expansion")

			for _, phase := range []string{"write", "delete"} {
				for cutAfter := 0; ; cutAfter++ {
					interrupted := false
					t.Run(fmt.Sprintf("cut %s after %d", phase, cutAfter), func(t *testing.T) {
						ctx := t.Context()
						s, dst := newExpansionReplaySyncer(t, tc, tc.carriers)
						cut := &cuttingGrantStore{Store: dst, putLimit: -1, deleteLimit: -1}
						if phase == "write" {
							cut.putLimit = cutAfter
						} else {
							cut.deleteLimit = cutAfter
						}
						s.setStore(cut)
						err := s.SyncExternalResourcesUsersAndGroups(ctx)
						if err == nil {
							return
						}
						require.ErrorIs(t, err, errExpansionReplayCut)
						interrupted = true

						s.setStore(dst)
						for attempt := 1; attempt <= 2; attempt++ {
							require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
							pending, grants := expansionReplaySnapshot(t, dst)
							require.Equal(t, wantPending, pending, "attempt %d", attempt)
							require.Equal(t, wantGrants, grants, "attempt %d", attempt)
						}
					})
					if !interrupted {
						break
					}
				}
			}
		})
	}
}

var errExpansionReplayCut = errors.New("injected cut")

// cuttingGrantStore writes or deletes grants one at a time and stops with
// errExpansionReplayCut after the limit, as a process killed inside the
// batched PutGrants or DeleteGrantsByRefs would. A negative limit disables the
// cut.
type cuttingGrantStore struct {
	c1zstore.Store
	putLimit, deleteLimit int
}

func (s *cuttingGrantStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	if s.putLimit < 0 || len(grants) <= s.putLimit {
		return s.Store.PutGrants(ctx, grants...)
	}
	for _, g := range grants[:s.putLimit] {
		if err := s.Store.PutGrants(ctx, g); err != nil {
			return err
		}
	}
	return errExpansionReplayCut
}

func (s *cuttingGrantStore) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	deleter := s.Store.(grantsByRefsBatchDeleter)
	if s.deleteLimit < 0 || len(grants) <= s.deleteLimit {
		return deleter.DeleteGrantsByRefs(ctx, grants...)
	}
	for _, g := range grants[:s.deleteLimit] {
		if err := deleter.DeleteGrantsByRefs(ctx, g); err != nil {
			return err
		}
	}
	return errExpansionReplayCut
}

func (s *cuttingGrantStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.Store.(grantByRefsDeleter).DeleteGrantByRefs(ctx, grant)
}

func (s *cuttingGrantStore) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return s.Store.(resourceRecordDeleter).DeleteResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *cuttingGrantStore) DeleteEntitlementByRefs(ctx context.Context, ent *v2.Entitlement) error {
	return s.Store.(entitlementRecordDeleter).DeleteEntitlementByRefs(ctx, ent)
}

// TestExternalMatchExpansionRemapsCarrierOnMatchedPrincipal covers a carrier
// whose principal is already the external principal: its BID entries still
// need the first run's remap.
func TestExternalMatchExpansionRemapsCarrierOnMatchedPrincipal(t *testing.T) {
	ctx := t.Context()
	tc := expansionReplayCase{
		resourceType: expandableUserResourceType,
		principals:   []*v2.Resource{expansionReplayUser(t, "fresh")},
		carriers:     []*v2.Grant{expansionReplayCarrier(t, "g", "user", "fresh", v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build())},
	}
	s, dst := newExpansionReplaySyncer(t, tc, tc.carriers)

	require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
	pending, _ := expansionReplaySnapshot(t, dst)
	require.Equal(t, map[string][]string{"group:g:member:user:fresh": {"user:fresh:member", "user:fresh:admin"}}, pending)
}

// TestExternalMatchExpansionConnectorLookalike covers connector grants shaped
// like a previously processed row: match-annotated, already addressed to
// the matched principal, with plain entitlement ids. One whose every entry is
// an entitlement on that principal is left as emitted; any other is rewritten
// exactly as main rewrites it, so no entry reaches the expander that
// loadEntitlementGraph would reject.
func TestExternalMatchExpansionConnectorLookalike(t *testing.T) {
	fresh := expansionReplayUser(t, "fresh")
	other := expansionReplayUser(t, "other")
	onOther := func(id string) *v2.Entitlement {
		ent := et.NewAssignmentEntitlement(other, "collides")
		ent.SetId(id)
		return ent
	}
	for _, tc := range []struct {
		name        string
		entries     []string
		extra       []*v2.Entitlement
		wantPending map[string][]string
	}{
		{
			name:        "entries on the principal",
			entries:     []string{"user:fresh:member", "user:fresh:admin"},
			wantPending: map[string][]string{"group:g:member:user:fresh": {"user:fresh:member", "user:fresh:admin"}},
		},
		{
			name:        "an entry on another resource",
			entries:     []string{"user:fresh:member", "user:other:member"},
			wantPending: map[string][]string{},
		},
		{
			name:        "an entry that does not exist",
			entries:     []string{"user:fresh:member", "user:fresh:owner"},
			wantPending: map[string][]string{},
		},
		{
			name:    "an entry on the principal with a custom id",
			entries: []string{"custom-member"},
			extra: []*v2.Entitlement{func() *v2.Entitlement {
				ent := et.NewAssignmentEntitlement(fresh, "custom")
				ent.SetId("custom-member")
				return ent
			}()},
			wantPending: map[string][]string{},
		},
		{
			name:        "an ambiguous entry",
			entries:     []string{"user:fresh:member"},
			extra:       []*v2.Entitlement{onOther("user:fresh:member")},
			wantPending: map[string][]string{},
		},
		{
			name:        "an entry whose id belongs to another resource",
			entries:     []string{"user:fresh:owner"},
			extra:       []*v2.Entitlement{onOther("user:fresh:owner")},
			wantPending: map[string][]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			group, err := rs.NewGroupResource("g", groupResourceType, "g", nil)
			require.NoError(t, err)
			lookalike := gt.NewGrant(group, "member", fresh.GetId(),
				gt.WithAnnotation(v2.ExternalResourceMatchID_builder{Id: "fresh"}.Build()),
				gt.WithAnnotation(v2.GrantExpandable_builder{EntitlementIds: tc.entries}.Build()),
			)
			rc := expansionReplayCase{
				resourceType:      expandableUserResourceType,
				principals:        []*v2.Resource{fresh, other},
				extraEntitlements: tc.extra,
			}
			s, dst := newExpansionReplaySyncer(t, rc, []*v2.Grant{lookalike})
			for attempt := 1; attempt <= 2; attempt++ {
				require.NoError(t, s.SyncExternalResourcesUsersAndGroups(ctx))
				pending, _ := expansionReplaySnapshot(t, dst)
				require.Equal(t, tc.wantPending, pending, "attempt %d", attempt)
			}
		})
	}
}

func expansionReplayUser(t *testing.T, id string) *v2.Resource {
	t.Helper()
	user, err := rs.NewUserResource(id, expandableUserResourceType, id, nil)
	require.NoError(t, err)
	return user
}

// expansionReplayCarrier builds a connector carrier: a grant of groupID's
// member entitlement to a placeholder principal, whose expansion names the
// placeholder's entitlements by BID (every slug in expansionReplaySlugs unless
// slugs are given).
func expansionReplayCarrier(t *testing.T, groupID, placeholderType, placeholderID string, match proto.Message, slugs ...string) *v2.Grant {
	t.Helper()
	group, err := rs.NewGroupResource(groupID, groupResourceType, groupID, nil)
	require.NoError(t, err)
	placeholder := v2.ResourceId_builder{ResourceType: placeholderType, Resource: placeholderID}.Build()
	if len(slugs) == 0 {
		slugs = expansionReplaySlugs
	}
	var entries []string
	for _, slug := range slugs {
		entBID, err := bid.MakeBid(v2.Entitlement_builder{Resource: v2.Resource_builder{Id: placeholder}.Build(), Slug: slug}.Build())
		require.NoError(t, err)
		entries = append(entries, entBID)
	}
	return gt.NewGrant(group, "member", placeholder,
		gt.WithAnnotation(match),
		gt.WithAnnotation(v2.GrantExpandable_builder{EntitlementIds: entries}.Build()),
	)
}

func newExpansionReplaySyncer(t *testing.T, tc expansionReplayCase, grants []*v2.Grant) (*syncer, c1zstore.Store) {
	t.Helper()
	ctx := t.Context()
	tmp := t.TempDir()

	ext, err := dotc1z.NewStore(ctx, filepath.Join(tmp, "external.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ext.Close(ctx) })
	_, err = ext.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, ext.PutResourceTypes(ctx, tc.resourceType))
	require.NoError(t, ext.PutResources(ctx, tc.principals...))
	for _, principal := range tc.principals {
		for _, slug := range expansionReplaySlugs {
			require.NoError(t, ext.PutEntitlements(ctx, et.NewAssignmentEntitlement(principal, slug)))
		}
	}
	if len(tc.extraEntitlements) > 0 {
		require.NoError(t, ext.PutEntitlements(ctx, tc.extraEntitlements...))
	}
	require.NoError(t, ext.EndSync(ctx))

	dst, err := dotc1z.NewStore(ctx, filepath.Join(tmp, "internal.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close(ctx) })
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	if len(grants) > 0 {
		require.NoError(t, dst.PutGrants(ctx, grants...))
	}

	run := newRunState()
	run.setFact(factHasExternalResourceGrants)
	s := &syncer{run: run, stats: newRunStats(), graph: newExpansionGraph(), externalResourceReader: ext}
	s.setStore(dst)
	return s, dst
}

// expansionReplaySnapshot returns the pending expansion rows by grant id and
// the deterministic wire form of every grant with its expansion annotation.
func expansionReplaySnapshot(t *testing.T, store c1zstore.Store) (map[string][]string, []string) {
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
	var grants []string
	for ga, err := range store.Grants().ListWithAnnotations(ctx) {
		require.NoError(t, err)
		grantWire, err := marshal.Marshal(ga.Grant)
		require.NoError(t, err)
		var expansionWire []byte
		if ga.Annotation != nil {
			expansionWire, err = marshal.Marshal(ga.Annotation)
			require.NoError(t, err)
		}
		grants = append(grants, hex.EncodeToString(grantWire)+"/"+hex.EncodeToString(expansionWire))
	}
	slices.Sort(grants)
	return pending, grants
}
