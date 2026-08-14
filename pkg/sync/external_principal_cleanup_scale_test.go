package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"fmt"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type cleanupScaleGrantStore struct {
	c1zstore.GrantStore
	rows  []c1zstore.GrantAnnotation
	scans int
}

func (s *cleanupScaleGrantStore) ListWithAnnotations(context.Context) iter.Seq2[c1zstore.GrantAnnotation, error] {
	s.scans++
	return func(yield func(c1zstore.GrantAnnotation, error) bool) {
		for _, row := range s.rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

type cleanupScaleStore struct {
	c1zstore.Store
	resources []*v2.Resource
	ents      []*v2.Entitlement
	grants    cleanupScaleGrantStore

	resourceScans      int
	entitlementScans   int
	resourceDeletes    int
	entitlementDeletes int
	grantDeletes       int
}

func (s *cleanupScaleStore) ListResources(
	context.Context,
	*v2.ResourcesServiceListResourcesRequest,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	s.resourceScans++
	return v2.ResourcesServiceListResourcesResponse_builder{List: s.resources}.Build(), nil
}

func (s *cleanupScaleStore) ListEntitlements(
	context.Context,
	*v2.EntitlementsServiceListEntitlementsRequest,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	s.entitlementScans++
	return v2.EntitlementsServiceListEntitlementsResponse_builder{List: s.ents}.Build(), nil
}

func (s *cleanupScaleStore) Grants() c1zstore.GrantStore {
	return &s.grants
}

func (s *cleanupScaleStore) DeleteGrantByRefs(context.Context, *v2.Grant) error {
	s.grantDeletes++
	return nil
}

func (s *cleanupScaleStore) DeleteEntitlementByRefs(context.Context, *v2.Entitlement) error {
	s.entitlementDeletes++
	return nil
}

func (s *cleanupScaleStore) DeleteResourceRecord(context.Context, string, string) error {
	s.resourceDeletes++
	return nil
}

// cleanupNoRefsStore presents the shape SQLite does: none of the optional
// refs-based deleters, only the id-based DeleteGrant every store implements.
// The embedded c1zstore.Store is left nil deliberately -- any call this test
// does not expect panics rather than quietly succeeding.
type cleanupNoRefsStore struct {
	c1zstore.Store
	resources []*v2.Resource
	grants    cleanupScaleGrantStore

	resourceScans    int
	entitlementScans int
	deletedGrantIDs  []string
}

func (s *cleanupNoRefsStore) ListResources(
	context.Context,
	*v2.ResourcesServiceListResourcesRequest,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	s.resourceScans++
	return v2.ResourcesServiceListResourcesResponse_builder{List: s.resources}.Build(), nil
}

func (s *cleanupNoRefsStore) ListEntitlements(
	context.Context,
	*v2.EntitlementsServiceListEntitlementsRequest,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	s.entitlementScans++
	return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
}

func (s *cleanupNoRefsStore) Grants() c1zstore.GrantStore {
	return &s.grants
}

func (s *cleanupNoRefsStore) DeleteGrant(_ context.Context, grantID string) error {
	s.deletedGrantIDs = append(s.deletedGrantIDs, grantID)
	return nil
}

// TestExternalPrincipalCleanupFallsBackToIDDeleteWithoutRefsDeleters covers the
// engine that actually matters for this reconciliation. deleteStaleExternalPrincipals
// used to require resourceRecordDeleter + entitlementRecordDeleter +
// grantByRefsDeleter all at once and return early without them, and only Pebble
// implements any of the three -- so on SQLite, the default engine, the pass did
// nothing at all and a grant pointing at a departed external principal stayed
// live. Grant cleanup now degrades to the id-based DeleteGrant instead of
// skipping, because a stale grant is real access while a stale resource or
// entitlement row is inert metadata.
//
// The end-to-end tests in stale_external_principal_test.go cannot pin this:
// there, the resolved replacement grant still carries its
// ExternalResourceMatch* annotation, so processGrantsWithExternalPrincipals's
// own scan revokes it first and the sync comes out clean whether or not this
// fallback exists.
func TestExternalPrincipalCleanupFallsBackToIDDeleteWithoutRefsDeleters(t *testing.T) {
	const size = 3
	store := &cleanupNoRefsStore{}
	wantDeleted := make([]string, 0, size)
	for i := range size {
		principal := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: fmt.Sprintf("external-type-%d", i),
				Resource:     fmt.Sprintf("external-resource-%d", i),
			}.Build(),
			Annotations: annotations.New(&v2.BatonID{}),
		}.Build()
		grant := v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: v2.Entitlement_builder{Id: fmt.Sprintf("entitlement-%d", i)}.Build(),
			Principal:   principal,
		}.Build()
		store.resources = append(store.resources, principal)
		store.grants.rows = append(store.grants.rows, c1zstore.GrantAnnotation{Grant: grant})
		wantDeleted = append(wantDeleted, grant.GetId())
	}

	// nil current: every stored external principal is stale.
	syncer := &syncer{store: store}
	require.NoError(t, syncer.deleteStaleExternalPrincipals(t.Context(), nil))

	require.Equal(t, wantDeleted, store.deletedGrantIDs,
		"every stale principal's grant must be removed through the id-based fallback, not skipped")
	require.Equal(t, 1, store.resourceScans)
	require.Equal(t, 1, store.grants.scans)
	require.Zero(t, store.entitlementScans,
		"row cleanup is unavailable without the refs deleters, so the entitlement keyspace should not be scanned at all")
}

func TestChaosConnectorExternalPrincipalCleanupUsesOnePassPerKeyspace(t *testing.T) {
	for _, size := range []int{1, 1_000} {
		t.Run(fmt.Sprintf("rows-%d", size), func(t *testing.T) {
			store := &cleanupScaleStore{}
			for i := 0; i < size; i++ {
				principal := v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: fmt.Sprintf("external-type-%d", i),
						Resource:     fmt.Sprintf("external-resource-%d", i),
					}.Build(),
					Annotations: annotations.New(&v2.BatonID{}),
				}.Build()
				entitlement := v2.Entitlement_builder{
					Id:       fmt.Sprintf("entitlement-%d", i),
					Resource: principal,
				}.Build()
				grant := v2.Grant_builder{
					Id:          fmt.Sprintf("grant-%d", i),
					Entitlement: entitlement,
					Principal:   principal,
				}.Build()
				store.resources = append(store.resources, principal)
				store.ents = append(store.ents, entitlement)
				store.grants.rows = append(store.grants.rows, c1zstore.GrantAnnotation{Grant: grant})
			}

			syncer := &syncer{store: store}
			require.NoError(t, syncer.deleteStaleExternalPrincipals(t.Context(), nil))
			require.Equal(t, 1, store.resourceScans)
			require.Equal(t, 1, store.entitlementScans)
			require.Equal(t, 1, store.grants.scans)
			require.Equal(t, size, store.grantDeletes)
			require.Equal(t, size, store.entitlementDeletes)
			require.Equal(t, size, store.resourceDeletes)
		})
	}
}
