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

// SyncMeta answers the capability resolution setStore performs at attach; see
// legacyPaginatedCheckpointStore.SyncMeta.
func (s *cleanupScaleStore) SyncMeta() c1zstore.SyncMeta { return nil }

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

// DeleteGrantsByRefs counts per GRANT, not per batch, so the scale assertion
// below (grantDeletes == number of stale grants) means the same thing on
// either delete route.
//
// deleteStaleExternalPrincipals — the only caller this double is wired to —
// still deletes stale grants one at a time and never asserts
// grantsByRefsBatchDeleter, so this method is not reached today. It exists so
// that if that cleanup is batched the way processGrantsWithExternalPrincipals
// already was, the double keeps measuring grant deletions rather than
// silently collapsing them to one.
func (s *cleanupScaleStore) DeleteGrantsByRefs(_ context.Context, grants ...*v2.Grant) error {
	s.grantDeletes += len(grants)
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

			syncer := &syncer{}
			syncer.setStore(store)
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
