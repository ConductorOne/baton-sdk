package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// batchDeleteRouteStore records which delete surface the apply phase of
// processGrantsWithExternalPrincipals reached. The embedded nil Store is
// deliberate: any surface the code path is not supposed to touch panics
// rather than silently succeeding.
type batchDeleteRouteStore struct {
	c1zstore.Store
	grants cleanupScaleGrantStore

	batchDeleted   [][]string
	refsDeleted    []string
	byIDDeleted    []string
	putGrantsSizes []int
}

func (s *batchDeleteRouteStore) Grants() c1zstore.GrantStore { return &s.grants }

// SyncMeta answers the capability resolution setStore performs at attach; see
// legacyPaginatedCheckpointStore.SyncMeta.
func (s *batchDeleteRouteStore) SyncMeta() c1zstore.SyncMeta { return nil }

func (s *batchDeleteRouteStore) PutGrants(_ context.Context, grants ...*v2.Grant) error {
	s.putGrantsSizes = append(s.putGrantsSizes, len(grants))
	return nil
}

func (s *batchDeleteRouteStore) DeleteGrant(_ context.Context, grantID string) error {
	s.byIDDeleted = append(s.byIDDeleted, grantID)
	return nil
}

// idOnlyDeleteStore implements neither optional delete interface — the SQLite
// shape, which must keep working through the plain DeleteGrant loop.
type idOnlyDeleteStore struct{ *batchDeleteRouteStore }

// refsDeleteStore implements only the singular refs delete.
type refsDeleteStore struct{ *batchDeleteRouteStore }

func (s *refsDeleteStore) DeleteGrantByRefs(_ context.Context, grant *v2.Grant) error {
	s.refsDeleted = append(s.refsDeleted, grant.GetId())
	return nil
}

// batchDeleteStore implements both, as the Pebble store does.
type batchDeleteStore struct{ *batchDeleteRouteStore }

func (s *batchDeleteStore) DeleteGrantByRefs(_ context.Context, grant *v2.Grant) error {
	s.refsDeleted = append(s.refsDeleted, grant.GetId())
	return nil
}

func (s *batchDeleteStore) DeleteGrantsByRefs(_ context.Context, grants ...*v2.Grant) error {
	ids := make([]string, 0, len(grants))
	for _, g := range grants {
		ids = append(ids, g.GetId())
	}
	s.batchDeleted = append(s.batchDeleted, ids)
	return nil
}

// newBatchDeleteRouteStore seeds grants carrying an ExternalResourceMatchID
// for a principal that does not exist, which is the "delete the carrier, emit
// nothing" case — every grant lands in the apply phase's delete set. It
// returns the store and the grant ids the apply phase must delete, in order.
func newBatchDeleteRouteStore(size int) (*batchDeleteRouteStore, []string) {
	store := &batchDeleteRouteStore{}
	want := make([]string, 0, size)
	for i := 0; i < size; i++ {
		grant := v2.Grant_builder{
			Id: fmt.Sprintf("grant-%d", i),
			Entitlement: v2.Entitlement_builder{
				Id: fmt.Sprintf("entitlement-%d", i),
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "group",
					Resource:     fmt.Sprintf("group-%d", i),
				}.Build(),
			}.Build(),
			Annotations: annotations.New(v2.ExternalResourceMatchID_builder{
				Id: fmt.Sprintf("no-such-principal-%d", i),
			}.Build()),
		}.Build()
		store.grants.rows = append(store.grants.rows, c1zstore.GrantAnnotation{Grant: grant})
		want = append(want, grant.GetId())
	}
	return store, want
}

func newExternalMatchSyncer(store c1zstore.Store) *syncer {
	st := newState()
	st.SetHasExternalResourcesGrants()
	s := &syncer{state: st}
	s.setStore(store)
	return s
}

// TestProcessGrantsWithExternalPrincipalsDeleteRouting pins the three-way
// routing of the apply phase: batch when the store offers it, singular refs
// when it only offers that, and the plain id delete otherwise. Each case must
// delete exactly the same grants — the batch is an amortization of the loop,
// not a change to what gets deleted.
func TestProcessGrantsWithExternalPrincipalsDeleteRouting(t *testing.T) {
	ctx := t.Context()

	t.Run("batch-capable store uses one batched call", func(t *testing.T) {
		base, want := newBatchDeleteRouteStore(5)
		store := &batchDeleteStore{batchDeleteRouteStore: base}
		require.NoError(t, newExternalMatchSyncer(store).processGrantsWithExternalPrincipals(ctx, nil))

		require.Equal(t, [][]string{want}, base.batchDeleted,
			"every unmatched carrier must be deleted through a single batched call")
		require.Empty(t, base.refsDeleted, "the batch path must not also delete grant-by-grant")
		require.Empty(t, base.byIDDeleted)
	})

	t.Run("refs-only store falls back to the singular loop", func(t *testing.T) {
		base, want := newBatchDeleteRouteStore(5)
		store := &refsDeleteStore{batchDeleteRouteStore: base}
		require.NoError(t, newExternalMatchSyncer(store).processGrantsWithExternalPrincipals(ctx, nil))

		require.Equal(t, want, base.refsDeleted)
		require.Empty(t, base.batchDeleted)
		require.Empty(t, base.byIDDeleted)
	})

	t.Run("store with neither interface falls back to DeleteGrant", func(t *testing.T) {
		base, want := newBatchDeleteRouteStore(5)
		store := &idOnlyDeleteStore{batchDeleteRouteStore: base}
		require.NoError(t, newExternalMatchSyncer(store).processGrantsWithExternalPrincipals(ctx, nil))

		require.Equal(t, want, base.byIDDeleted)
		require.Empty(t, base.batchDeleted)
		require.Empty(t, base.refsDeleted)
	})
}

// TestProcessGrantsWithExternalPrincipalsBatchSkipsReissuedGrants pins that
// the batch path applies the same newGrantIDs filter the loop did: a carrier
// whose id was re-issued as an expanded grant keeps its row.
func TestProcessGrantsWithExternalPrincipalsBatchSkipsReissuedGrants(t *testing.T) {
	ctx := t.Context()

	// A MatchAll carrier whose principal matches produces an expanded grant
	// keyed by the same (principal, entitlement) pair, so the carrier's id is
	// re-issued and it must NOT be deleted.
	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		Annotations: annotations.New(
			&v2.BatonID{},
			&v2.UserTrait{},
		),
	}.Build()
	entitlement := v2.Entitlement_builder{Id: "entitlement-0", Resource: principal}.Build()
	carrier := v2.Grant_builder{
		Id:          "grant-reissued",
		Entitlement: entitlement,
		Principal:   principal,
		Annotations: annotations.New(v2.ExternalResourceMatchAll_builder{
			ResourceType: v2.ResourceType_TRAIT_USER,
		}.Build()),
	}.Build()
	// newGrantForExternalPrincipal derives the expanded grant's id from the
	// (principal, entitlement) pair, so pin the carrier's id to that value.
	carrier.SetId(newGrantForExternalPrincipal(carrier, principal).GetId())

	base := &batchDeleteRouteStore{}
	base.grants.rows = append(base.grants.rows, c1zstore.GrantAnnotation{Grant: carrier})
	store := &batchDeleteStore{batchDeleteRouteStore: base}

	require.NoError(t, newExternalMatchSyncer(store).processGrantsWithExternalPrincipals(ctx, []*v2.Resource{principal}))

	require.Empty(t, base.batchDeleted,
		"a carrier whose id was re-issued as an expanded grant must not be deleted")
	require.Empty(t, base.refsDeleted)
	require.Empty(t, base.byIDDeleted)
	require.Equal(t, []int{1}, base.putGrantsSizes,
		"test premise: exactly one expanded grant was written, so the filter had something to skip")
}
