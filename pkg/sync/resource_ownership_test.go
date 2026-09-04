package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type ownershipGuardStore struct {
	c1zstore.Store
	putCalls int
}

// SyncMeta answers the capability resolution setStore performs at attach; see
// legacyPaginatedCheckpointStore.SyncMeta.
func (s *ownershipGuardStore) SyncMeta() c1zstore.SyncMeta { return nil }

func (s *ownershipGuardStore) PutResources(context.Context, ...*v2.Resource) error {
	s.putCalls++
	return nil
}

func TestPutConnectorResourcesRejectsReservedOwnership(t *testing.T) {
	store := &ownershipGuardStore{}
	syncer := &syncer{}
	syncer.setStore(store)
	reserved := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "connector-owned",
		}.Build(),
		Annotations: annotations.New(&v2.BatonID{}),
	}.Build()
	require.ErrorContains(t,
		syncer.putConnectorResources(t.Context(), reserved),
		"SDK-reserved BatonID ownership annotation",
	)
	require.Zero(t, store.putCalls, "reserved marker must be rejected before any store write")

	malformedReserved := v2.Resource_builder{
		Annotations: annotations.New(&v2.BatonID{}),
	}.Build()
	require.ErrorContains(t,
		syncer.putConnectorResources(t.Context(), malformedReserved),
		"SDK-reserved BatonID ownership annotation",
	)
	require.Zero(t, store.putCalls, "reserved ownership must remain fatal even when identity is missing")

	clean := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "clean",
		}.Build(),
	}.Build()
	require.NoError(t, syncer.putConnectorResources(t.Context(), clean))
	require.Equal(t, 1, store.putCalls)
}
