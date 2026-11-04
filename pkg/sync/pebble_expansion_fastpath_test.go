package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepebble "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/stretchr/testify/require"
)

func TestPebbleExpansionUsesSynthesizedFastPath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fastpath.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer store.Close(ctx)

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "org"}.Build(),
	}.Build()
	alice := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	bob := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
	}.Build()
	entSource := v2.Entitlement_builder{Id: "ent:source", Resource: group}.Build()
	entDest := v2.Entitlement_builder{Id: "ent:dest", Resource: group}.Build()

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	))
	require.NoError(t, store.PutResources(ctx, group, alice, bob))
	require.NoError(t, store.PutEntitlements(ctx, entSource, entDest))
	require.NoError(t, store.PutGrants(ctx,
		v2.Grant_builder{Id: "grant:alice:source", Entitlement: entSource, Principal: alice}.Build(),
		v2.Grant_builder{Id: "grant:bob:dest", Entitlement: entDest, Principal: bob}.Build(),
	))

	graph := expand.NewEntitlementGraph(ctx)
	// Graph node ids are the connector's raw entitlement ids.
	srcID := "ent:source"
	dstID := "ent:dest"
	graph.AddEntitlementID(srcID)
	graph.AddEntitlementID(dstID)
	require.NoError(t, graph.AddEdge(ctx, srcID, dstID, false, []string{"user"}))

	adapter, err := NewExpanderStore(store)
	require.NoError(t, err)

	// Pin the layer seam before running the projection. The write-path
	// assertions below cannot see it: the layered and unlayered synthesized
	// paths feed the same counters.
	layered, ok := adapter.(expandedGrantLayerStorer)
	require.True(t, ok)
	opened, err := layered.BeginExpandedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, opened, "Pebble must serve a layer session through NewExpanderStore")
	require.NoError(t, layered.AbortExpandedGrantLayer(ctx))

	require.NoError(t, expand.NewExpander(adapter, graph).RunTopologicalMergeProjection(ctx))

	eng, ok := enginepebble.AsEngine(store)
	require.True(t, ok)
	stats := eng.Expansion().ExpandWritePathStats()
	require.Greater(t, stats.SynthesizedRows, int64(0), "expected synthesized fast path rows")
	require.Greater(t, stats.SynthesizedCalls, int64(0), "expected synthesized fast path calls")
	require.Equal(t, int64(0), stats.ExpandedRows, "no base-update rows expected in this fixture")
}

func TestPebbleExpansionSplitsSynthesizedAndUpdatePaths(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "split-fastpath.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer store.Close(ctx)

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "org"}.Build()}.Build()
	alice := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()}.Build()
	bob := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build()}.Build()
	entSource := v2.Entitlement_builder{Id: "ent:source", Resource: group}.Build()
	entDest := v2.Entitlement_builder{Id: "ent:dest", Resource: group}.Build()

	require.NoError(t, store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build(), v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, store.PutResources(ctx, group, alice, bob))
	require.NoError(t, store.PutEntitlements(ctx, entSource, entDest))
	require.NoError(t, store.PutGrants(ctx,
		v2.Grant_builder{Id: "grant:alice:source", Entitlement: entSource, Principal: alice}.Build(),
		v2.Grant_builder{Id: "grant:bob:source", Entitlement: entSource, Principal: bob}.Build(),
		v2.Grant_builder{Id: "grant:bob:dest", Entitlement: entDest, Principal: bob}.Build(),
	))

	graph := expand.NewEntitlementGraph(ctx)
	// Graph node ids are the connector's raw entitlement ids.
	srcID := "ent:source"
	dstID := "ent:dest"
	graph.AddEntitlementID(srcID)
	graph.AddEntitlementID(dstID)
	require.NoError(t, graph.AddEdge(ctx, srcID, dstID, false, []string{"user"}))

	adapter, err := NewExpanderStore(store)
	require.NoError(t, err)
	require.NoError(t, expand.NewExpander(adapter, graph).RunTopologicalMergeProjection(ctx))

	eng, ok := enginepebble.AsEngine(store)
	require.True(t, ok)
	stats := eng.Expansion().ExpandWritePathStats()
	require.Greater(t, stats.SynthesizedRows, int64(0), "alice should synthesize")
	require.Greater(t, stats.ExpandedRows, int64(0), "bob base grant should update")
}

// TestPebbleExpanderStoreResolvesLayerCapability pins the grant-layer seam:
// a Pebble store implements syncBackend, and NewExpanderStore serves a layer
// session from it.
func TestPebbleExpanderStoreResolvesLayerCapability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "layer-seam.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer store.Close(ctx)

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.Implements(t, (*syncBackend)(nil), store)

	wiredStore, err := NewExpanderStore(store)
	require.NoError(t, err)
	wired, ok := wiredStore.(expandedGrantLayerStorer)
	require.True(t, ok)
	opened, err := wired.BeginExpandedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, opened, "Pebble must serve a layer session through NewExpanderStore")
	require.NoError(t, wired.AbortExpandedGrantLayer(ctx))
}

// TestPebbleSyncerExpanderStoreCarriesLayerCapability pins the production
// construction site. The sync builds its adapter through (*syncer).expanderStore
// from the store setStore attached, and that is the path a real sync's
// expansion runs on.
func TestPebbleSyncerExpanderStoreCarriesLayerCapability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "syncer-layer-seam.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer store.Close(ctx)

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	s := &syncer{}
	s.setStore(store)

	layered, ok := s.expanderStore().(expandedGrantLayerStorer)
	require.True(t, ok)
	opened, err := layered.BeginExpandedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, opened, "the syncer's own expansion adapter must serve a Pebble layer session")
	require.NoError(t, layered.AbortExpandedGrantLayer(ctx))
}
