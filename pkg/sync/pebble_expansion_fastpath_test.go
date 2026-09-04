package sync

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

	adapter := NewExpanderStore(store)

	// Pin the layer seam before running the projection. The write-path
	// assertions below cannot see it: the layered and unlayered synthesized
	// paths feed the same counters, so an adapter built without `layer:`
	// passes them while silently dropping expansion to the per-row path.
	layered, ok := adapter.(expandedGrantLayerStorer)
	require.True(t, ok)
	opened, err := layered.BeginExpandedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, opened, "Pebble must serve a layer session through NewExpanderStore")
	require.NoError(t, layered.AbortExpandedGrantLayer(ctx))

	require.NoError(t, expand.NewExpander(adapter, graph).RunTopologicalMergeProjection(ctx))

	eng, ok := enginepebble.AsEngine(store)
	require.True(t, ok)
	stats := eng.ExpandWritePathStats()
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

	adapter := NewExpanderStore(store)
	require.NoError(t, expand.NewExpander(adapter, graph).RunTopologicalMergeProjection(ctx))

	eng, ok := enginepebble.AsEngine(store)
	require.True(t, ok)
	stats := eng.ExpandWritePathStats()
	require.Greater(t, stats.SynthesizedRows, int64(0), "alice should synthesize")
	require.Greater(t, stats.ExpandedRows, int64(0), "bob base grant should update")
}

// TestPebbleExpanderStoreResolvesLayerCapability pins the grant-layer seam at
// both ends: the resolution site in store_caps.go must find the capability on
// a Pebble store, and NewExpanderStore must carry it into the adapter.
//
// This is a hot-path performance property with no single-run oracle. Dropping
// `layer:` from an expanderStoreAdapter literal compiles, keeps every
// write-path assertion in this file green, and quietly moves Pebble expansion
// from batched layer sessions to the per-row path. The planted violation below
// is the control: it shows what a dropped `layer:` looks like, and therefore
// that the assertions above it would catch one.
func TestPebbleExpanderStoreResolvesLayerCapability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "layer-seam.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer store.Close(ctx)

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// The resolution site itself, independent of the adapter.
	require.NotNil(t, resolveStoreCaps(store).expandedGrantLayer,
		"resolveStoreCaps must find the grant-layer capability on a Pebble store")

	// The wiring: NewExpanderStore resolves at entry, so the adapter it
	// returns serves a real session.
	wired, ok := NewExpanderStore(store).(expandedGrantLayerStorer)
	require.True(t, ok)
	opened, err := wired.BeginExpandedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, opened, "Pebble must serve a layer session through NewExpanderStore")
	require.NoError(t, wired.AbortExpandedGrantLayer(ctx))

	// Planted violation: the same Pebble store, adapter built without layer:.
	unwired := expanderStoreAdapter{store: store}
	openedUnwired, err := unwired.BeginExpandedGrantLayer(ctx)
	require.NoError(t, err)
	require.False(t, openedUnwired,
		"an adapter built without layer: must report no layer support")
	require.ErrorContains(t,
		unwired.AddExpandedGrantLayerContributions(ctx, nil, nil, nil),
		"does not support layer sessions")
}
