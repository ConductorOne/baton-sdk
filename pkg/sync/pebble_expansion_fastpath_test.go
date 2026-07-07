package sync

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepebble "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/stretchr/testify/require"
)

func TestPebbleExpansionUsesSynthesizedFastPath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fastpath.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
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

	adapter := expanderStoreAdapter{store: store}
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
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
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

	adapter := expanderStoreAdapter{store: store}
	require.NoError(t, expand.NewExpander(adapter, graph).RunTopologicalMergeProjection(ctx))

	eng, ok := enginepebble.AsEngine(store)
	require.True(t, ok)
	stats := eng.ExpandWritePathStats()
	require.Greater(t, stats.SynthesizedRows, int64(0), "alice should synthesize")
	require.Greater(t, stats.ExpandedRows, int64(0), "bob base grant should update")
}
