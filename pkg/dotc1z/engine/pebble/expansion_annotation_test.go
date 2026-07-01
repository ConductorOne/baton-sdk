package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestExpansionAnnotationRoundtrip closes a reviewer-flagged
// correctness gap: V2GrantToV3 / V3GrantToV2 didn't handle the
// GrantExpandable annotation. Connectors that wrote chained-
// membership grants with a GrantExpandable annotation would
// silently lose the annotation in v3 — NeedsExpansion stayed
// false, idxGrantByNeedsExpansion stayed empty, and
// PendingExpansion returned zero grants, breaking the
// syncer.ExpandGrants pipeline.
//
// The fix extracts the annotation at write time into the v3
// Expansion field, sets NeedsExpansion=true, and strips it
// from Annotations (matching SQLite's extractAndStripExpansion).
// On read, V3GrantToV2 re-attaches the annotation.
func TestExpansionAnnotationRoundtrip(t *testing.T) {
	ctx := context.Background()

	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "exp.c1z"), dotc1z.WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	user := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	require.NoError(t, store.PutResources(ctx, user))

	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()
	ent := v2.Entitlement_builder{Id: "ent-A", Resource: app}.Build()
	require.NoError(t, store.PutEntitlements(ctx, ent))

	expandable := v2.GrantExpandable_builder{
		EntitlementIds:  []string{"src-ent-1", "src-ent-2"},
		Shallow:         true,
		ResourceTypeIds: []string{"role"},
	}.Build()
	annAny, err := anypb.New(expandable)
	require.NoError(t, err)
	grant := v2.Grant_builder{
		Id:          "g1",
		Entitlement: ent,
		Principal:   user,
		Annotations: []*anypb.Any{annAny},
	}.Build()
	require.NoError(t, store.PutGrants(ctx, grant))

	t.Run("PendingExpansion sees expandable grant", func(t *testing.T) {
		count := 0
		var seen dotc1z.PendingExpansion
		for pe, perr := range store.Grants().PendingExpansion(ctx) {
			require.NoError(t, perr)
			count++
			seen = pe
		}
		require.Equal(t, 1, count)
		require.Equal(t, "g1", seen.GrantExternalID)
		require.Equal(t, "app:github:custom:ent-A", seen.TargetEntitlementID)
		require.True(t, seen.NeedsExpansion)
		require.NotNil(t, seen.Annotation)
		require.Equal(t, []string{"user:alice:custom:src-ent-1", "user:alice:custom:src-ent-2"}, seen.Annotation.GetEntitlementIds())
		require.True(t, seen.Annotation.GetShallow())
		require.Equal(t, []string{"role"}, seen.Annotation.GetResourceTypeIds())
	})

	t.Run("ListGrants re-attaches the annotation", func(t *testing.T) {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize: 100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1)
		gotGrant := resp.GetList()[0]
		var foundExp bool
		for _, a := range gotGrant.GetAnnotations() {
			ge := &v2.GrantExpandable{}
			if a.MessageIs(ge) && a.UnmarshalTo(ge) == nil {
				foundExp = true
				require.Equal(t, []string{"src-ent-1", "src-ent-2"}, ge.GetEntitlementIds())
				require.True(t, ge.GetShallow())
			}
		}
		require.True(t, foundExp, "GrantExpandable annotation should be re-attached on read")
	})

	t.Run("non-expandable grant doesn't appear in PendingExpansion", func(t *testing.T) {
		bob := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
		}.Build()
		require.NoError(t, store.PutResources(ctx, bob))
		plainGrant := v2.Grant_builder{
			Id:          "g2",
			Entitlement: ent,
			Principal:   bob,
		}.Build()
		require.NoError(t, store.PutGrants(ctx, plainGrant))
		count := 0
		for _, perr := range store.Grants().PendingExpansion(ctx) {
			require.NoError(t, perr)
			count++
		}
		require.Equal(t, 1, count, "PendingExpansion must skip non-expandable grants")
	})
}
