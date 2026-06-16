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

// TestStoreExpandedGrantsPreservesExpansion mirrors the SQLite
// conformance test `testStoreExpandedGrantsPreservesExpansionColumns`:
// when the expander writes a previously-expandable grant back to
// storage, the persisted record's Expansion + NeedsExpansion fields
// must NOT be overwritten. The expander rewrites the grant payload
// (e.g. updated display name) but the storage layer is responsible
// for keeping the expansion-metadata side-state intact so subsequent
// PendingExpansion walks still see the grant as expandable until
// the expander explicitly marks it done.
//
// Before this fix, Pebble's StoreExpandedGrants delegated straight
// to PutGrants — which routes through V2GrantToV3 → extractV2Expansion.
// With the GrantExpandable annotation stripped by the expander, the
// translated v3 record had Expansion=nil and NeedsExpansion=false,
// clobbering the side state. SQLite has held parity on this for years
// via `grantUpsertModePreserveExpansion`.
func TestStoreExpandedGrantsPreservesExpansion(t *testing.T) {
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "store_expanded.c1z"), dotc1z.WithEngine(dotc1z.EnginePebble))
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

	// Step 1: seed via PutGrants so the v3 Expansion + NeedsExpansion
	// fields land on the primary record (via the GrantExpandable
	// annotation extraction in V2GrantToV3).
	expandable := v2.GrantExpandable_builder{
		EntitlementIds: []string{"src-ent-1"},
	}.Build()
	annAny, err := anypb.New(expandable)
	require.NoError(t, err)
	seed := v2.Grant_builder{
		Id:          "g-preserve",
		Entitlement: ent,
		Principal:   user,
		Annotations: []*anypb.Any{annAny},
	}.Build()
	require.NoError(t, store.PutGrants(ctx, seed))

	beforeCount := 0
	for _, perr := range store.Grants().PendingExpansion(ctx) {
		require.NoError(t, perr)
		beforeCount++
	}
	require.Equal(t, 1, beforeCount, "seed grant must be in PendingExpansion")

	// Step 2: the expander rewrites the grant WITHOUT the
	// GrantExpandable annotation (matching pkg/sync/expand's
	// real behavior — the annotation is stripped from descendants).
	rewrite := v2.Grant_builder{
		Id:          "g-preserve",
		Entitlement: ent,
		Principal:   user,
	}.Build()
	require.NoError(t, store.Grants().StoreExpandedGrants(ctx, rewrite))

	// Step 3: the grant must STILL be in PendingExpansion. SQLite
	// preserves the expansion column and needs_expansion flag here;
	// Pebble must preserve Expansion + NeedsExpansion + the
	// idxGrantByNeedsExpansion entry.
	afterCount := 0
	for _, perr := range store.Grants().PendingExpansion(ctx) {
		require.NoError(t, perr)
		afterCount++
	}
	require.Equal(t, 1, afterCount, "StoreExpandedGrants must NOT clobber Expansion/NeedsExpansion — PendingExpansion should still see the grant")
}

func TestStoreExpandedGrantsStripsResidualExpandableForNewGrant(t *testing.T) {
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, filepath.Join(t.TempDir(), "store_expanded_new.c1z"), dotc1z.WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	user := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()
	ent := v2.Entitlement_builder{Id: "ent-A", Resource: app}.Build()

	annAny, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{"src-ent-1"},
	}.Build())
	require.NoError(t, err)
	grant := v2.Grant_builder{
		Id:          "g-new-residual-expandable",
		Entitlement: ent,
		Principal:   user,
		Annotations: []*anypb.Any{annAny},
	}.Build()
	require.NoError(t, store.Grants().StoreExpandedGrants(ctx, grant))

	count := 0
	for _, perr := range store.Grants().PendingExpansion(ctx) {
		require.NoError(t, perr)
		count++
	}
	require.Zero(t, count, "StoreExpandedGrants must strip residual GrantExpandable from new expanded grants")
}
