package pebble

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestStoreExpandedGrantsPreservesDiscoveredAt locks in parity with
// SQLite's PreserveExpansion upsert, whose on-conflict update set omits
// discovered_at (EXCLUDED.discovered_at is ignored), so re-storing an
// existing grant keeps its original timestamp.
//
// Before the fix, Pebble's StoreExpandedGrants unconditionally stamped
// discovered_at = now() on every record, so an expander rewrite of a
// grant already on disk would jump its discovered_at forward — a
// silent divergence that feeds the if_newer comparisons and any
// staleness logic. The fix carries prior.DiscoveredAt forward (and
// only stamps now for a genuinely new record).
func TestStoreExpandedGrantsPreservesDiscoveredAt(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Seed a primary GrantRecord with a deterministic, clearly-past
	// discovered_at (and expansion side-state) directly via the engine,
	// bypassing the adapter's now() stamp.
	seeded := timestamppb.New(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))
	seed := v3.GrantRecord_builder{
		ExternalId: "g-1",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "alice",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: true,
		DiscoveredAt:   seeded,
	}.Build()
	require.NoError(t, e.PutGrantRecord(ctx, seed))

	// The expander rewrites the grant (no GrantExpandable annotation —
	// it's been stripped) via the same external_id.
	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()
	rewrite := v2.Grant_builder{
		Id:          "g-1",
		Entitlement: v2.Entitlement_builder{Id: "ent-A", Resource: app}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build(),
	}.Build()
	require.NoError(t, a.Grants().StoreExpandedGrants(ctx, rewrite))

	got, err := e.GetGrantRecord(ctx, canonicalTestGrantID("ent-A", "user", "alice"))
	require.NoError(t, err)
	require.NotNil(t, got.GetDiscoveredAt(), "discovered_at must survive the rewrite")
	require.Equal(t, seeded.AsTime(), got.GetDiscoveredAt().AsTime(),
		"StoreExpandedGrants must preserve the prior discovered_at, not re-stamp it to now")
	// Sanity: the expansion side-state preservation still holds.
	require.True(t, got.GetNeedsExpansion())
	require.NotNil(t, got.GetExpansion())
}

func TestStoreExpandedGrantsBackfillsNilDiscoveredAt(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	seed := v3.GrantRecord_builder{
		ExternalId: "g-nil-discovered-at",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "alice",
		}.Build(),
	}.Build()
	require.Nil(t, seed.GetDiscoveredAt())
	require.NoError(t, e.PutGrantRecord(ctx, seed))

	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()
	rewrite := v2.Grant_builder{
		Id:          "g-nil-discovered-at",
		Entitlement: v2.Entitlement_builder{Id: "ent-A", Resource: app}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build(),
	}.Build()
	require.NoError(t, a.Grants().StoreExpandedGrants(ctx, rewrite))

	got, err := e.GetGrantRecord(ctx, canonicalTestGrantID("ent-A", "user", "alice"))
	require.NoError(t, err)
	require.NotNil(t, got.GetDiscoveredAt(), "StoreExpandedGrants should backfill nil discovered_at on existing grants")
}
