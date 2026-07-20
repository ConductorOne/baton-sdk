package pebble

import (
	"context"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestPendingExpansionIndexRoundtrip is the correctness regression
// for the by_needs_expansion index keyspace: grants written with
// NeedsExpansion=true must surface in IterateGrantsByNeedsExpansion;
// grants without the flag must not. Mirrors the SQLite partial
// index `WHERE needs_expansion = 1`.
func TestPendingExpansionIndexRoundtrip(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoErrorf(t, e.MarkFreshSync(syncID), "MarkFreshSync")

	gPending := v3.GrantRecord_builder{
		ExternalId: "g-pending",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "admins",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: true,
	}.Build()
	gProcessed := v3.GrantRecord_builder{
		ExternalId: "g-processed",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-B",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "users",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-2"},
		}.Build(),
		NeedsExpansion: false,
	}.Build()
	gPlain := v3.GrantRecord_builder{
		ExternalId: "g-plain",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-C",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "alice",
		}.Build(),
	}.Build()

	require.NoErrorf(t, e.PutGrantRecords(ctx, gPending, gProcessed, gPlain), "PutGrantRecords")

	seen := []string{}
	require.NoErrorf(t, e.IterateGrantsByNeedsExpansion(ctx, func(r *v3.GrantRecord) bool {
		seen = append(seen, r.GetExternalId())
		return true
	}), "IterateGrantsByNeedsExpansion")
	require.Equal(t, []string{"g-pending"}, seen, "IterateGrantsByNeedsExpansion")

	// Flip g-pending to NeedsExpansion=false. The index entry must
	// disappear (mirrors the syncer's post-expansion write).
	gPendingDone := v3.GrantRecord_builder{
		ExternalId: "g-pending",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "admins",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: false,
	}.Build()
	require.NoErrorf(t, e.PutGrantRecord(ctx, gPendingDone), "PutGrantRecord (flip)")
	seen = seen[:0]
	require.NoErrorf(t, e.IterateGrantsByNeedsExpansion(ctx, func(r *v3.GrantRecord) bool {
		seen = append(seen, r.GetExternalId())
		return true
	}), "IterateGrantsByNeedsExpansion (post-flip)")
	require.Empty(t, seen, "after flip to NeedsExpansion=false, IterateGrantsByNeedsExpansion")
}

// TestPebbleGrantStorePendingExpansionPage exercises the adapter
// layer: the GrantStore wrapper translates index iteration into
// PendingExpansion rows shaped for the syncer's ExpandGrants
// consumer.
func TestPebbleGrantStorePendingExpansionPage(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync")
	rec := v3.GrantRecord_builder{
		ExternalId: "g1",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "group", ResourceId: "admins",
		}.Build(),
		Expansion: v3.GrantExpandableRecord_builder{
			EntitlementIds: []string{"src-ent-1"},
		}.Build(),
		NeedsExpansion: true,
	}.Build()
	require.NoErrorf(t, a.PutGrantRecord(ctx, rec), "PutGrantRecord")

	rows, _, err := a.Grants().PendingExpansionPage(ctx, "")
	require.NoErrorf(t, err, "PendingExpansionPage")
	require.Len(t, rows, 1, "PendingExpansionPage rows")
	require.Equal(t, "g1", rows[0].GrantExternalID, "PendingExpansion[0].GrantExternalID")
	require.Equal(t, "ent-A", rows[0].TargetEntitlementID, "PendingExpansion[0].TargetEntitlementID")
	require.True(t, rows[0].NeedsExpansion, "PendingExpansion[0].NeedsExpansion")
	require.NotNil(t, rows[0].Annotation, "PendingExpansion[0].Annotation")
	require.Len(t, rows[0].Annotation.GetEntitlementIds(), 1, "PendingExpansion[0].Annotation entitlement ids")
}
