package pebble

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func newEngine(t *testing.T, name string) (*enginepkg.Engine, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, name)
	e, err := enginepkg.Open(context.Background(), dir)
	require.NoError(t, err, "Open %s", name)
	t.Cleanup(func() { _ = e.Close() })
	return e, dir
}

// grant builds a grant whose entitlement ref carries the SDK-shaped raw id
// ("app:github:"+entID), matching what connectors actually store, so
// bare-id entitlement scans resolve even without entitlement records.
func grant(syncID, externalID, entID, principalID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  "app:github:" + entID,
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     principalID,
		}.Build(),
	}.Build()
}

func countGrants(t *testing.T, e *enginepkg.Engine, syncID string) int {
	t.Helper()
	count := 0
	err := e.IterateGrants(context.Background(), func(*v3.GrantRecord) bool {
		count++
		return true
	})
	require.NoError(t, err, "IterateGrants")
	return count
}

func TestCompactBasicRoundtrip(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	require.NoError(t, src.SetCurrentSync(ctx, syncID))
	require.NoError(t, dst.SetCurrentSync(ctx, syncID))

	// 200 grants in source under syncID.
	for i := 0; i < 200; i++ {
		r := grant(syncID, ksuid.New().String(), "ent-A", ksuid.New().String())
		require.NoError(t, src.PutGrantRecord(ctx, r))
	}

	c, err := NewCompactor(dst, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, c.Compact(ctx, src, syncID), "Compact")

	require.Equal(t, 200, countGrants(t, dst, syncID), "destination grants")
}

func TestCompactReplacesExisting(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	require.NoError(t, src.SetCurrentSync(ctx, syncID))
	require.NoError(t, dst.SetCurrentSync(ctx, syncID))

	// Seed dst with 500 stale grants under syncID.
	for i := 0; i < 500; i++ {
		r := grant(syncID, ksuid.New().String(), "stale-ent", ksuid.New().String())
		require.NoError(t, dst.PutGrantRecord(ctx, r))
	}
	require.Equal(t, 500, countGrants(t, dst, syncID), "seed dst")

	// Put 50 fresh grants into src — none overlap external_id with dst.
	for i := 0; i < 50; i++ {
		r := grant(syncID, ksuid.New().String(), "fresh-ent", ksuid.New().String())
		require.NoError(t, src.PutGrantRecord(ctx, r))
	}

	c, err := NewCompactor(dst, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, c.Compact(ctx, src, syncID), "Compact")

	// After compact: dst should have exactly src's 50 grants, none of
	// the 500 stale ones.
	got := countGrants(t, dst, syncID)
	require.Equal(t, 50, got, "post-compact dst grants")

	// Also verify by_entitlement index — stale-ent should have 0 entries.
	staleCount := 0
	require.NoError(t, dst.IterateGrantsByEntitlement(ctx, "app:github:stale-ent", func(*v3.GrantRecord) bool {
		staleCount++
		return true
	}))
	require.Equal(t, 0, staleCount, "stale-ent index (compact should have excised them)")

	freshCount := 0
	require.NoError(t, dst.IterateGrantsByEntitlement(ctx, "app:github:fresh-ent", func(*v3.GrantRecord) bool {
		freshCount++
		return true
	}))
	require.Equal(t, 50, freshCount, "fresh-ent index")
}

func TestCompactReplacesAllImplementedBuckets(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	for _, e := range []*enginepkg.Engine{src, dst} {
		require.NoError(t, e.SetCurrentSync(ctx, syncID))
	}

	require.NoError(t, dst.PutResourceTypeRecord(ctx, v3.ResourceTypeRecord_builder{
		ExternalId: "rt-stale", DisplayName: "stale",
	}.Build()))
	require.NoError(t, dst.PutResourceRecord(ctx, v3.ResourceRecord_builder{
		ResourceTypeId: "user", ResourceId: "stale-child",
		Parent: v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()))
	require.NoError(t, dst.PutEntitlementRecord(ctx, v3.EntitlementRecord_builder{
		ExternalId: "ent-stale",
		Resource:   v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()))
	require.NoError(t, dst.PutAssetRecord(ctx, v3.AssetRecord_builder{
		SyncId: syncID, ExternalId: "asset-stale", Data: []byte("stale"),
	}.Build()))
	require.NoError(t, dst.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId: syncID, Type: v3.SyncType_SYNC_TYPE_FULL, SyncToken: "stale",
	}.Build()))

	require.NoError(t, src.PutResourceTypeRecord(ctx, v3.ResourceTypeRecord_builder{
		ExternalId: "rt-fresh", DisplayName: "fresh",
	}.Build()))
	require.NoError(t, src.PutResourceRecord(ctx, v3.ResourceRecord_builder{
		ResourceTypeId: "group", ResourceId: "admins",
	}.Build()))
	require.NoError(t, src.PutResourceRecord(ctx, v3.ResourceRecord_builder{
		ResourceTypeId: "user", ResourceId: "fresh-child",
		Parent: v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()))
	require.NoError(t, src.PutEntitlementRecord(ctx, v3.EntitlementRecord_builder{
		ExternalId: "ent-fresh",
		Resource:   v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()))
	require.NoError(t, src.PutAssetRecord(ctx, v3.AssetRecord_builder{
		SyncId: syncID, ExternalId: "asset-fresh", Data: []byte("fresh"),
	}.Build()))
	require.NoError(t, src.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId: syncID, Type: v3.SyncType_SYNC_TYPE_FULL, SyncToken: "fresh",
	}.Build()))

	c, err := NewCompactor(dst, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, c.Compact(ctx, src, syncID), "Compact")

	rtCount := 0
	require.NoError(t, dst.IterateResourceTypes(ctx, func(*v3.ResourceTypeRecord) bool {
		rtCount++
		return true
	}))
	require.Equal(t, 1, rtCount, "resource_types")
	_, err = dst.GetResourceTypeRecord(ctx, "rt-stale")
	require.Error(t, err, "stale resource type survived compaction")

	childCount := 0
	require.NoError(t, dst.IterateResourcesByParent(ctx, "group", "admins", func(r *v3.ResourceRecord) bool {
		require.Equal(t, "fresh-child", r.GetResourceId(), "unexpected child resource")
		childCount++
		return true
	}))
	require.Equal(t, 1, childCount, "child resources")

	entCount := 0
	require.NoError(t, dst.IterateEntitlementsByResource(ctx, "group", "admins", func(r *v3.EntitlementRecord) bool {
		require.Equal(t, "ent-fresh", r.GetExternalId(), "unexpected entitlement")
		entCount++
		return true
	}))
	require.Equal(t, 1, entCount, "entitlements")

	assetCount := 0
	require.NoError(t, dst.IterateAssets(ctx, func(r *v3.AssetRecord) bool {
		require.Equal(t, "asset-fresh", r.GetExternalId(), "unexpected asset")
		require.Equal(t, "fresh", string(r.GetData()), "unexpected asset data")
		assetCount++
		return true
	}))
	require.Equal(t, 1, assetCount, "assets")

	syncRun, err := dst.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.Equal(t, "fresh", syncRun.GetSyncToken(), "sync_run token")
}

// TestCompactReplacesDestData confirms Compact excises the
// destination's existing data and replaces it with the source's view.
// A v3 Pebble engine holds exactly one sync (keys carry no sync_id), so
// there is no "other sync" to isolate — compaction is a full replace of
// the destination keyspace.
func TestCompactReplacesDestData(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()

	// dst starts with 100 grants that the compaction must replace.
	require.NoError(t, dst.SetCurrentSync(ctx, syncID))
	for i := 0; i < 100; i++ {
		r := grant(syncID, ksuid.New().String(), "ent-old", ksuid.New().String())
		require.NoError(t, dst.PutGrantRecord(ctx, r))
	}

	// src has 25 (different) grants.
	require.NoError(t, src.SetCurrentSync(ctx, syncID))
	for i := 0; i < 25; i++ {
		r := grant(syncID, ksuid.New().String(), "ent-new", ksuid.New().String())
		require.NoError(t, src.PutGrantRecord(ctx, r))
	}

	c, err := NewCompactor(dst, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, c.Compact(ctx, src, syncID), "Compact")

	// dst now holds exactly src's 25 grants; the original 100 are gone.
	require.Equal(t, 25, countGrants(t, dst, syncID), "grants in dst after compact")
}

func TestCompactEmptySource(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	require.NoError(t, dst.SetCurrentSync(ctx, syncID))
	// Seed dst with grants we expect to be wiped.
	for i := 0; i < 20; i++ {
		r := grant(syncID, ksuid.New().String(), "ent", ksuid.New().String())
		require.NoError(t, dst.PutGrantRecord(ctx, r))
	}

	c, err := NewCompactor(dst, t.TempDir())
	require.NoError(t, err)
	err = c.Compact(ctx, src, syncID)
	// Even when source is empty we expect destination to be wiped
	// (compact says: "dst should match src under this sync"). The
	// compactor signals "source had nothing" with ErrEmptySync but
	// still performs the DeleteRange.
	require.True(t, errors.Is(err, ErrEmptySync), "expected ErrEmptySync, got %v", err)
	require.Equal(t, 0, countGrants(t, dst, syncID), "dst grants after empty-source compact")
}

func TestCompactBadInputs(t *testing.T) {
	ctx := context.Background()
	dst, _ := newEngine(t, "dst")

	c, err := NewCompactor(dst, t.TempDir())
	require.NoError(t, err)
	// nil source.
	err = c.Compact(ctx, nil, ksuid.New().String())
	require.Error(t, err, "expected error for nil source")
	// empty syncID.
	src, _ := newEngine(t, "src")
	err = c.Compact(ctx, src, "")
	require.Error(t, err, "expected error for empty syncID")
	// NewCompactor with nil base.
	_, err = NewCompactor(nil, "")
	require.Error(t, err, "expected error for nil base")
}
