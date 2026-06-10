package pebble

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func newEngine(t *testing.T, name string) (*enginepkg.Engine, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, name)
	e, err := enginepkg.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("Open %s: %v", name, err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, dir
}

func grant(syncID, externalID, entID, principalID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  entID,
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
	if err := e.IterateGrantsBySync(context.Background(), syncID, func(*v3.GrantRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("IterateGrantsBySync: %v", err)
	}
	return count
}

func TestCompactBasicRoundtrip(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	if err := src.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	if err := dst.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	// 200 grants in source under syncID.
	for i := 0; i < 200; i++ {
		r := grant(syncID, ksuid.New().String(), "ent-A", ksuid.New().String())
		if err := src.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	c, err := NewCompactor(dst, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Compact(ctx, src, syncID); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := countGrants(t, dst, syncID); got != 200 {
		t.Errorf("destination grants: got %d, want 200", got)
	}
}

func TestCompactReplacesExisting(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	if err := src.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	if err := dst.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}

	// Seed dst with 500 stale grants under syncID.
	for i := 0; i < 500; i++ {
		r := grant(syncID, ksuid.New().String(), "stale-ent", ksuid.New().String())
		if err := dst.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if got := countGrants(t, dst, syncID); got != 500 {
		t.Fatalf("seed dst: got %d, want 500", got)
	}

	// Put 50 fresh grants into src — none overlap external_id with dst.
	for i := 0; i < 50; i++ {
		r := grant(syncID, ksuid.New().String(), "fresh-ent", ksuid.New().String())
		if err := src.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	c, err := NewCompactor(dst, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Compact(ctx, src, syncID); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// After compact: dst should have exactly src's 50 grants, none of
	// the 500 stale ones.
	got := countGrants(t, dst, syncID)
	if got != 50 {
		t.Errorf("post-compact dst grants: got %d, want 50", got)
	}

	// Also verify by_entitlement index — stale-ent should have 0 entries.
	staleCount := 0
	if err := dst.IterateGrantsByEntitlement(ctx, syncID, "stale-ent", func(*v3.GrantRecord) bool {
		staleCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if staleCount != 0 {
		t.Errorf("stale-ent index: got %d entries, want 0 (compact should have excised them)", staleCount)
	}

	freshCount := 0
	if err := dst.IterateGrantsByEntitlement(ctx, syncID, "fresh-ent", func(*v3.GrantRecord) bool {
		freshCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if freshCount != 50 {
		t.Errorf("fresh-ent index: got %d, want 50", freshCount)
	}
}

func TestCompactReplacesAllImplementedBuckets(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	for _, e := range []*enginepkg.Engine{src, dst} {
		if err := e.SetCurrentSync(syncID); err != nil {
			t.Fatal(err)
		}
	}

	if err := dst.PutResourceTypeRecord(ctx, v3.ResourceTypeRecord_builder{
		ExternalId: "rt-stale", DisplayName: "stale",
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := dst.PutResourceRecord(ctx, v3.ResourceRecord_builder{
		ResourceTypeId: "user", ResourceId: "stale-child",
		Parent: v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := dst.PutEntitlementRecord(ctx, v3.EntitlementRecord_builder{
		ExternalId: "ent-stale",
		Resource:   v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := dst.PutAssetRecord(ctx, v3.AssetRecord_builder{
		SyncId: syncID, ExternalId: "asset-stale", Data: []byte("stale"),
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := dst.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId: syncID, Type: v3.SyncType_SYNC_TYPE_FULL, SyncToken: "stale",
	}.Build()); err != nil {
		t.Fatal(err)
	}

	if err := src.PutResourceTypeRecord(ctx, v3.ResourceTypeRecord_builder{
		ExternalId: "rt-fresh", DisplayName: "fresh",
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := src.PutResourceRecord(ctx, v3.ResourceRecord_builder{
		ResourceTypeId: "group", ResourceId: "admins",
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := src.PutResourceRecord(ctx, v3.ResourceRecord_builder{
		ResourceTypeId: "user", ResourceId: "fresh-child",
		Parent: v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := src.PutEntitlementRecord(ctx, v3.EntitlementRecord_builder{
		ExternalId: "ent-fresh",
		Resource:   v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "admins"}.Build(),
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := src.PutAssetRecord(ctx, v3.AssetRecord_builder{
		SyncId: syncID, ExternalId: "asset-fresh", Data: []byte("fresh"),
	}.Build()); err != nil {
		t.Fatal(err)
	}
	if err := src.PutSyncRunRecord(ctx, v3.SyncRunRecord_builder{
		SyncId: syncID, Type: v3.SyncType_SYNC_TYPE_FULL, SyncToken: "fresh",
	}.Build()); err != nil {
		t.Fatal(err)
	}

	c, err := NewCompactor(dst, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Compact(ctx, src, syncID); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	rtCount := 0
	if err := dst.IterateResourceTypesBySync(ctx, syncID, func(*v3.ResourceTypeRecord) bool {
		rtCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if rtCount != 1 {
		t.Fatalf("resource_types: got %d, want 1", rtCount)
	}
	if _, err := dst.GetResourceTypeRecord(ctx, syncID, "rt-stale"); err == nil {
		t.Fatal("stale resource type survived compaction")
	}

	childCount := 0
	if err := dst.IterateResourcesByParent(ctx, syncID, "group", "admins", func(r *v3.ResourceRecord) bool {
		if r.GetResourceId() != "fresh-child" {
			t.Fatalf("unexpected child resource %q", r.GetResourceId())
		}
		childCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if childCount != 1 {
		t.Fatalf("child resources: got %d, want 1", childCount)
	}

	entCount := 0
	if err := dst.IterateEntitlementsByResource(ctx, syncID, "group", "admins", func(r *v3.EntitlementRecord) bool {
		if r.GetExternalId() != "ent-fresh" {
			t.Fatalf("unexpected entitlement %q", r.GetExternalId())
		}
		entCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if entCount != 1 {
		t.Fatalf("entitlements: got %d, want 1", entCount)
	}

	assetCount := 0
	if err := dst.IterateAssetsBySync(ctx, syncID, func(r *v3.AssetRecord) bool {
		if r.GetExternalId() != "asset-fresh" || string(r.GetData()) != "fresh" {
			t.Fatalf("unexpected asset %q data=%q", r.GetExternalId(), string(r.GetData()))
		}
		assetCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if assetCount != 1 {
		t.Fatalf("assets: got %d, want 1", assetCount)
	}

	syncRun, err := dst.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		t.Fatal(err)
	}
	if syncRun.GetSyncToken() != "fresh" {
		t.Fatalf("sync_run token: got %q, want fresh", syncRun.GetSyncToken())
	}
}

func TestCompactIsolatesOtherSyncs(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncA := ksuid.New().String()
	syncB := ksuid.New().String()

	// dst holds 100 grants under sync_A and 100 under sync_B.
	if err := dst.SetCurrentSync(syncA); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		r := grant(syncA, ksuid.New().String(), "ent-A", ksuid.New().String())
		if err := dst.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.SetCurrentSync(syncB); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		r := grant(syncB, ksuid.New().String(), "ent-B", ksuid.New().String())
		if err := dst.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	// src has 25 grants under sync_A only.
	if err := src.SetCurrentSync(syncA); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		r := grant(syncA, ksuid.New().String(), "ent-A-new", ksuid.New().String())
		if err := src.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	c, err := NewCompactor(dst, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Compact(ctx, src, syncA); err != nil {
		t.Fatalf("Compact sync_A: %v", err)
	}

	// sync_A in dst: 25 (src's). sync_B in dst: 100 (untouched).
	if got := countGrants(t, dst, syncA); got != 25 {
		t.Errorf("sync_A grants in dst: got %d, want 25", got)
	}
	if got := countGrants(t, dst, syncB); got != 100 {
		t.Errorf("sync_B grants in dst: got %d, want 100 — compaction leaked into another sync", got)
	}
}

func TestCompactEmptySource(t *testing.T) {
	ctx := context.Background()

	src, _ := newEngine(t, "src")
	dst, _ := newEngine(t, "dst")

	syncID := ksuid.New().String()
	if err := dst.SetCurrentSync(syncID); err != nil {
		t.Fatal(err)
	}
	// Seed dst with grants we expect to be wiped.
	for i := 0; i < 20; i++ {
		r := grant(syncID, ksuid.New().String(), "ent", ksuid.New().String())
		if err := dst.PutGrantRecord(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	c, err := NewCompactor(dst, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = c.Compact(ctx, src, syncID)
	// Even when source is empty we expect destination to be wiped
	// (compact says: "dst should match src under this sync"). The
	// compactor signals "source had nothing" with ErrEmptySync but
	// still performs the DeleteRange.
	if !errors.Is(err, ErrEmptySync) {
		t.Errorf("expected ErrEmptySync, got %v", err)
	}
	if got := countGrants(t, dst, syncID); got != 0 {
		t.Errorf("dst grants after empty-source compact: got %d, want 0", got)
	}
}

func TestCompactBadInputs(t *testing.T) {
	ctx := context.Background()
	dst, _ := newEngine(t, "dst")

	c, err := NewCompactor(dst, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// nil source.
	if err := c.Compact(ctx, nil, ksuid.New().String()); err == nil {
		t.Error("expected error for nil source")
	}
	// empty syncID.
	src, _ := newEngine(t, "src")
	if err := c.Compact(ctx, src, ""); err == nil {
		t.Error("expected error for empty syncID")
	}
	// NewCompactor with nil base.
	if _, err := NewCompactor(nil, ""); err == nil {
		t.Error("expected error for nil base")
	}
}
