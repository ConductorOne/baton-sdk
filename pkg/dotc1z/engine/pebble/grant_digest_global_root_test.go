package pebble

import (
	"bytes"
	"context"
	"testing"

	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestGrantDigestGlobalRootMatchesFold verifies the whole-file grant
// digest root (the manifest-level summary) equals the XOR/count fold
// of every entitlement's own digest root, independently recomputed via
// the authoritative on-demand fold (ComputeEntitlementBucketDigest) —
// not via the same fold code path that produced the global root.
func TestGrantDigestGlobalRootMatchesFold(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	counts := map[string]int64{"ent-a": 5, "ent-b": 3, "ent-zero": 0}
	for entID, n := range counts {
		putEnt(t, e, ctx, entID)
		grants := make([]*v3.GrantRecord, 0, n)
		for range n {
			grants = append(grants, makeGrant("", ksuid.New().String(), entID, ksuid.New().String()))
		}
		if len(grants) > 0 {
			if err := e.PutGrantRecords(ctx, grants...); err != nil {
				t.Fatalf("PutGrantRecords(%s): %v", entID, err)
			}
		}
	}
	sealGrantDigests(t, e)

	var wantXor [hashLen]byte
	var wantCount int64
	for entID := range counts {
		digest, count, err := e.ComputeEntitlementBucketDigest(ctx, testEntIdentity(entID), DigestBucket{})
		if err != nil {
			t.Fatalf("ComputeEntitlementBucketDigest(%s): %v", entID, err)
		}
		xorInto(wantXor[:], digest)
		wantCount += count
	}

	got, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	if err != nil {
		t.Fatalf("GetGrantDigestGlobalRoot: %v", err)
	}
	if !ok {
		t.Fatal("expected global root present after seal")
	}
	if got.Count != wantCount {
		t.Fatalf("global root count = %d, want %d", got.Count, wantCount)
	}
	if !bytes.Equal(got.Hash, wantXor[:]) {
		t.Fatalf("global root digest = %x, want %x", got.Hash, wantXor[:])
	}
}

// TestGrantDigestGlobalRootEmptySync verifies a sync with no grants at
// all still gets a PRESENT (count 0, zero digest) global root — the
// "digest was built" marker, distinguishing "nothing to diff" from
// "never built" (present-means-exact, digest.go).
func TestGrantDigestGlobalRootEmptySync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	sealGrantDigests(t, e)

	root, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	if err != nil {
		t.Fatalf("GetGrantDigestGlobalRoot: %v", err)
	}
	if !ok {
		t.Fatal("expected global root present even with zero grants")
	}
	if root.Count != 0 {
		t.Fatalf("global root count = %d, want 0", root.Count)
	}
	if !bytes.Equal(root.Hash, zeroDigest[:]) {
		t.Fatalf("global root digest = %x, want zero", root.Hash)
	}
}

// TestGrantDigestGlobalRootDroppedOnInvalidation verifies that
// invalidating any single entitlement's digest (a post-seal
// DeleteGrantRecord) also drops the whole-file global root: the
// aggregate is a fold over every partition, so it goes stale the
// moment any partition changes and must read as "missing —
// recalculate" rather than a silently wrong cached value.
func TestGrantDigestGlobalRootDroppedOnInvalidation(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	seedEntitlement(t, e, "ent-A", []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrant("", "g2", "ent-A", "bob"),
	})

	if _, ok, err := e.GetGrantDigestGlobalRoot(ctx); err != nil || !ok {
		t.Fatalf("global root before delete: ok=%v err=%v", ok, err)
	}

	if err := e.DeleteGrantRecord(ctx, "g1"); err != nil {
		t.Fatalf("DeleteGrantRecord: %v", err)
	}
	if _, ok, err := e.GetGrantDigestGlobalRoot(ctx); err != nil || ok {
		t.Fatalf("global root after delete: ok=%v err=%v, want missing (invalidated)", ok, err)
	}

	// Reseal recalculates it from the surviving primaries.
	sealGrantDigests(t, e)
	root, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	if err != nil || !ok {
		t.Fatalf("global root after reseal: ok=%v err=%v", ok, err)
	}
	if root.Count != 1 {
		t.Fatalf("resealed global root count = %d, want 1", root.Count)
	}
}

// TestManifestGrantDigestRoot verifies BuildManifestWithSyncRuns stamps
// the manifest's GrantDigestRoot from the engine's stored global root,
// byte-identical to reading it directly, with the current ABI version.
func TestManifestGrantDigestRoot(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	seedEntitlement(t, e, "ent-A", []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
	})

	m, err := BuildManifestWithSyncRuns(ctx, e, c1zstore.PayloadEncodingUnspecified)
	if err != nil {
		t.Fatalf("BuildManifestWithSyncRuns: %v", err)
	}
	root := m.GetGrantDigestRoot()
	if root == nil {
		t.Fatal("expected manifest GrantDigestRoot to be present")
	}
	if root.GetAbiVersion() != GrantDigestABIVersion {
		t.Fatalf("manifest abi_version = %d, want %d", root.GetAbiVersion(), GrantDigestABIVersion)
	}
	want, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	if err != nil || !ok {
		t.Fatalf("GetGrantDigestGlobalRoot: ok=%v err=%v", ok, err)
	}
	if root.GetCount() != want.Count {
		t.Fatalf("manifest count = %d, want %d", root.GetCount(), want.Count)
	}
	if !bytes.Equal(root.GetXorDigest(), want.Hash) {
		t.Fatalf("manifest xor_digest = %x, want %x", root.GetXorDigest(), want.Hash)
	}
}

// TestManifestGrantDigestRootAbsentWithoutDigests verifies that a file
// sealed with the digest index disabled — never having built any
// digest state — has no GrantDigestRoot in its manifest: absent means
// "no digest, full read", never "zero grants" (see the manifest proto
// doc comment).
func TestManifestGrantDigestRootAbsentWithoutDigests(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t, WithGrantDigestIndex(false))
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-A")
	if err := e.PutGrantRecords(ctx, makeGrant("", "g1", "ent-A", "alice")); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	if err := e.BuildDeferredGrantIndexes(ctx); err != nil {
		t.Fatalf("BuildDeferredGrantIndexes: %v", err)
	}

	m, err := BuildManifestWithSyncRuns(ctx, e, c1zstore.PayloadEncodingUnspecified)
	if err != nil {
		t.Fatalf("BuildManifestWithSyncRuns: %v", err)
	}
	if m.GetGrantDigestRoot() != nil {
		t.Fatalf("expected absent GrantDigestRoot, got %v", m.GetGrantDigestRoot())
	}
}
