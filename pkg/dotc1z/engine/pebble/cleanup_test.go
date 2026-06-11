package pebble

import (
	"bytes"
	"context"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Store-level Cleanup behavior (retention policy, dirty bit, env
// skips) is tested in pkg/dotc1z/pebble_store_cleanup_test.go where
// the Pebble store wrapper lives. The tests here cover engine-level
// invariants only.

// TestResetForNewSyncRefusesActiveSync verifies the engine-level
// guard: ResetForNewSync refuses to wipe the keyspace while a sync is
// in progress (between MarkFreshSync and EndFreshSync), which would
// otherwise corrupt the in-flight write path.
func TestResetForNewSyncRefusesActiveSync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := e.ResetForNewSync(ctx); err == nil {
		t.Fatal("ResetForNewSync while a sync is active: expected error, got nil")
	}
}

// TestSyncScopedRangesCoverEveryWrittenIndex asserts the cleanup range
// list returned by syncScopedRanges contains every secondary-index key
// the write path can produce. grant_by_entitlement_resource regressed
// out of that list once, leaking its index keys past a sync delete; this
// pins every index keyspace, so a new index added to the writers without
// a matching cleanup range fails here instead of leaking orphan keys.
func TestSyncScopedRangesCoverEveryWrittenIndex(t *testing.T) {
	// Fixed 16-byte sync id; the encoders and *SyncLowerBound bounds
	// only append it, so any consistent value exercises the keyspace.
	syncIDBytes := []byte{
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	}
	ranges := syncScopedRanges(syncIDBytes)

	// One grant carrying an entitlement (with a resource), a principal,
	// and the needs-expansion flag emits all five grant indexes:
	// by_entitlement, by_entitlement_resource, by_principal,
	// by_principal_resource_type, needs_expansion.
	g := v3.GrantRecord_builder{
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "u1",
		}.Build(),
		ExternalId:     "g1",
		NeedsExpansion: true,
	}.Build()

	type writtenKey struct {
		idx  byte
		name string
		key  []byte
	}
	written := make([]writtenKey, 0, 7)
	for _, k := range grantIndexKeys(syncIDBytes, g) {
		// Layout for every index key: versionV3, typeIndex, idxByte, ...
		written = append(written, writtenKey{idx: k[2], name: "grant", key: k})
	}
	written = append(written,
		writtenKey{idxResourceByParent, "resource_by_parent",
			encodeResourceByParentIndexKey(syncIDBytes, "folder", "root", "doc", "d1")},
		writtenKey{idxEntitlementByResource, "entitlement_by_resource",
			encodeEntitlementByResourceIndexKey(syncIDBytes, "app", "github", "ent-A")},
	)

	covered := func(k []byte) bool {
		for _, r := range ranges {
			if bytes.Compare(k, r[0]) >= 0 && bytes.Compare(k, r[1]) < 0 {
				return true
			}
		}
		return false
	}

	seen := map[byte]bool{}
	for _, w := range written {
		seen[w.idx] = true
		if !covered(w.key) {
			t.Errorf("written index key (idx=0x%02x, %s) not covered by any syncScopedRanges entry: %x", w.idx, w.name, w.key)
		}
	}

	// Every secondary index (0x01..0x07) must be exercised above so the
	// coverage assertion is actually complete; a new idx constant that no
	// representative record produces trips this guard.
	for _, idx := range []byte{
		idxResourceByParent,
		idxEntitlementByResource,
		idxGrantByEntitlement,
		idxGrantByPrincipal,
		idxGrantByNeedsExpansion,
		idxGrantByPrincipalResourceType,
		idxGrantByEntitlementResource,
	} {
		if !seen[idx] {
			t.Errorf("index 0x%02x not exercised by this test; add a representative record so the coverage check stays complete", idx)
		}
	}
}
