package pebble

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/keys"
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
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync")
	err = e.ResetForNewSync(ctx)
	require.Error(t, err, "ResetForNewSync while a sync is active: expected error, got nil")
}

// TestResetForNewSyncReclaimsDiskImmediately pins the excise-based wipe
// contract: replacing a sync must physically drop the prior sync's
// SSTs from the LSM (manifest-level excise), not merely tombstone them.
// With DeleteRange tombstones the dead bytes would survive until a
// compaction ran — and a short replacement sync could hard-link them
// into the CheckpointTo envelope at save, shipping deleted data.
func TestResetForNewSyncReclaimsDiskImmediately(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	// Sync 1: enough grant data to materialize real SSTs, then EndSync
	// (EndFreshSync flushes the memtable to L0).
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync")
	for i := 0; i < 50; i++ {
		batch := make([]*v3.GrantRecord, 0, 100)
		for j := 0; j < 100; j++ {
			batch = append(batch, v3.GrantRecord_builder{
				ExternalId: fmt.Sprintf("g-%d-%d", i, j),
				Entitlement: v3.EntitlementRef_builder{
					ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "ent-A",
				}.Build(),
				Principal: v3.PrincipalRef_builder{
					ResourceTypeId: "user", ResourceId: fmt.Sprintf("u-%d-%d", i, j),
				}.Build(),
			}.Build())
		}
		require.NoErrorf(t, e.PutGrantRecords(ctx, batch...), "PutGrantRecords")
	}
	require.NoErrorf(t, a.EndSync(ctx), "EndSync")

	dataLo := []byte{versionV3, typeResourceType}
	dataHi := []byte{versionV3, typeEngineMeta}
	before, err := e.DB().EstimateDiskUsage(dataLo, dataHi)
	require.NoErrorf(t, err, "EstimateDiskUsage (before)")
	require.NotZero(t, before, "sanity: expected non-zero on-disk usage for the finished sync's data span")

	// Replacement sync: StartNewSync excises the prior sync's data.
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "StartNewSync (replacement)")
	after, err := e.DB().EstimateDiskUsage(dataLo, dataHi)
	require.NoErrorf(t, err, "EstimateDiskUsage (after)")
	// The excise drops fully-covered SSTs from the manifest, so the
	// data span's estimated usage collapses to (near) zero immediately
	// — no compaction required. Allow a small slop for the replacement
	// sync's own sync-run record landing in the span.
	require.Less(t, after, before/10, "post-reset disk usage = %d, want < %d (10%% of pre-reset %d); excise did not reclaim the prior sync's SSTs", after, before/10, before)
}

// TestSyncScopedRangesCoverEveryWrittenIndex asserts the cleanup range
// list returned by syncScopedRanges contains every secondary-index key
// the write path can produce. grant_by_entitlement_resource regressed
// out of that list once, leaking its index keys past a sync delete; this
// pins every index keyspace, so a new index added to the writers without
// a matching cleanup range fails here instead of leaking orphan keys.
func TestSyncScopedRangesCoverEveryWrittenIndex(t *testing.T) {
	ranges := scopedRanges()

	// One grant carrying an entitlement, a principal, and the needs-expansion flag
	// emits all written grant indexes: skinny by_entitlement, by_principal, and
	// needs_expansion. Folded families are intentionally not written.
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
	for _, k := range grantIndexKeys(g) {
		// Layout for every index key: versionV3, typeIndex, idxByte, ...
		written = append(written, writtenKey{idx: k[2], name: "grant", key: k})
	}
	written = append(written,
		writtenKey{idxResourceByParent, "resource_by_parent",
			keys.EncodeResourceByParentIndexKey("folder", "root", "doc", "d1")},
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
		require.Truef(t, covered(w.key), "written index key (idx=0x%02x, %s) not covered by any syncScopedRanges entry: %x", w.idx, w.name, w.key)
	}

	// Every written secondary index must be exercised above so the coverage
	// assertion is complete; folded/dropped discriminators are intentionally absent.
	for _, idx := range []byte{
		idxResourceByParent,
		idxGrantByPrincipal,
		idxGrantByNeedsExpansion,
	} {
		require.Truef(t, seen[idx], "index 0x%02x not exercised by this test; add a representative record so the coverage check stays complete", idx)
	}
}
