package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// The spill merges only interleave once a sorter has cut more than one
// chunk, which needs deferredIndexSpillChunkBytes (128MiB) of keys — far
// more than a test wants to write. These tests therefore build the sorted
// runs directly
// with writeSortedSpillChunk, which is what the sorter's background
// goroutine calls, and hand them to the real merges.
//
// TestGrantDigestSpillMerge reaches the same paths from the other side, by
// forcing a 512-byte chunk size through a real digest build, and so also
// covers multi-chunk exhaustion and the k-way interleave (and, since the
// digest merge moved onto spillChunkCursors, release-on-exhaustion). What
// is only covered here is spillChunkCursors itself and the merges the
// digest build does not run.

type spillKV struct{ k, v string }

// writeTestSpillChunk lays entries into an arena the way spillSorter.add
// does and flushes them as one sorted run. Entries must already be in key
// order: writeSortedSpillChunk preserves the order it is given, and the
// sorter is what sorts.
func writeTestSpillChunk(t *testing.T, dir, name string, entries []spillKV) string {
	t.Helper()
	var arena []byte
	views := make([]kvView, 0, len(entries))
	for _, e := range entries {
		off := len(arena)
		arena = append(arena, e.k...)
		arena = append(arena, e.v...)
		views = append(views, kvView{keyOff: off, keyEnd: off + len(e.k), valEnd: off + len(e.k) + len(e.v)})
	}
	path := filepath.Join(dir, name+".bin")
	require.NoError(t, writeSortedSpillChunk(path, arena, views), "writeSortedSpillChunk %s", name)
	return path
}

// ingestAndDump ingests the merged SST and reads every key back. Keys in
// these tests are prefixed 0xFE so they cannot collide with a real family.
func ingestAndDump(t *testing.T, e *Engine, sstPath string) []spillKV {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, e.IngestSSTs(ctx, []string{sstPath}), "IngestSSTs")
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0xFE},
		UpperBound: []byte{0xFF},
	})
	require.NoError(t, err)
	defer iter.Close()
	var got []spillKV
	for iter.First(); iter.Valid(); iter.Next() {
		got = append(got, spillKV{k: string(iter.Key()), v: string(iter.Value())})
	}
	require.NoError(t, iter.Error())
	return got
}

func requireChunksReleased(t *testing.T, chunks []string) {
	t.Helper()
	for _, c := range chunks {
		_, err := os.Stat(c)
		require.Truef(t, os.IsNotExist(err), "chunk %s should have been unlinked by the merge, stat err = %v", c, err)
	}
}

// TestMergeSortedSpillChunksInterleavesAndReleases drives the strict merge
// across chunks that run dry at different points — one empty, one
// single-entry, two staggered — and pins both halves of the contract: the
// SST is the full sorted union, and every chunk is unlinked by the time
// the merge returns. A successful merge also proves the interleave is
// globally ordered on its own, because bulkSSTWriter.add rejects any key
// that is not strictly greater than the last.
func TestMergeSortedSpillChunksInterleavesAndReleases(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	chunkDir := t.TempDir()

	k := func(s string) string { return "\xfe" + s }
	chunks := []string{
		// Empty: exhausts on its very first advance, before the heap is built.
		writeTestSpillChunk(t, chunkDir, "c0", nil),
		writeTestSpillChunk(t, chunkDir, "c1", []spillKV{{k("b"), "vb"}}),
		writeTestSpillChunk(t, chunkDir, "c2", []spillKV{{k("a"), "va"}, {k("d"), "vd"}, {k("g"), "vg"}}),
		// Trailing empty value: the merge maps len 0 to a nil value.
		writeTestSpillChunk(t, chunkDir, "c3", []spillKV{{k("c"), "vc"}, {k("e"), "ve"}, {k("f"), "vf"}, {k("h"), ""}}),
	}

	sstPath := filepath.Join(dir, "merged.sst")
	require.NoError(t, mergeSortedSpillChunksToSST(ctx, e.fs(), sstPath, "merged", chunks))
	requireChunksReleased(t, chunks)

	require.Equal(t, []spillKV{
		{k("a"), "va"}, {k("b"), "vb"}, {k("c"), "vc"}, {k("d"), "vd"},
		{k("e"), "ve"}, {k("f"), "vf"}, {k("g"), "vg"}, {k("h"), ""},
	}, ingestAndDump(t, e, sstPath))
}

// TestMergeSortedSpillChunksRejectsDuplicateAcrossChunks pins that a
// duplicate key is still corruption when the two copies live in different
// runs — which is the only way the k-way path can see one, and a shape a
// single-chunk test cannot produce.
func TestMergeSortedSpillChunksRejectsDuplicateAcrossChunks(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	chunkDir := t.TempDir()

	k := func(s string) string { return "\xfe" + s }
	chunks := []string{
		writeTestSpillChunk(t, chunkDir, "c0", []spillKV{{k("a"), "v1"}, {k("dup"), "v2"}}),
		writeTestSpillChunk(t, chunkDir, "c1", []spillKV{{k("dup"), "v3"}, {k("z"), "v4"}}),
	}

	err := mergeSortedSpillChunksToSST(ctx, e.fs(), filepath.Join(dir, "dup.sst"), "dup", chunks)
	require.ErrorIs(t, err, errBulkImportDuplicateKey)
}

// TestMergeSpillChunksResolvingDuplicatesAcrossChunks pins the
// duplicate-tolerant variant on the same cross-chunk shape: every copy of
// a key reaches resolve, in a group assembled from more than one run, and
// the chunks are still released.
func TestMergeSpillChunksResolvingDuplicatesAcrossChunks(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	chunkDir := t.TempDir()

	k := func(s string) string { return "\xfe" + s }
	chunks := []string{
		writeTestSpillChunk(t, chunkDir, "c0", []spillKV{{k("a"), "a0"}, {k("dup"), "d0"}}),
		writeTestSpillChunk(t, chunkDir, "c1", []spillKV{{k("dup"), "d1"}, {k("z"), "z0"}}),
		writeTestSpillChunk(t, chunkDir, "c2", []spillKV{{k("dup"), "d2"}}),
	}

	sstPath := filepath.Join(dir, "resolved.sst")
	dupGroups, err := mergeSpillChunksToSSTResolvingDuplicates(ctx, e.fs(), sstPath, "resolved", chunks,
		func(_ []byte, values [][]byte) ([]byte, error) {
			parts := make([]string, 0, len(values))
			for _, v := range values {
				parts = append(parts, string(v))
			}
			return []byte(strings.Join(parts, "+")), nil
		})
	require.NoError(t, err)
	require.Equal(t, int64(1), dupGroups)
	requireChunksReleased(t, chunks)

	require.Equal(t, []spillKV{
		{k("a"), "a0"}, {k("dup"), "d0+d1+d2"}, {k("z"), "z0"},
	}, ingestAndDump(t, e, sstPath))
}

// TestMergeGrantPrimaryMigrationChunksFoldsAcrossChunks covers the merge
// with the densest advance pattern: advanceMigrationChunk fires once for a
// duplicate group's leader and again for every same-key follower popped
// inside the inner loop, so one group can drain several chunks. The
// existing TestIDIndexMigrationSemantics drives this merge end to end
// through Open, but with few enough rows to fit a single chunk, so the
// cross-chunk fold was never exercised. It is worth pinning here rather
// than elsewhere because this merge runs on the open-time id-index
// migration, and a row dropped or double-counted by it stays sorted —
// bulkSSTWriter.add would not notice — and is written into the c1z.
//
// The rows are laid out globally sorted and then dealt round-robin, which
// keeps every chunk internally sorted while guaranteeing the duplicate
// group spans three of them.
func TestMergeGrantPrimaryMigrationChunksFoldsAcrossChunks(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	chunkDir := t.TempDir()

	older := timestamppb.New(time.Unix(1000, 0).UTC())
	middle := timestamppb.New(time.Unix(2000, 0).UTC())
	newer := timestamppb.New(time.Unix(3000, 0).UTC())

	mkRow := func(externalID, entResourceID, entID, principalID string, needsExpansion bool, at *timestamppb.Timestamp) (string, []byte) {
		rec := v3.GrantRecord_builder{
			ExternalId: externalID,
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "group", ResourceId: entResourceID, EntitlementId: entID,
			}.Build(),
			Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: principalID}.Build(),
			DiscoveredAt:   at,
			NeedsExpansion: needsExpansion,
		}.Build()
		id, err := grantIdentityFromRecord(rec)
		require.NoError(t, err)
		val, err := marshalRecord(rec)
		require.NoError(t, err)
		return string(encodeGrantIdentityKey(id)), val
	}

	// Three rows with distinct external ids folding to one identity: the
	// merged row must keep the earliest-discovered external id and OR the
	// needs_expansion flags. Plus three singleton identities.
	rows := make([]spillKV, 0, 6)
	var dupKey string
	for _, r := range []struct {
		extID, entRID, entID, principal string
		needsExpansion                  bool
		at                              *timestamppb.Timestamp
	}{
		{"dup-a", "g1", "member", "u1", false, newer},
		{"dup-b", "g1", "member", "u1", false, older},
		{"dup-c", "g1", "member", "u1", true, middle},
		{"solo-u2", "g1", "member", "u2", false, older},
		{"solo-u3", "g1", "member", "u3", true, older},
		{"solo-g2", "g2", "admin", "u1", false, older},
	} {
		k, v := mkRow(r.extID, r.entRID, r.entID, r.principal, r.needsExpansion, r.at)
		if r.extID == "dup-a" {
			dupKey = k
		}
		rows = append(rows, spillKV{k: k, v: string(v)})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].k < rows[j].k })

	dealt := make([][]spillKV, 3)
	for i, r := range rows {
		dealt[i%3] = append(dealt[i%3], r)
	}
	chunks := make([]string, 0, len(dealt))
	var chunksHoldingDup int
	for i, entries := range dealt {
		for _, kv := range entries {
			if kv.k == dupKey {
				chunksHoldingDup++
				break
			}
		}
		chunks = append(chunks, writeTestSpillChunk(t, chunkDir, fmt.Sprintf("grant-primary-%d", i), entries))
	}
	// Everything below only tests the cross-chunk fold if the deal actually
	// split the group; a key-order change upstream could quietly undo that.
	require.Equal(t, 3, chunksHoldingDup, "duplicate group must span all three chunks")

	sem := make(chan struct{}, 2)
	byPrincipal := newSpillSorter(chunkDir, "idx-by-principal", sem, bulkSpillKeyChunkBytes)
	byNeedsExpansion := newSpillSorter(chunkDir, "idx-by-needs-expansion", sem, bulkSpillKeyChunkBytes)

	sstPath := filepath.Join(dir, "grant-primary.sst")
	require.NoError(t, mergeGrantPrimaryMigrationChunksToSST(ctx, e.fs(), sstPath, "grant-primary", chunks, byPrincipal, byNeedsExpansion))
	requireChunksReleased(t, chunks)

	// Close the index sinks the way production does. teardown() documents
	// why this has to happen before the staging dir goes away: a chunk sort
	// racing the removal can re-create a file mid-walk. finalize rather
	// than abort because it cuts the pending arena and so actually
	// dispatches the background sort this waits for — abort would leave
	// these sorters having never spawned one, and the wait would stay a
	// no-op however far the fixture grew.
	idxChunks, err := byPrincipal.finalize()
	require.NoError(t, err)
	require.NotEmpty(t, idxChunks, "finalize must flush the tail chunk")
	_, err = byNeedsExpansion.finalize()
	require.NoError(t, err)

	require.NoError(t, e.IngestSSTs(ctx, []string{sstPath}))
	got := map[string]*v3.GrantRecord{}
	require.NoError(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		got[r.GetExternalId()] = r
		return true
	}))

	// Four identities in, four rows out: the three duplicates folded and
	// nothing was dropped on the way through three chunks.
	require.Len(t, got, 4)
	require.Contains(t, got, "dup-b", "fold must keep the earliest-discovered external id")
	require.NotContains(t, got, "dup-a")
	require.NotContains(t, got, "dup-c")
	require.True(t, got["dup-b"].GetNeedsExpansion(), "needs_expansion ORs across the group, including the follower in a later chunk")
	require.False(t, got["solo-u2"].GetNeedsExpansion())

	// The derived indexes must be emitted per folded identity, not per
	// input row — one entry per duplicate would leave rows dangling against
	// a primary that no longer has them.
	require.Equal(t, int64(4), byPrincipal.count, "by_principal must be one key per folded identity")
	require.Equal(t, int64(2), byNeedsExpansion.count, "by_needs_expansion covers the folded dup group and solo-u3")
}

// TestSpillChunkCursorsAdvancePastExhaustion pins that advancing a drained
// chunk keeps reporting false. The merges only re-push a chunk after a
// true, so none of them reach this today, but releasing the chunk nils the
// reader and the inline readSpillEntry calls this replaced were idempotent
// at EOF — this keeps that true for the merges now sharing the cursor.
func TestSpillChunkCursorsAdvancePastExhaustion(t *testing.T) {
	chunkDir := t.TempDir()
	chunks := []string{
		writeTestSpillChunk(t, chunkDir, "c0", []spillKV{{"k1", "v1"}}),
		writeTestSpillChunk(t, chunkDir, "c1", nil),
	}
	cursors, err := openSpillChunks(chunks)
	require.NoError(t, err)
	defer cursors.closeAll()

	ok, err := cursors.advance(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "k1", string(cursors.key(0)))

	// Draining chunk 0 releases it; chunk 1 was empty and releases on its
	// first advance. Both must keep answering false rather than faulting.
	for _, idx := range []int{0, 1} {
		for range 3 {
			ok, err := cursors.advance(idx)
			require.NoError(t, err)
			require.False(t, ok)
		}
	}
	requireChunksReleased(t, chunks)
}
