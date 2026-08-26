package pebble

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// The spill merges only interleave once a sorter has cut more than one
// chunk, which needs bulkSpillKeyChunkBytes (8MiB) or
// deferredIndexSpillChunkBytes (128MiB) of keys — more than any test in
// this package drives through them. These tests therefore build the sorted
// runs directly with writeSortedSpillChunk, which is what the sorter's
// background goroutine calls, and hand them to the real merges. That is
// the only coverage of multi-chunk exhaustion, of the k-way interleave
// itself, and of the release-on-exhaustion behavior in spillChunkCursors.

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
