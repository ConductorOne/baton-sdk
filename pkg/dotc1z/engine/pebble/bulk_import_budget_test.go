package pebble

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBulkImportChunkBytesStaysWithinBudget pins the property the chunk
// derivation exists for: across the shard counts callers actually use, the
// simultaneously-live arenas (one per sorter plus one per sort slot) fit
// in bulkImportSortBudgetBytes, except where the fan-in floor deliberately
// wins. Written against len(grantIndexFamilies) rather than a literal so
// adding an index family shrinks the chunks instead of failing the test.
func TestBulkImportChunkBytesStaysWithinBudget(t *testing.T) {
	sortSlots := 4
	for _, shards := range []int{0, 1, 2, 4, 8, 16, 64} {
		chunk := bulkImportChunkBytes(shards, sortSlots)
		liveArenas := 3 + (1+len(grantIndexFamilies))*max(1, shards) + sortSlots
		require.LessOrEqual(t, chunk, deferredIndexSpillChunkBytes, "shards=%d: chunk above the validated ceiling", shards)
		require.GreaterOrEqual(t, chunk, bulkImportMinChunkBytes, "shards=%d: chunk below the fan-in floor", shards)
		if chunk > bulkImportMinChunkBytes {
			require.LessOrEqual(t, chunk*liveArenas, bulkImportSortBudgetBytes,
				"shards=%d: %d live arenas x %d bytes exceeds the budget", shards, liveArenas, chunk)
		}
	}

	// Monotone: more shards never means bigger chunks.
	prev := bulkImportChunkBytes(1, sortSlots)
	for shards := 2; shards <= 32; shards++ {
		cur := bulkImportChunkBytes(shards, sortSlots)
		require.LessOrEqual(t, cur, prev, "shards=%d grew the chunk size", shards)
		prev = cur
	}

	// Non-positive hints size for one shard, not zero (which would divide
	// the budget across the lane-independent sorters alone and overshoot
	// once the first shard opens).
	require.Equal(t, bulkImportChunkBytes(1, sortSlots), bulkImportChunkBytes(0, sortSlots))
	require.Equal(t, bulkImportChunkBytes(1, sortSlots), bulkImportChunkBytes(-3, sortSlots))
}

// TestSpillSorterCutsBeforeArenaOverflow pins that add() cuts a chunk
// before an entry would push the arena past its capacity, so arenas never
// append-grow. Without the pre-check every fresh arena's first cut
// reallocates at ~1.25x with a full copy and the freelist keeps the larger
// buffer; the budget derivation assumes arenas stay at chunkBytes.
func TestSpillSorterCutsBeforeArenaOverflow(t *testing.T) {
	const chunkBytes = 1024
	const entryBytes = 100 // 60-byte key + 40-byte value; ten fit, an eleventh would overflow.
	const entries = 55

	dir := t.TempDir()
	s := newSpillSorter(dir, "budget", make(chan struct{}, 1), chunkBytes)
	s.free = newSpillArenaFreeList(chunkBytes, 2)

	for i := 0; i < entries; i++ {
		key := []byte(fmt.Sprintf("%060d", i))
		val := make([]byte, 40)
		require.NoError(t, s.add(key, val))
		if s.arena != nil {
			require.Equal(t, chunkBytes, cap(s.arena), "arena grew past its allocation after entry %d", i)
			require.LessOrEqual(t, len(s.arena), chunkBytes)
		}
	}

	chunks, err := s.finalize()
	require.NoError(t, err)
	// 55 entries at 10 per full chunk: five full chunks plus a 5-entry tail.
	require.Len(t, chunks, 6)

	var total int
	for _, path := range chunks {
		f, err := os.Open(path)
		require.NoError(t, err)
		r := bufio.NewReader(f)
		var key, val []byte
		var lenBuf [4]byte
		n := 0
		for {
			ok, err := readSpillEntry(r, &key, &val, &lenBuf)
			require.NoError(t, err)
			if !ok {
				break
			}
			require.Len(t, key, 60)
			require.Len(t, val, 40)
			n++
		}
		require.NoError(t, f.Close())
		require.LessOrEqual(t, n*entryBytes, chunkBytes, "chunk %s holds more than one arena's worth", path)
		total += n
	}
	require.Equal(t, entries, total, "every entry must land in exactly one chunk")
}
