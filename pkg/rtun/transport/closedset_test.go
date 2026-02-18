package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClosedSetBasicPrefixAndRanges(t *testing.T) {
	var cs closedSet

	// Initially everything open except sid 0 considered closed by spec
	require.True(t, cs.IsClosed(0))
	require.False(t, cs.IsClosed(1))

	// Close(1) promotes prefix to 1
	cs.Close(1)
	require.True(t, cs.IsClosed(1))
	require.Equal(t, uint32(1), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Close non-contiguous sid produces a range
	cs.Close(3)
	require.False(t, cs.IsClosed(2))
	require.True(t, cs.IsClosed(3))
	require.Len(t, cs.ranges, 1)
	require.Equal(t, interval{start: 3, end: 3}, cs.ranges[0])

	// Close(2) creates [2,3] which begins at highClosed+1=2, so prefix promotes to 3 and ranges empty
	cs.Close(2)
	require.True(t, cs.IsClosed(2))
	require.Equal(t, uint32(3), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Closing 4 promotes prefix to 4 (no ranges kept)
	cs.Close(4)
	require.True(t, cs.IsClosed(4))
	require.Equal(t, uint32(4), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Closing 5 promotes prefix to 5 (no ranges kept)
	cs.Close(5)
	require.True(t, cs.IsClosed(5))
	require.Equal(t, uint32(5), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Closing 0 ignored; does not change state (highClosed already 5 and no ranges)
	cs.Close(0)
	require.Equal(t, uint32(5), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Closing 6 promotes prefix further
	cs.Close(6)
	require.Equal(t, uint32(6), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Now closing 2..6 already closed does nothing
	for sid := uint32(2); sid <= 6; sid++ {
		cs.Close(sid)
	}
	require.Equal(t, uint32(6), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Close(7) continues extension
	cs.Close(7)
	require.Equal(t, uint32(7), cs.highClosed)
	require.Empty(t, cs.ranges)

	// Close(8) continues extension
	cs.Close(8)
	require.Equal(t, uint32(8), cs.highClosed)
	require.Empty(t, cs.ranges)
}

func TestClosedSetMergeBothSides(t *testing.T) {
	var cs closedSet
	// Create two ranges [10,12] and [14,16], then close 13 to merge both into [10,16]
	cs.Close(11)
	cs.Close(12)
	cs.Close(10)
	require.Equal(t, []interval{{start: 10, end: 12}}, cs.ranges)

	cs.Close(15)
	cs.Close(16)
	cs.Close(14)
	require.Equal(t, []interval{{start: 10, end: 12}, {start: 14, end: 16}}, cs.ranges)

	cs.Close(13)
	require.Equal(t, []interval{{start: 10, end: 16}}, cs.ranges)
}

func TestClosedSetPromotionFromRange(t *testing.T) {
	var cs closedSet
	// Close a distant block [5,7]
	cs.Close(7)
	cs.Close(6)
	cs.Close(5)
	require.Equal(t, uint32(0), cs.highClosed)
	require.Equal(t, []interval{{start: 5, end: 7}}, cs.ranges)

	// Now close 1..4; once 4 closes, the first range starts at 5 which is highClosed+1 => promotion to 7
	cs.Close(1)
	cs.Close(2)
	cs.Close(3)
	require.Equal(t, uint32(3), cs.highClosed)
	cs.Close(4)
	require.Equal(t, uint32(7), cs.highClosed)
	require.Empty(t, cs.ranges)
}

func TestClosedSetIdempotentAndOrderInvariant(t *testing.T) {
	var a, b closedSet
	order1 := []uint32{100, 1, 3, 2, 5, 4}
	order2 := []uint32{1, 2, 3, 4, 5, 100}
	for _, sid := range order1 {
		a.Close(sid)
	}
	for _, sid := range order2 {
		b.Close(sid)
	}
	require.Equal(t, a.highClosed, b.highClosed)
	require.Equal(t, a.ranges, b.ranges)
}

func TestClosedSetLargeValues(t *testing.T) {
	var cs closedSet
	cs.Close(1_000_000)
	cs.Close(1_000_002)
	require.False(t, cs.IsClosed(1))
	require.True(t, cs.IsClosed(1_000_000))
	require.True(t, cs.IsClosed(1_000_002))
	require.False(t, cs.IsClosed(1_000_001))

	// Merge into single interval [1_000_000, 1_000_010]
	for sid := uint32(1_000_001); sid <= 1_000_010; sid++ {
		cs.Close(sid)
	}
	require.Equal(t, []interval{{start: 1_000_000, end: 1_000_010}}, cs.ranges)
}

func TestClosedSetHighClosedQuery(t *testing.T) {
	var cs closedSet
	for sid := uint32(1); sid <= 50; sid++ {
		cs.Close(sid)
	}
	require.Equal(t, uint32(50), cs.highClosed)
	// Queries under prefix are true without ranges
	for sid := uint32(1); sid <= 50; sid++ {
		require.True(t, cs.IsClosed(sid))
	}
	require.Empty(t, cs.ranges)
}
