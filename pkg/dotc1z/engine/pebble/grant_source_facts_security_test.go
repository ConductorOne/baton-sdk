package pebble

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A stored grant value decides the length and order of its source facts.
// Descending order is a quadratic sort's worst case; the bound fails one by a
// wide margin and leaves an O(n log n) sort far below it.
func TestSecurity_SortGrantSourceFactsDescendingInput(t *testing.T) {
	const n = 1 << 16
	srcs := make([]grantSourceFact, n)
	for i := range srcs {
		srcs[i] = grantSourceFact{key: fmt.Appendf(nil, "src-%06d", n-i)}
	}

	start := time.Now()
	got := sortGrantSourceFacts(srcs)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Len(t, got, n)
	require.True(t, slices.IsSortedFunc(got, func(a, b grantSourceFact) int { return bytes.Compare(a.key, b.key) }))
}
