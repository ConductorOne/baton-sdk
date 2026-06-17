package scc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitsetBasicSetTest(t *testing.T) {
	b := newBitset(130) // spans 3 words
	require.True(t, b.isEmpty(), "new bitset should be empty")

	// Set and test a few indices across word boundaries
	indices := []int{0, 1, 63, 64, 65, 129}
	for _, i := range indices {
		b.set(i)
		require.True(t, b.test(i), "expected bit %d to be set", i)
	}
	require.False(t, b.isEmpty(), "bitset should not be empty after sets")
}

func TestBitsetCloneAndOps(t *testing.T) {
	b1 := newBitset(128)
	b2 := newBitset(128)
	for _, i := range []int{0, 2, 64, 127} {
		b1.set(i)
	}
	for _, i := range []int{2, 3, 64, 100} {
		b2.set(i)
	}

	c := b1.clone().and(b2)
	for _, i := range []int{2, 64} {
		require.True(t, c.test(i), "AND missing expected bit %d", i)
	}
	for _, i := range []int{0, 3, 100, 127} {
		require.False(t, c.test(i), "AND has unexpected bit %d", i)
	}

	u := b1.clone().or(b2)
	for _, i := range []int{0, 2, 3, 64, 100, 127} {
		require.True(t, u.test(i), "OR missing expected bit %d", i)
	}

	d := b1.clone().andNot(c)
	for _, i := range []int{0, 127} {
		require.True(t, d.test(i), "ANDNOT missing expected bit %d", i)
	}
	for _, i := range []int{2, 64} {
		require.False(t, d.test(i), "ANDNOT has unexpected bit %d", i)
	}
}

func TestBitsetForEachSetOrder(t *testing.T) {
	b := newBitset(70)
	for _, i := range []int{69, 0, 1, 63, 64} {
		b.set(i)
	}

	var seen []int
	b.forEachSet(func(i int) { seen = append(seen, i) })
	expected := []int{0, 1, 63, 64, 69}
	require.Len(t, seen, len(expected), "unexpected count")
	require.Equal(t, expected, seen, "order mismatch")
}

func TestBitsetAtomicOps(t *testing.T) {
	b := newBitset(128)
	// Concurrent testAndSetAtomic should set each bit exactly once
	var wg sync.WaitGroup
	N := 1000
	idx := 73 // arbitrary
	wg.Add(N)
	setCount := 0
	var mu sync.Mutex
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			if !b.testAndSetAtomic(idx) {
				mu.Lock()
				setCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, setCount, "expected exactly one set")
	require.True(t, b.test(idx), "bit should be set after atomic operations")

	// clearAtomic should clear once and be idempotent
	b.clearAtomic(idx)
	require.False(t, b.test(idx), "bit should be cleared")
	b.clearAtomic(idx)
	require.False(t, b.test(idx), "bit should remain cleared after second clear")
}
