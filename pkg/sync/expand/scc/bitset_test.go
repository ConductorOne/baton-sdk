package scc

import (
	"sync"
	"testing"
)

func TestBitsetBasicSetTest(t *testing.T) {
	b := newBitset(130) // spans 3 words
	if b.isEmpty() == false {
		t.Fatalf("new bitset should be empty")
	}

	// Set and test a few indices across word boundaries
	indices := []int{0, 1, 63, 64, 65, 129}
	for _, i := range indices {
		b.set(i)
		if !b.test(i) {
			t.Fatalf("expected bit %d to be set", i)
		}
	}
	if b.isEmpty() {
		t.Fatalf("bitset should not be empty after sets")
	}
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
		if !c.test(i) {
			t.Fatalf("AND missing expected bit %d", i)
		}
	}
	for _, i := range []int{0, 3, 100, 127} {
		if c.test(i) {
			t.Fatalf("AND has unexpected bit %d", i)
		}
	}

	u := b1.clone().or(b2)
	for _, i := range []int{0, 2, 3, 64, 100, 127} {
		if !u.test(i) {
			t.Fatalf("OR missing expected bit %d", i)
		}
	}

	d := b1.clone().andNot(c)
	for _, i := range []int{0, 127} {
		if !d.test(i) {
			t.Fatalf("ANDNOT missing expected bit %d", i)
		}
	}
	for _, i := range []int{2, 64} {
		if d.test(i) {
			t.Fatalf("ANDNOT has unexpected bit %d", i)
		}
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
	if len(seen) != len(expected) {
		t.Fatalf("unexpected count: got %d, want %d", len(seen), len(expected))
	}
	for i := range expected {
		if seen[i] != expected[i] {
			t.Fatalf("order mismatch at %d: got %d, want %d", i, seen[i], expected[i])
		}
	}
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
	if setCount != 1 {
		t.Fatalf("expected exactly one set, got %d", setCount)
	}
	if !b.test(idx) {
		t.Fatalf("bit should be set after atomic operations")
	}

	// clearAtomic should clear once and be idempotent
	b.clearAtomic(idx)
	if b.test(idx) {
		t.Fatalf("bit should be cleared")
	}
	b.clearAtomic(idx)
	if b.test(idx) {
		t.Fatalf("bit should remain cleared after second clear")
	}
}
