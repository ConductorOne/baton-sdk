package scc

import (
	"math/bits"
	"sync/atomic"
)

// bitset is a packed, atomically updatable bitset.
//
// Concurrency notes:
//   - Only testAndSetAtomic and clearAtomic are safe concurrently.
//   - All other methods must not race with writers.
//   - Slice storage aligns on 64-bit boundaries for atomic ops.
type bitset struct{ w []uint64 }

func newBitset(n int) *bitset {
	if n <= 0 {
		return &bitset{}
	}
	return &bitset{w: make([]uint64, (n+63)>>6)}
}

func (b *bitset) test(i int) bool {
	if i < 0 {
		return false
	}
	w := i >> 6
	if w >= len(b.w) {
		return false
	}
	return (b.w[w] & (1 << (uint(i) & 63))) != 0
}

// set sets the bit at index i.
func (b *bitset) set(i int) {
	if i < 0 {
		return
	}
	w := i >> 6
	if w >= len(b.w) {
		return
	}
	b.w[w] |= 1 << (uint(i) & 63)
}

// testAndSetAtomic sets the bit at index i and returns true if the bit was already set, false otherwise.
func (b *bitset) testAndSetAtomic(i int) bool {
	if i < 0 {
		return false
	}
	w := i >> 6
	if w >= len(b.w) {
		return false
	}
	mask := uint64(1) << (uint(i) & 63)
	addr := &b.w[w]
	for {
		old := atomic.LoadUint64(addr)
		if old&mask != 0 {
			return true
		}
		if atomic.CompareAndSwapUint64(addr, old, old|mask) {
			return false
		}
	}
}

func (b *bitset) clearAtomic(i int) {
	if i < 0 {
		return
	}
	w := i >> 6
	if w >= len(b.w) {
		return
	}
	mask := ^(uint64(1) << (uint(i) & 63))
	addr := &b.w[w]
	for {
		old := atomic.LoadUint64(addr)
		if atomic.CompareAndSwapUint64(addr, old, old&mask) {
			return
		}
	}
}

func (b *bitset) clone() *bitset {
	cp := &bitset{w: make([]uint64, len(b.w))}
	copy(cp.w, b.w)
	return cp
}

func (b *bitset) and(x *bitset) *bitset {
	for i := range b.w {
		b.w[i] &= x.w[i]
	}
	return b
}

func (b *bitset) or(x *bitset) *bitset {
	for i := range b.w {
		b.w[i] |= x.w[i]
	}
	return b
}

func (b *bitset) andNot(x *bitset) *bitset {
	for i := range b.w {
		b.w[i] &^= x.w[i]
	}
	return b
}

func (b *bitset) isEmpty() bool {
	for _, w := range b.w {
		if w != 0 {
			return false
		}
	}
	return true
}

func (b *bitset) forEachSet(fn func(i int)) {
	for wi, w := range b.w {
		for w != 0 {
			tz := bits.TrailingZeros64(w)
			i := (wi << 6) + tz
			fn(i)
			w &^= 1 << uint(tz) //nolint:gosec // trailing zeros is non-negative
		}
	}
}
