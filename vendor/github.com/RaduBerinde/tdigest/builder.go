package tdigest

import (
	"math"
	"math/bits"
)

type Builder struct {
	processed   Merger
	unprocessed []Centroid
}

// MakeBuilder initializes a Builder; delta is the compression factor, which
// determines the size of the digest: the digest has at most 2*Delta centroids,
// with ~1.3*Delta in practice.
func MakeBuilder(delta int) Builder {
	var b Builder
	b.processed.t.delta = delta
	b.processed.t.centroids, b.processed.scratch, b.unprocessed =
		alloc3Bufs(2*delta, 2*delta, 2*unprocessedFactor*delta)
	b.Reset()
	return b
}

// unprocessedFactor is the relative size of accumulated samples before a merge
// and recompress step. The goal of this value is to balance the cost of the
// sort against the cost of evaluating the scale function.
const unprocessedFactor = 2

func (b *Builder) Reset() {
	b.processed.Reset()
	b.unprocessed = b.unprocessed[:0]
}

// Digest returns the merged TDigest. The returned value is only valid until the
// next call to Add(), Merge(), or Reset().
func (b *Builder) Digest() TDigest {
	if len(b.unprocessed) > 0 {
		b.process()
	}
	return b.processed.Digest()
}

// Add adds a value x with a weight w to the distribution.
func (b *Builder) Add(x, w float64) {
	if math.IsNaN(x) || !(w > 0) || math.IsInf(w, 1) {
		return
	}
	b.unprocessed = append(b.unprocessed, Centroid{Mean: x, Weight: w})
	if len(b.unprocessed) == cap(b.unprocessed) {
		b.process()
	}
}

// Merge a digest into the builder. The digest does not need to have the same
// compression factor (delta).
func (b *Builder) Merge(t *TDigest) {
	if len(b.unprocessed) > 0 {
		b.process()
	}
	b.processed.Merge(t)
}

// process merges the processed and unprocessed centroids.
func (b *Builder) process() {
	pdqsort(b.unprocessed, 0, len(b.unprocessed), bits.Len(uint(len(b.unprocessed))))
	b.processed.mergeCentroids(b.unprocessed)
	b.unprocessed = b.unprocessed[:0]
}

// alloc2Bufs allocates two centroid slices of the specified lengths.
func alloc2Bufs(len1, len2 int) ([]Centroid, []Centroid) {
	b := make([]Centroid, len1+len2)
	return b[:len1:len1], b[len1:]
}

// alloc3Bufs allocates three centroid slices of the specified lengths.
func alloc3Bufs(len1, len2, len3 int) ([]Centroid, []Centroid, []Centroid) {
	b := make([]Centroid, len1+len2+len3)
	return b[:len1:len1], b[len1 : len1+len2 : len1+len2], b[len1+len2:]
}
