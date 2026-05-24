package tdigest

import (
	"iter"
	"math"
	"slices"
)

// TDigest is a t-digest, produced by a Builder or a Merger.
//
// A TDigest is read-only (none of its methods modify it).
type TDigest struct {
	// delta is the compression factor that was used to generate this digest.
	delta int

	// Centroids, sorted by Mean.
	centroids   []Centroid
	min         float64
	max         float64
	totalWeight float64
}

// Delta returns the compression factor that was used to generate this digest.
func (t *TDigest) Delta() int {
	return t.delta
}

// Clone creates a deep copy of the TDigest.
func (t *TDigest) Clone() TDigest {
	tCopy := *t
	tCopy.centroids = make([]Centroid, len(t.centroids), cap(t.centroids))
	copy(tCopy.centroids, t.centroids)
	return tCopy
}

// Centroids returns an iterator over the centroids in the digest.
func (t *TDigest) Centroids() iter.Seq[Centroid] {
	return slices.Values(t.centroids)
}

// Quantile returns the (approximate) quantile of the distribution. Accepted
// values for q are between 0.0 and 1.0.
func (t *TDigest) Quantile(q float64) float64 {
	if q < 0 || q > 1 || len(t.centroids) == 0 {
		return math.NaN()
	}
	if len(t.centroids) == 1 {
		return t.centroids[0].Mean
	}
	index := q * t.totalWeight
	if index <= t.centroids[0].Weight/2.0 {
		return t.min + 2.0*index/t.centroids[0].Weight*(t.centroids[0].Mean-t.min)
	}

	var sum, cumulative, prevCumulative float64
	for i := range t.centroids {
		cur := t.centroids[i].Weight
		prevCumulative = cumulative
		// cumulative is the sum of all previous weights plus half of this
		// centroid's weight.
		cumulative = sum + cur/2.0
		sum += cur
		if cumulative >= index {
			z1 := index - prevCumulative
			z2 := cumulative - index
			return weightedAverage(t.centroids[i-1].Mean, z2, t.centroids[i].Mean, z1)
		}
	}
	z1 := index - t.totalWeight - t.centroids[len(t.centroids)-1].Weight/2.0
	z2 := (t.centroids[len(t.centroids)-1].Weight / 2.0) - z1
	return weightedAverage(t.centroids[len(t.centroids)-1].Mean, z1, t.max, z2)
}

// CDF returns the cumulative distribution function for a given value x.
func (t *TDigest) CDF(x float64) float64 {
	switch len(t.centroids) {
	case 0:
		return 0.0
	case 1:
		width := t.max - t.min
		if x <= t.min {
			return 0.0
		}
		if x >= t.max {
			return 1.0
		}
		if (x - t.min) <= width {
			// min and max are too close together to do any viable interpolation
			return 0.5
		}
		return (x - t.min) / width
	}

	if x <= t.min {
		return 0.0
	}
	if x >= t.max {
		return 1.0
	}
	m0 := t.centroids[0].Mean
	// Left Tail
	if x <= m0 {
		if m0-t.min > 0 {
			return (x - t.min) / (m0 - t.min) * t.centroids[0].Weight / t.totalWeight / 2.0
		}
		return 0.0
	}

	var sum, cumulative, prevCumulative float64
	for i := range t.centroids {
		cur := t.centroids[i].Weight
		prevCumulative = cumulative
		// cumulative is the sum of all previous weights plus half of this
		// centroid's weight.
		cumulative = sum + cur/2.0
		sum += cur
		if t.centroids[i].Mean > x {
			z1 := x - t.centroids[i-1].Mean
			z2 := t.centroids[i].Mean - x
			return weightedAverage(prevCumulative, z2, cumulative, z1) / t.totalWeight
		}
	}

	// Right Tail
	mn := t.centroids[len(t.centroids)-1].Mean
	if t.max-mn > 0.0 {
		return 1.0 - (t.max-x)/(t.max-mn)*t.centroids[len(t.centroids)-1].Weight/t.totalWeight/2.0
	}
	return 1.0
}
