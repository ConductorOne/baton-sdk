package tdigest

import "math"

type Merger struct {
	t       TDigest
	scratch []Centroid
}

// MakeMerger initializes a Merger; delta is the compression factor, which
// determines the size of the digest: the digest has at most 2*Delta centroids,
// with ~1.3*Delta in practice.
func MakeMerger(delta int) Merger {
	var m Merger
	m.t.delta = delta
	m.t.centroids, m.scratch = alloc2Bufs(2*delta, 2*delta)
	m.Reset()
	return m
}

// Reset resets the Merger to an empty state.
func (m *Merger) Reset() {
	m.t.centroids = m.t.centroids[:0]
	m.t.min = math.MaxFloat64
	m.t.max = -math.MaxFloat64
	m.t.totalWeight = 0
}

// Digest returns the merged TDigest. The returned value is only valid until the
// next call to Merge() or Reset().
func (m *Merger) Digest() TDigest {
	return m.t
}

// Merge a digest into the merger. The digest does not need to have the same
// compression factor (delta).
func (m *Merger) Merge(t *TDigest) {
	if len(m.t.centroids) == 0 && m.t.delta == t.delta {
		m.t.centroids = append(m.t.centroids, t.centroids...)
		m.t.min = t.min
		m.t.max = t.max
		m.t.totalWeight = t.totalWeight
		return
	}
	m.mergeCentroids(t.centroids)
}

func (m *Merger) mergeCentroids(centroids []Centroid) {
	if len(centroids) == 0 {
		return
	}
	cm := centroidMerger{a: m.t.centroids, b: centroids}

	m.t.centroids, m.scratch = m.scratch[:0], m.t.centroids[:0]

	centroid, _ := cm.Next()
	m.t.centroids = append(m.t.centroids, centroid)

	for i := range centroids {
		m.t.totalWeight += centroids[i].Weight
	}
	soFar := centroid.Weight
	limit := m.t.totalWeight * integratedQ(m.t.delta, 1.0)
	for {
		centroid, ok := cm.Next()
		if !ok {
			break
		}

		projected := soFar + centroid.Weight
		if projected <= limit {
			soFar = projected
			_ = m.t.centroids[len(m.t.centroids)-1].Add(centroid)
		} else {
			k1 := integratedLocation(m.t.delta, soFar/m.t.totalWeight)
			limit = m.t.totalWeight * integratedQ(m.t.delta, k1+1.0)
			soFar += centroid.Weight
			m.t.centroids = append(m.t.centroids, centroid)
		}
	}
	m.t.min = math.Min(m.t.min, m.t.centroids[0].Mean)
	m.t.max = math.Max(m.t.max, m.t.centroids[len(m.t.centroids)-1].Mean)
}

type centroidMerger struct {
	a, b centroidList
}

func (m *centroidMerger) Next() (Centroid, bool) {
	if len(m.a) > 0 {
		if len(m.b) == 0 || m.a[0].Mean <= m.b[0].Mean {
			c := m.a[0]
			m.a = m.a[1:]
			return c, true
		}
	} else if len(m.b) == 0 {
		return Centroid{}, false
	}
	c := m.b[0]
	m.b = m.b[1:]
	return c, true
}

// -- Scale function note --
//
// The functions below implement a shifted-and-rescaled version of the paper’s
// scale k(q) and its inverse q(k).
//
// This implementation:
//   k(q) = delta * (asin(2q - 1) + pi/2) / pi
//   q(k) = (sin( clamp(k, 0, delta) * pi/delta - pi/2 ) + 1) / 2
//
// Paper’s form (no +pi/2 and divided by 2pi):
//   k_paper(q) = (delta / (2pi)) * asin(2q - 1)
//
// They are the same up to an affine transform of k:
//   k(q)       = 2 * k_paper(q) + delta/2
//   k_paper(q) = (k(q) - delta/2) / 2
//
// Consequences:
//   • Endpoints map nicely here: k(0)=0, k(1)=delta, k(0.5)=delta/2.
//   • The q<->k mapping is identical; only the origin and unit of k differ.
//   • Any Δk computed here is exactly 2x the paper’s Δk. For example, a bound
//     like “Δk ≤ 1” here corresponds to “Δk ≤ 1/2” in the paper’s parameterization.
//   • The clamp in integratedQ keeps k in [0, delta], so q saturates at 0 or 1
//     outside that range.
//
// This difference in normalization doesn’t change behavior; it only affects how
// you compare constants to those stated in the paper.

func integratedQ(delta int, k float64) float64 {
	return 0.5 * (math.Sin(min(k, float64(delta))*math.Pi/float64(delta)-math.Pi*0.5) + 1.0)
}

func integratedLocation(delta int, q float64) float64 {
	return float64(delta) * (math.Asin(2.0*q-1.0) + math.Pi*0.5) / math.Pi
}

func weightedAverage(x1, w1, x2, w2 float64) float64 {
	if x1 <= x2 {
		return weightedAverageSorted(x1, w1, x2, w2)
	}
	return weightedAverageSorted(x2, w2, x1, w1)
}

func weightedAverageSorted(x1, w1, x2, w2 float64) float64 {
	x := (x1*w1 + x2*w2) / (w1 + w2)
	return math.Max(x1, math.Min(x, x2))
}
