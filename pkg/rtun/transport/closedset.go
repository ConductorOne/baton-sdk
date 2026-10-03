package transport

// closedSet maintains a compact set of closed SIDs using a run-length encoded
// interval list and a high-water mark for the largest contiguous closed prefix
// starting from 1. It is package-local and not concurrency-safe; callers should
// provide external synchronization.
//
// Invariants:
//   - cs.ranges is sorted by start and contains disjoint, non-adjacent intervals
//     with start <= end.
//   - All SIDs in [1..highClosed] are considered closed and not represented in
//     cs.ranges (auto-pruned on each Close call).
type closedSet struct {
	highClosed uint32
	ranges     []interval
}

type interval struct {
	start uint32
	end   uint32
}

// IsClosed returns true if sid has been marked closed.
func (cs *closedSet) IsClosed(sid uint32) bool {
	if sid == 0 {
		return true
	}
	if sid <= cs.highClosed {
		return true
	}
	i := cs.find(sid)
	if i < len(cs.ranges) {
		iv := cs.ranges[i]
		if sid >= iv.start && sid <= iv.end {
			return true
		}
	}
	if i > 0 {
		iv := cs.ranges[i-1]
		return sid >= iv.start && sid <= iv.end
	}
	return false
}

// Close marks sid as closed. This merges adjacent/overlapping intervals and
// auto-prunes intervals that become part of the contiguous closed prefix.
func (cs *closedSet) Close(sid uint32) {
	if sid == 0 {
		return
	}
	// If already in the contiguous prefix, nothing to do.
	if sid <= cs.highClosed {
		return
	}

	i := cs.find(sid)
	// Check if already closed by neighboring interval
	if i < len(cs.ranges) {
		iv := cs.ranges[i]
		if sid >= iv.start && sid <= iv.end {
			return
		}
	}
	if i > 0 {
		iv := cs.ranges[i-1]
		if sid >= iv.start && sid <= iv.end {
			return
		}
	}

	// New interval initially [sid, sid]. Merge with previous/next if adjacent or overlapping.
	start, end := sid, sid
	// Merge with previous
	if i > 0 {
		prev := cs.ranges[i-1]
		if prev.end+1 >= sid { // adjacent or overlap
			start = prev.start
			if prev.end > end {
				end = prev.end
			}
			// remove prev
			cs.ranges = append(cs.ranges[:i-1], cs.ranges[i:]...)
			i--
		}
	}
	// Merge with next (note: i points to the first interval with start > sid or the merged position)
	if i < len(cs.ranges) {
		next := cs.ranges[i]
		if next.start <= end+1 { // adjacent or overlap
			if next.end > end {
				end = next.end
			}
			// remove next
			cs.ranges = append(cs.ranges[:i], cs.ranges[i+1:]...)
		}
	}
	// Insert merged interval at position i
	cs.ranges = append(cs.ranges, interval{})
	copy(cs.ranges[i+1:], cs.ranges[i:])
	cs.ranges[i] = interval{start: start, end: end}

	// Update highClosed: if the merged interval starts at highClosed+1, extend the prefix.
	cs.promotePrefix()
}

// find returns the index of the first interval with start > sid, or len(ranges)
// if none; suitable for insertion and neighbor checks.
func (cs *closedSet) find(sid uint32) int {
	lo, hi := 0, len(cs.ranges)
	for lo < hi {
		mid := (lo + hi) >> 1
		if cs.ranges[mid].start <= sid {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// promotePrefix extends highClosed if there is an interval that begins at
// highClosed+1. It also prunes any intervals fully included in the prefix by
// removing them from the ranges slice.
func (cs *closedSet) promotePrefix() {
	changed := true
	for changed {
		changed = false
		if len(cs.ranges) == 0 {
			return
		}
		// After insert/merge, the first interval that could extend the prefix is the earliest one
		// whose start is <= highClosed+1. Because we always keep ranges sorted and disjoint, it must
		// be at index 0 if it can extend the prefix.
		iv := cs.ranges[0]
		want := cs.highClosed + 1
		if iv.start == want {
			// Extend prefix to this interval's end and prune it
			cs.highClosed = iv.end
			cs.ranges = cs.ranges[1:]
			changed = true
			continue
		}
		// Prune only intervals fully covered by the prefix.
		if iv.end <= cs.highClosed {
			cs.ranges = cs.ranges[1:]
			changed = true
			continue
		}
		// Otherwise, iv lies strictly above the prefix and cannot extend it now.
	}
}
