// Copyright 2025 Radu Berinde.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package regiontree

import (
	"fmt"
	"iter"
	"strings"

	"github.com/RaduBerinde/axisds/v3"
	"github.com/RaduBerinde/btreemap"
)

type Boundary = axisds.Boundary

// LowerBound defines an (optional) lower bound for region tree query methods.
type LowerBound[B Boundary] struct {
	isMin bool
	key   B
}

// Min returns a LowerBound that does not constrain the lower end of the query.
func Min[B Boundary]() LowerBound[B] { return LowerBound[B]{isMin: true} }

// GE returns a lower bound that includes all regions at or after key.
func GE[B Boundary](key B) LowerBound[B] { return LowerBound[B]{key: key} }

func (lb LowerBound[B]) btreemapBound() btreemap.LowerBound[B] {
	if lb.isMin {
		return btreemap.Min[B]()
	}
	return btreemap.GE(lb.key)
}

// UpperBound defines an (optional) upper bound for region tree query methods.
type UpperBound[B Boundary] struct {
	isMax bool
	key   B
}

// Max returns an UpperBound that does not constrain the upper end of the query.
func Max[B Boundary]() UpperBound[B] { return UpperBound[B]{isMax: true} }

// LT returns an upper bound that excludes regions at or after key.
func LT[B Boundary](key B) UpperBound[B] { return UpperBound[B]{key: key} }

func (ub UpperBound[B]) btreemapBound() btreemap.UpperBound[B] {
	if ub.isMax {
		return btreemap.Max[B]()
	}
	return btreemap.LT(ub.key)
}

// Property is an arbitrary type that represents a property of a region of a
// one-dimensional axis.
type Property any

// PropertyEqualFn is a function used to compare properties of two regions. If
// it returns true, the two property values can be used interchangeably.
//
// Note that it is allowed for the function to "evolve" over time (but not
// concurrently with a region tree method), with values that were not equal
// becoming equal (but not the opposite: once two values are equal, they must
// stay equal forever). For example, the property can be a monotonic expiration
// time and as we update the current time, expired times become equal to the
// zero property.
//
// A zero property value is any value that is equal to the zero P value.
type PropertyEqualFn[P Property] func(a, b P) bool

// Option configures the behavior of region tree query methods.
type Option struct {
	withGC bool
}

// WithGC is an option that enables garbage collection of unnecessary
// boundaries between regions with properties that have become equal.
//
// This option is only useful to improve performance when the PropertyEqualFn
// can change over time. Methods called with this option cannot be called
// concurrently with any other methods.
var WithGC = Option{withGC: true}

func hasWithGC(opts []Option) bool {
	for _, o := range opts {
		if o.withGC {
			return true
		}
	}
	return false
}

// T is a tree of regions which fragment a one-dimensional space. Regions have
// boundaries of type B and each region maintains a property P. Neighboring
// regions with equal properties are automatically merged.
//
// T supports lazy (copy-on-write) cloning via Clone().
type T[B Boundary, P Property] struct {
	cmp    axisds.CompareFn[B]
	propEq PropertyEqualFn[P]
	// Tree maps each region start boundary to its property. The region ends at
	// the next region's start boundary. The last region has zero property.
	tree *btreemap.BTreeMap[B, P]
}

// Make creates a new region tree with the given boundary and property
// comparison functions.
func Make[B Boundary, P Property](cmp axisds.CompareFn[B], propEq PropertyEqualFn[P]) T[B, P] {
	t := T[B, P]{
		cmp:    cmp,
		propEq: propEq,
	}
	t.tree = btreemap.New[B, P](8, btreemap.CmpFunc[B](cmp))
	return t
}

// Update the property for the given range. The updateProp function is called
// for all the regions within the range to calculate the new property.
//
// The runtime complexity is O(log N + K) where K is the number of regions we
// are updating. Note that if the ranges we update are mostly non-overlapping,
// this will be O(log N) on average.
func (t *T[B, P]) Update(start, end B, updateProp func(p P) P) {
	// Get information about the region before start.
	startBoundaryExists, beforeProp := t.startBoundaryInfo(start)
	endBoundaryExists, afterProp := t.endBoundaryInfo(end)

	lastProp := beforeProp
	var startProp P
	var addStartBoundary bool
	if !startBoundaryExists {
		// See if we need to add the start boundary.
		startProp = updateProp(beforeProp)
		if !t.propEq(startProp, lastProp) {
			// We will add the start boundary with startProp.
			addStartBoundary = true
		}
		lastProp = startProp
	}

	type update struct {
		start  B
		prop   P
		delete bool
	}
	var updates []update
	// Collect all the boundaries in the range that need to be updated or deleted.
	for rStart, rProp := range t.tree.Ascend(btreemap.GE(start), btreemap.LT(end)) {
		prop := updateProp(rProp)
		if t.propEq(prop, lastProp) {
			// Boundary not necessary; remove it.
			updates = append(updates, update{start: rStart, delete: true})
		} else if !t.propEq(prop, rProp) {
			updates = append(updates, update{start: rStart, prop: prop, delete: false})
		}
		lastProp = prop
	}

	if addStartBoundary {
		t.tree.ReplaceOrInsert(start, startProp)
	}

	for _, u := range updates {
		if u.delete {
			t.tree.Delete(u.start)
		} else {
			t.tree.ReplaceOrInsert(u.start, u.prop)
		}
	}

	if t.propEq(lastProp, afterProp) {
		if endBoundaryExists {
			// End boundary can be removed.
			t.tree.Delete(end)
		}
	} else {
		if !endBoundaryExists {
			// End boundary needs to be added.
			t.tree.ReplaceOrInsert(end, afterProp)
		}
	}
}

// startBoundaryInfo checks if the boundary exists and returns the property
// for the region that contains or ends at the boundary.
//
// exists=true:
//
//	                  start
//	                    |
//	                    v
//	---|---beforeProp---|---------|---
//
// exists=false:
//
//	         start
//	           |
//	           v
//	---|---beforeProp---|---
//
// If no regions contain start, beforeProp is zero.
func (t *T[B, P]) startBoundaryInfo(start B) (exists bool, beforeProp P) {
	for rStart, rProp := range t.tree.Descend(btreemap.LE(start), btreemap.Min[B]()) {
		if !exists && t.cmp(rStart, start) == 0 {
			exists = true
			// Do one more step to get the property before the boundary.
			continue
		}
		return exists, rProp
	}
	return exists, beforeProp
}

// endBoundaryInfo checks if the boundary exists and returns the property for
// the region that contains or starts at the boundary.
//
// exists=true:
//
//	              end
//	               |
//	               v
//	---|-----------|---afterProp---|---
//
// exists=false:
//
//	          end
//	           |
//	           v
//	---|---afterProp---|---
//
// If no regions contain end, afterProp is zero.
func (t *T[B, P]) endBoundaryInfo(end B) (exists bool, afterProp P) {
	if rStart, rProp, ok := t.tree.SeekLE(end); ok {
		return t.cmp(rStart, end) == 0, rProp
	}
	return false, afterProp
}

// Enumerate all regions in the given range with non-zero property.
//
// Two consecutive regions can "touch" but not overlap; if they touch, their
// properties are not equal.
//
// Enumerate can be called concurrently with other read-only methods (as long as
// WithGC is not used).
func (t *T[B, P]) Enumerate(
	lower LowerBound[B], upper UpperBound[B], opts ...Option,
) iter.Seq2[axisds.Interval[B], P] {
	gc := hasWithGC(opts)
	return func(yield func(i axisds.Interval[B], prop P) bool) {
		t.enumerate(lower, upper, yield, gc)
	}
}

func (t *T[B, P]) enumerate(
	lower LowerBound[B],
	upper UpperBound[B],
	emit func(i axisds.Interval[B], prop P) bool,
	withGC bool,
) {
	if t.tree.Len() < 2 {
		return
	}
	// If both bounds have keys, check ordering.
	if !lower.isMin && !upper.isMax && t.cmp(lower.key, upper.key) >= 0 {
		return
	}

	var eh enumerateHelper[B, P]

	// For GE(start): find region containing start via SeekLE.
	// For Min(): skip this step.
	if !lower.isMin {
		if rStart, rProp, ok := t.tree.SeekLE(lower.key); ok && t.cmp(rStart, lower.key) < 0 {
			// This is the first addRegion call, so we won't emit anything.
			eh.addRegion(lower.key, rProp, t.propEq, nil)
		}
	}

	var toDelete []B
	for rStart, rProp := range t.tree.Ascend(lower.btreemapBound(), upper.btreemapBound()) {
		eh.addRegion(rStart, rProp, t.propEq, emit)
		if withGC && eh.canDeleteLastBoundary {
			toDelete = append(toDelete, rStart)
		}
		if eh.stopEmitting {
			break
		}
	}

	// For LT(end): clamp last region via finish.
	// For Max(): skip (last tree region has zero prop, naturally terminates).
	if !upper.isMax {
		eh.finish(upper.key, t.propEq, emit)
	}

	for _, b := range toDelete {
		t.tree.Delete(b)
	}
}

// EnumerateDesc enumerates all regions with non-zero property in the given
// range, in descending order of position. The upper bound is listed first and
// the lower bound second, matching the direction of iteration.
//
// The emitted intervals still have Start < End; only the iteration order is
// reversed.
//
// EnumerateDesc can be called concurrently with other read-only methods (as
// long as WithGC is not used).
func (t *T[B, P]) EnumerateDesc(
	upper UpperBound[B], lower LowerBound[B], opts ...Option,
) iter.Seq2[axisds.Interval[B], P] {
	gc := hasWithGC(opts)
	return func(yield func(i axisds.Interval[B], prop P) bool) {
		t.enumerateDesc(upper, lower, yield, gc)
	}
}

func (t *T[B, P]) enumerateDesc(
	upper UpperBound[B],
	lower LowerBound[B],
	emit func(i axisds.Interval[B], prop P) bool,
	withGC bool,
) {
	if t.tree.Len() < 2 {
		return
	}
	// If both bounds have keys, check ordering.
	if !lower.isMin && !upper.isMax && t.cmp(lower.key, upper.key) >= 0 {
		return
	}

	var dh enumerateDescHelper[B, P]

	// For LT(end): set the initial upper boundary for the first region.
	if !upper.isMax {
		dh.regionEnd = upper.key
		dh.regionEndSet = true
	}

	var toDelete []B
	for rStart, rProp := range t.tree.Descend(upper.btreemapBound(), lower.btreemapBound()) {
		if deleteKey, ok := dh.addBoundary(rStart, rProp, t.propEq, emit); ok && withGC {
			toDelete = append(toDelete, deleteKey)
		}
		if dh.stopEmitting {
			break
		}
	}

	// For GE(start): check if there's a region containing start that extends
	// below the lowest boundary in the descend range.
	if !lower.isMin && dh.regionEndSet && !dh.stopEmitting {
		if _, rProp, ok := t.tree.SeekLT(lower.key); ok && t.cmp(dh.regionEnd, lower.key) > 0 {
			if deleteKey, ok := dh.addLowerBound(lower.key, rProp, t.propEq, emit); ok && withGC {
				toDelete = append(toDelete, deleteKey)
			}
		}
	}

	dh.emitPending(t.propEq, emit)

	for _, b := range toDelete {
		t.tree.Delete(b)
	}
}

// Any returns true if the given range overlaps any region with property that
// satisfies the given function.
//
// Any can be called concurrently with other read-only methods (as long as
// WithGC is not used).
func (t *T[B, P]) Any(
	lower LowerBound[B], upper UpperBound[B], propFn func(prop P) bool, opts ...Option,
) bool {
	return t.any(lower, upper, propFn, hasWithGC(opts))
}

func (t *T[B, P]) any(
	lower LowerBound[B], upper UpperBound[B], propFn func(prop P) bool, withGC bool,
) bool {
	// If both bounds have keys, check ordering.
	if !lower.isMin && !upper.isMax && t.cmp(lower.key, upper.key) >= 0 {
		return false
	}

	// For GE(start): check the region containing start (it may not have a
	// boundary). For Min(): the implicit region before the first boundary has
	// zero property; check propFn against it.
	var lastProp P
	if !lower.isMin {
		startBoundaryExists, beforeProp := t.startBoundaryInfo(lower.key)
		if !startBoundaryExists && propFn(beforeProp) {
			return true
		}
		lastProp = beforeProp
	} else if propFn(lastProp) {
		return true
	}

	found := false
	var toDelete []B
	for rStart, rProp := range t.tree.Ascend(lower.btreemapBound(), upper.btreemapBound()) {
		if withGC && t.propEq(rProp, lastProp) {
			toDelete = append(toDelete, rStart)
		}
		lastProp = rProp
		if propFn(rProp) {
			found = true
			break
		}
	}
	for _, b := range toDelete {
		t.tree.Delete(b)
	}
	return found
}

// All emits all regions with non-zero property. It is a shorthand for
// Enumerate(Min[B](), Max[B]()).
//
// Two consecutive regions can "touch" but not overlap; if they touch, their
// properties are not equal.
//
// All can be called concurrently with other read-only methods (as long as
// WithGC is not used).
func (t *T[B, P]) All(opts ...Option) iter.Seq2[axisds.Interval[B], P] {
	gc := hasWithGC(opts)
	return func(yield func(i axisds.Interval[B], prop P) bool) {
		t.all(yield, gc)
	}
}

func (t *T[B, P]) all(emit func(i axisds.Interval[B], prop P) bool, withGC bool) {
	t.enumerate(Min[B](), Max[B](), emit, withGC)
}

type enumerateHelper[B Boundary, P Property] struct {
	lastBoundary B
	lastProp     P
	initialized  bool
	stopEmitting bool
	// canDeleteLastBoundary is set by addRegion when the two last regions had
	// equal properties.
	canDeleteLastBoundary bool
}

func (eh *enumerateHelper[B, P]) addRegion(
	boundary B, prop P, propEq PropertyEqualFn[P], emitFn func(i axisds.Interval[B], prop P) bool,
) {
	if !eh.initialized {
		eh.lastBoundary = boundary
		eh.lastProp = prop
		eh.initialized = true
		return
	}
	eh.canDeleteLastBoundary = propEq(eh.lastProp, prop)
	if eh.canDeleteLastBoundary || eh.stopEmitting {
		return
	}
	if !propEq(zero[P](), eh.lastProp) {
		interval := axisds.Interval[B]{Start: eh.lastBoundary, End: boundary}
		if !emitFn(interval, eh.lastProp) {
			eh.stopEmitting = true
		}
	}
	eh.lastBoundary = boundary
	eh.lastProp = prop
}

func (eh *enumerateHelper[B, P]) finish(
	end B, propEq PropertyEqualFn[P], emitFn func(interval axisds.Interval[B], prop P) bool,
) {
	if eh.initialized && !eh.stopEmitting && !propEq(zero[P](), eh.lastProp) {
		interval := axisds.Interval[B]{Start: eh.lastBoundary, End: end}
		emitFn(interval, eh.lastProp)
	}
}

type enumerateDescHelper[B Boundary, P Property] struct {
	// regionEnd is the end boundary for the next region to be formed.
	regionEnd    B
	regionEndSet bool

	// Pending region [pendingStart, pendingEnd) with pendingProp, waiting to
	// be emitted. Emission is delayed to allow merging adjacent regions with
	// equal properties (needed for correct GC output).
	pendingStart B
	pendingEnd   B
	pendingProp  P
	hasPending   bool

	stopEmitting bool
}

// addBoundary processes a boundary seen during descending iteration. Each
// boundary marks the start of a region extending up to regionEnd.
//
// Returns (deleteKey, true) if a boundary should be deleted for GC.
func (dh *enumerateDescHelper[B, P]) addBoundary(
	boundary B, prop P, propEq PropertyEqualFn[P], emitFn func(i axisds.Interval[B], prop P) bool,
) (deleteKey B, shouldDelete bool) {
	if !dh.regionEndSet {
		// First boundary (Max() upper bound): record as region end.
		dh.regionEnd = boundary
		dh.regionEndSet = true
		return
	}

	// Region [boundary, regionEnd) has property prop.
	if dh.hasPending && propEq(prop, dh.pendingProp) {
		// Merge with pending: extend it downward. The boundary at pendingStart
		// separates two equal-property regions and can be deleted.
		deleteKey = dh.pendingStart
		shouldDelete = true
		dh.pendingStart = boundary
	} else {
		// Emit existing pending region, then start a new one.
		dh.emitPending(propEq, emitFn)
		if dh.stopEmitting {
			return
		}
		dh.pendingStart = boundary
		dh.pendingEnd = dh.regionEnd
		dh.pendingProp = prop
		dh.hasPending = true
	}
	dh.regionEnd = boundary
	return
}

// addLowerBound handles the region extending below the lowest boundary in the
// descend range, clamped at start.
//
// Returns (deleteKey, true) if a boundary should be deleted for GC.
func (dh *enumerateDescHelper[B, P]) addLowerBound(
	start B, prop P, propEq PropertyEqualFn[P], emitFn func(i axisds.Interval[B], prop P) bool,
) (deleteKey B, shouldDelete bool) {
	if dh.hasPending && propEq(prop, dh.pendingProp) {
		// Merge with pending.
		deleteKey = dh.pendingStart
		shouldDelete = true
		dh.pendingStart = start
	} else {
		dh.emitPending(propEq, emitFn)
		if dh.stopEmitting {
			return
		}
		dh.pendingStart = start
		dh.pendingEnd = dh.regionEnd
		dh.pendingProp = prop
		dh.hasPending = true
	}
	return
}

func (dh *enumerateDescHelper[B, P]) emitPending(
	propEq PropertyEqualFn[P], emitFn func(i axisds.Interval[B], prop P) bool,
) {
	if !dh.hasPending || dh.stopEmitting {
		return
	}
	if !propEq(zero[P](), dh.pendingProp) {
		interval := axisds.Interval[B]{Start: dh.pendingStart, End: dh.pendingEnd}
		if !emitFn(interval, dh.pendingProp) {
			dh.stopEmitting = true
		}
	}
	dh.hasPending = false
}

// IsEmpty returns true if the tree contains no regions with non-zero property.
func (t *T[B, P]) IsEmpty() bool {
	if t.tree.Len() < 2 {
		return true
	}
	// Check that we have regions with non-zero property.
	for _, rProp := range t.tree.Ascend(btreemap.Min[B](), btreemap.Max[B]()) {
		if !t.propEq(rProp, zero[P]()) {
			return false
		}
	}
	return true
}

// InternalLen returns the number of region boundaries stored internally.
func (t *T[B, P]) InternalLen() int {
	return t.tree.Len()
}

// Clone creates a lazy clone of T with the same properties and regions. The new
// tree can be modified independently.
//
// This operation is constant time; it can cause some minor slowdown of future
// updates because of copy-on-write logic.
func (t *T[B, P]) Clone() T[B, P] {
	return T[B, P]{
		cmp:    t.cmp,
		propEq: t.propEq,
		tree:   t.tree.Clone(),
	}
}

// String formats all regions, one per line.
func (t *T[B, P]) String(iFmt axisds.IntervalFormatter[B]) string {
	var b strings.Builder
	var eh enumerateHelper[B, P]
	for rStart, rProp := range t.tree.Ascend(btreemap.Min[B](), btreemap.Max[B]()) {
		eh.addRegion(rStart, rProp, t.propEq, func(i axisds.Interval[B], prop P) bool {
			fmt.Fprintf(&b, "%s = %v\n", iFmt(i), prop)
			return true
		})
	}
	if b.Len() == 0 {
		return "<empty>"
	}
	return b.String()
}

// CheckInvariants can be used in testing builds to verify internal invariants.
func (t *T[B, P]) CheckInvariants() {
	var lastBoundary B
	var lastProp P
	lastBoundarySet := false
	for rStart, rProp := range t.tree.Ascend(btreemap.Min[B](), btreemap.Max[B]()) {
		if lastBoundarySet && t.cmp(lastBoundary, rStart) >= 0 {
			panic("region boundaries not increasing")
		}
		if !t.propEq(rProp, rProp) {
			panic("region property is not equal to itself")
		}
		lastBoundary = rStart
		lastBoundarySet = true
		lastProp = rProp
	}

	// Last region should have the zero property.
	if !t.propEq(lastProp, zero[P]()) {
		panic("last region must always have zero property")
	}
}

func zero[T any]() T {
	var t T
	return t
}
