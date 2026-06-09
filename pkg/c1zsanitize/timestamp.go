package c1zsanitize

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// timestampShifter applies a constant offset Δ = anchor - tMax to
// every observed timestamp so the newest input timestamp lands on
// the anchor and all relative deltas are preserved exactly.
type timestampShifter struct {
	delta time.Duration
}

func newTimestampShifter(anchor, tMax time.Time) *timestampShifter {
	if tMax.IsZero() {
		return &timestampShifter{}
	}
	return &timestampShifter{delta: anchor.Sub(tMax)}
}

// shift returns a new timestamp shifted by Δ. nil and zero
// timestamps map to themselves so "field unset" semantics survive.
func (s *timestampShifter) shift(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	// No-op anchor (anchor == tMax, or no tMax): the shift is identity, so
	// skip the AsTime + timestamppb.New allocation pair on every timestamp.
	if s.delta == 0 {
		return ts
	}
	t := ts.AsTime()
	if t.IsZero() {
		return ts
	}
	return timestamppb.New(t.Add(s.delta))
}
