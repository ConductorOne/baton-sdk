package c1zsanitize

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestShifterAnchorMapsTMaxToAnchor(t *testing.T) {
	tMax := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTimestampShifter(anchor, tMax)
	got := s.shift(timestamppb.New(tMax)).AsTime()
	if !got.Equal(anchor) {
		t.Fatalf("expected tMax to map to anchor; got %s want %s", got, anchor)
	}
}

func TestShifterPreservesDeltas(t *testing.T) {
	tMax := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTimestampShifter(anchor, tMax)
	t1 := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 5, 15, 12, 0, 0, 0, time.UTC)
	g1 := s.shift(timestamppb.New(t1)).AsTime()
	g2 := s.shift(timestamppb.New(t2)).AsTime()
	if g2.Sub(g1) != t2.Sub(t1) {
		t.Fatalf("expected deltas preserved; got %s want %s", g2.Sub(g1), t2.Sub(t1))
	}
}

func TestShifterNilNil(t *testing.T) {
	s := newTimestampShifter(time.Now(), time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	if got := s.shift(nil); got != nil {
		t.Fatalf("expected nil input -> nil output")
	}
}
