package c1zsanitize

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestShifterAnchorMapsTMaxToAnchor(t *testing.T) {
	tMax := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTimestampShifter(anchor, tMax)
	got := s.shift(timestamppb.New(tMax)).AsTime()
	require.True(t, got.Equal(anchor), "expected tMax to map to anchor; got %s want %s", got, anchor)
}

func TestShifterPreservesDeltas(t *testing.T) {
	tMax := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTimestampShifter(anchor, tMax)
	t1 := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 5, 15, 12, 0, 0, 0, time.UTC)
	g1 := s.shift(timestamppb.New(t1)).AsTime()
	g2 := s.shift(timestamppb.New(t2)).AsTime()
	require.Equal(t, t2.Sub(t1), g2.Sub(g1), "expected deltas preserved")
}

func TestShifterNilNil(t *testing.T) {
	s := newTimestampShifter(time.Now(), time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	require.Nil(t, s.shift(nil), "expected nil input -> nil output")
}
