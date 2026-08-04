package chaosconnector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

func testEvents(n int) []*v2.Event {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([]*v2.Event, n)
	for i := range n {
		out[i] = v2.Event_builder{
			Id:         "e" + string(rune('0'+i)),
			OccurredAt: timestamppb.New(base.Add(time.Duration(i) * time.Minute)),
		}.Build()
	}
	return out
}

func TestEventFeedSpec_Serve_HonorsPageSizeAcrossSizes(t *testing.T) {
	spec := EventFeedSpec{Events: testEvents(5)}

	tests := []struct {
		name       string
		pageSize   uint32
		wantPages  int
		wantCounts []int
	}{
		{name: "size 1", pageSize: 1, wantPages: 5, wantCounts: []int{1, 1, 1, 1, 1}},
		{name: "size 2", pageSize: 2, wantPages: 3, wantCounts: []int{2, 2, 1}},
		{name: "size 100 (larger than log)", pageSize: 100, wantPages: 1, wantCounts: []int{5}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				cursor    string
				pages     int
				collected []*v2.Event
			)
			for {
				events, state, err := spec.serve(cursor, tc.pageSize, nil)
				require.NoError(t, err)
				require.LessOrEqual(t, pages, len(tc.wantCounts)-1, "served more pages than expected")
				require.Equal(t, tc.wantCounts[pages], len(events))
				collected = append(collected, events...)
				pages++
				cursor = state.Cursor
				if !state.HasMore {
					break
				}
			}
			require.Equal(t, tc.wantPages, pages)
			require.Len(t, collected, len(spec.Events))
			for i, event := range collected {
				require.Equal(t, spec.Events[i].GetId(), event.GetId())
			}
		})
	}
}

func TestEventFeedSpec_Serve_CursorReachedEndIsIdempotent(t *testing.T) {
	spec := EventFeedSpec{Events: testEvents(2)}

	events, state, err := spec.serve("", 10, nil)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.False(t, state.HasMore)
	caughtUpCursor := state.Cursor

	// Re-querying at the caught-up cursor is legitimate ("nothing new since
	// last check"), not an error: a stable cursor with has_more=false is a
	// valid response, unlike has_more=true with an unchanged cursor.
	events, state, err = spec.serve(caughtUpCursor, 10, nil)
	require.NoError(t, err)
	require.Empty(t, events)
	require.False(t, state.HasMore)
	require.Equal(t, caughtUpCursor, state.Cursor)
}

// TestEventFeedSpec_Serve_MakesProgressWheneverRequestedSizeIsPositive is a
// regression test for a slicing bug found and fixed during development but
// never captured as a permanent case: computing the page end as
// offset+size-1 instead of offset+size left end == offset for every
// pageSize-1 request, so the cursor never advanced while has_more stayed
// true. Unlike the legitimate caught-up case in
// TestEventFeedSpec_Serve_CursorReachedEndIsIdempotent (offset == len(Events),
// has_more false), that bug produced a stuck cursor with has_more still
// true -- which is exactly the shape pkg/tasks/local.Process's page loop has
// no guard against (it loops until has_more is false), so the practical
// blast radius of this specific bug class is an infinite loop in the local
// runner, not merely a wrong answer.
func TestEventFeedSpec_Serve_MakesProgressWheneverRequestedSizeIsPositive(t *testing.T) {
	spec := EventFeedSpec{Events: testEvents(5)}

	for offset := 0; offset < len(spec.Events); offset++ {
		for _, size := range []uint32{1, 2, 3, 100} {
			cursor := encodeEventCursor(offset)
			_, state, err := spec.serve(cursor, size, nil)
			require.NoError(t, err)

			newOffset, ok := decodeEventCursor(state.Cursor)
			require.True(t, ok)
			require.Greaterf(t, newOffset, offset,
				"serve must advance past offset %d for a positive page size %d; got cursor %q with has_more=%v",
				offset, size, state.Cursor, state.HasMore)
		}
	}
}

func TestEventFeedSpec_Serve_UnknownCursorIsRejected(t *testing.T) {
	spec := EventFeedSpec{Events: testEvents(2)}

	_, _, err := spec.serve("not-a-real-cursor", 10, nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// An offset past the end of the declared log is equally invalid, even
	// though it parses -- it doesn't correspond to any point this log ever
	// produced.
	_, _, err = spec.serve(encodeEventCursor(99), 10, nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestEventFeedSpec_Serve_StartAtFiltersReturnedEventsNotCursorAdvance(t *testing.T) {
	events := testEvents(4) // occurred_at at :00, :01, :02, :03
	spec := EventFeedSpec{Events: events}

	earliest := events[2].GetOccurredAt() // exclude the first two events
	page, state, err := spec.serve("", 10, earliest)
	require.NoError(t, err)
	require.False(t, state.HasMore)
	require.Equal(t, encodeEventCursor(4), state.Cursor, "cursor advances past the full log regardless of filtering")
	require.Len(t, page, 2)
	require.Equal(t, events[2].GetId(), page[0].GetId())
	require.Equal(t, events[3].GetId(), page[1].GetId())
}

func TestEventFeedSpec_Validate(t *testing.T) {
	valid := EventFeedSpec{
		Metadata: v2.EventFeedMetadata_builder{
			Id:                  "feed-a",
			SupportedEventTypes: []v2.EventType{v2.EventType_EVENT_TYPE_RESOURCE_CHANGE},
		}.Build(),
	}
	require.NoError(t, valid.validate("feed-a"))

	require.Error(t, EventFeedSpec{}.validate("feed-a"), "nil metadata must be rejected")

	mismatched := EventFeedSpec{
		Metadata: v2.EventFeedMetadata_builder{Id: "feed-b"}.Build(),
	}
	require.Error(t, mismatched.validate("feed-a"), "metadata id must match the dataset key")

	invalidMetadata := EventFeedSpec{
		Metadata: v2.EventFeedMetadata_builder{Id: ""}.Build(),
	}
	require.Error(t, invalidMetadata.validate(""), "generated EventFeedMetadata.Validate() must be applied")
}

// TestScenarioValidate_RejectsMalformedEventFeed is the instrument-validation
// check for the new EventFeeds mechanics: Scenario.Validate must reject a
// planted violation (metadata id not matching the dataset key), the same way
// it already rejects a duplicate resource type id.
func TestScenarioValidate_RejectsMalformedEventFeed(t *testing.T) {
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	dataset := scenario.Epochs[scenario.InitialEpoch]
	dataset.EventFeeds["broken"] = EventFeedSpec{
		Metadata: v2.EventFeedMetadata_builder{Id: "not-broken"}.Build(),
	}

	err = scenario.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "broken")
}

// TestEventFeed_MissingFeedReturnsInternalError proves the connector-facing
// eventFeed rejects a feed id its own dataset never declared, rather than
// silently returning an empty response the way the pre-existing stub did for
// every id.
func TestEventFeed_MissingFeedReturnsInternalError(t *testing.T) {
	scenario, err := NewFullScenario()
	require.NoError(t, err)
	run, err := NewRun(scenario, NewSchedule())
	require.NoError(t, err)

	feed := &eventFeed{run: run, id: "does-not-exist"}
	_, _, _, err = feed.ListEvents(context.Background(), nil, &pagination.StreamToken{})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}
