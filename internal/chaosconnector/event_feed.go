package chaosconnector

import (
	"fmt"
	"strconv"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventFeedSpec declares one scenario-driven event feed as a flat,
// deterministically ordered event log. ListEvents slices the log by the
// requested page size, so page count varies with page size while the
// declared event set does not -- that invariant is the property chaos
// coverage for event feeds exists to check.
//
// Cursor and has_more are independently derived from the same offset, not
// conflated the way Page[T].Next is: the wire contract allows a stable
// cursor with has_more=false (caught up) as a legitimate response, which is
// exactly the shape the resource-pagination Page[T]/servePage model doesn't
// need to distinguish.
type EventFeedSpec struct {
	Metadata *v2.EventFeedMetadata
	Events   []*v2.Event

	// DefaultPageSize is served when the request's page_size is 0. 0 (the
	// zero value) serves the entire remaining log in one page.
	DefaultPageSize uint32
}

const eventCursorPrefix = "off:"

// encodeEventCursor renders an offset into the declared log as a cursor. The
// root request cursor is "" for offset 0.
func encodeEventCursor(offset int) string {
	if offset == 0 {
		return ""
	}
	return eventCursorPrefix + strconv.Itoa(offset)
}

// decodeEventCursor parses a cursor produced by encodeEventCursor. It never
// re-derives the offset from anything except the cursor string itself, so it
// can validate a cursor supplied by an untrusted caller.
func decodeEventCursor(cursor string) (int, bool) {
	if cursor == "" {
		return 0, true
	}
	if !strings.HasPrefix(cursor, eventCursorPrefix) {
		return 0, false
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(cursor, eventCursorPrefix))
	if err != nil || offset < 0 {
		return 0, false
	}
	return offset, true
}

// serve slices the declared log at the requested cursor and page size. An
// optional start_at filters the returned page without changing how far the
// cursor advances, so page-to-page cursor progress stays independent of the
// filter.
func (s EventFeedSpec) serve(
	cursor string,
	pageSize uint32,
	earliest *timestamppb.Timestamp,
) ([]*v2.Event, *pagination.StreamState, error) {
	offset, ok := decodeEventCursor(cursor)
	if !ok || offset > len(s.Events) {
		return nil, nil, status.Errorf(codes.InvalidArgument, "chaosconnector: unknown event cursor %q", cursor)
	}

	size := int(pageSize)
	if size == 0 {
		size = int(s.DefaultPageSize)
	}
	if size == 0 {
		size = len(s.Events) - offset
	}
	end := min(offset+size, len(s.Events))

	var page []*v2.Event
	for _, event := range s.Events[offset:end] {
		if earliest != nil && event.GetOccurredAt() != nil && event.GetOccurredAt().AsTime().Before(earliest.AsTime()) {
			continue
		}
		page = append(page, event)
	}

	return cloneMessages(page), &pagination.StreamState{
		Cursor:  encodeEventCursor(end),
		HasMore: end < len(s.Events),
	}, nil
}

// validate checks the premises servePage's resource-pagination counterpart
// checks for Pages[T]: the declared id matches the dataset key, and the
// metadata itself satisfies the same generated validation the real
// connectorbuilder applies in addEventFeed.
func (s EventFeedSpec) validate(id string) error {
	if s.Metadata == nil {
		return fmt.Errorf("chaosconnector: event feed %q has no metadata", id)
	}
	if s.Metadata.GetId() != id {
		return fmt.Errorf("chaosconnector: event feed %q metadata id is %q", id, s.Metadata.GetId())
	}
	if err := s.Metadata.Validate(); err != nil {
		return fmt.Errorf("chaosconnector: event feed %q metadata is invalid: %w", id, err)
	}
	return nil
}

func cloneEventFeeds(in map[string]EventFeedSpec) map[string]EventFeedSpec {
	out := make(map[string]EventFeedSpec, len(in))
	for id, spec := range in {
		out[id] = EventFeedSpec{
			Metadata:        proto.Clone(spec.Metadata).(*v2.EventFeedMetadata),
			Events:          cloneMessages(spec.Events),
			DefaultPageSize: spec.DefaultPageSize,
		}
	}
	return out
}
