package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	LegacyBatonFeedId = "baton_feed_event"
)

// Deprecated: This interface is deprecated in favor of EventProviderV2 which supports
// multiple event feeds. Implementing this interface indicates the connector can provide
// a single stream of events from the external system, enabling near real-time updates
// in Baton. New connectors should implement EventProviderV2 instead.
type EventProvider interface {
	ConnectorBuilder
	EventLister
}

// NewEventProviderV2 is a new interface that allows connectors to provide multiple event feeds.
//
// This is the recommended interface for implementing event feed support in new connectors.
type EventProviderV2 interface {
	ConnectorBuilder
	EventFeedsLimited
}

type EventFeedsLimited interface {
	EventFeeds(ctx context.Context) []EventFeed
}

// EventFeed is a single stream of events from the external system.
//
// EventFeedMetadata describes this feed, and a connector can have multiple feeds.
type EventFeed interface {
	EventLister
	EventFeedLimited
}

type EventFeedLimited interface {
	EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata
}

// Compatibility interface lets us handle both EventFeed and EventProvider the same.
type EventLister interface {
	ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error)
}

func newEventFeedV1to2(eventFeed EventLister) EventFeed {
	return &oldEventFeedWrapper{
		feed: eventFeed,
	}
}

type oldEventFeedWrapper struct {
	feed EventLister
}

func (e *oldEventFeedWrapper) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return v2.EventFeedMetadata_builder{
		Id:                  LegacyBatonFeedId,
		SupportedEventTypes: []v2.EventType{v2.EventType_EVENT_TYPE_UNSPECIFIED},
	}.Build()
}

func (e *oldEventFeedWrapper) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return e.feed.ListEvents(ctx, earliestEvent, pToken)
}

func (b *builder) ListEventFeeds(ctx context.Context, request *v2.ListEventFeedsRequest) (*v2.ListEventFeedsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListEventFeeds")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListEventFeedsType

	feeds := make([]*v2.EventFeedMetadata, 0, len(b.eventFeeds))

	for _, feed := range b.eventFeeds {
		feeds = append(feeds, feed.EventFeedMetadata(ctx))
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.ListEventFeedsResponse_builder{
		List: feeds,
	}.Build(), nil
}

func (b *builder) ListEvents(ctx context.Context, request *v2.ListEventsRequest) (*v2.ListEventsResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.ListEvents")
	defer span.End()

	start := b.nowFunc()
	feedId := request.GetEventFeedId()

	// If no feedId is provided, use the legacy Baton feed Id
	if feedId == "" {
		feedId = LegacyBatonFeedId
	}

	feed, ok := b.eventFeeds[feedId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "error: event feed not found")
	}

	tt := tasks.ListEventsType
	events, streamState, annotations, err := feed.ListEvents(ctx, request.GetStartAt(), &pagination.StreamToken{
		Size:   int(request.GetPageSize()),
		Cursor: request.GetCursor(),
	})
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: listing events failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.ListEventsResponse_builder{
		Events:      events,
		Cursor:      streamState.Cursor,
		HasMore:     streamState.HasMore,
		Annotations: annotations,
	}.Build(), nil
}

func (b *builder) addEventFeed(ctx context.Context, in interface{}) error {
	if ep, ok := in.(EventFeedsLimited); ok {
		for _, ef := range ep.EventFeeds(ctx) {
			feedData := ef.EventFeedMetadata(ctx)
			if feedData == nil {
				return fmt.Errorf("error: event feed metadata is nil")
			}
			if err := feedData.Validate(); err != nil {
				return fmt.Errorf("error: event feed metadata for %s is invalid: %w", feedData.GetId(), err)
			}
			if _, ok := b.eventFeeds[feedData.GetId()]; ok {
				return fmt.Errorf("error: duplicate event feed id found: %s", feedData.GetId())
			}
			b.eventFeeds[feedData.GetId()] = ef
		}
	}

	if ep, ok := in.(EventLister); ok {
		// Register the legacy Baton feed as a v2 event feed
		// implementing both v1 and v2 event feeds is not supported.
		if len(b.eventFeeds) != 0 {
			return fmt.Errorf("error: using legacy event feed is not supported when using EventProviderV2")
		}
		b.eventFeeds[LegacyBatonFeedId] = newEventFeedV1to2(ep)
	}
	return nil
}
