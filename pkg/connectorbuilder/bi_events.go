package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *builderImpl) ListEventFeeds(ctx context.Context, request *v2.ListEventFeedsRequest) (*v2.ListEventFeedsResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListEventFeeds")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListEventFeedsType

	feeds := make([]*v2.EventFeedMetadata, 0, len(b.eventFeeds))

	for _, feed := range b.eventFeeds {
		feeds = append(feeds, feed.EventFeedMetadata(ctx))
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ListEventFeedsResponse{
		List: feeds,
	}, nil
}

func (b *builderImpl) ListEvents(ctx context.Context, request *v2.ListEventsRequest) (*v2.ListEventsResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListEvents")
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
	events, streamState, annotations, err := feed.ListEvents(ctx, request.StartAt, &pagination.StreamToken{
		Size:   int(request.PageSize),
		Cursor: request.Cursor,
	})
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing events failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ListEventsResponse{
		Events:      events,
		Cursor:      streamState.Cursor,
		HasMore:     streamState.HasMore,
		Annotations: annotations,
	}, nil
}
