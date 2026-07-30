package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type fakeListEventFeedsClient struct {
	types.ConnectorClient
	calls int
	resp  *v2.ListEventFeedsResponse
	err   error
}

func (f *fakeListEventFeedsClient) ListEventFeeds(
	_ context.Context,
	_ *v2.ListEventFeedsRequest,
	_ ...grpc.CallOption,
) (*v2.ListEventFeedsResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func TestListEventFeeds_Next_ReturnsTaskOnce(t *testing.T) {
	ctx := context.Background()
	mgr := NewListEventFeeds(ctx)

	task, _, err := mgr.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, task)
	require.True(t, task.HasListEventFeeds())

	// sync.Once: subsequent calls return nil.
	task2, _, err := mgr.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, task2)
}

func TestListEventFeeds_Process_CallsListEventFeeds(t *testing.T) {
	ctx := context.Background()
	feeds := []*v2.EventFeedMetadata{
		v2.EventFeedMetadata_builder{Id: "user-change-feed"}.Build(),
		v2.EventFeedMetadata_builder{Id: "group-change-feed"}.Build(),
	}
	cc := &fakeListEventFeedsClient{
		resp: v2.ListEventFeedsResponse_builder{List: feeds}.Build(),
	}

	mgr := NewListEventFeeds(ctx)
	task, _, err := mgr.Next(ctx)
	require.NoError(t, err)

	err = mgr.Process(ctx, task, cc)
	require.NoError(t, err)
	require.Equal(t, 1, cc.calls)
}

func TestListEventFeeds_Process_PropagatesError(t *testing.T) {
	ctx := context.Background()
	wantErr := context.DeadlineExceeded
	cc := &fakeListEventFeedsClient{err: wantErr}

	mgr := NewListEventFeeds(ctx)
	task, _, err := mgr.Next(ctx)
	require.NoError(t, err)

	err = mgr.Process(ctx, task, cc)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, cc.calls)
}
