package ratelimit

import (
	"context"
	"testing"
	"time"

	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/stretchr/testify/require"
)

func TestObserveWaitReportsToObserver(t *testing.T) {
	var gotWait time.Duration
	var calls int
	ctx := WithWaitObserver(t.Context(), func(ctx context.Context, wait time.Duration) {
		gotWait = wait
		calls++
	})

	ObserveWait(ctx, 5*time.Second)

	require.Equal(t, 1, calls)
	require.Equal(t, 5*time.Second, gotWait)
}

func TestObserveWaitWithoutObserverIsNoOp(t *testing.T) {
	require.NotPanics(t, func() {
		ObserveWait(t.Context(), time.Second)
	})
}

func TestWithWaitObserverNilFn(t *testing.T) {
	ctx := WithWaitObserver(t.Context(), nil)
	require.Equal(t, t.Context(), ctx)
}

func TestResourceTypeFromDescriptors(t *testing.T) {
	descriptors := ratelimitV1.RateLimitDescriptors_builder{
		Entries: []*ratelimitV1.RateLimitDescriptors_Entry{
			ratelimitV1.RateLimitDescriptors_Entry_builder{
				Key:   descriptorKeyConnectorMethod,
				Value: "list_grants",
			}.Build(),
			ratelimitV1.RateLimitDescriptors_Entry_builder{
				Key:   descriptorKeyConnectorResourceType,
				Value: "repository",
			}.Build(),
		},
	}.Build()

	require.Equal(t, "repository", resourceTypeFromDescriptors(descriptors))
	require.Equal(t, "", resourceTypeFromDescriptors(nil))
}
