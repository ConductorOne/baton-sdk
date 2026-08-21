package connectorrunner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// applyOptions runs the given options against a fresh runnerConfig, the way
// NewConnectorRunner does.
func applyOptions(t *testing.T, opts ...Option) *runnerConfig {
	t.Helper()
	cfg := &runnerConfig{}
	for _, opt := range opts {
		require.NoError(t, opt(context.Background(), cfg))
	}
	return cfg
}

// TestWithOnDemandEventStream_LeavesPageSizeUnset documents that
// WithOnDemandEventStream on its own selects no page size, so the local task
// manager falls back to local.EventsPerPageLocally. This is what keeps the
// three-argument call shape behaving exactly as it did before
// --event-feed-page-size existed.
func TestWithOnDemandEventStream_LeavesPageSizeUnset(t *testing.T) {
	cfg := applyOptions(t, WithOnDemandEventStream("feed", time.Now(), "cursor"))

	require.True(t, cfg.onDemand)
	require.NotNil(t, cfg.eventFeedConfig)
	require.Equal(t, "feed", cfg.eventFeedConfig.feedId)
	require.Equal(t, "cursor", cfg.eventFeedConfig.cursor)
	require.Nil(t, cfg.eventFeedPageSize, "no page size should be selected unless WithEventFeedPageSize is applied")
}

// TestWithEventFeedPageSize_OrderIndependent is the point of the separate
// option: it must not matter whether the caller applies it before or after
// WithOnDemandEventStream, and neither option may clobber the other.
func TestWithEventFeedPageSize_OrderIndependent(t *testing.T) {
	startAt := time.Now()

	t.Run("page size after the stream option", func(t *testing.T) {
		cfg := applyOptions(t,
			WithOnDemandEventStream("feed", startAt, "cursor"),
			WithEventFeedPageSize(7),
		)
		require.NotNil(t, cfg.eventFeedConfig)
		require.Equal(t, "feed", cfg.eventFeedConfig.feedId)
		require.NotNil(t, cfg.eventFeedPageSize)
		require.Equal(t, uint32(7), *cfg.eventFeedPageSize)
	})

	t.Run("page size before the stream option", func(t *testing.T) {
		cfg := applyOptions(t,
			WithEventFeedPageSize(7),
			WithOnDemandEventStream("feed", startAt, "cursor"),
		)
		require.NotNil(t, cfg.eventFeedConfig)
		require.Equal(t, "feed", cfg.eventFeedConfig.feedId)
		require.NotNil(t, cfg.eventFeedPageSize)
		require.Equal(t, uint32(7), *cfg.eventFeedPageSize)
	})
}

// TestWithEventFeedPageSize_ZeroIsDistinctFromUnset guards the reason
// eventFeedPageSize is a pointer: an explicit 0 means "let the connector use
// its own default" and must be distinguishable from never having been set,
// which means "use local.EventsPerPageLocally".
func TestWithEventFeedPageSize_ZeroIsDistinctFromUnset(t *testing.T) {
	unset := applyOptions(t, WithOnDemandEventStream("feed", time.Now(), ""))
	require.Nil(t, unset.eventFeedPageSize)

	zero := applyOptions(t, WithOnDemandEventStream("feed", time.Now(), ""), WithEventFeedPageSize(0))
	require.NotNil(t, zero.eventFeedPageSize)
	require.Zero(t, *zero.eventFeedPageSize)
}

// TestWithEventFeedPageSize_DoesNotEnableOnDemand checks that the page-size
// option is purely a modifier: applying it alone must not flip the runner into
// on-demand mode or invent an event feed task.
func TestWithEventFeedPageSize_DoesNotEnableOnDemand(t *testing.T) {
	cfg := applyOptions(t, WithEventFeedPageSize(7))

	require.False(t, cfg.onDemand)
	require.Nil(t, cfg.eventFeedConfig)
	require.NotNil(t, cfg.eventFeedPageSize)
}
