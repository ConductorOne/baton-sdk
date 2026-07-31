package field

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventFeedPageSizeField_DefaultValue(t *testing.T) {
	require.Equal(t, 100, EventFeedPageSizeField.DefaultValue)
}

func TestEventFeedPageSizeField_Ruler(t *testing.T) {
	run := func(value int) error {
		return ValidateIntRules(EventFeedPageSizeField.Rules.i, value, EventFeedPageSizeField.FieldName)
	}

	t.Run("0 is accepted (bypasses the ruler entirely; lets the connector use its own default)", func(t *testing.T) {
		require.NoError(t, run(0))
	})

	t.Run("1 is accepted", func(t *testing.T) {
		require.NoError(t, run(1))
	})

	t.Run("1000 is accepted", func(t *testing.T) {
		require.NoError(t, run(1000))
	})

	t.Run("100 (default) is accepted", func(t *testing.T) {
		require.NoError(t, run(100))
	})

	t.Run("1001 is rejected", func(t *testing.T) {
		err := run(1001)
		require.Error(t, err)
	})

	t.Run("negative values are still rejected", func(t *testing.T) {
		err := run(-1)
		require.Error(t, err)
	})
}

func TestListEventFeedsField_MutuallyExclusiveWithOtherOnDemandFlags(t *testing.T) {
	for _, relationship := range DefaultRelationships {
		if relationship.Kind != MutuallyExclusive {
			continue
		}

		var hasListEventFeeds, hasEventFeed bool
		for _, f := range relationship.Fields {
			if f.FieldName == listEventFeedsField.FieldName {
				hasListEventFeeds = true
			}
			if f.FieldName == eventFeedField.FieldName {
				hasEventFeed = true
			}
		}

		if hasListEventFeeds {
			require.True(t, hasEventFeed, "list-event-feeds should share every mutually-exclusive group it's in with event-feed")
		}
	}
}

func TestValidate_ListEventFeedsRejectedAlongsideEventFeed(t *testing.T) {
	carrier := Configuration{
		Fields:      DefaultFields,
		Constraints: DefaultRelationships,
	}

	AssertOutcome(
		t,
		carrier,
		fieldsPresent("event-feed", "list-event-feeds"),
		"fields marked as mutually exclusive were set: ('event-feed' and 'list-event-feeds')",
	)
}
