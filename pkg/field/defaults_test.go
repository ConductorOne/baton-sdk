package field

import (
	"math"
	"testing"

	"github.com/spf13/viper"
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

	// No policy ceiling: this is a manual-testing knob. The proto's own
	// `lte: 1000` on ListEventsRequest.page_size is what rejects oversized
	// pages, at the RPC boundary rather than here.
	t.Run("values above 1000 are not rejected by the field", func(t *testing.T) {
		require.NoError(t, run(1001))
		require.NoError(t, run(100000))
		require.NoError(t, run(math.MaxUint32))
	})

	// The one bound that remains is representability -- page_size is a
	// uint32 on the wire, and silently truncating 1<<32 to 0 would flip the
	// meaning to "use the connector default".
	t.Run("values above MaxUint32 are rejected so they cannot truncate", func(t *testing.T) {
		require.Error(t, run(math.MaxUint32+1))
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

// defaultSchema is the schema every connector effectively runs with.
func defaultSchema() Configuration {
	return Configuration{
		Fields:      DefaultFields,
		Constraints: DefaultRelationships,
	}
}

// TestDefaultFields_NamesAreUnique guards the duplicate-field path in
// pkg/config: a duplicated name in DefaultFields makes
// DefineConfiguration fail with ErrDuplicateField for every connector.
func TestDefaultFields_NamesAreUnique(t *testing.T) {
	seen := make(map[string]int, len(DefaultFields))
	for _, f := range DefaultFields {
		seen[f.FieldName]++
	}
	for name, count := range seen {
		require.Equalf(t, 1, count, "default field %q is declared %d times", name, count)
	}
}

// TestDefaultRelationships_AllValid is the important one. The
// FieldsMutuallyExclusive/FieldsRequiredTogether constructors do not return an
// error on bad input -- they silently yield SchemaFieldRelationship{Kind:
// Invalid}, and validateConstraints then fails EVERY connector run with
// "invalid relationship constraint". A duplicated field in one of the groups
// (easy to do when adding a flag to several groups at once) is exactly how that
// happens, so assert no default relationship is Invalid.
func TestDefaultRelationships_AllValid(t *testing.T) {
	for i, r := range DefaultRelationships {
		require.NotEqualf(t, Invalid, r.Kind,
			"DefaultRelationships[%d] is Invalid (duplicate field in the group, a Required field, or fewer than 2 unique fields); "+
				"this would break every connector run", i)
	}
}

// TestValidate_DefaultSchemaWithNothingSet proves the default schema is
// satisfiable with no flags at all -- i.e. none of the added fields is
// accidentally required or spuriously "present".
func TestValidate_DefaultSchemaWithNothingSet(t *testing.T) {
	AssertOutcome(t, defaultSchema(), nil, "")
}

// TestValidate_ExistingOnDemandFlagsStillValidateAlone guards against the new
// flags having been added to a mutually-exclusive group in a way that breaks
// the pre-existing triggers. Each case sets one trigger plus whatever
// FieldsRequiredTogether demands of it, and expects no error.
func TestValidate_ExistingOnDemandFlagsStillValidateAlone(t *testing.T) {
	cases := map[string][]string{
		"grant":               {"grant-entitlement", "grant-principal"},
		"revoke":              {"revoke-grant"},
		"create-account":      {"create-account-login", "create-account-email"},
		"delete-resource":     {"delete-resource", "delete-resource-type"},
		"rotate-credentials":  {"rotate-credentials", "rotate-credentials-type"},
		"event-feed":          {"event-feed"},
		"create-ticket":       {"create-ticket", "ticket-template-path"},
		"bulk-create-ticket":  {"bulk-create-ticket", "bulk-ticket-template-path"},
		"get-ticket":          {"get-ticket", "ticket-id"},
		"list-ticket-schemas": {"list-ticket-schemas"},
		"diff-syncs":          {"diff-syncs", "base-sync-id", "applied-sync-id"},
		"compact-syncs":       {"compact-syncs", "compact-sync-ids", "compact-file-paths", "compact-output-path"},
		"list-action-schemas": {"list-action-schemas"},
		"list-event-feeds":    {"list-event-feeds"},
	}

	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			AssertOutcome(t, defaultSchema(), fieldsPresent(fields...), "")
		})
	}
}

// TestValidate_EventFeedPageSizeDoesNotTripOnDemandExclusivity checks that the
// new value-carrying flags are NOT part of the mutually-exclusive trigger
// groups: passing a page size alongside the trigger it configures must be
// legal, and so must passing it alongside an unrelated trigger.
func TestValidate_EventFeedPageSizeDoesNotTripOnDemandExclusivity(t *testing.T) {
	t.Run("with the event-feed trigger it configures", func(t *testing.T) {
		AssertOutcome(t, defaultSchema(), fieldsPresent("event-feed", "event-feed-page-size", "event-feed-id", "event-feed-cursor"), "")
	})

	t.Run("alongside an unrelated trigger", func(t *testing.T) {
		AssertOutcome(t, defaultSchema(), fieldsPresent("list-ticket-schemas", "event-feed-page-size"), "")
	})
}

// TestValidate_EventFeedBoolFalseIsNotPresent covers a behavior change from
// making event-feed a BoolField. As a StringField, --event-feed=false was
// "present" (a non-empty string), so it wrongly tripped mutual exclusivity
// against other on-demand flags. As a BoolField, presence is the bool's own
// value, so an explicit false correctly does not.
func TestValidate_EventFeedBoolFalseIsNotPresent(t *testing.T) {
	v := viper.New()
	v.Set("event-feed", false)
	v.Set("revoke-grant", "some-grant-id")

	require.NoError(t, Validate(defaultSchema(), v))
}
