package field

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func fieldsPresent(fieldNames ...string) map[string]string {
	output := make(map[string]string)
	for _, fieldName := range fieldNames {
		output[fieldName] = "1"
	}
	return output
}

func AssertInvalidRelationshipConstraint(t *testing.T, configSchema Configuration) {
	AssertOutcome(
		t,
		configSchema,
		fieldsPresent("required"),
		"invalid relationship constraint",
	)
}

func AssertOutcome(
	t *testing.T,
	configSchema Configuration,
	config map[string]string,
	expectedErr string,
) {
	v := viper.New()
	for key, value := range config {
		v.Set(key, value)
	}
	err := Validate(configSchema, v)
	if expectedErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, expectedErr)
	}
}

func TestValidate(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")
	baz := StringSliceField("baz")
	required := StringField("required", WithRequired(true))

	t.Run("no relationships", func(t *testing.T) {
		carrier := Configuration{
			Fields: []SchemaField{required, foo},
		}

		t.Run("should NOT error when config is valid", func(t *testing.T) {
			AssertOutcome(t, carrier, fieldsPresent("required"), "")
		})

		t.Run("should error when a REQUIRED field is missing", func(t *testing.T) {
			AssertOutcome(
				t,
				carrier,
				nil,
				"errors found:\nfield required of type string is marked as required but it has a zero-value",
			)
		})
	})

	t.Run("should error when mutually exclusive relationship has ONE field", func(t *testing.T) {
		AssertInvalidRelationshipConstraint(
			t,
			Configuration{
				Fields: []SchemaField{foo},
				Constraints: []SchemaFieldRelationship{
					FieldsMutuallyExclusive(foo),
				},
			},
		)
	})

	t.Run("should error when mutually exclusive relationship has DUPLICATE field", func(t *testing.T) {
		AssertInvalidRelationshipConstraint(
			t,
			Configuration{
				Fields: []SchemaField{foo},
				Constraints: []SchemaFieldRelationship{
					FieldsMutuallyExclusive(foo, foo),
				},
			},
		)
	})

	t.Run("should error when mutually exclusive relationship and any fields are REQUIRED", func(t *testing.T) {
		AssertInvalidRelationshipConstraint(
			t,
			Configuration{
				Fields: []SchemaField{foo, required},
				Constraints: []SchemaFieldRelationship{
					FieldsMutuallyExclusive(foo, required),
				},
			},
		)
	})

	t.Run("mutually exclusive relationship", func(t *testing.T) {
		carrier := Configuration{
			Fields: []SchemaField{foo, bar},
			Constraints: []SchemaFieldRelationship{
				FieldsMutuallyExclusive(foo, bar),
			},
		}

		t.Run("should error when multiple values are present", func(t *testing.T) {
			AssertOutcome(
				t,
				carrier,
				fieldsPresent("foo", "bar"),
				"fields marked as mutually exclusive were set: ('foo' and 'bar')",
			)
		})

		t.Run("should NOT error when only one value is present", func(t *testing.T) {
			AssertOutcome(
				t,
				carrier,
				fieldsPresent("foo"),
				"",
			)
		})

		t.Run("should not error when NO values are present", func(t *testing.T) {
			AssertOutcome(t, carrier, nil, "")
		})
	})

	t.Run("should error when required together relationship has DUPLICATE field", func(t *testing.T) {
		AssertInvalidRelationshipConstraint(
			t,
			Configuration{
				Fields: []SchemaField{foo},
				Constraints: []SchemaFieldRelationship{
					FieldsRequiredTogether(foo, foo),
				},
			},
		)
	})

	t.Run("required together relationship", func(t *testing.T) {
		carrier := Configuration{
			Fields: []SchemaField{foo, bar},
			Constraints: []SchemaFieldRelationship{
				FieldsRequiredTogether(foo, bar),
			},
		}

		t.Run("should NOT error when ALL fields are MISSING", func(t *testing.T) {
			AssertOutcome(t, carrier, nil, "")
		})

		t.Run("should NOT error when ALL fields are present", func(t *testing.T) {
			AssertOutcome(
				t,
				carrier,
				fieldsPresent("foo", "bar"),
				"",
			)
		})

		t.Run("should error when one field is missing", func(t *testing.T) {
			AssertOutcome(
				t,
				carrier,
				fieldsPresent("foo"),
				"fields marked as needed together are missing: ('bar')",
			)
		})
	})

	t.Run("at least one used relationship", func(t *testing.T) {
		carrier := Configuration{
			Fields: []SchemaField{foo, bar},
			Constraints: []SchemaFieldRelationship{
				FieldsAtLeastOneUsed(bar, foo),
			},
		}

		t.Run("should not error when NO fields are missing", func(t *testing.T) {
			AssertOutcome(
				t,
				carrier,
				fieldsPresent("foo", "bar"),
				"",
			)
		})

		t.Run("should error when all fields are missing", func(t *testing.T) {
			AssertOutcome(
				t,
				carrier,
				nil,
				"at least one field was expected, any of: ('bar' and 'foo')",
			)
		})
	})

	t.Run("dependency relationship", func(t *testing.T) {
		carrier := Configuration{
			Fields: []SchemaField{foo, bar, baz},
			Constraints: []SchemaFieldRelationship{
				FieldsDependentOn(
					[]SchemaField{foo},
					[]SchemaField{bar, baz},
				),
			},
		}

		testCases := []struct {
			fields   string
			expected string
		}{
			{"foo bar baz", ""},
			{"foo bar", "set fields ('foo') are dependent on ('baz') being set"},
			{"foo", "set fields ('foo') are dependent on ('bar' and 'baz') being set"},
			{"bar baz", ""},
			{"bar", ""},
			{"baz", ""},
			{"", ""},
		}
		for _, testCase := range testCases {
			t.Run(testCase.fields, func(t *testing.T) {
				config := fieldsPresent(strings.Split(testCase.fields, " ")...)
				AssertOutcome(t, carrier, config, testCase.expected)
			})
		}
	})
}
