package field

import (
	"strings"
	"testing"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
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

func sP(s string) *string {
	return &s
}

func uintP(u uint64) *uint64 {
	return &u
}

func intP(u int) *int64 {
	i := int64(u)
	return &i
}

func TestIntRules_Validate(t *testing.T) {
	run := func(value int, r *v1_conf.Int64Rules) error {
		return ValidateIntRules(r, value, "TestField")
	}

	t.Run("valid value", func(t *testing.T) {
		err := run(40, &v1_conf.Int64Rules{
			Gt:    intP(30),
			Gte:   intP(40),
			Lt:    intP(50),
			Lte:   intP(60),
			In:    []int64{10, 20, 30, 40, 50},
			NotIn: []int64{60, 70, 80, 90, 100},
		})
		require.NoError(t, err)
	})

	t.Run("required field with zero-value", func(t *testing.T) {
		err := run(0, &v1_conf.Int64Rules{IsRequired: true})
		require.EqualError(t, err, "field TestField of type int is marked as required but it has a zero-value")
	})

	t.Run("value not equal to expected", func(t *testing.T) {
		err := run(42, &v1_conf.Int64Rules{Eq: intP(50)})
		require.EqualError(t, err, "field TestField: expected 50 but got 42")
	})

	t.Run("value not greater than expected", func(t *testing.T) {
		err := run(20, &v1_conf.Int64Rules{Gt: intP(30)})
		require.EqualError(t, err, "field TestField: value must be greater than 30 but got 20")
	})

	t.Run("value not greater than or equal to expected", func(t *testing.T) {
		err := run(30, &v1_conf.Int64Rules{Gte: intP(40)})
		require.EqualError(t, err, "field TestField: value must be greater than or equal to 40 but got 30")
	})

	t.Run("value not less than expected", func(t *testing.T) {
		err := run(60, &v1_conf.Int64Rules{Lt: intP(50)})
		require.EqualError(t, err, "field TestField: value must be less than 50 but got 60")
	})

	t.Run("value not less than or equal to expected", func(t *testing.T) {
		err := run(60, &v1_conf.Int64Rules{Lte: intP(50)})
		require.EqualError(t, err, "field TestField: value must be less than or equal to 50 but got 60")
	})

	t.Run("value is not one of the expected values", func(t *testing.T) {
		err := run(42, &v1_conf.Int64Rules{In: []int64{10, 20, 30, 40, 50}})
		require.EqualError(t, err, "field TestField: value must be one of [10 20 30 40 50] but got 42")
	})

	t.Run("value is one of the not expected values", func(t *testing.T) {
		err := run(60, &v1_conf.Int64Rules{NotIn: []int64{60, 70, 80, 90, 100}})
		require.EqualError(t, err, "field TestField: value must not be one of [60 70 80 90 100] but got 60")
	})
}

func TestStringRules_Validate(t *testing.T) {
	run := func(value string, r *v1_conf.StringRules) error {
		return ValidateStringRules(r, value, "TestField")
	}

	t.Run("valid value", func(t *testing.T) {
		err := run("test", &v1_conf.StringRules{
			Pattern:  sP("^[a-z]+$"),
			Prefix:   sP("t"),
			Suffix:   sP("st"),
			Contains: sP("es"),
			In:       []string{"foo", "bar", "test"},
			NotIn:    []string{"foo", "bar"},
		})
		require.NoError(t, err)
	})

	t.Run("value contains only digits", func(t *testing.T) {
		err := run("12345", &v1_conf.StringRules{Pattern: sP("^[0-9]+$")})
		require.NoError(t, err)
	})

	t.Run("value starts with 'abc'", func(t *testing.T) {
		err := run("abcdef", &v1_conf.StringRules{Pattern: sP("^abc.*")})
		require.NoError(t, err)
	})

	t.Run("value ends with 'xyz'", func(t *testing.T) {
		err := run("pqrxyz", &v1_conf.StringRules{Pattern: sP(".*xyz$")})
		require.NoError(t, err)
	})

	t.Run("value matches a specific format", func(t *testing.T) {
		err := run("2022-01-01", &v1_conf.StringRules{Pattern: sP("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")})
		require.NoError(t, err)
	})

	t.Run("value does not match pattern", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{Pattern: sP("^[a-z]+$")})
		require.EqualError(t, err, "field TestField: value must match pattern ^[a-z]+$ but got '123'")
	})

	t.Run("value does not match pattern with special characters", func(t *testing.T) {
		err := run("123@", &v1_conf.StringRules{Pattern: sP("^[a-z]+$")})
		require.EqualError(t, err, "field TestField: value must match pattern ^[a-z]+$ but got '123@'")
	})

	t.Run("value does not match pattern with multiple conditions", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{Pattern: sP("^[a-z]+$")})
		require.EqualError(t, err, "field TestField: value must match pattern ^[a-z]+$ but got '123'")
	})

	t.Run("required field with zero-value", func(t *testing.T) {
		err := run("", &v1_conf.StringRules{IsRequired: true})
		require.EqualError(t, err, "field TestField of type string is marked as required but it has a zero-value")
	})

	t.Run("value not equal to expected", func(t *testing.T) {
		err := run("1", &v1_conf.StringRules{Eq: sP("12")})
		require.EqualError(t, err, "field TestField: expected '12' but got '1'")
	})

	t.Run("value length not equal to expected", func(t *testing.T) {
		err := run("123456", &v1_conf.StringRules{Len: uintP(5)})
		require.EqualError(t, err, "field TestField: value must be exactly 5 characters long but got 6")
	})

	t.Run("value length less than minimum", func(t *testing.T) {
		err := run("te", &v1_conf.StringRules{MinLen: uintP(10)})
		require.EqualError(t, err, "field TestField: value must be at least 10 characters long but got 2")
	})

	t.Run("value length greater than maximum", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{MaxLen: uintP(2)})
		require.EqualError(t, err, "field TestField: value must be at most 2 characters long but got 3")
	})

	t.Run("value does not match pattern", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{Pattern: sP("^[a-z]+$")})
		require.EqualError(t, err, "field TestField: value must match pattern ^[a-z]+$ but got '123'")
	})

	t.Run("value does not have expected prefix", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{Prefix: sP("pre")})
		require.EqualError(t, err, "field TestField: value must have prefix 'pre' but got '123'")
	})

	t.Run("value does not have expected suffix", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{Suffix: sP("suf")})
		require.EqualError(t, err, "field TestField: value must have suffix 'suf' but got '123'")
	})

	t.Run("value does not contain expected substring", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{Contains: sP("abc")})
		require.EqualError(t, err, "field TestField: value must contain 'abc' but got '123'")
	})

	t.Run("value is not one of the expected values", func(t *testing.T) {
		err := run("123", &v1_conf.StringRules{In: []string{"foo", "bar", "baz"}})
		require.EqualError(t, err, "field TestField: value must be one of [foo bar baz] but got '123'")
	})

	t.Run("ignore empty value", func(t *testing.T) {
		err := run("", &v1_conf.StringRules{Eq: sP("test")})
		require.NoError(t, err)
	})

	t.Run("value is a well-known URL", func(t *testing.T) {
		err := run("https://example.com", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_URI})
		require.NoError(t, err)
	})

	t.Run("value is not a well-known URL", func(t *testing.T) {
		err := run("example", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_URI})
		require.EqualError(t, err, "field TestField: value must be a valid URL but got 'example'")
	})

	t.Run("value is a well-known IP address", func(t *testing.T) {
		err := run("192.168.0.1", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_IP})
		require.NoError(t, err)
	})

	t.Run("value is not a well-known IP address", func(t *testing.T) {
		err := run("example", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_IP})
		require.EqualError(t, err, "field TestField: value must be a valid IP address but got 'example'")
	})

	t.Run("value is a well-known UUID", func(t *testing.T) {
		err := run("6ba7b810-9dad-11d1-80b4-00c04fd430c8", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_UUID})
		require.NoError(t, err)
	})

	t.Run("value is not a well-known UUID", func(t *testing.T) {
		err := run("example", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_UUID})
		require.EqualError(t, err, "field TestField: value must be a valid UUID but got 'example'")
	})

	t.Run("value is a well-known email address", func(t *testing.T) {
		err := run("test@example.com", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_EMAIL})
		require.NoError(t, err)
	})

	t.Run("value is not a well-known email address", func(t *testing.T) {
		err := run("test", &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_EMAIL})
		require.EqualError(t, err, "field TestField: value must be a valid email address but got 'test'")
	})

	t.Run("hostnames", func(t *testing.T) {
		valid := []string{
			"example.com",
			"192.168.0.1",
			"sub.domain.com",
		}

		invalid := []string{
			"example.com.",
			// "192.168.0.1.1", TODO(kans): Fixme!
			"-invalid.com",
			"invalid-.com",
			"a..b.com",
			"invalid@domain",
			"toolong." + strings.Repeat("a", 300) + ".com",
		}

		for _, v := range valid {
			err := run(v, &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_HOSTNAME})
			require.NoError(t, err)
		}

		for _, v := range invalid {
			err := run(v, &v1_conf.StringRules{WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_HOSTNAME})
			require.EqualError(t, err, "field TestField: value must be a valid hostname but got '"+v+"'")
		}
	})
}

func TestRepeatedRulesStringRules_Validate(t *testing.T) {
	run := func(value []string, r *v1_conf.RepeatedStringRules) error {
		return ValidateRepeatedStringRules(r, value, "TestField")
	}

	t.Run("not unique", func(t *testing.T) {
		err := run([]string{"a", "a"}, &v1_conf.RepeatedStringRules{Unique: true})
		require.EqualError(t, err, "field TestField: value must not contain duplicate items but got multiple \"a\"")
	})

	t.Run("empty input", func(t *testing.T) {
		err := run([]string{}, &v1_conf.RepeatedStringRules{IsRequired: true})
		require.EqualError(t, err, "field TestField of type []string is marked as required but it has a zero-value")
	})

	t.Run("fewer items than MinItems", func(t *testing.T) {
		err := run([]string{"a"}, &v1_conf.RepeatedStringRules{MinItems: uintP(2)})
		require.EqualError(t, err, "field TestField: value must have at least 2 items but got 1")
	})

	t.Run("more items than MaxItems", func(t *testing.T) {
		err := run([]string{"a", "b", "c", "d", "e", "f"}, &v1_conf.RepeatedStringRules{MaxItems: uintP(5)})
		require.EqualError(t, err, "field TestField: value must have at most 5 items but got 6")
	})

	t.Run("items not matching innner constraints", func(t *testing.T) {
		err := run([]string{"example", "invalid"}, &v1_conf.RepeatedStringRules{
			ItemRules: &v1_conf.StringRules{
				IsRequired: true,
				Eq:         sP("example"),
			},
		})
		require.EqualError(t, err, "field TestField invalid item at field 1: expected 'example' but got 'invalid'")
	})

	t.Run("items matching innner constraints", func(t *testing.T) {
		err := run([]string{"a@b.com", "a@b"}, &v1_conf.RepeatedStringRules{
			ItemRules: &v1_conf.StringRules{
				WellKnown: v1_conf.WellKnownString_WELL_KNOWN_STRING_EMAIL,
			},
		})
		require.NoError(t, err)
	})
}

func TestBoolValidation(t *testing.T) {
	run := func(value bool, r *v1_conf.BoolRules) error {
		return ValidateBoolRules(r, value, "TestField")
	}

	t.Run("valid true", func(t *testing.T) {
		err := run(true, &v1_conf.BoolRules{})
		require.NoError(t, err)
	})

	t.Run("valid false", func(t *testing.T) {
		err := run(false, &v1_conf.BoolRules{})
		require.NoError(t, err)
	})
}

func TestStringMapRules_Validate(t *testing.T) {
	run := func(value map[string]any, r *v1_conf.StringMapRules) error {
		return ValidateStringMapRules(r, value, "TestField")
	}

	t.Run("string map", func(t *testing.T) {
		err := run(map[string]any{"key1": "value1", "key2": "value2"}, &v1_conf.StringMapRules{})
		require.NoError(t, err)

		err = run(map[string]any{}, &v1_conf.StringMapRules{})
		require.NoError(t, err)
	})

	t.Run("required string map", func(t *testing.T) {
		err := run(map[string]any{}, &v1_conf.StringMapRules{
			IsRequired: true,
		})
		require.EqualError(t, err, "field TestField of type map[string]any is marked as required but it has a zero-value")
		err = run(map[string]any{"key1": "value1"}, &v1_conf.StringMapRules{
			IsRequired: true,
		})
		require.NoError(t, err)
	})
}

func TestFieldGroupMapping(t *testing.T) {
	t.Run("field group mapping", func(t *testing.T) {
		carrier := Configuration{
			Fields: []SchemaField{
				StringField("key"),
			},
			FieldGroups: []SchemaFieldGroup{
				{
					Name:        "group1",
					DisplayName: "Group 1",
					HelpText:    "This is group 1",
					Fields: []SchemaField{
						StringField("field1"),
						StringField("field2"),
					},
				},
			},
		}

		marshal, err := carrier.marshal()
		require.NoError(t, err)

		require.Len(t, marshal.FieldGroups, 1)
		require.Equal(t, "group1", marshal.FieldGroups[0].Name)
		require.Equal(t, "Group 1", marshal.FieldGroups[0].DisplayName)
		require.Equal(t, "This is group 1", marshal.FieldGroups[0].HelpText)
		require.Len(t, marshal.FieldGroups[0].Fields, 2)
	})
}
