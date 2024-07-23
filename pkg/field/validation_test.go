package field

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestValidateRequiredFieldsNotFound(t *testing.T) {
	carrier := Configuration{
		Fields: []SchemaField{
			StringField("foo", WithRequired(true)),
			StringField("bar", WithRequired(false)),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "")
	v.Set("bar", "")

	err := Validate(carrier, v)
	require.Error(t, err)
	require.EqualError(t, err, "errors found:\nfield foo of type string is marked as required but it has a zero-value")
}

func TestValidateRelationshipMutuallyExclusiveAllPresent(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
		},
		Constraints: []SchemaFieldRelationshipI{
			FieldsMutuallyExclusive(foo, bar),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "hello")
	v.Set("bar", "world")

	err := Validate(carrier, v)
	require.Error(t, err)
	require.EqualError(t, err, "fields marked as mutually exclusive were set: foo, bar")
}

func TestValidationRequiredTogetherOneMissing(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
		},
		Constraints: []SchemaFieldRelationshipI{
			FieldsRequiredTogether(foo, bar),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "hello")
	v.Set("bar", "")

	err := Validate(carrier, v)
	require.Error(t, err)
	require.EqualError(t, err, "fields marked as needed together are missing: bar")
}

func TestValidationRequiredTogetherAllMissing(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
		},
		Constraints: []SchemaFieldRelationshipI{
			FieldsRequiredTogether(foo, bar),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "")
	v.Set("bar", "")

	err := Validate(carrier, v)
	require.NoError(t, err)
}

func TestValidationDependentFieldsRequiredPresent(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
		},
		Constraints: []SchemaFieldRelationshipI{
			FieldsDependentOn([]SchemaField{foo}, []SchemaField{bar}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "fooField")
	v.Set("bar", "barField")

	err := Validate(carrier, v)
	require.NoError(t, err)
}

func TestValidationDependentFieldsBoolRequiredNotPresent(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
		},
		Constraints: []SchemaFieldRelationshipI{
			FieldsDependentOn([]SchemaField{foo}, []SchemaField{bar}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("bar", "barSet")

	err := Validate(carrier, v)
	require.Error(t, err)
}

func TestValidationDependentFieldsBoolRequiredPresent(t *testing.T) {
	foo := BoolField("boolField")
	bar := StringField("bar")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
		},
		Constraints: []SchemaFieldRelationshipI{
			FieldsDependentOn([]SchemaField{foo}, []SchemaField{bar}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("boolField", "true")
	v.Set("stringField", "something")

	err := Validate(carrier, v)
	require.NoError(t, err)
}

func TestValidationDependentFieldsRequiredNotPresent(t *testing.T) {
	foo := BoolField("boolField")
	bar := StringField("stringField")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
		},
		Constraints: []SchemaFieldRelationshipI{
			FieldsDependentOn([]SchemaField{foo}, []SchemaField{bar}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("stringField", "stringFieldSet")

	err := Validate(carrier, v)
	require.Error(t, err)
}
