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
		Constraints: []SchemaFieldRelationship{
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
		Constraints: []SchemaFieldRelationship{
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
		Constraints: []SchemaFieldRelationship{
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

func TestValidationDependentFieldsAllPresent(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")
	baz := StringField("baz")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
			baz,
		},
		Constraints: []SchemaFieldRelationship{
			FieldsDependOn([]SchemaField{foo}, []SchemaField{bar, baz}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "present")
	v.Set("bar", "present")
	v.Set("baz", "present")

	err := Validate(carrier, v)
	require.NoError(t, err)
}

func TestValidationDependentFieldsExpectedFieldBazMissing(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")
	baz := StringField("baz")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
			baz,
		},
		Constraints: []SchemaFieldRelationship{
			FieldsDependOn([]SchemaField{foo}, []SchemaField{bar, baz}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "present")
	v.Set("bar", "present")
	v.Set("baz", "")

	err := Validate(carrier, v)
	require.Error(t, err)
}

func TestValidationDependentFieldsExpectedFieldBazBarMissing(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")
	baz := StringField("baz")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
			baz,
		},
		Constraints: []SchemaFieldRelationship{
			FieldsDependOn([]SchemaField{foo}, []SchemaField{bar, baz}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "present")
	v.Set("bar", "")
	v.Set("baz", "")

	err := Validate(carrier, v)
	require.Error(t, err)
}

func TestValidationDependentFieldsDepedentFieldMissing(t *testing.T) {
	foo := StringField("foo")
	bar := StringField("bar")
	baz := StringField("baz")

	carrier := Configuration{
		Fields: []SchemaField{
			foo,
			bar,
			baz,
		},
		Constraints: []SchemaFieldRelationship{
			FieldsDependOn([]SchemaField{foo}, []SchemaField{bar, baz}),
		},
	}

	// create configuration using viper
	v := viper.New()
	v.Set("foo", "")
	v.Set("bar", "present")
	v.Set("baz", "present")

	err := Validate(carrier, v)
	require.NoError(t, err)
}
