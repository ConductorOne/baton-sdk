package tests

import (
	"context"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

const timeoutIn = time.Millisecond * 10

func TestPassingValuesToFieldsViaCLI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	stringfield := field.StringField("string-field", field.WithRequired(true))
	intfield := field.IntField("int-field", field.WithRequired(true))
	boolfield := field.BoolField("bool-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield, boolfield})

	v, err := entrypoint(ctx, carrier,
		"--string-field", "foo",
		"--int-field", "100",
		"--bool-field",
	)

	require.NoError(t, err)
	require.EqualValues(t, "foo", v.GetString("string-field"))
	require.EqualValues(t, 100, v.GetInt("int-field"))
	require.EqualValues(t, true, v.GetBool("bool-field"))
}

func TestPassingValuesToFieldsViaENVVARS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	stringfield := field.StringField("string-field", field.WithRequired(true))
	intfield := field.IntField("int-field", field.WithRequired(true))
	boolfield := field.BoolField("bool-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield, boolfield})

	// set envvars
	t.Setenv("BATON_STRING_FIELD", "bar")
	t.Setenv("BATON_INT_FIELD", "200")
	t.Setenv("BATON_BOOL_FIELD", "true")
	v, err := entrypoint(ctx, carrier)

	require.NoError(t, err)
	require.EqualValues(t, "bar", v.GetString("string-field"))
	require.EqualValues(t, 200, v.GetInt("int-field"))
	require.EqualValues(t, true, v.GetBool("bool-field"))
}

func TestRequiredValuesAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	stringfield := field.StringField("string-field", field.WithRequired(true))
	intfield := field.IntField("int-field", field.WithRequired(true))
	boolfield := field.BoolField("bool-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield, boolfield})

	_, err := entrypoint(ctx, carrier)

	require.Error(t, err)
	require.EqualError(t, err, "(Cobra) Execute failed: required flag(s) \"int-field\", \"string-field\" not set")
}

func TestTriggerRequiredTogetherError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	stringfield := field.StringField("string-field")
	intfield := field.IntField("int-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield},
		field.FieldsRequiredTogether(stringfield, intfield),
	)

	_, err := entrypoint(ctx, carrier, "--string-field", "foo")

	require.Error(t, err)
	require.EqualError(t, err, "(Cobra) Execute failed: if any flags in the group [string-field int-field] are set they must all be set; missing [int-field]")
}

func TestTriggerMutuallyExclusiveError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	stringfield := field.StringField("string-field")
	intfield := field.IntField("int-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield},
		field.FieldsMutuallyExclusive(stringfield, intfield),
	)

	_, err := entrypoint(ctx, carrier, "--string-field", "foo", "--int-field", "200")

	require.Error(t, err)
	require.EqualError(t, err, "(Cobra) Execute failed: if any flags in the group [string-field int-field] are set none of the others can be; [int-field string-field] were all set")
}

func TestTriggerDependsOnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	stringfield := field.StringField("string-field")
	intfield := field.IntField("int-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield},
		field.FieldsDependentOn([]field.SchemaField{stringfield}, []field.SchemaField{intfield}),
	)

	_, err := entrypoint(ctx, carrier, "--string-field", "foo")

	require.Error(t, err)
	require.EqualError(t, err, "(Cobra) Execute failed: set fields string-field are dependent on int-field being set")
}

func TestTriggerAtLeastOneError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	stringfield := field.StringField("string-field")
	intfield := field.IntField("int-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield},
		field.FieldsAtLeastOneUsed(stringfield, intfield),
	)

	_, err := entrypoint(ctx, carrier)

	require.Error(t, err)
	require.EqualError(t, err, "(Cobra) Execute failed: at least one of the flags in the group [string-field int-field] is required")
}
