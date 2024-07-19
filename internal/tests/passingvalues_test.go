package tests

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func TestPassingValuesToFieldsViaCLI(t *testing.T) {
	stringfield := field.StringField("string-field", field.WithRequired(true))
	intfield := field.IntField("int-field", field.WithRequired(true))
	boolfield := field.BoolField("bool-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield, boolfield})

	v, err := entrypoint(carrier,
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
	stringfield := field.StringField("string-field", field.WithRequired(true))
	intfield := field.IntField("int-field", field.WithRequired(true))
	boolfield := field.BoolField("bool-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield, boolfield})

	// set envvars
	t.Setenv("BATON_STRING_FIELD", "bar")
	t.Setenv("BATON_INT_FIELD", "200")
	t.Setenv("BATON_BOOL_FIELD", "true")
	v, err := entrypoint(carrier)

	require.NoError(t, err)
	require.EqualValues(t, "bar", v.GetString("string-field"))
	require.EqualValues(t, 200, v.GetInt("int-field"))
	require.EqualValues(t, true, v.GetBool("bool-field"))
}
