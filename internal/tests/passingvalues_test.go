package tests

import (
	"os"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func TestPassingValuesToFieldsViaCLI(t *testing.T) {
	stringfield := field.StringField("string-field", field.WithRequired(true))
	intfield := field.IntField("int-field", field.WithRequired(true))
	boolfield := field.BoolField("bool-field")

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield, boolfield})

	os.Args = []string{"cmd",
		"--string-field", "foo",
		"--int-field", "100",
		"--bool-field",
	}

	v, err := entrypoint(carrier)

	require.NoError(t, err)
	require.EqualValues(t, "foo", v.GetString("string-field"))
	require.EqualValues(t, 100, v.GetInt("int-field"))
	require.EqualValues(t, true, v.GetBool("bool-field"))
}
