package tests

import (
	"os"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func TestPassingValuesToFields(t *testing.T) {
	stringfield := field.StringField("string-field", field.WithRequired(true))
	intfield := field.IntField("int-field", field.WithRequired(true))
	boolfield := field.BoolField("bool-field")
	stringslicefield := field.StringSliceField("string-slice-field", field.WithRequired(true))

	carrier := field.NewConfiguration([]field.SchemaField{stringfield, intfield, boolfield, stringslicefield})

	os.Args = []string{"cmd",
		"--string-field", "foo",
		"--int-field", "100",
		"--bool-field",
		`--string-slice-field="foo,bar,foobarr"`,
	}

	v, err := entrypoint(carrier)

	require.NoError(t, err)
	require.EqualValues(t, "foo", v.GetString("string-field"))
	require.EqualValues(t, 100, v.GetInt("int-field"))
	require.EqualValues(t, true, v.GetBool("bool-field"))
	require.EqualValues(t, []string{"foo", "bar", "foobar"}, v.GetStringSlice("string-slice-field"))
}
