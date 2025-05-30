package tests

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func TestEntryPoint(t *testing.T) {
	// We want a context that is already DeadlineExceeded so that entrypoint() doesn't hang
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	stringRequiredField := field.StringField(
		"string-field",
		field.WithRequired(true),
	)
	intRequiredField := field.IntField(
		"int-field",
		field.WithRequired(true),
	)
	stringField := field.StringField("string-field")
	boolField := field.BoolField("bool-field")

	t.Run("should receive values from CLI", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				stringRequiredField,
				intRequiredField,
				boolField,
			},
		)

		v, err := entrypoint(
			ctx,
			carrier,
			"--string-field",
			"foo",
			"--int-field",
			"100",
			"--bool-field",
		)

		require.NoError(t, err)
		require.EqualValues(t, "foo", v.GetString("string-field"))
		require.EqualValues(t, 100, v.GetInt("int-field"))
		require.EqualValues(t, true, v.GetBool("bool-field"))
	})

	t.Run("should receive values from ENVVARS", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				stringRequiredField,
				intRequiredField,
				boolField,
			},
		)

		// set envvars
		t.Setenv("BATON_STRING_FIELD", "bar")
		t.Setenv("BATON_INT_FIELD", "200")
		t.Setenv("BATON_BOOL_FIELD", "true")
		v, err := entrypoint(ctx, carrier)

		require.NoError(t, err)
		require.EqualValues(t, "bar", v.GetString("string-field"))
		require.EqualValues(t, 200, v.GetInt("int-field"))
		require.EqualValues(t, true, v.GetBool("bool-field"))
	})

	t.Run("should error when required values are absent", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				stringRequiredField,
				intRequiredField,
				boolField,
			},
		)

		_, err := entrypoint(ctx, carrier)

		require.Error(t, err)
		require.EqualError(t, err, "(Cobra) Execute failed: required flag(s) \"int-field\", \"string-field\" not set")
	})

	t.Run("should error when default field is copied", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				field.StringField(
					"client-id",
					field.WithRequired(true),
				),
			},
		)

		_, err := entrypoint(ctx, carrier)

		require.Error(t, err)
		require.EqualError(t, err, "DefineConfiguration failed: multiple fields with the same name: "+
			"client-id.If you want to use a default field in the SDK, use ExportAs on the connector schema field")
	})

	t.Run("should not error when default field is retargeted", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				field.ListTicketSchemasField.ExportAs(field.ExportTargetGUI),
			},
		)

		_, err := entrypoint(ctx, carrier)
		require.NoError(t, err)
	})

	t.Run("should error when fields are required together", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{stringField, boolField},
			field.WithConstraints(field.FieldsRequiredTogether(stringField, boolField)),
		)

		_, err := entrypoint(ctx, carrier, "--string-field", "foo")

		require.Error(t, err)
		require.EqualError(t, err, "(Cobra) Execute failed: if any flags in the group [string-field bool-field] are set they must all be set; missing [bool-field]")
	})

	t.Run("should error when fields are mutually exclusive", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{stringField, boolField},
			field.WithConstraints(field.FieldsMutuallyExclusive(stringField, boolField)),
		)

		_, err := entrypoint(
			ctx,
			carrier,
			"--string-field",
			"foo",
			"--bool-field",
		)

		require.Error(t, err)
		require.EqualError(t, err, "(Cobra) Execute failed: if any flags in the group [string-field bool-field] are set none of the others can be; [bool-field string-field] were all set")
	})

	t.Run("should error when fields are dependent", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{stringField, boolField},
			field.WithConstraints(field.FieldsDependentOn(
				[]field.SchemaField{stringField},
				[]field.SchemaField{boolField},
			)),
		)

		_, err := entrypoint(ctx, carrier, "--string-field", "foo")

		require.Error(t, err)
		require.EqualError(t, err, "(Cobra) Execute failed: set fields ('string-field') are dependent on ('bool-field') being set")
	})

	t.Run("should error when at least one field must be set", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{stringField, boolField},
			field.WithConstraints(field.FieldsAtLeastOneUsed(stringField, boolField)),
		)

		_, err := entrypoint(ctx, carrier)

		require.Error(t, err)
		require.EqualError(t, err, "(Cobra) Execute failed: at least one of the flags in the group [string-field bool-field] is required")
	})
}
