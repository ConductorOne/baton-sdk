package tests

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func duplicateDefaultFieldError(fieldName string) string {
	return "DefineConfiguration failed: multiple fields with the same name: " +
		fieldName +
		". If you want to use a default field in the SDK, use ExportAs on the connector schema field"
}

func duplicateFieldError(fieldName string) string {
	return "DefineConfiguration failed: multiple fields with the same name: " + fieldName
}

func TestEntryPoint(t *testing.T) {
	// We want a context that is already DeadlineExceeded so that entrypoint() doesn't hang
	ctx, cancel := context.WithTimeout(t.Context(), 0)
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
		require.ErrorIs(t, err, config.ErrDuplicateField)
		require.EqualError(t, err, duplicateDefaultFieldError("client-id"))
	})

	t.Run("should error when fields have the same name", func(t *testing.T) {
		field1 := field.StringField("string-field")
		field2 := field.StringField("string-field")
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				field1,
				field2.ExportAs(field.ExportTargetGUI),
			},
		)
		_, err := entrypoint(ctx, carrier)
		require.Error(t, err)
		require.ErrorIs(t, err, config.ErrDuplicateField)
		require.EqualError(t, err, duplicateFieldError("string-field"))
	})

	t.Run("should not error when default field is retargeted", func(t *testing.T) {
		// Copy the field so that we can export it as GUI
		listTicketSchemasField := field.ListTicketSchemasField
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				listTicketSchemasField.ExportAs(field.ExportTargetGUI),
			},
		)

		found := false
		for _, f := range carrier.Fields {
			if f.FieldName == listTicketSchemasField.FieldName {
				require.Equal(t, field.ExportTargetGUI, f.ExportTarget)
				require.Equal(t, true, f.WasReExported)
				found = true
			}
		}

		require.True(t, found)
		require.Equal(t, field.ListTicketSchemasField.FieldName, listTicketSchemasField.FieldName)
		require.Equal(t, false, field.ListTicketSchemasField.WasReExported)

		_, err := entrypoint(ctx, carrier)
		require.NoError(t, err)
	})

	t.Run("should error when default field is exported twice", func(t *testing.T) {
		listTicketSchemasField := field.ListTicketSchemasField
		listTicketSchemasField2 := field.ListTicketSchemasField
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				listTicketSchemasField.ExportAs(field.ExportTargetGUI),
				listTicketSchemasField2,
			},
		)

		_, err := entrypoint(ctx, carrier)
		require.Error(t, err)
		require.ErrorIs(t, err, config.ErrDuplicateField)
		require.EqualError(t, err, duplicateDefaultFieldError("list-ticket-schemas"))
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

	t.Run("should error when required values are absent group field", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				stringRequiredField,
				intRequiredField,
			},
			field.WithFieldGroups(
				[]field.SchemaFieldGroup{
					{
						Name:   "group1",
						Fields: []field.SchemaField{stringRequiredField},
					},
					{
						Name:   "group2",
						Fields: []field.SchemaField{intRequiredField},
					},
				},
			),
		)

		_, err := entrypoint(
			ctx,
			carrier,
			"--auth-method",
			"group1",
		)

		require.Error(t, err)
		require.EqualError(t, err, "(Cobra) Execute failed: errors found:\nfield string-field of type string is marked as required but it has a zero-value")
	})
}
