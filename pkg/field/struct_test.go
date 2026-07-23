package field

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func normalizeJSON(jsonStr string) (string, error) {
	var obj map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &obj)
	if err != nil {
		return "", err
	}

	normalized, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}

	return string(normalized), nil
}
func TestConfiguration_MarshalJSON(t *testing.T) {
	ss := StringField("ss", WithDescription("Field 1"), WithDefaultValue("default"))
	intF := IntField("if", WithDescription("Field 2"), WithDefaultValue(42))
	bf := BoolField("bf", WithDescription("Field 3"))
	ssf := StringSliceField("ssf", WithDescription("Field 4"), WithDefaultValue([]string{"default"}))
	smf := StringMapField("smf", WithDescription("Field 5"), WithDefaultValue(map[string]any{"key": "value"}))

	fields := []SchemaField{ss, intF, bf, ssf, smf}
	constraints := []SchemaFieldRelationship{
		FieldsMutuallyExclusive(ss, intF),
		FieldsRequiredTogether(bf, ssf),
	}

	config := NewConfiguration(fields,
		WithConnectorDisplayName("Great Connector"),
		WithHelpUrl("https://great-connector.com/help"),
		WithIconUrl("https://great-connector.com/icon.png"),
		WithCatalogId("ABC123"),
		WithIsDirectory(false),
		WithSupportsExternalResources(true),
		WithConstraints(constraints...),
		WithFieldGroups([]SchemaFieldGroup{
			{
				Name:        "group1",
				DisplayName: "Group 1",
				HelpText:    "This is group 1",
				Fields:      []SchemaField{StringField("onlyInGroup1"), StringField("onlyInGroup2")},
				Default:     true,
			},
		}),
	)

	data, err := json.Marshal(&config)
	require.NoError(t, err, "Failed to marshal Configuration")

	expected := `{
    "catalogId": "ABC123",
    "constraints": [
        {
            "fieldNames": [
                "ss",
                "if"
            ],
            "kind": "CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE"
        },
        {
            "fieldNames": [
                "bf",
                "ssf"
            ],
            "kind": "CONSTRAINT_KIND_REQUIRED_TOGETHER"
        }
    ],
    "displayName": "Great Connector",
    "fieldGroups": [
        {
            "displayName": "Group 1",
            "fields": [
                "onlyInGroup1",
                "onlyInGroup2"
            ],
            "helpText": "This is group 1",
            "name": "group1",
            "default": true
        }
    ],
    "fields": [
        {
            "description": "Field 1",
            "name": "ss",
            "stringField": {
                "defaultValue": "default"
            }
        },
        {
            "description": "Field 2",
            "intField": {
                "defaultValue": "42"
            },
            "name": "if"
        },
        {
            "boolField": {},
            "description": "Field 3",
            "name": "bf"
        },
        {
            "description": "Field 4",
            "name": "ssf",
            "stringSliceField": {
                "defaultValue": [
                    "default"
                ]
            }
        },
        {
            "description": "Field 5",
            "name": "smf",
            "stringMapField": {
                "defaultValue": {
                    "key": {
                        "@type": "type.googleapis.com/google.protobuf.Value",
                        "value": "value"
                    }
                }
            }
        }
    ],
    "helpUrl": "https://great-connector.com/help",
    "iconUrl": "https://great-connector.com/icon.png",
    "supportsExternalResources": true
}`

	n1, err := normalizeJSON(expected)
	require.NoError(t, err, "Failed to normalize json")
	n2, err := normalizeJSON(string(data))
	require.NoError(t, err, "Failed to normalize json")
	require.Equal(t, n1, n2, "Expected JSON: \n%s\n\n but got: \n%s\n", n1, n2)
}

func TestSuggestedValue(t *testing.T) {
	// A field configured with WithSuggestedValue should export the suggested
	// value in the distinct suggested_value field (so the GUI pre-populates it)
	// while leaving the schema default_value empty. The runtime/flag default
	// (DefaultValue) also stays at the type's zero value.
	suggested := StringSliceField("noun", WithSuggestedValue([]string{"space"}))

	require.Empty(t, suggested.DefaultValue, "SuggestedValue must leave the runtime DefaultValue at its zero value")

	// Runtime/flag default path: unchanged, so nothing gets injected at runtime.
	runtimeDefault, err := GetDefaultValue[[]string](suggested)
	require.NoError(t, err)
	require.Empty(t, *runtimeDefault, "runtime default should be empty for a suggested-only field")

	v1, err := schemaFieldToV1(suggested)
	require.NoError(t, err)
	// suggested_value carries the suggestion; default_value stays empty (no
	// longer folded together).
	require.Empty(t, v1.GetStringSliceField().GetDefaultValue(), "default_value must be empty for a suggested-only field")
	require.Equal(t, []string{"space"}, v1.GetStringSliceField().GetSuggestedValue())
}

func TestDefaultValueOnlyExport(t *testing.T) {
	// A field built with WithDefaultValue exports default_value populated and
	// suggested_value empty.
	def := StringSliceField("noun", WithDefaultValue([]string{"runtime"}))

	v1, err := schemaFieldToV1(def)
	require.NoError(t, err)
	require.Equal(t, []string{"runtime"}, v1.GetStringSliceField().GetDefaultValue())
	require.Empty(t, v1.GetStringSliceField().GetSuggestedValue(), "suggested_value must be empty for a default-only field")
}

func TestSuggestedValuePrecedence(t *testing.T) {
	// When both are set, WithSuggestedValue wins for schema export while
	// WithDefaultValue continues to govern the CLI/runtime flag default.
	both := StringSliceField("noun",
		WithDefaultValue([]string{"runtime"}),
		WithSuggestedValue([]string{"suggested"}),
	)

	runtimeDefault, err := GetDefaultValue[[]string](both)
	require.NoError(t, err)
	require.Equal(t, []string{"runtime"}, *runtimeDefault)

	exported, err := GetExportedDefaultValue[[]string](both)
	require.NoError(t, err)
	require.Equal(t, []string{"suggested"}, *exported)
}
