package field

import (
	"encoding/json"
	"testing"
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
		WithFieldGroups([]FieldGroup{
			{
				Name:        "group1",
				DisplayName: "Group 1",
				HelpText:    "This is group 1",
				Fields:      []SchemaField{StringField("onlyInGroup1"), StringField("onlyInGroup2")},
			},
		}),
	)

	data, err := json.Marshal(&config)
	if err != nil {
		t.Fatalf("Failed to marshal Configuration: %v", err)
	}

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
            "name": "group1"
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
	if err != nil {
		t.Fatalf("Failed to normalize json: %v", err)
	}
	n2, err := normalizeJSON(string(data))
	if err != nil {
		t.Fatalf("Failed to normalize json: %v", err)
	}
	if n1 != n2 {
		t.Errorf("Expected JSON: \n%s\n\n but got: \n%s\n", n1, n2)
	}
}
