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

	fields := []SchemaField{ss, intF, bf, ssf}
	constraints := []SchemaFieldRelationship{
		FieldsMutuallyExclusive(ss, intF),
		FieldsRequiredTogether(bf, ssf),
	}

	config := NewConfiguration(fields,
		WithSupportsExternalResources(true),
		WithConstraints(constraints...),
	)

	data, err := json.Marshal(&config)
	if err != nil {
		t.Fatalf("Failed to marshal Configuration: %v", err)
	}

	expected := `{
		"fields": [
			{
				"name": "ss",
				"description": "Field 1",
				"stringField": {
					"defaultValue": "default"
				}
			},
			{
				"name": "if",
				"description": "Field 2",
				"intField": {
					"defaultValue": "42"
				}
			},
			{
				"name": "bf",
				"description": "Field 3",
				"boolField": {}
			},
			{
				"name": "ssf",
				"description": "Field 4",
				"stringSliceField": {
					"defaultValue": [
						"default"
					]
				}
			}
		],
		"constraints": [
			{
				"kind": "CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE",
				"fieldNames": [
					"ss",
					"if"
				]
			},
			{
				"kind": "CONSTRAINT_KIND_REQUIRED_TOGETHER",
				"fieldNames": [
					"bf",
					"ssf"
				]
			}
		],
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
