package field

import (
	"encoding/json"
	"testing"
)

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

	config := NewConfiguration(fields, constraints...)

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal Configuration: %v", err)
	}

	// fmt.Printf("JSON Output: \n\n%s\n\n", data)
	expected := `{"Fields":[{"FieldName":"ss","FieldType":"StringField","Description":"Field 1","Rules":{},"DefaultValue":"default"},{"FieldName":"if","FieldType":"IntField","Description":"Field 2","Rules":{},"DefaultValue":42},{"FieldName":"bf","FieldType":"BoolField","Description":"Field 3","Rules":{}},{"FieldName":"ssf","FieldType":"StringSliceField","Description":"Field 4","Rules":{},"DefaultValue":["default"]}],"Constraints":[{"Kind":"MUTUALLY_EXCLUSIVE","Fields":["ss","if"]},{"Kind":"REQUIRED_TOGETHER","Fields":["bf","ssf"]}]}`
	if string(data) != expected {
		t.Errorf("Expected JSON: %s, but got: %s", expected, string(data))
	}
}
