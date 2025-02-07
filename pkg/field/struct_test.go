package field

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConfiguration_MarshalJSON(t *testing.T) {
	ss := StringField("ss", WithDescription("Field 1"), WithDefaultValue("default"))
	intF := IntField("if", WithDescription("Field 2"), WithDefaultValue(42))
	bf := BoolField("bf", WithDescription("Field 3"), WithDefaultValue(true))
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

	fmt.Printf("JSON Output: %s\n", data)
	expected := `{"Fields":[{"FieldName":"ss","FieldType":"StringField","CLIShortHand":"","Required":false,"Hidden":false,"Persistent":false,"Description":"Field 1","Secret":false,"HelpURL":"","DisplayName":"","Placeholder":"","Rules":{},"DefaultValue":"default"},{"FieldName":"if","FieldType":"IntField","CLIShortHand":"","Required":false,"Hidden":false,"Persistent":false,"Description":"Field 2","Secret":false,"HelpURL":"","DisplayName":"","Placeholder":"","Rules":{},"DefaultValue":42},{"FieldName":"bf","FieldType":"BoolField","CLIShortHand":"","Required":false,"Hidden":false,"Persistent":false,"Description":"Field 3","Secret":false,"HelpURL":"","DisplayName":"","Placeholder":"","Rules":{},"DefaultValue":true},{"FieldName":"ssf","FieldType":"StringSliceField","CLIShortHand":"","Required":false,"Hidden":false,"Persistent":false,"Description":"Field 4","Secret":false,"HelpURL":"","DisplayName":"","Placeholder":"","Rules":{},"DefaultValue":["default"]}],"Constraints":[]}`
	if string(data) != expected {
		t.Errorf("Expected JSON: %s, but got: %s", expected, string(data))
	}
}
