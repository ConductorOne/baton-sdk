package field

import (
	"encoding/json"
	"testing"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
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

func TestExportTargetPublicValues(t *testing.T) {
	cases := []struct {
		target ExportTarget
		want   string
	}{
		{ExportTargetNone, "none"},
		{ExportTargetGUI, "gui"},
		{ExportTargetOps, "ops"},
		{ExportTargetSelfHosted, "self_hosted"},
		{ExportTargetCLI, "cli"},
		{ExportTargetCLIOnly, "cli"},
	}
	for _, tc := range cases {
		if string(tc.target) != tc.want {
			t.Fatalf("ExportTarget value = %q, want %q", tc.target, tc.want)
		}
	}
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

func TestConfiguration_MarshalSelfHosted(t *testing.T) {
	config := NewConfiguration([]SchemaField{
		StringField("multi-target", WithExportTargets(ExportTargetOps, ExportTargetSelfHosted, ExportTargetCLI)),
		StringField("self-hosted-only", WithExportTargets(ExportTargetSelfHosted)),
		StringField("gui-and-self-hosted", WithExportTargets(ExportTargetGUI, ExportTargetSelfHosted)),
	})

	marshaled, err := config.marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	fields := make(map[string]*v1_conf.Field)
	for _, f := range marshaled.GetFields() {
		fields[f.GetName()] = f
	}

	multi := fields["multi-target"]
	if multi == nil {
		t.Fatal("multi-target field missing")
	}
	if !multi.GetIsOps() {
		t.Fatal("multi-target should set legacy is_ops")
	}
	if !multi.GetIsSelfHosted() {
		t.Fatal("multi-target should set is_self_hosted")
	}

	selfHostedOnly := fields["self-hosted-only"]
	if selfHostedOnly == nil {
		t.Fatal("self-hosted-only field missing")
	}
	if !selfHostedOnly.GetIsOps() {
		t.Fatal("self-hosted-only should set legacy is_ops to hide from old GUI consumers")
	}
	if !selfHostedOnly.GetIsSelfHosted() {
		t.Fatal("self-hosted-only should set is_self_hosted")
	}

	guiAndSelfHosted := fields["gui-and-self-hosted"]
	if guiAndSelfHosted == nil {
		t.Fatal("gui-and-self-hosted field missing")
	}
	if guiAndSelfHosted.GetIsOps() {
		t.Fatal("gui-and-self-hosted should not set legacy is_ops")
	}
	if !guiAndSelfHosted.GetIsSelfHosted() {
		t.Fatal("gui-and-self-hosted should set is_self_hosted")
	}
}
