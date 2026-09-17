package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/field"
)

// TestGoTypeForField pins the generated config struct's Go type for every
// field variant and WebFieldType reachable through the public field
// constructors. FileUploadField is the only case whose value arrives as raw
// bytes rather than text -- everything else built on StringVariant, including
// MultilineField, must stay "string" or a connector's generated config field
// silently changes type out from under it.
func TestGoTypeForField(t *testing.T) {
	tests := []struct {
		name string
		f    field.SchemaField
		want string
	}{
		{name: "plain string field", f: field.StringField("s"), want: "string"},
		{name: "multiline field stays string", f: field.MultilineField("s"), want: "string"},
		{name: "random field stays string", f: field.RandomField("s"), want: "string"},
		{name: "oauth2 field stays string", f: field.Oauth2Field("s"), want: "string"},
		{name: "select field stays string", f: field.SelectField("s", []string{"a", "b"}), want: "string"},
		{name: "file upload field becomes []byte", f: field.FileUploadField("s", []string{".pem"}), want: "[]byte"},
		{name: "bool field", f: field.BoolField("b"), want: "bool"},
		{name: "int field", f: field.IntField("i"), want: "int"},
		{name: "string slice field", f: field.StringSliceField("ss"), want: "[]string"},
		{name: "string map field", f: field.StringMapField("sm"), want: "map[string]any"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, goTypeForField(tt.f))
		})
	}
}
