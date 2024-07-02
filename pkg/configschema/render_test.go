package configschema

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

var renderConfigExpected = `// THIS FILE WAS AUTO GENERATED. DO NOT EDIT
package main

type Configuration struct {
	Field1String string
	// this is a description
	Field2Int int
}
`

var renderCLIExpected = `// THIS FILE WAS AUTO GENERATED. DO NOT EDIT
package main

import (
	"github.com/spf13/cobra"
)

func NewCLI(cmd *cobra.Command) {
	cmd.PersistentFlags().String("field-1-string",
		"value",
		"($BATON_FIELD_1_STRING)")
	cmd.PersistentFlags().Int("field-2-int",
		0,
		"this is a description ($BATON_FIELD_2_INT)")

}
`

func TestRenderConfig(t *testing.T) {
	fields := []SchemaField{
		StringField("field-1-string", WithDefaultValue("value")),
		IntField("field-2-int", WithDescription("this is a description")),
	}

	b := bytes.NewBuffer(nil)

	err := renderConfig(TemplateData{PackageName: "main", Fields: fields}, b)
	require.NoError(t, err)

	require.Equal(t, renderConfigExpected, b.String())
}

func TestRenderCobra(t *testing.T) {
	fields := []SchemaField{
		StringField("field-1-string", WithDefaultValue("value")),
		IntField("field-2-int", WithDescription("this is a description")),
	}

	b := bytes.NewBuffer(nil)

	err := renderCLI(TemplateData{PackageName: "main", Fields: fields}, b)
	require.NoError(t, err)

	require.Equal(t, renderCLIExpected, b.String())
}
