package configschema

var configurationStructTemplate = `// THIS FILE WAS AUTO GENERATED. DO NOT EDIT
package {{.PackageName}}

type Configuration struct {
{{range .Fields}}{{ if .Description }}	// {{.Description}}
{{end}}	{{ToCamelCase .FieldName}} {{.FieldType}}
{{end}}
}`

var cobraCLITemplate = `// THIS FILE WAS AUTO GENERATED. DO NOT EDIT
package {{.PackageName}}

import (
	"github.com/spf13/cobra"
)

func NewCLI(cmd *cobra.Command) {
{{range .Fields}}	cmd.PersistentFlags().{{KindToCobra .FieldType}}("{{.FieldName}}",
		{{if eq .FieldType.String "string"}}"{{.DefaultValue}}"{{else}}{{.DefaultValue}}{{end}},
		"{{.Description}}{{if .Description}} {{end}}($BATON_{{ToUpperCase .FieldName}})")
{{end}}
}
`
