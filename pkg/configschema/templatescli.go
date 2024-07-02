package configschema

var cobraCLITemplate = `package {{.PackageName}}

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
