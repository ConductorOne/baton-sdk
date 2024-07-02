package configschema

var configurationStructTemplate = `package {{.PackageName}}

type Configuration struct {
{{range .Fields}}{{ if .Description }}	// {{.Description}}
{{end}}	{{ToCamelCase .FieldName}} {{.FieldType}}
{{end}}
}`
