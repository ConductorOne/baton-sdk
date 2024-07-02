package configschema

import (
	"bytes"
	"fmt"
	"go/format"
	"io"
	"reflect"
	"strings"
	"text/template"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var funcsMap = template.FuncMap{
	"ToCamelCase": ToCamelCase,
	"ToUpperCase": ToUpperCase,
	"KindToCobra": reflectKindToCobraType,
}

type TemplateData struct {
	PackageName string
	Fields      []ConfigField
}

// ToCamelCase converts a field name from kebab-case to CamelCase.
func ToCamelCase(s string) string {
	parts := strings.Split(s, "-")
	for i := range parts {
		parts[i] = cases.Title(language.English).String(parts[i])
	}

	return strings.Join(parts, "")
}

// ToUpperCase converts a field name from kebab-case to UPPER_SNAKE_CASE.
func ToUpperCase(s string) string {
	parts := strings.Split(s, "-")
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i])
	}

	return strings.Join(parts, "_")
}

func reflectKindToCobraType(t reflect.Kind) string {
	switch t {
	case reflect.Bool:
		return "Bool"
	case reflect.Int:
		return "Int"
	case reflect.String:
		return "String"
	default:
		panic(fmt.Sprintf("reflectKindToCobraType: unkown kind %v", t))
	}
}

func RenderConfig(input TemplateData, output io.Writer) error {
	t := template.Must(
		template.New("configuration").Funcs(funcsMap).Parse(configurationStructTemplate),
	)

	b := bytes.NewBuffer(nil)

	err := t.Execute(b, input)
	if err != nil {
		return err
	}
	formatted, err := format.Source(b.Bytes())
	if err != nil {
		return err
	}
	_, err = output.Write(formatted)
	if err != nil {
		return err
	}

	return nil
}

func RenderCLI(input TemplateData, output io.Writer) error {
	t := template.Must(template.New("cli").Funcs(funcsMap).Parse(cobraCLITemplate))

	b := bytes.NewBuffer(nil)

	err := t.Execute(b, input)
	if err != nil {
		return err
	}
	formatted, err := format.Source(b.Bytes())
	if err != nil {
		return err
	}
	_, err = output.Write(formatted)
	if err != nil {
		return err
	}

	return nil
}
