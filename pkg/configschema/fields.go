package configschema

import "reflect"

type SchemaField struct {
	FieldName    string
	FieldType    reflect.Kind
	CLIShortHand string
	Required     bool
	Hidden       bool
	Description  string
	DefaultValue any
}

func BoolField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		FieldType:    reflect.Bool,
		DefaultValue: false,
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func StringField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		FieldType:    reflect.String,
		DefaultValue: "",
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func IntField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		FieldType:    reflect.Int,
		DefaultValue: 0,
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}
