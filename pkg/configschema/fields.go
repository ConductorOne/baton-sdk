package configschema

import "reflect"

type ConfigField struct {
	FieldName    string
	FieldType    reflect.Kind
	Required     bool
	Description  string
	DefaultValue any
}

func BoolField(name string, optional ...fieldOption) ConfigField {
	field := ConfigField{
		FieldName:    name,
		FieldType:    reflect.Bool,
		DefaultValue: false,
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func StringField(name string, optional ...fieldOption) ConfigField {
	field := ConfigField{
		FieldName:    name,
		FieldType:    reflect.String,
		DefaultValue: "",
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func IntField(name string, optional ...fieldOption) ConfigField {
	field := ConfigField{
		FieldName:    name,
		FieldType:    reflect.Int,
		DefaultValue: 0,
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}