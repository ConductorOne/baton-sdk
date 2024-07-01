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
		FieldName: name,
		FieldType: reflect.Bool,
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func StringField(name string, optional ...fieldOption) ConfigField {
	field := ConfigField{
		FieldName: name,
		FieldType: reflect.String,
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func IntField(name string, optional ...fieldOption) ConfigField {
	field := ConfigField{
		FieldName: name,
		FieldType: reflect.Int,
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}
