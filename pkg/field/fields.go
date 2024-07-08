package field

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	WrongValueTypeErr = errors.New("unable to cast any to concrete type")
)

type SchemaField struct {
	FieldName    string
	FieldType    reflect.Kind
	CLIShortHand string
	Required     bool
	Hidden       bool
	Description  string
	DefaultValue any
}

// Bool returns the default value as a boolean.
func (s SchemaField) Bool() (bool, error) {
	value, ok := s.DefaultValue.(bool)
	if !ok {
		return false, WrongValueTypeErr
	}

	return value, nil
}

// Int returns the default value as a integer.
func (s SchemaField) Int() (int, error) {
	value, ok := s.DefaultValue.(int)
	if !ok {
		return 0, WrongValueTypeErr
	}

	return value, nil
}

// String returns the default value as a string.
func (s SchemaField) String() (string, error) {
	value, ok := s.DefaultValue.(string)
	if !ok {
		return "", WrongValueTypeErr
	}

	return value, nil
}

func (s SchemaField) GetDescription() string {
	if s.Description == "" {
		return fmt.Sprintf("($BATON_%s)", toUpperCase(s.FieldName))
	}

	return fmt.Sprintf("%s ($BATON_%s)", s.Description, toUpperCase(s.FieldName))
}

func (s SchemaField) GetName() string {
	return s.FieldName
}

func (s SchemaField) GetType() reflect.Kind {
	return s.FieldType
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

func toUpperCase(i string) string {
	return strings.ReplaceAll(strings.ToUpper(i), "-", "_")
}
