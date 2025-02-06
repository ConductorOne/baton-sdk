package field

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var WrongValueTypeErr = errors.New("unable to cast any to concrete type")

type SchemaTypes interface {
	~string | ~bool | ~int | ~uint | ~[]string
}

type Variant uint

const (
	InvalidVariant Variant = iota
	StringVariant
	BoolVariant
	IntVariant
	UintVariant
	StringSliceVariant
)

type FieldRule struct {
	s *StringRules
	// e  *EnumRules
	ss *RepeatedRules[StringRules]
	b  *BoolRules
	i  *IntRules
	ui *UintRules
}

type SchemaField struct {
	FieldName string
	// deprecated
	FieldType    reflect.Kind
	CLIShortHand string
	Required     bool
	Hidden       bool
	Persistent   bool
	Description  string
	Secret       bool
	DefaultValue any

	Variant Variant

	HelpURL     string
	DisplayName string
	Placeholder string
	Rules       FieldRule
}

// SchemaField can't be generic over SchemaTypes without breaking backwards compatibility :-/
func GetDefaultValue[T SchemaTypes](s SchemaField) (*T, error) {
	value, ok := s.DefaultValue.(T)
	if !ok {
		return nil, WrongValueTypeErr
	}
	return &value, nil
}

func (s SchemaField) GetDescription() string {
	var line string
	if s.Description == "" {
		line = fmt.Sprintf("($BATON_%s)", toUpperCase(s.FieldName))
	} else {
		line = fmt.Sprintf("%s ($BATON_%s)", s.Description, toUpperCase(s.FieldName))
	}

	if s.Required {
		line = fmt.Sprintf("required: %s", line)
	}

	return line
}

func (s SchemaField) GetName() string {
	return s.FieldName
}

func (s SchemaField) Validate(value any) error {
	switch s.Variant {
	case StringVariant:
		v, ok := value.(string)
		if !ok {
			return WrongValueTypeErr
		}
		return s.Rules.s.Validate(v)
	case BoolVariant:
		v, ok := value.(bool)
		if !ok {
			return WrongValueTypeErr
		}
		return s.Rules.b.Validate(v)
	case IntVariant:
		v, ok := value.(int64)
		if !ok {
			return WrongValueTypeErr
		}
		return s.Rules.i.Validate(v)
	case UintVariant:
		v, ok := value.(uint64)
		if !ok {
			return WrongValueTypeErr
		}
		return s.Rules.ui.Validate(v)
	case StringSliceVariant:
		v, ok := value.([]string)
		if !ok {
			return WrongValueTypeErr
		}
		return s.Rules.ss.Validate(v)
	default:
		return fmt.Errorf("unknown field variant %d", s.Variant)
	}
}

func toUpperCase(i string) string {
	return strings.ReplaceAll(strings.ToUpper(i), "-", "_")
}

func BoolField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName: name,
		// FieldType:    reflect.Bool,
		Variant:      BoolVariant,
		DefaultValue: false,
	}

	for _, o := range optional {
		field = o(field)
	}

	if field.Required {
		panic(fmt.Sprintf("requiring %s of type %s does not make sense", field.FieldName, field.FieldType))
	}

	return field
}

func StringField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		FieldType:    reflect.String,
		Variant:      StringVariant,
		DefaultValue: "",
		Rules: FieldRule{
			s: &StringRules{},
		},
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
		Variant:      IntVariant,
		DefaultValue: 0,
		Rules: FieldRule{
			i: &IntRules{},
		},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func StringSliceField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		FieldType:    reflect.Slice,
		Variant:      StringSliceVariant,
		DefaultValue: []string{},
		Rules: FieldRule{
			ss: &RepeatedRules[StringRules]{},
		},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func SelectField(name string, options []string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		FieldType:    reflect.String,
		Variant:      StringVariant,
		DefaultValue: "",
		Rules: FieldRule{
			s: &StringRules{
				In: options,
			},
		},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func OptionsField(name string, options []string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		FieldType:    reflect.Slice,
		Variant:      StringSliceVariant,
		DefaultValue: "",
		Rules: FieldRule{
			s: &StringRules{
				In: options,
			},
		},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}
