package field

import (
	"encoding/json"
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

type BaseFieldSchema struct {
	FieldName    string `json:"FieldName"`
	FieldType    string `json:"FieldType"`
	CLIShortHand string `json:"CLIShortHand"`
	Required     bool   `json:"Required"`
	Hidden       bool   `json:"Hidden"`
	Persistent   bool   `json:"Persistent"`
	Description  string `json:"Description"`
	Secret       bool   `json:"Secret"`
	HelpURL      string `json:"HelpURL"`
	DisplayName  string `json:"DisplayName"`
	Placeholder  string `json:"Placeholder"`
}

type StringFieldSchema struct {
	BaseFieldSchema
	Rules        StringRules `json:"Rules"`
	DefaultValue *string     `json:"DefaultValue,omitempty"`
}

type IntFieldSchema struct {
	BaseFieldSchema
	Rules        IntRules `json:"Rules"`
	DefaultValue *int     `json:"DefaultValue,omitempty"`
}

type UintFieldSchema struct {
	BaseFieldSchema
	Rules        UintRules `json:"Rules"`
	DefaultValue *uint     `json:"DefaultValue,omitempty"`
}

type BoolFieldSchema struct {
	BaseFieldSchema
	Rules        BoolRules `json:"Rules"`
	DefaultValue *bool     `json:"DefaultValue,omitempty"`
}

type StringSliceFieldSchema struct {
	BaseFieldSchema
	Rules        RepeatedRules[StringRules] `json:"Rules"`
	DefaultValue *[]string                  `json:"DefaultValue,omitempty"`
}

// SchemaField can't be generic over SchemaTypes without breaking backwards compatibility :-/
func GetDefaultValue[T SchemaTypes](s SchemaField) (*T, error) {
	value, ok := s.DefaultValue.(T)
	if !ok {
		return nil, WrongValueTypeErr
	}
	return &value, nil
}

func (s SchemaField) MarshalJSON() ([]byte, error) {
	b := BaseFieldSchema{
		FieldName:    s.FieldName,
		CLIShortHand: s.CLIShortHand,
		Required:     s.Required,
		Hidden:       s.Hidden,
		Persistent:   s.Persistent,
		Description:  s.Description,
		Secret:       s.Secret,
		HelpURL:      s.HelpURL,
		DisplayName:  s.DisplayName,
		Placeholder:  s.Placeholder,
	}
	switch s.Variant {
	case StringVariant:
		b.FieldType = "StringField"
		return json.Marshal(StringFieldSchema{
			BaseFieldSchema: b,
			Rules:           *s.Rules.s,
			DefaultValue: func() *string {
				if s.DefaultValue == nil {
					return nil
				}
				if val, ok := s.DefaultValue.(string); ok {
					return &val
				}
				return nil
			}(),
		})
	case IntVariant:
		b.FieldType = "IntField"
		return json.Marshal(IntFieldSchema{
			BaseFieldSchema: b,
			Rules:           *s.Rules.i,
			DefaultValue: func() *int {
				if s.DefaultValue == nil {
					return nil
				}
				if val, ok := s.DefaultValue.(int); ok {
					return &val
				}
				return nil
			}(),
		})
	case BoolVariant:
		b.FieldType = "BoolField"
		return json.Marshal(BoolFieldSchema{
			BaseFieldSchema: b,
			Rules:           *s.Rules.b,
			DefaultValue: func() *bool {
				if s.DefaultValue == nil {
					return nil
				}
				if val, ok := s.DefaultValue.(bool); ok {
					return &val
				}
				return nil
			}(),
		})
	case UintVariant:
		b.FieldType = "UintField"
		return json.Marshal(UintFieldSchema{
			BaseFieldSchema: b,
			Rules:           *s.Rules.ui,
			DefaultValue: func() *uint {
				if s.DefaultValue == nil {
					return nil
				}
				if val, ok := s.DefaultValue.(uint); ok {
					return &val
				}
				return nil
			}(),
		})
	case StringSliceVariant:
		b.FieldType = "StringSliceField"
		return json.Marshal(StringSliceFieldSchema{
			BaseFieldSchema: b,
			Rules:           *s.Rules.ss,
			DefaultValue: func() *[]string {
				if s.DefaultValue == nil {
					return nil
				}
				if val, ok := s.DefaultValue.([]string); ok {
					return &val
				}
				return nil
			}(),
		})
	default:
		return json.Marshal(b)
	}
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
		Rules: FieldRule{
			b: &BoolRules{},
		},
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
