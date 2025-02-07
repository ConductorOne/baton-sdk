package field

import "encoding/json"

type baseFieldSchema struct {
	FieldName   string  `json:"FieldName"`
	FieldType   string  `json:"FieldType"`
	DisplayName *string `json:"DisplayName,omitempty"`
	Required    *bool   `json:"Required,omitempty"`
	Description *string `json:"Description,omitempty"`
	HelpURL     *string `json:"HelpURL,omitempty"`
	Hidden      *bool   `json:"Hidden,omitempty"`
	Secret      *bool   `json:"Secret,omitempty"`
	Placeholder *string `json:"Placeholder,omitempty"`
}

type stringFieldSchema struct {
	baseFieldSchema
	Rules        *StringRules `json:"Rules,omitempty"`
	DefaultValue *string      `json:"DefaultValue,omitempty"`
}

type intFieldSchema struct {
	baseFieldSchema
	Rules        *IntRules `json:"Rules,omitempty"`
	DefaultValue *int      `json:"DefaultValue,omitempty"`
}

type uintFieldSchema struct {
	baseFieldSchema
	Rules        UintRules `json:"Rules,omitempty"`
	DefaultValue *uint     `json:"DefaultValue,omitempty"`
}

type boolFieldSchema struct {
	baseFieldSchema
	Rules        *BoolRules `json:"Rules,omitempty"`
	DefaultValue *bool      `json:"DefaultValue,omitempty"`
}

type stringSliceFieldSchema struct {
	baseFieldSchema
	Rules        *RepeatedRules[StringRules] `json:"Rules,omitempty"`
	DefaultValue *[]string                   `json:"DefaultValue,omitempty"`
}

func (s SchemaField) MarshalJSON() ([]byte, error) {
	if s.WebConfig.Ignore {
		return []byte{}, nil
	}

	omitEmpty := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	omitFalse := func(b bool) *bool {
		if !b {
			return nil
		}
		return &b
	}

	b := baseFieldSchema{
		FieldName:   s.FieldName,
		Required:    omitFalse(s.Required),
		Description: omitEmpty(s.Description),
		HelpURL:     omitEmpty(s.HelpURL),
		DisplayName: omitEmpty(s.DisplayName),
		Hidden:      omitFalse(s.WebConfig.Hidden),
		Secret:      omitFalse(s.WebConfig.Secret),
		Placeholder: omitEmpty(s.WebConfig.Placeholder),
	}
	switch s.Variant {
	case StringVariant:
		b.FieldType = "StringField"
		return json.Marshal(stringFieldSchema{
			baseFieldSchema: b,
			Rules:           s.Rules.s,
			DefaultValue: func() *string {
				if s.DefaultValue == nil {
					return nil
				}
				val, ok := s.DefaultValue.(string)
				if !ok {
					panic("unable to cast any to string for a stringfield")
				}
				if val == "" {
					return nil
				}

				return &val
			}(),
		})
	case IntVariant:
		b.FieldType = "IntField"
		return json.Marshal(intFieldSchema{
			baseFieldSchema: b,
			Rules:           s.Rules.i,
			DefaultValue: func() *int {
				if s.DefaultValue == nil {
					return nil
				}
				val, ok := s.DefaultValue.(int)
				if !ok {
					panic("unable to cast any to int for an IntField")
				}
				if val == 0 {
					return nil
				}
				return &val
			}(),
		})
	case BoolVariant:
		b.FieldType = "BoolField"
		return json.Marshal(boolFieldSchema{
			baseFieldSchema: b,
			Rules:           s.Rules.b,
			DefaultValue: func() *bool {
				if s.DefaultValue == nil {
					return nil
				}
				val, ok := s.DefaultValue.(bool)
				if !ok {
					panic("unable to cast any default value to bool for a BoolField")
				}
				if !val {
					return nil
				}
				return &val
			}(),
		})
	case UintVariant:
		b.FieldType = "UintField"
		return json.Marshal(uintFieldSchema{
			baseFieldSchema: b,
			Rules:           *s.Rules.ui,
			DefaultValue: func() *uint {
				if s.DefaultValue == nil {
					return nil
				}
				val, ok := s.DefaultValue.(uint)
				if !ok {
					panic("unable to cast any to uint for a UintField")
				}
				if val == 0 {
					return nil
				}
				return &val
			}(),
		})
	case StringSliceVariant:
		b.FieldType = "StringSliceField"
		return json.Marshal(stringSliceFieldSchema{
			baseFieldSchema: b,
			Rules:           s.Rules.ss,
			DefaultValue: func() *[]string {
				if s.DefaultValue == nil {
					return nil
				}
				val, ok := s.DefaultValue.([]string)
				if !ok {
					panic("unable to cast any to []string for a StringSliceField")
				}
				if len(val) == 0 {
					return nil
				}
				return &val
			}(),
		})
	default:
		return json.Marshal(b)
	}
}
