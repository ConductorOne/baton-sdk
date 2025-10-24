package field

import (
	"errors"
	"fmt"
	"strings"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
)

var ErrWrongValueType = errors.New("unable to cast any to concrete type")

type Variant string

const (
	StringVariant      Variant = "StringField"
	BoolVariant        Variant = "BoolField"
	IntVariant         Variant = "IntField"
	StringSliceVariant Variant = "StringSliceField"
	StringMapVariant   Variant = "StringMapField"
)

type WebFieldType string

const (
	Text                    WebFieldType = "TEXT"
	Randomize               WebFieldType = "RANDOMIZE"
	OAuth2                  WebFieldType = "OAUTH2"
	ConnectorDerivedOptions WebFieldType = "CONNECTOR_DERIVED_OPTIONS"
	FileUpload              WebFieldType = "FILE_UPLOAD"
)

type FieldRule struct {
	s  *v1_conf.StringRules
	ss *v1_conf.RepeatedStringRules
	b  *v1_conf.BoolRules
	i  *v1_conf.Int64Rules
	sm *v1_conf.StringMapRules
}

type syncerConfig struct {
	Required   bool
	Hidden     bool
	ShortHand  string
	Persistent bool
}

type connectorConfig struct {
	DisplayName string
	Required    bool
	Placeholder string
	FieldType   WebFieldType
	// Only used by file uploads atm.
	BonusStrings []string
}

type SchemaField struct {
	FieldName    string
	Required     bool
	DefaultValue any
	Description  string
	ExportTarget ExportTarget
	HelpURL      string

	Variant         Variant
	Rules           FieldRule
	Secret          bool
	StructFieldName string

	// Default fields - syncer side
	SyncerConfig syncerConfig

	// Config acutally ingested on the connector side - auth, regions, etc
	ConnectorConfig connectorConfig

	WasReExported bool

	// Groups
	FieldGroups []SchemaFieldGroup
}

type SchemaTypes interface {
	~string | ~bool | ~int | ~[]string | ~map[string]any
}

func (s SchemaField) GetName() string {
	return s.FieldName
}

func (s SchemaField) GetCLIShortHand() string {
	return s.SyncerConfig.ShortHand
}

func (s SchemaField) IsPersistent() bool {
	return s.SyncerConfig.Persistent
}

func (s SchemaField) IsHidden() bool {
	return s.SyncerConfig.Hidden
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

func (s SchemaField) ExportAs(et ExportTarget) SchemaField {
	c := s
	c.ExportTarget = et
	c.WasReExported = true
	return c
}

// Go doesn't allow generic methods on a non-generic struct.
func ValidateField[T SchemaTypes](s *SchemaField, value T) (bool, error) {
	return s.validate(value)
}

func (s SchemaField) validate(value any) (bool, error) {
	switch s.Variant {
	case StringVariant:
		v, ok := value.(string)
		if !ok {
			return false, ErrWrongValueType
		}
		return v != "", ValidateStringRules(s.Rules.s, v, s.FieldName)
	case BoolVariant:
		v, ok := value.(bool)
		if !ok {
			return false, ErrWrongValueType
		}
		return v, ValidateBoolRules(s.Rules.b, v, s.FieldName)
	case IntVariant:
		v, ok := value.(int)
		if !ok {
			return false, ErrWrongValueType
		}
		return v != 0, ValidateIntRules(s.Rules.i, v, s.FieldName)
	case StringSliceVariant:
		v, ok := value.([]string)
		if !ok {
			return false, ErrWrongValueType
		}
		return len(v) != 0, ValidateRepeatedStringRules(s.Rules.ss, v, s.FieldName)
	case StringMapVariant:
		v, ok := value.(map[string]any)
		if !ok {
			return false, ErrWrongValueType
		}
		return len(v) != 0, ValidateStringMapRules(s.Rules.sm, v, s.FieldName)
	default:
		return false, fmt.Errorf("unknown field type %s", s.Variant)
	}
}

func toUpperCase(i string) string {
	return strings.ReplaceAll(strings.ToUpper(i), "-", "_")
}

// SchemaField can't be generic over SchemaTypes without breaking backwards compatibility :-/.
func GetDefaultValue[T SchemaTypes](s SchemaField) (*T, error) {
	value, ok := s.DefaultValue.(T)
	if !ok {
		return nil, ErrWrongValueType
	}
	return &value, nil
}

func BoolField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:       name,
		Variant:         BoolVariant,
		DefaultValue:    false,
		ExportTarget:    ExportTargetGUI,
		Rules:           FieldRule{},
		SyncerConfig:    syncerConfig{},
		ConnectorConfig: connectorConfig{},
	}

	for _, o := range optional {
		field = o(field)
	}

	if field.Required {
		panic(fmt.Sprintf("requiring %s of type %s does not make sense", field.FieldName, field.Variant))
	}

	return field
}

func StringField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:       name,
		Variant:         StringVariant,
		DefaultValue:    "",
		ExportTarget:    ExportTargetGUI,
		Rules:           FieldRule{},
		SyncerConfig:    syncerConfig{},
		ConnectorConfig: connectorConfig{FieldType: Text},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func FileUploadField(name string, bonusStrings []string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		Variant:      StringVariant,
		DefaultValue: "",
		ExportTarget: ExportTargetGUI,
		Rules:        FieldRule{},
		SyncerConfig: syncerConfig{},
		ConnectorConfig: connectorConfig{
			FieldType:    FileUpload,
			BonusStrings: bonusStrings,
		},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func IntField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:       name,
		Variant:         IntVariant,
		DefaultValue:    0,
		ExportTarget:    ExportTargetGUI,
		Rules:           FieldRule{},
		SyncerConfig:    syncerConfig{},
		ConnectorConfig: connectorConfig{},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func StringSliceField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:       name,
		Variant:         StringSliceVariant,
		DefaultValue:    []string{},
		ExportTarget:    ExportTargetGUI,
		Rules:           FieldRule{},
		SyncerConfig:    syncerConfig{},
		ConnectorConfig: connectorConfig{},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func StringMapField(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:       name,
		Variant:         StringMapVariant,
		DefaultValue:    map[string]any{},
		ExportTarget:    ExportTargetGUI,
		Rules:           FieldRule{},
		SyncerConfig:    syncerConfig{},
		ConnectorConfig: connectorConfig{},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func SelectField(name string, options []string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:    name,
		Variant:      StringVariant,
		DefaultValue: "",
		ExportTarget: ExportTargetGUI,
		Rules: FieldRule{
			s: v1_conf.StringRules_builder{In: options}.Build(),
		},
		SyncerConfig:    syncerConfig{},
		ConnectorConfig: connectorConfig{FieldType: Text},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}

func Oauth2Field(name string, optional ...fieldOption) SchemaField {
	field := SchemaField{
		FieldName:       name,
		Variant:         StringVariant,
		DefaultValue:    "",
		ExportTarget:    ExportTargetGUI,
		Rules:           FieldRule{},
		SyncerConfig:    syncerConfig{},
		ConnectorConfig: connectorConfig{FieldType: OAuth2},
	}

	for _, o := range optional {
		field = o(field)
	}

	return field
}
