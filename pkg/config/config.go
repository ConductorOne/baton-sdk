package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

type ConfigField interface {
	Required() bool
	Description() string
	FieldName() string
}

type StringField struct {
	required     bool
	description  string
	fieldName    string
	defaultValue string
	value        string
}

func (f *StringField) Required() bool {
	return f.required
}
func (f *StringField) Description() string {
	return f.description
}
func (f *StringField) FieldName() string {
	return f.fieldName
}
func (f *StringField) Value() string {
	return f.value
}
func NewStringField(name string, defaultValue string, description string, required bool) *StringField {
	return &StringField{
		required:     required,
		description:  description,
		fieldName:    name,
		defaultValue: defaultValue,
	}
}

type BoolField struct {
	required     bool
	description  string
	fieldName    string
	defaultValue bool
	value        bool
}

func (f *BoolField) Required() bool {
	return f.required
}
func (f *BoolField) Description() string {
	return f.description
}
func (f *BoolField) FieldName() string {
	return f.fieldName
}
func (f *BoolField) Value() bool {
	return f.value
}
func NewBoolField(name string, defaultValue bool, description string, required bool) *BoolField {
	return &BoolField{
		required:     required,
		description:  description,
		fieldName:    name,
		defaultValue: defaultValue,
	}
}

type IntField struct {
	Required     bool
	Description  string
	FieldName    string
	DefaultValue int
}

type BaseConfig_ struct {
	LogLevel           string `mapstructure:"log-level"`
	LogFormat          string `mapstructure:"log-format"`
	C1zPath            string `mapstructure:"file"`
	ClientID           string `mapstructure:"client-id"`
	ClientSecret       string `mapstructure:"client-secret"`
	GrantEntitlementID string `mapstructure:"grant-entitlement"`
	GrantPrincipalID   string `mapstructure:"grant-principal"`
	GrantPrincipalType string `mapstructure:"grant-principal-type"`
	RevokeGrantID      string `mapstructure:"revoke-grant"`
	C1zTempDir         string `mapstructure:"c1z-temp-dir"`
}

type BaseConfig struct {
	LogLevel           *StringField
	LogFormat          *StringField
	C1zPath            *StringField
	ClientID           *StringField
	ClientSecret       *StringField
	GrantEntitlementID *StringField
	GrantPrincipalID   *StringField
	GrantPrincipalType *StringField
	RevokeGrantID      *StringField
	C1zTempDir         *StringField
}

func NewBaseConfig() *BaseConfig {
	return &BaseConfig{
		LogLevel:           NewStringField("log-level", "info", "", false),
		LogFormat:          NewStringField("log-format", logging.LogFormatJSON, "The log level: debug, info, warn, error", false),
		C1zPath:            NewStringField("file", "", "", false),
		ClientID:           NewStringField("client-id", "", "", false),
		ClientSecret:       NewStringField("client-secret", "", "", false),
		GrantEntitlementID: NewStringField("grant-entitlement", "", "", false),
		GrantPrincipalID:   NewStringField("grant-principal", "", "", false),
		GrantPrincipalType: NewStringField("grant-principal-type", "", "", false),
		RevokeGrantID:      NewStringField("revoke-grant", "", "", false),
		C1zTempDir:         NewStringField("c1z-temp-dir", "", "", false),
	}
}

func ConfigToCmdFlags[T any, PtrT *T](cmd *cobra.Command, cfg PtrT) error {
	fields := reflect.VisibleFields(reflect.TypeOf(*cfg))
	for _, field := range fields {
		_, ok := field..(ConfigField)
		if !ok {
			continue
		}
		if field.Type.Kind() != reflect. {
			continue
		}
		cfgField := field.Tag.Get("mapstructure")
		if cfgField == "" {
			return fmt.Errorf("mapstructure tag is required on config field %s", field.Name)
		}
		description := field.Tag.Get("description")
		if description == "" {
			// Skip fields without descriptions for backwards compatibility
			continue
		}
		defaultValueStr := field.Tag.Get("defaultValue")

		envVarName := strings.ReplaceAll(strings.ToUpper(cfgField), "-", "_")
		description = fmt.Sprintf("%s ($BATON_%s)", description, envVarName)
		switch field.Type.Kind() {
		case reflect.String:
			cmd.PersistentFlags().String(cfgField, defaultValueStr, description)
		case reflect.Bool:
			defaultValue, err := strconv.ParseBool(defaultValueStr)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Bool(cfgField, defaultValue, description)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			defaultValue, err := strconv.ParseInt(defaultValueStr, 10, 64)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Int64(cfgField, defaultValue, description)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			defaultValue, err := strconv.ParseUint(defaultValueStr, 10, 64)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Uint64(cfgField, defaultValue, description)
		case reflect.Float32, reflect.Float64:
			defaultValue, err := strconv.ParseFloat(defaultValueStr, 64)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Float64(cfgField, defaultValue, description)
		default:
			return fmt.Errorf("unsupported type %s for config field %s", field.Type.Kind(), field.Name)
		}
	}

	return nil
}
