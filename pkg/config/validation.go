package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/viper"
)

type ConfigurationError struct {
	errs []error
}

func (c *ConfigurationError) Error() string {
	amount := len(c.errs)
	var errstrings []string
	for _, err := range c.errs {
		errstrings = append(errstrings, err.Error())
	}

	return fmt.Sprintf("found %d error(s) in the configuration:\n%s", amount, strings.Join(errstrings, "\n"))
}

func (c *ConfigurationError) PushError(err error) {
	c.errs = append(c.errs, err)
}

// ValidateConfiguration checks if fields marked as required have a non
// zero-value set either from the CLI or from the configuration file.
func ValidateConfiguration(v *viper.Viper, fields []field.SchemaField) error {
	errorsFound := &ConfigurationError{}

	for _, field := range fields {
		var fieldError error
		switch field.FieldType {
		case reflect.Bool:
			value := v.GetBool(field.FieldName)
			if !value && field.Required {
				fieldError = fmt.Errorf("field %s of type %s is marked as required, but no value was provided (value %t)", field.FieldName, field.FieldType, value)
			}
		case reflect.Int:
			value := v.GetInt(field.FieldName)
			if value == 0 && field.Required {
				fieldError = fmt.Errorf("field %s of type %s is marked as required, but no value was provided (value %d)", field.FieldName, field.FieldType, value)
			}
		case reflect.String:
			value := v.GetString(field.FieldName)
			if value == "" && field.Required {
				fieldError = fmt.Errorf("field %s of type %s is marked as required, but no value was provided (value '%s')", field.FieldName, field.FieldType, value)
			}
		default:
			fieldError = fmt.Errorf("field %s has unsupported type %s ", field.FieldName, field.FieldType)
		}
		errorsFound.PushError(fieldError)
	}

	if len(errorsFound.errs) > 0 {
		return errorsFound
	}

	return nil
}
