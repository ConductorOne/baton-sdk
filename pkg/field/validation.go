package field

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

type ErrConfigurationMissingFields struct {
	errors []error
}

func (e *ErrConfigurationMissingFields) Error() string {
	var messages []string

	for _, err := range e.errors {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("errors found:\n%s", strings.Join(messages, "\n"))
}

func (e *ErrConfigurationMissingFields) Push(err error) {
	e.errors = append(e.errors, err)
}

// Validate perform validation of field requirement and constraints
// relationships after the configuration is read.
// We don't check the following:
//   - if required fields are mutually exclusive
//   - repeated fields (by name) are defined
//   - if sets of fields are mutually exclusive and required
//     together at the same time
func Validate(c Configuration, v *viper.Viper) error {
	present := make(map[string]int)
	missingFieldsError := &ErrConfigurationMissingFields{}

	// check if required fields are present
	for _, f := range c.Fields {
		isNonZero := false
		switch f.FieldType {
		case reflect.Bool:
			isNonZero = v.GetBool(f.FieldName)
		case reflect.Int:
			isNonZero = v.GetInt(f.FieldName) != 0
		case reflect.String:
			isNonZero = v.GetString(f.FieldName) != ""
		default:
			return fmt.Errorf("field %s has unsupported type %s", f.FieldName, f.FieldType)
		}

		if isNonZero {
			present[f.FieldName] = 1
		}

		if f.Required && !isNonZero {
			missingFieldsError.Push(fmt.Errorf("field %s of type %s is marked as required but it has a zero-value", f.FieldName, f.FieldType))
		}
	}

	if len(missingFieldsError.errors) > 0 {
		return missingFieldsError
	}

	// check constraints
	return validateConstraints(present, c.Constraints)
}

func validateConstraints(fieldsPresent map[string]int, relationships []SchemaFieldRelationship) error {
	for _, relationship := range relationships {
		var present int
		for _, f := range relationship.Fields {
			present += fieldsPresent[f.FieldName]
		}
		if present > 1 && relationship.Kind == MutuallyExclusive {
			return makeMutuallyExclusiveError(fieldsPresent, relationship)
		}
		if present < len(relationship.Fields) && relationship.Kind == RequiredTogether {
			return makeNeededTogetherError(fieldsPresent, relationship)
		}
	}

	return nil
}

func makeMutuallyExclusiveError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 1 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf("fields marked as mutually exclusive were set: %s", strings.Join(found, ", "))
}

func makeNeededTogetherError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 0 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf("fields marked as needed together are missing: %s", strings.Join(found, ", "))
}