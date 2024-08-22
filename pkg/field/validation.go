package field

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/ustrings"
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
		case reflect.Slice:
			isNonZero = len(v.GetStringSlice(f.FieldName)) != 0
		default:
			return fmt.Errorf("field %s has unsupported type %s", f.FieldName, f.FieldType)
		}

		if isNonZero {
			present[f.FieldName] = 1
		}

		if f.Required && !isNonZero {
			missingFieldsError.Push(
				fmt.Errorf(
					"field %s of type %s is marked as required but it has a zero-value",
					f.FieldName,
					f.FieldType,
				),
			)
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

		var expected int
		for _, e := range relationship.ExpectedFields {
			expected += fieldsPresent[e.FieldName]
		}

		switch relationship.Kind {
		case MutuallyExclusive:
			if present > 1 {
				return makeMutuallyExclusiveError(fieldsPresent, relationship)
			}
		case RequiredTogether:
			if present > 0 && present < len(relationship.Fields) {
				return makeNeededTogetherError(fieldsPresent, relationship)
			}
		case AtLeastOne:
			if present == 0 {
				return makeAtLeastOneError(fieldsPresent, relationship)
			}
		case Dependents:
			if present > 0 && expected != len(relationship.ExpectedFields) {
				return makeDependentFieldsError(fieldsPresent, relationship)
			}
		default:
			return fmt.Errorf("invalid relationship constraint")
		}
	}

	return nil
}

func nice(elements []string) string {
	return ustrings.OxfordizeList(
		elements,
		ustrings.WithInnerWrappers(ustrings.SingleQuotes),
		ustrings.WithOuterWrappers(ustrings.Parentheses),
	)
}

func makeMutuallyExclusiveError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 1 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf(
		"fields marked as mutually exclusive were set: %s",
		nice(found),
	)
}

func makeNeededTogetherError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 0 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf(
		"fields marked as needed together are missing: %s",
		nice(found),
	)
}

func makeAtLeastOneError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 0 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf(
		"at least one field was expected, any of: %s",
		nice(found),
	)
}

func makeDependentFieldsError(fields map[string]int, relation SchemaFieldRelationship) error {
	var notfoundExpected []string
	for _, n := range relation.ExpectedFields {
		if fields[n.FieldName] == 0 {
			notfoundExpected = append(notfoundExpected, n.FieldName)
		}
	}

	var foundDependent []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 1 {
			foundDependent = append(foundDependent, f.FieldName)
		}
	}

	return fmt.Errorf(
		"set fields %s are dependent on %s being set",
		nice(foundDependent),
		nice(notfoundExpected),
	)
}
