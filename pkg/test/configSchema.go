package test

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/viper"
)

type TestCase = struct {
	configs map[string]string
	isValid bool
	message string
}

type TestCaseFromExpression = struct {
	expression string
	isValid    bool
	message    string
}

func MakeViper(input map[string]string) *viper.Viper {
	output := viper.New()
	for key, value := range input {
		output.Set(key, value)
	}
	return output
}

func exerciseTestCase(
	t *testing.T,
	configurationSchema field.Configuration,
	extraValidationFunction func(*viper.Viper) error,
	configs map[string]string,
	isValid bool,
) {
	AssertValidation(
		t,
		func() error {
			v := MakeViper(configs)
			err := field.Validate(configurationSchema, v)
			if err != nil {
				return err
			}
			if extraValidationFunction != nil {
				return extraValidationFunction(v)
			}
			return nil
		},
		isValid,
	)
}

// ExerciseTestCases - this helper function is meant to be called by each
// connector to make sure that the every `Field` and `Relationship` do what we
// expect. Some connectors need to run custom validations, and they can be added
// as the `extraValidationFunction` parameter.
func ExerciseTestCases(
	t *testing.T,
	configurationSchema field.Configuration,
	extraValidationFunction func(*viper.Viper) error,
	testCases []TestCase,
) {
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			exerciseTestCase(
				t,
				configurationSchema,
				extraValidationFunction,
				testCase.configs,
				testCase.isValid,
			)
		})
	}
}

// ExerciseTestCasesFromExpressions - Like ExerciseTestCases, but instead of
// passing a `map[string]string` to each test case, pass a function that parses
// configs from strings and pass each test case an expression as a string.
func ExerciseTestCasesFromExpressions(
	t *testing.T,
	configurationSchema field.Configuration,
	extraValidationFunction func(*viper.Viper) error,
	expressionParser func(string) (map[string]string, error),
	testCases []TestCaseFromExpression,
) {
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			values, err := expressionParser(testCase.expression)
			if err != nil {
				t.Fatal("could not parse flags:", err)
			}
			exerciseTestCase(
				t,
				configurationSchema,
				extraValidationFunction,
				values,
				testCase.isValid,
			)
		})
	}
}
