package test

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/ustrings"
	"github.com/spf13/viper"
)

func MakeViper(input map[string]string) *viper.Viper {
	output := viper.New()
	for key, value := range input {
		output.Set(key, value)
	}
	return output
}

// AssertOutcome - call a validation function and assert the result.
func AssertOutcome(
	t *testing.T,
	function func() error,
	expectedSuccess bool,
) {
	err := function()
	if err != nil {
		if expectedSuccess {
			t.Fatal("expected function to succeed, but", err.Error())
		}
	} else {
		if !expectedSuccess {
			t.Fatal("expected function to fail, but it succeeded")
		}
	}
}

type TestCase = struct {
	expression string
	isValid    bool
	message    string
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
			values, err := ustrings.ParseFlags(testCase.expression)
			if err != nil {
				t.Fatal("could not parse flags:", err)
			}
			AssertOutcome(
				t,
				func() error {
					v := MakeViper(values)
					err = field.Validate(configurationSchema, v)
					if err != nil {
						return err
					}
					if extraValidationFunction != nil {
						return extraValidationFunction(v)
					}
					return nil
				},
				testCase.isValid,
			)
		})
	}
}
