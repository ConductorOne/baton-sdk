package ustrings

import (
	"reflect"
	"testing"
)

func TestParseFlags(t *testing.T) {
	testCases := []struct {
		expression string
		expected   map[string]string
		message    string
	}{
		{
			"1",
			nil,
			"invalid syntax",
		},
		{
			"",
			map[string]string{},
			"empty expression",
		},
		{
			"--a 1",
			map[string]string{"a": "1"},
			"simple case",
		},
		{
			"--a 1 --a 2",
			map[string]string{"a": "1,2"},
			"duplicate flag",
		},
		{
			"--a 1,2",
			map[string]string{"a": "1,2"},
			"comma-separated list",
		},
		{
			"--a",
			map[string]string{"a": "true"},
			"flag without a value",
		},
		{
			"--a 1 --a",
			nil,
			"duplicate flag without a value",
		},
		{
			"--a \"1\" --b '2' --c `3`",
			map[string]string{
				"a": "1",
				"b": "2",
				"c": "3",
			},
			"handle quotes",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			values, err := ParseFlags(testCase.expression)
			if err != nil && testCase.expected != nil {
				t.Fatal("could not parse flags:", err.Error())
			} else if !reflect.DeepEqual(values, testCase.expected) {
				t.Fatal("parsing did not match expected value:", err.Error())
			}
		})
	}
}
