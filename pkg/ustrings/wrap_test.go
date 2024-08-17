package ustrings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrimQuotes(t *testing.T) {
	testCases := []struct {
		expression string
		expected   string
		message    string
	}{
		{"", "", "not quoted"},
		{"\"\"", "", "quoted"},
		{"'", "'", "unmatched quote"},
		{"'``'", "``", "two layers of quotes"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			trimmed := TrimQuotes(testCase.expression)
			require.Equal(t, testCase.expected, trimmed)
		})
	}
}
